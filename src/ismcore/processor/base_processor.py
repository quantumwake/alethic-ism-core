import json
import asyncio
from datetime import datetime, timezone
from typing import Any, List, Dict, Union

from ismcore.messaging.base_message_route_model import BaseRoute
from ismcore.processor.monitored_processor_state import MonitoredProcessorState
from ismcore.storage.processor_state_storage import StateMachineStorage, FieldConfig
from ismcore.utils.general_utils import build_template_text_v2, is_json_serializable
from ismcore.utils.state_utils import validate_processor_status_change
from ismcore.utils.ism_logger import ism_logger
from ismcore.model.base_model import (
    ProcessorStateDirection,
    InstructionTemplate,
    ProcessorProvider,
    ProcessorPropertiesBase,
    Processor,
    ProcessorState,
    ProcessorStatusCode)
from ismcore.model.processor_state import (
    State,
    StateDataRowColumnData,
    StateDataColumnDefinition,
    StateDataKeyDefinition,
    StateConfig,
    StateDataColumnIndex,
    ExecutionStrategy,
    RoutingMode,
    PersistenceMode,
    OutputEnrichment,
)

logging = ism_logger(__name__)


class StatePropagationProvider:
    async def apply_state(self, processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict],
                          input_route_id: str = None) -> [dict]:
        raise NotImplementedError()


class StatePropagationProviderRouter(StatePropagationProvider):

    def __init__(self, route: BaseRoute = None):
        self.route = route

    async def apply_state(self,
                          processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict],
                          input_route_id: str = None) -> [dict]:
        """
        Route the processed new query states from the response to a synchronization topic.

        Publishes to {subject}.{state_id} to enable parallel processing while ensuring
        messages for the same state_id are processed by the same consumer.

        Args:
            processor (List[Dict]): The processor instance that is processing this input query state entry
            input_query_state (Any): The initial input query state.
            output_query_states (List[Dict]): The processed output query states.
            input_route_id (str): The input route id where the input came from (for calibration/retry).

        Returns:
            List[Any]: The result of applying the query states to the output state.
        """
        state_id = processor.output_state.id

        route_message = {
            "route_id": processor.output_processor_state.id,
            "input_route_id": input_route_id,
            "type": "query_state_route",
            "input_query_state": input_query_state,
            "query_state": output_query_states
        }

        # Publish to {subject}.{state_id} for partitioned processing
        subject = self.route.get_publish_subject(partition_key=state_id)
        await self.route.publish(json.dumps(route_message), subject=subject)
        return output_query_states


class StatePropagationProviderRouterStateRouter(StatePropagationProviderRouter):

    def __init__(self, route: BaseRoute, storage: StateMachineStorage):
        """
            route (BaseRoute): the route to propagate messages to, as per conditions in apply_state(..)
            storage (StateMachineStorage): The storage system used fetch a list of state id -> processors

        :param route:
        :param storage:
        """
        super().__init__(route=route)
        self.storage = storage

    async def apply_state(self,
                          processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict],
                          input_route_id: str = None) -> [dict]:
        """
        Persists the processed new query states from the response.

        Args:
            processor (List[Dict]): Processor instance that is processing this input query state entry
            input_query_state (Any): Initial input query state.
            output_query_states (List[Dict]): Processed output states given the input, for a processor id.
            input_route_id (str): The input route id where the input came from (for calibration/retry).

        Returns:
            List[Any]: The result of applying the query states to the output state.
        """

        output_state = processor.output_state

        props = processor.output_state.typed_properties
        routing_mode = props.routing.mode if props.routing else RoutingMode.DISABLED
        if routing_mode == RoutingMode.DISABLED:
            logging.debug(f'skipping auto route of output state events, for state id: {output_state.id}')
            return output_query_states

        # I know this is confusing: the current processor handles an input -> task -> output
        #
        # the `output state id` of this processor IS an `input state id' of other processor(s)
        # thus if we want the data to route from the current state to downstream states through their
        # respective processors, we need to:
        #   1. find all processors that have an `input state id` == `output state id`
        #   2. iterate each processor state route id (state -> processor)
        #   3. for each route publish current state data processed to next route hop (as per processor)
        #
        forward_routes = self.storage.fetch_processor_state_route(
            state_id=output_state.id,
            direction=ProcessorStateDirection.INPUT
        )

        # ensure there are forwarding hop(s)
        if not forward_routes:
            logging.debug(f'no forward routes found for state id: {processor.output_state.id}')
            return

        # iterate and send query states to next hops
        # include input_route_id for calibration/retry support
        [await self.route.publish(json.dumps(
            {
                "type": "query_state_entry",
                "route_id": forward_route.id,
                "input_route_id": input_route_id,
                "query_state": output_query_states,
            }
        )) for forward_route in forward_routes]


class StatePropagationProviderRouterStateSyncStore(StatePropagationProviderRouter):
    async def apply_state(self,
                          processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict],
                          input_route_id: str = None) -> [dict]:
        """
        Persists the processed new query states from the response.

        Args:
            processor (List[Dict]): The processor instance that is processing this input query state entry
            input_query_state (Any): The initial input query state.
            output_query_states (List[Dict]): The processed output query states.
            input_route_id (str): The input route id where the input came from (for calibration/retry).

        Returns:
            List[Any]: The result of applying the query states to the output state.
        """

        props = processor.output_state.typed_properties
        persistence_mode = props.persistence.mode if props.persistence else PersistenceMode.DISABLED
        if persistence_mode == PersistenceMode.DISABLED:
            logging.debug(f'skipping persistence of state events, for state id: {processor.output_state.id}')
            return output_query_states

        return await super().apply_state(
            processor=processor,
            input_query_state=input_query_state,
            output_query_states=output_query_states,
            input_route_id=input_route_id,
        )


class StatePropagationProviderCore(StatePropagationProvider):

    async def apply_state(self,
                          processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict],
                          input_route_id: str = None) -> [dict]:
        """
        Writes the output_query_states to the state object, in memory

        Args:
            processor (List[Dict]): The processor instance that is processing this input query state entry
            input_query_state (Any): The initial input query state.
            output_query_states (List[Dict]): The processed output query states.
            input_route_id (str): The input route id where the input came from (for calibration/retry).

        Returns:
            List[Any]: The result of applying the query states to the output state.
        """
        # Otherwise attempt to persist the data
        logging.debug(f'persisting processed new query states from response. query states: {output_query_states} ')
        applied_query_states = []
        for query_state in output_query_states:
            # Iterate each query state and apply it to the output state. A query state
            # containing an array fans out into multiple rows (apply_query_state returns
            # a list) when persistence mode is INDIVIDUAL_ROWS; otherwise it returns a dict.
            applied = processor.output_state.apply_query_state(
                query_state=query_state,
                scope_variable_mappings={
                    "provider": processor.provider,
                    "processor": processor.processor,
                    "input_query_state": input_query_state
                }
            )
            if isinstance(applied, list):
                applied_query_states.extend(applied)
            else:
                applied_query_states.append(applied)
        return applied_query_states


class StatePropagationProviderEdgeFunction(StatePropagationProvider):
    """
    Routes output to edge function service if edge function is configured on the output processor state.
    Edge functions process data on the edge (e.g., calibration, validation) before it reaches the state.
    """

    def __init__(self, route: BaseRoute = None):
        self.route = route

    async def apply_state(self,
                          processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict],
                          input_route_id: str = None) -> [dict]:
        """
        Route to edge function service if configured.

        Returns output_query_states if edge function handled it, None otherwise.
        """
        # Check if edge function is configured and enabled
        edge_function = processor.output_processor_state.edge_function
        if not edge_function or not edge_function.enabled:
            return None  # Signal that edge function didn't handle this

        if not self.route:
            logging.warning(f'edge function enabled but no route configured, skipping edge function')
            return None

        # Extract attempt from route_metadata if present (for retry tracking)
        route_metadata = {}
        if isinstance(input_query_state, dict):
            route_metadata = input_query_state.get("route_metadata", {})
        elif isinstance(input_query_state, list) and len(input_query_state) > 0:
            route_metadata = input_query_state[0].get("route_metadata", {})

        attempt = route_metadata.get("attempt", 1)

        # Build minimal message - service looks up config from DB via route_id
        edge_function_message = {
            "type": "edge_function",
            "route_id": processor.output_processor_state.id,
            "input_route_id": input_route_id,
            "input_query_state": input_query_state,
            "query_state": output_query_states,
            "attempt": attempt
        }

        logging.info(f'routing to edge function service for route_id: {processor.output_processor_state.id}, attempt: {attempt}')
        await self.route.publish(json.dumps(edge_function_message))
        return output_query_states


class StatePropagationProviderDistributor(StatePropagationProvider):

    def __init__(self, propagators: List[StatePropagationProvider], edge_function_route: BaseRoute = None):
        self.propagators = propagators
        self.edge_function_provider = StatePropagationProviderEdgeFunction(route=edge_function_route) if edge_function_route else None

    async def apply_state(
            self,
            processor: 'BaseProcessor',
            input_query_state: Any,
            output_query_states: [dict],
            input_route_id: str = None) -> [dict]:
        """
        Distributes output to propagators, checking for edge functions first.

        If edge function is configured and route is available, routes to edge function
        service and skips normal propagation. The edge function service handles
        routing to sync/router after processing.
        """
        # Check for edge function first - if enabled, route there and skip normal propagation
        if self.edge_function_provider:
            edge_result = await self.edge_function_provider.apply_state(
                processor=processor,
                input_query_state=input_query_state,
                output_query_states=output_query_states,
                input_route_id=input_route_id
            )

            if edge_result is not None:
                # Edge function is handling this, skip normal propagation
                logging.debug(f'edge function handling output, skipping normal propagation')
                return output_query_states

        # No edge function or not enabled, run normal propagators
        for propagator in self.propagators:
            await propagator.apply_state(
                processor=processor,
                input_query_state=input_query_state,
                output_query_states=output_query_states,
                input_route_id=input_route_id
            )

        return output_query_states


class BaseProcessor(MonitoredProcessorState):

    @property
    def template(self) -> InstructionTemplate | None:
        """Fetch the instruction template from storage, if one is configured."""
        template_id = getattr(self.config, 'template_id', None)
        if template_id:
            return self.storage.fetch_template(template_id)
        return None

    @property
    def properties(self) -> ProcessorPropertiesBase:
        """Return typed processor properties (e.g. requestDelay, concurrency settings)."""
        if not self.processor.properties:
            return ProcessorPropertiesBase()
        return ProcessorPropertiesBase(**self.processor.properties)

    def __init__(self,
                 output_state: State,
                 state_machine_storage: StateMachineStorage,
                 provider: ProcessorProvider = None,
                 processor: Processor = None,
                 output_processor_state: ProcessorState = None,
                 state_propagation_provider: StatePropagationProvider = StatePropagationProviderCore(),
                 stream_route: BaseRoute = None,
                 **kwargs):
        """Initialize a processor bound to an output state.

        Args:
            output_state: The State object this processor writes results to.
            state_machine_storage: Storage backend for fetching processors, templates, usage, etc.
            provider: The provider (model/service) backing this processor.
            processor: The Processor entity from the database.
            output_processor_state: The ProcessorState linking processor to output state.
            state_propagation_provider: Strategy for propagating output (core, router, sync-store, etc.).
            stream_route: Route for streaming output, required when execution strategy is STREAM.
        """
        super().__init__(**kwargs)

        self.state_propagation_provider = state_propagation_provider
        self.current_status = ProcessorStatusCode.CREATED
        self.output_state = output_state
        self.storage = state_machine_storage
        self.provider = provider
        self.processor = processor
        self.output_processor_state = output_processor_state
        self.stream_route = stream_route
        self.input_route_id = None  # set by execute() for calibration/retry support

        logging.info(
            f'setting up processor: {self.processor.id if processor else None}, '
            f'provider: {self.provider.name if provider else None} '
            f'v{self.provider.version if provider else None}'
        )

    @property
    def config(self) -> StateConfig:
        """Shortcut to the output state's configuration."""
        return self.output_state.config

    @config.setter
    def config(self, config):
        self.output_state.config = config

    @property
    def data(self):
        """Shortcut to the output state's row data."""
        return self.output_state.data

    @property
    def columns(self):
        """Shortcut to the output state's column definitions."""
        return self.output_state.columns

    @columns.setter
    def columns(self, columns):
        self.output_state.columns = columns

    @property
    def mapping(self):
        """Shortcut to the output state's key → row index mapping."""
        return self.output_state.mapping

    def fetch_session_data(self, input_data):
        """Retrieve prior session messages for context-aware processing.

        Looks up the session history by (source, session_id) from the input dict
        and returns a list of deserialized message dicts.

        Returns:
            List[dict]: Prior session messages, empty list if unavailable.
        """
        if not isinstance(input_data, dict):
            return []

        if 'session_id' not in input_data:
            return []

        user_id = input_data['source']
        session_id = input_data['session_id']
        session_history = self.storage.fetch_session_messages(
            user_id=user_id, session_id=session_id
        )

        if not session_history:
            return []

        return [json.loads(entry.original_content) for entry in session_history]

    def has_query_state(self, query_state_key: str, force: bool = False):
        """Check whether a query state key has already been processed.

        Args:
            query_state_key: The state key hash to look up.
            force: If True, ignore cached state and return False.

        Returns:
            None if mapping is not initialized, True if cached, False otherwise.
        """
        if not self.mapping:
            return None

        if not force and query_state_key in self.mapping:
            logging.info(f'query {query_state_key}, cached, on config: {self.config}')
            return True

        logging.info(f'query {query_state_key}, not cached, on config: {self.config}')
        return False

    def get_current_status(self):
        """Return the current in-memory processor status."""
        return self.current_status

    def update_current_status(self, new_status: ProcessorStatusCode):
        """Transition the processor to a new status with validation.

        Raises if the transition is not allowed by the status state machine.
        """
        validate_processor_status_change(
            current_status=self.get_current_status(),
            new_status=new_status
        )
        self.current_status = new_status

    _TERMINAL_STATUSES = frozenset({
        ProcessorStatusCode.TERMINATE,
        ProcessorStatusCode.FAILED,
        ProcessorStatusCode.STOPPED,
    })

    async def _can_processor_process_data(self) -> bool:
        """Determine whether this processor is allowed to execute.

        Performs three sequential checks:
          1. Processor existence — guards against stale references.
          2. Processor status — rejects terminal states (TERMINATE, FAILED, STOPPED).
          3. Usage limits — fetches the owning project's current usage report and
             short-circuits on "block" decisions from the usage tier.

        Returns:
            True if execution should proceed, False otherwise.
        """
        # 1. Verify processor still exists in storage
        processor = self.storage.fetch_processor(processor_id=self.processor.id)
        if not processor:
            logging.error(
                f'processor {self.processor.id} not found in storage — '
                f'likely a stale reference or storage implementation issue'
            )
            return False

        # 2. Reject terminal statuses
        if processor.status in self._TERMINAL_STATUSES:
            logging.debug(
                f'processor {self.processor.id} is in terminal state '
                f'{processor.status}, skipping execution'
            )
            return False

        # 3. Check usage limits against the owning project
        # TODO: cache project lookup or add project_id to Processor model
        project = self.storage.fetch_user_project(project_id=processor.project_id)
        if not project:
            logging.error(
                f'project {processor.project_id} not found for '
                f'processor {self.processor.id}'
            )
            return False

        user_id = project.user_id
        usage = self.storage.fetch_user_project_current_usage_report(user_id=user_id)
        if not usage:
            # No usage record yet — first-time user, allow processing
            logging.info(f'no usage record for user {user_id}, allowing processing')
            return True

        decision, detail = usage.is_allowed()
        if decision == "block":
            logging.warning(
                f'usage limit reached for user {user_id}: {detail} — blocking'
            )
            return False

        if decision == "warn":
            logging.warning(
                f'usage limit approaching for user {user_id}: {detail} — allowing'
            )

        return True

    def _should_use_stream(self) -> bool:
        """Check whether this processor should use streaming execution."""
        props = self.output_state.typed_properties
        if props.execution and props.execution.strategy:
            strategy = ExecutionStrategy(props.execution.strategy)
            return strategy == ExecutionStrategy.STREAM
        return False

    async def _apply_request_delay(self):
        """Sleep for the configured requestDelay (ms) between executions.

        Used to rate-limit calls to external providers (e.g. LLM APIs).
        No-op if requestDelay is 0.
        """
        if self.properties.requestDelay > 0:
            logging.debug(
                f'processor {self.processor.id} for {self.provider.id} '
                f'applying request delay of {self.properties.requestDelay} ms'
            )
            await asyncio.sleep(self.properties.requestDelay / 1000.0)

    async def execute(self, input_data, force=False, input_route_id=None) -> list:
        """
        Unified execution pipeline for both individual entries and batch sets.

        Args:
            input_data: dict (single entry) or List[dict] (batch).
            force: Flag to force the process.
            input_route_id: The input route id where the input came from (for calibration/retry).

        Returns:
            List[dict]: The processed output query states.
        """
        if not await self._can_processor_process_data():
            logging.debug(
                f'processor {self.processor.id} for {self.provider.id} '
                f'is in a stopped state, skipping'
            )
            return []

        self.input_route_id = input_route_id

        try:
            # send state processor status running event
            route_id = self.output_processor_state.id
            await self.send_processor_state_update(
                route_id=route_id,
                status=ProcessorStatusCode.RUNNING
            )

            output_query_states = []
            if self._should_use_stream():
                await self._process_input_data_stream(
                    input_data=input_data
                )
            else:
                # invoke processor with inbound input
                output_query_states, raw_output = await self.process_input_data(
                    input_data=input_data, force=force
                )

                # enrich output with metadata (raw_output, provider, created_at)
                output_query_states = self._apply_flag_outputs(
                    output_query_states=output_query_states,
                    raw_output=raw_output
                )

            # apply a delay if configured in next inbound processing event
            await self._apply_request_delay()

            # send state processor status completed event
            await self.send_processor_state_update(
                route_id=route_id,
                status=ProcessorStatusCode.COMPLETED
            )
            return output_query_states

        except Exception as ex:
            await self.fail_execute_processor_state(
                route_id=self.output_processor_state.id,
                exception=ex,
                data=input_data,
            )
            return []

    async def finalize_result(self,
        result: dict | List[dict] | str, input_data: dict | List[dict],
        additional_query_state: any,
        input_route_id: str = None,
        raw_output: any = None,
    ) -> List[any]:
        """Finalize processor output: apply inheritance, propagate to downstream.

        Called by concrete processor subclasses at the end of process_input_data().
        Applies the result to the output state (inheritance, key generation),
        then propagates to downstream consumers via the state propagation provider.

        Note: Output enrichment (raw_output, provider, created_at) is applied
        in execute(), not here. The raw_output param is kept for signature
        compatibility but is no longer used in this method.

        Args:
            result: The processor's output — dict, list of dicts, or str.
            input_data: The original input query state.
            additional_query_state: Extra key-value pairs to merge into output.
            input_route_id: Source route for calibration/retry. Falls back to
                            self.input_route_id if not provided.
            raw_output: Deprecated here — enrichment now happens in execute().

        Returns:
            List[dict]: The finalized output query states after propagation.
        """

        # Use instance variable as fallback for input_route_id
        if input_route_id is None:
            input_route_id = self.input_route_id

        # Apply the result from the execution (inheritance, key generation, etc.)
        output_query_states = await self.output_state.apply_result(
            result=result,
            input_data=input_data,
            additional_query_state=additional_query_state
        )

        # Propagate output to downstream consumers (sync-store, router, etc.)
        output_query_states = await self.state_propagation_provider.apply_state(
            processor=self,
            input_query_state=input_data,
            output_query_states=output_query_states,
            input_route_id=input_route_id
        )

        return output_query_states

    async def process_input_data(self, input_data: dict | List[dict], force: bool = False) -> tuple[dict | List[any] | None, any]:
        """Process input data and return (output_query_states, raw_output).

        Must be implemented by concrete processor subclasses (e.g. LM, DB, Code).

        Args:
            input_data: Single entry dict or batch list depending on execution strategy.
            force: If True, re-process even if the entry was already cached.

        Returns:
            Tuple of (processed output states, raw provider response).
        """
        raise NotImplementedError("event processing is not supported by this processor")

    async def process_input_data_stream(self, input_data: dict | List[dict]):
        """Yield streaming content chunks for the given input.

        Must be implemented by streaming-capable processor subclasses.
        Each yielded str is published to the stream route by _process_input_data_stream.
        """
        raise NotImplementedError("streaming is not supported by this processor")

    async def _process_input_data_stream(self, input_data: dict | List[dict]):
        """Internal stream orchestrator: route setup, chunk publishing, cleanup."""
        if not self.stream_route:
            raise ValueError(
                f"streams are not supported by provider: {self.output_processor_state.id}, "
                f"route_id {self.output_processor_state.id}")

        if not input_data:
            raise ValueError("invalid input state, cannot be empty")

        if 'session_id' in input_data:
            session_id = input_data["session_id"]
            subject = f"processor.state.{self.output_state.id}.{session_id}"
        else:
            subject = f"processor.state.{self.output_state.id}"

        name = subject.replace("-", "_")
        logging.debug(f"entered streaming mode, state_id: {self.output_state.id}")

        stream_route = self.stream_route.clone(
            route_config_updates={"subject": subject, "name": name}
        )

        try:
            async for content in self.process_input_data_stream(input_data=input_data):
                try:
                    if isinstance(content, str):
                        await stream_route.publish(content)
                        await stream_route.flush()
                    elif content is None:
                        logging.warning('Received NoneType content, skipping...')
                    else:
                        logging.warning(f'Unexpected content type: {type(content)}')
                except Exception as critical:
                    logging.critical(f'Exception encountered during streaming: {critical}', exc_info=True)

            await stream_route.disconnect()
            logging.debug(f"exit streaming mode, state_id: {self.output_state.id}")
        except Exception as exception:
            await self.fail_execute_processor_state(
                route_id=self.output_processor_state.id,
                exception=exception,
                data=input_data
            )
    def _apply_flag_outputs(self, output_query_states: dict | list, raw_output) -> [dict | list[dict] | None]:
        """Enrich output query states with metadata based on output properties.

        Applies optional enrichments controlled by State.properties.output.enrichments:
          - _raw_output: the raw provider response (OutputEnrichment.RAW_OUTPUT)
          - provider: "{name}.{version}" string (OutputEnrichment.PROVIDER)
          - created_at: UTC ISO timestamp (OutputEnrichment.CREATED_AT)

        Called from execute() after process_input_data() completes.
        """
        props = self.output_state.typed_properties
        enrichments = set(props.output.enrichments) if props.output and props.output.enrichments else set()

        additional_query_state = {}

        # Attach raw provider response for auditing/retrieval
        if OutputEnrichment.RAW_OUTPUT in enrichments and raw_output:
            if is_json_serializable(raw_output):
                additional_query_state['_raw_output'] = raw_output
            else:
                additional_query_state['_raw_output'] = str(raw_output)

        # include provider information in the query state,
        # useful when multiple processing providers are publishing to the same state output
        if OutputEnrichment.PROVIDER in enrichments:
            provider_info = f"{self.provider.name}.{self.provider.version}".lower()
            additional_query_state["provider"] = provider_info

        if OutputEnrichment.CREATED_AT in enrichments:
            additional_query_state["created_at"] = datetime.now(timezone.utc).isoformat()

        if not additional_query_state:
            return output_query_states

        if isinstance(output_query_states, list):
            for i in range(len(output_query_states)):
                output_query_states[i] = { **output_query_states[i], **additional_query_state }
        else:
            output_query_states = { **output_query_states, **additional_query_state }

        return output_query_states

if __name__ == '__main__':
    # build a test state
    test_state = State(
        config=StateConfig(
            name='test state 1',
            # input_path='../states/07c5ea7bfa7e9c6ffd93848a9be3c2e712a0e6ca43cc0ad12b6dd24ebd788d6f.json',
            # output_path='../../states/',
            # output_path='../dataset/examples/states/184fef148b36325a9f01eff757f0d90af535f4259c105fc612887d5fad34ce11.json',
            primary_key=[
                StateDataKeyDefinition(name='query'),
                StateDataKeyDefinition(name='context'),
            ],
            query_state_inheritance=[
                StateDataKeyDefinition(name='query', alias='input_query'),
                StateDataKeyDefinition(name='context', alias='input_context'),
            ]
        ),
        columns={
            'query': StateDataColumnDefinition(name='query'),
            'context': StateDataColumnDefinition(name='context'),
            'response': StateDataColumnDefinition(name='response'),
            'analysis_dimension': StateDataColumnDefinition(name='response'),
            'analysis_dimension_score': StateDataColumnDefinition(name='response')
        },
        data={
            'query': StateDataRowColumnData(
                values=['tell me about dogs.', 'where do cows live?', 'why do cows exist?']),
            'context': StateDataRowColumnData(values=['Education', 'Education', 'Education']),
            'response': StateDataRowColumnData(values=['dogs are pets', 'cows live on farms', 'as a food source']),
            'analysis_dimension': StateDataRowColumnData(values=['Person-Centric', 'Person-Centric', 'Person-Centric']),
            'analysis_dimension_score': StateDataRowColumnData(values=[63, 68, 20])
        },
        mapping={
            'abc': StateDataColumnIndex(key='abc', values=[0]),
            'def': StateDataColumnIndex(key='def', values=[1]),
            'ghi': StateDataColumnIndex(key='jkl', values=[2])
        }
    )

    test_state.save_state(output_path='../states/test_state.pickle')
    test_state.save_state(output_path='../states/test_state.json')

    # when adding a new row you only provide the values, it must match the same
    # number of columns and in the order of the columns that were added, otherwise
    # there will be data / column misalignment
    test_state.add_row_data(StateDataRowColumnData(values=[
        'why are we ....?',  # query
        'Education',  # context
        'cats are ....',  # response
        'Instrumentalist',  # analysis_dimension
        45,  # analysis_dimension_score
    ]))

    test_state.add_row_data(StateDataRowColumnData(values=[
        'why are cats and dogs....?',  # query
        'Education',  # context
        'cats and dogs ....',  # response
        'Person-Centric',  # analysis_dimension
        88,  # analysis_dimension_score
    ]))

    print(test_state)
