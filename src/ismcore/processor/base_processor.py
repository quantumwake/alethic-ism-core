import json
import asyncio
from typing import Any, List, Dict, Union

from ismcore.messaging.base_message_route_model import BaseRoute
from ismcore.processor.monitored_processor_state import MonitoredProcessorState
from ismcore.storage.processor_state_storage import StateMachineStorage, FieldConfig
from ismcore.utils.general_utils import build_template_text_v2
from ismcore.utils.state_utils import validate_processor_status_change
from ismcore.utils.ism_logger import ism_logger
from ismcore.model.base_model import (
    ProcessorStateDirection,
    InstructionTemplate,
    ProcessorProvider,
    ProcessorPropertiesBase,
    Processor,
    ProcessorState,
    ProcessorStatusCode,
    EdgeFunctionConfig)
from ismcore.model.processor_state import (
    State,
    StateDataRowColumnData,
    StateDataColumnDefinition,
    StateDataKeyDefinition,
    StateConfig,
    StateDataColumnIndex, StateConfigStream,
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
        Route the processed new query states from the response to a synchronization topic

        Args:
            processor (List[Dict]): The processor instance that is processing this input query state entry
            input_query_state (Any): The initial input query state.
            output_query_states (List[Dict]): The processed output query states.
            input_route_id (str): The input route id where the input came from (for calibration/retry).

        Returns:
            List[Any]: The result of applying the query states to the output state.
        """

        # create a new message for routing purposes
        route_message = {
            "route_id": processor.output_processor_state.id,
            "input_route_id": input_route_id,
            "type": "query_state_route",
            "input_query_state": input_query_state,
            "query_state": output_query_states
        }

        await self.route.publish(json.dumps(route_message))
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

        # If the flag is set to false, then skip it
        if not processor.config.flag_auto_route_output_state:
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

        # If the flag is set and the flat is false, then skip it
        if not processor.config.flag_auto_save_output_state:
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
        return [processor.output_state.apply_query_state(  # Iterate each query state and apply it to the output state
            query_state=query_state,
            scope_variable_mappings={
                "provider": processor.provider,
                "processor": processor.processor,
                "input_query_state": input_query_state
            }
        ) for query_state in output_query_states]


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
        # if not isinstance(self.config, StateConfigStream):
        #     raise ValueError("system template cannot be set for streaming configuration, use template instead")

        if self.config.template_id:
            template = self.storage.fetch_template(self.config.template_id)
            return template

        return None

    @property
    def properties(self) -> ProcessorPropertiesBase:
        """Override base class to return typed base properties"""
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

        super().__init__(**kwargs)

        # TODO move into a Syncable and StateRouteable feature class
        # self.sync_store_route = sync_store_route
        # self.state_router_route = state_router_route

        self.state_propagation_provider = state_propagation_provider

        self.current_status = ProcessorStatusCode.CREATED
        self.output_state = output_state
        self.storage = state_machine_storage
        self.provider = provider
        self.processor = processor
        self.output_processor_state = output_processor_state
        self.stream_route = stream_route
        self.input_route_id = None  # set by execute_set/execute_entry for calibration/retry support

        logging.info(f'setting up processor: {self.processor.id if processor else None},'
                     f'provider id: {self.provider.id if provider else None}, '
                     f'provider name: {self.provider.name if provider else None}, '
                     f'provider version: {self.provider.version if provider else None}, '
                     f'config: {self.config}')

    @property
    def properties(self) -> ProcessorPropertiesBase:
        """Return typed processor properties"""
        if not self.processor.properties:
            return ProcessorPropertiesBase()
        return ProcessorPropertiesBase(**self.processor.properties)

    @property
    def config(self) -> Union[StateConfig, StateConfigStream]:
        return self.output_state.config

    @config.setter
    def config(self, config):
        self.output_state.config = config

    @property
    def data(self):
        return self.output_state.data

    @property
    def columns(self):
        return self.output_state.columns

    @columns.setter
    def columns(self, columns):
        self.output_state.columns = columns

    @property
    def mapping(self):
        return self.output_state.mapping

    def fetch_session_data(self, input_data):
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

        messages_dict = [json.loads(entry.original_content) for entry in session_history]

        # messages_dict = []
        # for entry in session_history:
        #     try:
        #         entry = json.loads(entry)
        #     except:
        #         pass
        #
        #     if isinstance(entry, dict):
        #         # role = entry['role'] if 'role' in entry else 'history'
        #         # content = entry['content'].strip() if 'content' in entry else str(entry)
        #         messages_dict.append(entry)
        #     else:
        #         messages_dict.append({"role": "user", "content": str(entry)})

        return messages_dict

    def has_query_state(self, query_state_key: str, force: bool = False):
        # make sure that the state is initialized and that there is a data key
        if not self.mapping:
            return None

        # skip if not forced and state exists
        if not force and query_state_key in self.mapping:
            logging.info(f'query {query_state_key}, cached, on config: {self.config}')
            return True

        # otherwise return none, which means no state exists
        logging.info(f'query {query_state_key}, not cached, on config: {self.config}')
        return False

    def get_current_status(self):
        return self.current_status

    def update_current_status(self, new_status: ProcessorStatusCode):
        validate_processor_status_change(
            current_status=self.get_current_status(),
            new_status=new_status
        )

        self.current_status = new_status

    async def can_processor_process_data(self):
        processor = self.storage.fetch_processor(processor_id=self.processor.id)
        if not processor:
            logging.error(f'critical, unable to find processor id: {self.processor.id}, '
                          f'likely a storage implementation issue, should have not got this far.')
            return False

        # ensure that the processor status is not in terminated state
        if processor.status in [
            ProcessorStatusCode.TERMINATE,
            ProcessorStatusCode.FAILED,
            ProcessorStatusCode.STOPPED
        ]: return False

        # check usage limits
        ## TODO add project id to processor model so we do not need to fetch processor again (OR CACHE THIS IN THE DB STORAGE SIMILAR TO ALETHIC-ISM-CORE-GO sdk)
        project = self.storage.fetch_user_project(project_id=processor.project_id)
        if not project:
            logging.error(f'critical, unable to find project id: {processor.project_id} for processor id: {self.processor.id}')
            return False

        ##
        user_id = project.user_id
        # project_id = project.id   ## TODO use this later when we add project based tiers
        # processor_provider = self.storage.fetch_processor_provider(id=processor.provider_id) ## TODO use this later when we add processor based tiers

        ## TODO need to check the user limits in addition to project limits, we need to add tiers per project as well; and we also need to add tiers per user&project&processor if any
        user_current_usage = self.storage.fetch_user_project_current_usage_report(user_id=project.user_id)
        if not user_current_usage:
            logging.info(f"user has no usage yet, allowing processing for user: {project.user_id}")
        else:
            decision, ok = user_current_usage.is_allowed()
            if decision == "ok":
                logging.debug(f'user usage within limits for user: {user_id} - allowing processing {decision}')
            elif decision == "warn":
                logging.warning(f'usage limit approaching for user: {user_id} - allowing processing {decision}')
            elif decision == "block":
                logging.warning(f'usage limit reached for user: {user_id} - blocking processing {decision}')
                return False

        return True

    async def execute_set(self, input_query_state: List[dict] = None, force: bool = False, input_route_id: str = None):
        is_allowed_to_process = await self.can_processor_process_data()
        if not is_allowed_to_process:
            logging.debug(f'processor {self.processor.id} for {self.provider.id}' f'is in a stopped state, skipping input query processing')
            return []

        if self.config.flag_dedup_drop_enabled:
            pass  # TODO need to check input for deduplication (need to keep the hash of the input if this is enabled)

        # store input_route_id for use in finalize_result (calibration/retry support)
        self.input_route_id = input_route_id

        # execute the input entry given the processor implementation
        try:
            route_id = self.output_processor_state.id

            # RUNNING: the processor is about to execute the instructions
            await self.send_processor_state_update(route_id=route_id, status=ProcessorStatusCode.RUNNING)

            # RUNNING (INTRA): the processor is executing the output instructions on the input
            output_query_states = []  # TODO not sure if we should do something with if the config is a streams?
            if self.config.flag_expect_stream:
                await self.process_input_data_stream(input_data=input_query_state)
            else:
                output_query_states = await self.process_input_data(input_data=input_query_state, force=force)

            # Apply request delay if configured
            if self.properties.requestDelay > 0:
                logging.debug(f'processor {self.processor.id} for {self.provider.id} applying request delay of {self.properties.requestDelay} ms')
                await asyncio.sleep(self.properties.requestDelay / 1000.0)  # Convert ms to seconds

            # COMPLETED: the processor has completed execution of instructions
            await self.send_processor_state_update(route_id=route_id, status=ProcessorStatusCode.COMPLETED)
            return output_query_states
        except Exception as ex:
            await self.fail_execute_processor_state(route_id=self.output_processor_state.id, exception=ex)

    async def execute_entry(self, input_query_state: dict, force: bool = False, input_route_id: str = None):
        """
        Executes the processor state update and processes the input data entry.

        Args:
            input_query_state (Dict): The input query state to process.
            force (bool, optional): Flag to force the process. Defaults to False.
            input_route_id (str): The input route id where the input came from (for calibration/retry).

        Returns:
            List[Dict]: The processed output query states.

        Raises:
            Exception: If an error occurs during execution.
        """
        is_allowed_to_process = await self.can_processor_process_data()
        if not is_allowed_to_process:
            logging.debug(f'processor {self.processor.id} for {self.provider.id}'
                          f'is in a stopped state, skipping input query processing')
            return []

        if self.config.flag_dedup_drop_enabled:
            pass  # TODO need to check input for deduplication (need to keep the hash of the input if this is enabled)

        # store input_route_id for use in finalize_result (calibration/retry support)
        self.input_route_id = input_route_id

        # execute the input entry given the processor implementation
        try:
            route_id = self.output_processor_state.id

            # RUNNING: the processor is about to execute the instructions
            await self.send_processor_state_update(route_id=route_id, status=ProcessorStatusCode.RUNNING)

            # RUNNING (INTRA): the processor is executing the output instructions on the input
            output_query_states = []  # TODO not sure if we should do something with if the config is a streams?
            if isinstance(self.config, StateConfigStream) or self.config.flag_expect_stream:
                await self.process_input_data_stream(input_data=input_query_state)
            else:
                output_query_states = await self.process_input_data(input_data=input_query_state, force=force)

            # Apply request delay if configured
            if self.properties.requestDelay > 0:
                logging.debug(f'processor {self.processor.id} for {self.provider.id} applying request delay of {self.properties.requestDelay} ms')
                await asyncio.sleep(self.properties.requestDelay / 1000.0)  # Convert ms to seconds

            # COMPLETED: the processor has completed execution of instructions
            await self.send_processor_state_update(route_id=route_id, status=ProcessorStatusCode.COMPLETED)
            return output_query_states
        except Exception as ex:
            await self.fail_execute_processor_state(
                route_id=self.output_processor_state.id,
                exception=ex,
                data=input_query_state
            )

    async def finalize_result(self, result: dict | List[dict] | str, input_data: dict | List[dict], additional_query_state: Any, input_route_id: str = None) -> List[Any]:
        """
        Finalizes the result by applying the result to the output state.

        Args:
            result (Any): The result of the execution.
            input_data (Any): The initial input query state.
            additional_query_state (Any): Any additional output values.
            input_route_id (str): The input route id where the input came from (for calibration/retry).
                                  Falls back to self.input_route_id if not provided.

        Returns:
            List[Any]: The final applied states.
        """

        # Use instance variable as fallback for input_route_id
        if input_route_id is None:
            input_route_id = self.input_route_id

        # Apply the result from the execution
        output_query_states = await self.output_state.apply_result(
            result=result,  # the result of the execution
            input_data=input_data,  # the initial input state
            additional_query_state=additional_query_state  # any additional output values
        )

        # Apply the new query states to the state propagator, if defined
        output_query_states = await self.state_propagation_provider.apply_state(
            processor=self,
            input_query_state=input_data,
            output_query_states=output_query_states,
            input_route_id=input_route_id
        )

        # Apply the new query state to the persistent storage class defined
        # output_query_states = await self.save_states(
        #     input_query_state=input_query_state,
        #     output_query_states=output_query_states
        # )

        # return the results
        return output_query_states

    async def process_input_data(self, input_data: dict | List[dict], force: bool = False):
        raise NotImplementedError("event processing is not supported by this processor")

    ## TODO need to clean up these methods
    async def _stream(self, input_data: Any, template: str):
        raise NotImplementedError()

    async def process_input_data_stream(self, input_data: dict | List[dict], force: bool = False):
        if not self.stream_route:
            raise ValueError(
                f"streams are not supported by provider: {self.output_processor_state.id}, "
                f"route_id {self.output_processor_state.id}")

        if not input_data:
            raise ValueError("invalid input state, cannot be empty")

        # TODO this such a terrible HACK to use a session id for a given processor state stream
        if 'session_id' in input_data:
            session_id = input_data["session_id"]
            subject = f"processor.state.{self.output_state.id}.{session_id}"
        else:
            subject = f"processor.state.{self.output_state.id}"

        name = f"{subject}".replace("-", "_")

        # begin the processing of the prompts
        logging.debug(f"entered streaming mode, state_id: {self.output_state.id}")

        # submit to the fully qualified subject, which may include a session id
        stream_route = self.stream_route.clone(
            route_config_updates={
                "subject": subject,
                "name": name
            }
        )

        # check if template attribute exists in
        if hasattr(self, 'template'):
            if not self.template:
                template = None
            else:
                template = build_template_text_v2(self.template, input_data)
        else:
            template = None


        try:
            # submit the original request to the stream, to all subscribers of the subject
            # TODO this needs to be invoked at the LM processor level, pre-stream-processing
            if 'source' in input_data:
                await stream_route.publish(input_data['source'])
                await stream_route.publish("<<>>SOURCE<<>>")

            if 'input' in input_data:
                await stream_route.publish(input_data['input'])
                await stream_route.publish("<<>>INPUT<<>>")

            # flush the stream to ensure the messages are sent to the stream server
            await stream_route.flush()

            # build a coroutine calling the concrete implementation of the stream
            stream = self._stream(input_data=input_data, template=template)

            # execute and iterate the yielded data directly into the upstream route
            async for content in stream:
                try:
                    if isinstance(content, str):
                        await stream_route.publish(content)
                        await stream_route.flush()
                    elif content is None:
                        # Log or handle the None case if necessary
                        logging.warning('Received NoneType content, skipping...')
                    else:
                        # Handle unexpected types
                        logging.warning(f'Unexpected content type: {type(content)}')
                except Exception as critical:
                    # Provide more detailed exception handling
                    logging.critical(f'Exception encountered during streaming: {critical}', exc_info=True)

            # TODO this needs to be invoked at the LM processor level, post-stream-processing
            # submit the response message to the stream.
            await stream_route.publish("<<>>ASSISTANT<<>>")
            await stream_route.flush()

            # should gracefully close the connection
            await stream_route.disconnect()

            logging.debug(f"exit streaming mode, state_id: {self.output_state.id}")
        except Exception as exception:
            # submit the response message to the stream.
            await stream_route.publish("<<>>ERROR<<>>")
            await stream_route.flush()
            await self.fail_execute_processor_state(
                # self.output_processor_state,
                route_id=self.output_processor_state.id,
                exception=exception,
                data=input_data
            )

    #
    # async def stream_input_data_entry(self, input_query_state: dict):
    #     raise NotImplementedError("stream processing is not supported by this processor")


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
