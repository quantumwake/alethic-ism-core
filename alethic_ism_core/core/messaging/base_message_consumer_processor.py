from .base_message_provider import BaseMessageConsumer
from .base_message_route_model import BaseRoute
from ..base_model import ProcessorStateDirection, ProcessorProvider, Processor, ProcessorState
from ..base_processor import BaseProcessor
from ..monitored_processor_state import MonitoredProcessorState
from ..processor_state import State
from ..processor_state_storage import StateMachineStorage
from ..utils.ismlogging import ism_logger

logging = ism_logger(__name__)


class BaseMessageConsumerProcessor(BaseMessageConsumer):

    def __init__(self, route: BaseRoute, monitor_route: BaseRoute, storage: StateMachineStorage, **kwargs):
        # BaseMessageConsumer.__init__(self, route=route, monitor_route=monitor_route)
        # MonitoredProcessorState.__init__(self, monitor_route=monitor_route)
        super().__init__(route=route, monitor_route=monitor_route)
        self.storage = storage

    def create_processor(self,
                         processor: Processor,
                         provider: ProcessorProvider,
                         output_processor_state: ProcessorState,
                         output_state: State) -> BaseProcessor:
        # create (or fetch cached state) processor handling this state output instruction
        raise NotImplementedError(f'must return an instance of BaseProcessorLM for provider {provider.id}, '
                                  f'output state: {output_state.id}')

    async def fetch_processor_state_outputs(self, consumer_message_mapping: dict):
        try:
            # validate that the type is defined
            if 'type' not in consumer_message_mapping:
                raise ValueError(f'unable to identity type for consumed message {consumer_message_mapping}')

            message_type = consumer_message_mapping['type']
            # validate the message type is supported
            if message_type != 'query_state':
                raise NotImplemented(
                    f'unsupported message format, must be a dictionary defined as type '
                    f'query state with data field value as query_state: {{}} : {message_type}'
                )

            # validate processor id exists in consumed from streaming sub-system
            # if 'processor_id' not in consumer_message_mapping:
            #     raise ValueError(f'no processor_id found in consumed message content')
            if 'route_id' not in consumer_message_mapping:
                raise ValueError(f'no route id found in consumed message content')

            # fetch and validate the processor association, including provider selector
            # processor_id = consumer_message_mapping['processor_id']
            # processor = self.storage.fetch_processor(processor_id=processor_id)
            # if not processor:
            #     raise ValueError(f'no processor found for processor id: {processor_id} ')
            #
            # provider = self.storage.fetch_processor_provider(id=processor.provider_id)
            # if not provider:
            #     raise ValueError(f'no provider found for processor {processor_id}')

            # fetch the processors to forward the state query to, state must be an input of the state id

            # fetch the processor state route such that we can find the processor and all its output states
            route_id = consumer_message_mapping['route_id']
            processor_state_route = self.storage.fetch_processor_state_route(route_id=route_id)
            processor_state_route = processor_state_route[0] \
                if processor_state_route and len(processor_state_route) == 1 else None

            if not processor_state_route:
                raise ValueError(f'invalid processor state route id: {route_id}')

            # identify all output states
            output_processor_states = self.storage.fetch_processor_state_route(
                processor_id=processor_state_route.processor_id,
                direction=ProcessorStateDirection.OUTPUT
            )

            # validate there are output states to submit the query state input to
            if not output_processor_states:
                raise ValueError(f'no output state found for processor id: {processor_state_route.processor_id}')

        except Exception as exception:
            logging.error(f'invalid query state entry: {consumer_message_mapping} provided, ignoring message')
            raise exception

        return output_processor_states


    async def execute(self, consumer_message_mapping: dict):

        # identifies all the output states for this new input state entry(s)
        output_processor_states = await self.fetch_processor_state_outputs(
            consumer_message_mapping=consumer_message_mapping
        )

        # extract the input query state entry(s)
        route_id = consumer_message_mapping['route_id']
        query_states = consumer_message_mapping['query_state']
        logging.info(f'found {len(output_processor_states)} output routes given input route_id: {route_id}')

        # change query state input to a mono list of dict, if instanceof dict, passed to each output state
        query_states = [query_states] \
            if isinstance(query_states, dict) \
            else query_states

        if not query_states:
            raise ValueError(f'no input query state defined in received message: {consumer_message_mapping}')

        # iterate each output state and forward the query state input
        for output_processor_state in output_processor_states:
            try:

                # load the output state and relevant state instruction
                output_state = self.storage.load_state(state_id=output_processor_state.state_id, load_data=False)
                if not output_state:
                    raise ValueError(f'unable to load state {output_processor_state.state_id}')

                logging.debug(
                    f'creating processor state route: {route_id}, '
                    f'processor id: {output_processor_state.processor_id}, '
                    f'output state id: {output_processor_state.state_id}, '
                    f'current index: {output_processor_state.current_index}, '
                    f'maximum processed index: {output_processor_state.maximum_index}, '
                    f'count: {output_processor_state.count}'
                )

                # load processor and provider information
                processor = self.storage.fetch_processor(processor_id=output_processor_state.processor_id)
                provider = self.storage.fetch_processor_provider(id=processor.provider_id)

                # create (or fetch cached state) process handling this state output instruction
                runnable_processor = self.create_processor(
                    processor=processor,
                    provider=provider,
                    output_processor_state=output_processor_state,
                    output_state=output_state
                )

                logging.debug(
                    f'submitting batch query state entries count: {len(query_states)}, '
                    f'with processor_id: {processor.id}, '
                    f'provider_id: {provider.id}'
                )

                # update the processor state with the relevant status of RUNNING
                await self.intra_execute(
                    consumer_message_mapping=consumer_message_mapping
                )

                # iterate each query state entry and forward it to the processor
                for query_state_entry in query_states:
                    await runnable_processor.execute(
                        input_query_state=query_state_entry
                    )

                # submit completed execution
                await self.post_execute(
                    consumer_message_mapping=consumer_message_mapping
                )

            except ValueError as ex:
                # submit failed execution log to the output processor state
                await self.fail_execute_processor_state(
                    route_id=output_processor_state.id,
                    # processor_state=output_processor_state,
                    exception=ex,
                    message=consumer_message_mapping,
                    data=consumer_message_mapping
                )

                logging.warning(f'unable to execute processor state flow, received exception {ex}')
            finally:
                pass
