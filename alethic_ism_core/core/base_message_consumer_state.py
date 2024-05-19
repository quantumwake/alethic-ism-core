from pydantic import ValidationError
import logging as logging

from .base_message_consumer import BaseMessagingConsumer
from .base_model import ProcessorStateDirection, ProcessorProvider
from .processor_state import State


class BaseMessagingConsumerState(BaseMessagingConsumer):

    def create_processor(self, provider: ProcessorProvider, output_state: State):
        # create (or fetch cached state) processor handling this state output instruction
        raise NotImplementedError(f'must return an instance of BaseProcessorLM for provider {provider.id}, '
                                  f'output state: {output_state.id}')

    async def execute(self, message: dict):
        try:

            if 'type' not in message:
                raise ValidationError(f'unable to identity type for consumed message {message}')

            message_type = message['type']
            if message_type != 'query_state':
                raise NotImplemented(f'unsupported message type: {message_type}')

            processor_id = message['processor_id']
            processor = self.storage.fetch_processor(processor_id=processor_id)
            provider = self.storage.fetch_processor_provider(id=processor.provider_id)

            # fetch the processors to forward the state query to, state must be an input of the state id
            output_states = self.storage.fetch_processor_state(
                processor_id=processor_id,
                direction=ProcessorStateDirection.OUTPUT
            )

            if not output_states:
                raise BrokenPipeError(f'no output state found for processor id: {processor_id} provider {provider.id}')

            # fetch query state input entries
            query_states = message['query_state']

            logging.info(f'found {len(output_states)} on processor id {processor_id} with provider {provider.id}')

            # iterate each output state and forward the query state input
            for state in output_states:
                # load the output state and relevant state instruction
                output_state = self.storage.load_state(state_id=state.state_id, load_data=False)

                logging.info(f'creating processor provider {processor_id} on output state id {state.state_id} with '
                             f'current index: {state.current_index}, '
                             f'maximum processed index: {state.maximum_index}, '
                             f'count: {state.count}')

                # create (or fetch cached state) process handling this state output instruction
                processor = self.create_processor(provider=provider, output_state=output_state)

                # iterate each query state entry and forward it to the processor
                if isinstance(query_states, dict):
                    logging.debug(f'submitting single query state entry count: solo, '
                                  f'with processor_id: {processor_id}, '
                                  f'provider_id: {provider.id}')

                    processor.process_input_data_entry(input_query_state=query_states)
                elif isinstance(query_states, list):
                    logging.debug(f'submitting batch query state entries count: {len(query_states)}, '
                                  f'with processor_id: {processor_id}, '
                                  f'provider_id: {provider.id}')

                    # iterate each individual entry and submit
                    # TODO modify to submit as a batch?? although this consumer should be handling 1 request
                    for query_state_entry in query_states:
                        processor.process_input_data_entry(input_query_state=query_state_entry)
                else:
                    raise NotImplemented('unsupported query state entry, it must be a Dict or a List[Dict] where Dict is a '
                                         'key value pair of values, defining a single row and a column per key entry')

        except Exception as exception:
            # processor_state.status = ProcessorStatus.FAILED
            logging.error(f'critical error {exception}')
        finally:
            pass
            # TODO need to update the state of the processor_state for the purpose of providing updates to any listener
            # state_storage.update_processor_state(processor_state=processor_state)
