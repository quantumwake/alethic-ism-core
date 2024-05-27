from pydantic import ValidationError
import logging as logging

from .base_message_consumer import BaseMessagingConsumer
from .base_model import ProcessorStateDirection, ProcessorProvider, Processor, StatusCode, ProcessorState
from .processor_state import State


class BaseMessagingConsumerState(BaseMessagingConsumer):

    def create_processor(self,
                         processor: Processor,
                         provider: ProcessorProvider,
                         output_processor_state: ProcessorState,
                         output_state: State):
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
            output_processor_states = self.storage.fetch_processor_state(
                processor_id=processor_id,
                direction=ProcessorStateDirection.OUTPUT
            )

            if not output_processor_states:
                raise BrokenPipeError(f'no output state found for processor id: {processor_id} provider {provider.id}')

            # fetch query state input entries
            query_states = message['query_state']

            logging.info(f'found {len(output_processor_states)} on processor id {processor_id} with provider {provider.id}')

            # iterate each output state and forward the query state input
            for output_processor_state in output_processor_states:
                # load the output state and relevant state instruction
                output_state = self.storage.load_state(state_id=output_processor_state.state_id, load_data=False)

                logging.info(f'creating processor provider {processor_id} on output state id {output_processor_state.state_id} with '
                             f'current index: {output_processor_state.current_index}, '
                             f'maximum processed index: {output_processor_state.maximum_index}, '
                             f'count: {output_processor_state.count}')

                # create (or fetch cached state) process handling this state output instruction
                processor = self.create_processor(
                    processor=processor,
                    provider=provider,
                    output_processor_state=output_processor_state,
                    output_state=output_state
                )

                # submit execution
                await self.pre_execute(message=message)

                if isinstance(query_states, dict):
                    query_states = [query_states]

                logging.debug(f'submitting batch query state entries count: {len(query_states)}, '
                              f'with processor_id: {processor_id}, '
                              f'provider_id: {provider.id}')

                # iterate each query state entry and forward it to the processor
                for query_state_entry in query_states:
                    processor.process_input_data_entry(
                        input_query_state=query_state_entry
                    )

            # submit completed execution
            await self.post_execute(
                processor_id=processor_id,
                message=message
            )

        except Exception as exception:
            # submit failed execution
            await self.fail_execute(
                processor_id=processor_id,
                message=message,
                ex=exception
            )
            logging.error(f'critical error {exception}')
        finally:
            pass
