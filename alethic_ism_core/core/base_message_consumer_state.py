import logging as logging

from .base_message_provider import BaseMessagingConsumer
from .base_model import ProcessorStateDirection, ProcessorProvider, Processor, ProcessorStatusCode, ProcessorState
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
            if 'processor_id' not in consumer_message_mapping:
                raise ValueError(f'no processor_id found in consumed message content')

            # fetch and validate the processor association, including provider selector
            processor_id = consumer_message_mapping['processor_id']
            processor = self.storage.fetch_processor(processor_id=processor_id)
            if not processor:
                raise ValueError(f'no processor found for processor id: {processor_id} ')

            provider = self.storage.fetch_processor_provider(id=processor.provider_id)

            if not provider:
                raise ValueError(f'no provider found for processor {processor_id}')

            # fetch the processors to forward the state query to, state must be an input of the state id
            output_processor_states = self.storage.fetch_processor_state(
                processor_id=processor_id,
                direction=ProcessorStateDirection.OUTPUT
            )

            # validate there are output states to submit the query state input to
            if not output_processor_states:
                raise ValueError(f'no output state found for processor id: {processor_id} provider {provider.id}')

        except Exception as exception:
            logging.error(f'invalid query state entry: {consumer_message_mapping} provided, ignoring message')
            raise exception

        return output_processor_states, processor, provider

    async def execute(self, consumer_message_mapping: dict):

        # identifies all the output states for this new input state entry(s)
        output_processor_states, processor, provider = await self.fetch_processor_state_outputs(
            consumer_message_mapping=consumer_message_mapping
        )

        # extract the input query state entry(s)
        query_states = consumer_message_mapping['query_state']
        logging.info(f'found {len(output_processor_states)} on processor id {processor.id} with provider {provider.id}')

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
                if not output_state: raise ValueError(f'unable to load state {output_processor_state.state_id}')

                logging.debug(
                    f'creating processor provider {processor.id},'
                    f'output state id: {output_processor_state.state_id} with '
                    f'current index: {output_processor_state.current_index}, '
                    f'maximum processed index: {output_processor_state.maximum_index}, '
                    f'count: {output_processor_state.count}'
                )

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
                    runnable_processor.process_input_data_entry(
                        input_query_state=query_state_entry
                    )

                # submit completed execution
                await self.post_execute(
                    consumer_message_mapping=consumer_message_mapping
                )

            except Exception as ex:
                # submit failed execution log to the output processor state
                await self.fail_execute_processor_state(
                    processor_state=output_processor_state,
                    exception=ex,
                    message=consumer_message_mapping
                )

                logging.warning(f'unable to execute processor state flow, received exception {ex}')
            finally:
                pass
