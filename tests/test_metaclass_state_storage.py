import uuid
import pytest
from typing import Optional, List
from ismcore.model.processor_state import State, StateDataKeyDefinition, StateConfigLM, StateConfig
from ismcore.model.base_model import (
    ProcessorStateDirection,
    ProcessorStatusCode,
    ProcessorState,
    InstructionTemplate,
    Processor,
    ProcessorProvider)
from ismcore.storage.processor_state_storage import (
    ProcessorStateRouteStorage,
    StateStorage,
    TemplateStorage,
    ProcessorStorage,
    ProcessorProviderStorage,
    StateMachineStorage)


class MockProcessorStateRouteStorage(ProcessorStateRouteStorage):

    def fetch_processor_state_route(self,
                                    route_id: str = None,
                                    processor_id: str = None,
                                    state_id: str = None,
                                    direction: ProcessorStateDirection = None,
                                    status: ProcessorStatusCode = None) \
            -> Optional[List[ProcessorState]]:

        # route_id = (f'{state_id}:{processor_id}'
        #       if not direction or direction.value == ProcessorStateDirection.INPUT
        #       else f'{processor_id}:{state_id}',)

        if route_id:
            from_to_ids = route_id.split(':')
            state_id = from_to_ids[0]
            processor_id = from_to_ids[1]

        if state_id:
            return [
                ProcessorState(
                    id=route_id,
                    processor_id=processor_id,
                    state_id=state_id,
                    direction=direction if direction else ProcessorStateDirection.INPUT
                )
            ]
        else:
            if direction:
                state_id = state_id if state_id else f"test {direction} state id"
                if direction == ProcessorStateDirection.INPUT:
                    route_id = f'{state_id}:{processor_id}'
                else:
                    route_id = f'{processor_id}:{state_id}'

                return [
                    ProcessorState(
                        id=route_id,
                        processor_id=processor_id,
                        state_id=state_id,
                        direction=direction
                )]
            else:
                state_id_input = str(uuid.uuid4()) if not state_id else state_id
                state_id_output = str(uuid.uuid4()) if not state_id else state_id
                return [
                    ProcessorState(
                        id=f'{state_id_input}:{processor_id}',
                        processor_id=processor_id,
                        state_id=state_id_input,
                        direction=direction if direction else ProcessorStateDirection.INPUT
                    ),

                    ProcessorState(
                        id=f'{processor_id}:{state_id_output}',
                        processor_id=processor_id,
                        state_id=state_id_output,
                        direction=direction if direction else ProcessorStateDirection.OUTPUT
                    ),
                ]

class MockStateStorage(StateStorage):
    def save_state(self, state: State) -> State:
        state.id = "test state id"
        return state

    def load_state(self, state_id: str, load_data: bool = True) -> Optional[State]:

        return State(
            id=state_id,
            state_type="StateConfig",
            config=StateConfigLM(
                name="mock state",
                primary_key=[
                    StateDataKeyDefinition(name="question")
                ],
                user_template_id="test question user template",
            )
        )

class MockTemplateStorage(TemplateStorage):

    def fetch_template(self, template_id: str) \
            -> InstructionTemplate:

        return InstructionTemplate(
            template_id="test question user template",
            template_path="test/user/question",
            template_content="answer the following {question}",
            template_type="user template"
        )



class MockProcessorStorage(ProcessorStorage):

    def fetch_processor(self, processor_id: str) \
            -> Optional[Processor]:

        return Processor(
            id=processor_id,
            project_id="test project id",
            provider_id="test/mocked/provider"
        )

class MockProcessorProviderStorage(ProcessorProviderStorage):
    def fetch_processor_provider(self, id: str) -> Optional[ProcessorProvider]:
        return ProcessorProvider(
            id=id,
            name="mock_provider",
            version="test version",
            class_name="MockClass"
        )


def test_state_machine_storage_method_derive():

    test_state_machine = StateMachineStorage(
        state_storage=MockStateStorage(),
        processor_storage=MockProcessorStorage(),
        processor_state_storage=MockProcessorStateRouteStorage(),
        processor_provider_storage=MockProcessorProviderStorage(),
        template_storage=MockTemplateStorage()
    )

    test_state = State(
        config=StateConfig(
            name="hello world"
        )
    )

    saved_state = test_state_machine.save_state(state=test_state)
    assert saved_state.id == "test state id"

    with pytest.raises(NotImplementedError) as exc_info:
        test_state_machine.fetch_state(state_id=saved_state.id)

