import uuid
from typing import Optional, List

import pytest

from alethic_ism_core.core.base_model import ProcessorProvider, ProcessorStateDirection, ProcessorState, Processor, \
    InstructionTemplate
from alethic_ism_core.core.processor_state import State, StateConfig, StateDataKeyDefinition, StateConfigLM
from alethic_ism_core.core.processor_state_storage import StateStorage, \
    StateMachineStorage, ProcessorStateStorage, ProcessorProviderStorage, ProcessorStorage, TemplateStorage


class MockProcessorStateStorage(ProcessorStateStorage):

    def fetch_processor_state(self, processor_id: str = None, state_id: str = None,
                              direction: ProcessorStateDirection = None) \
            -> Optional[List[ProcessorState]]:

        if state_id:
            return [
                ProcessorState(
                    processor_id=processor_id,
                    state_id=state_id,
                    direction=direction if direction else ProcessorStateDirection.INPUT
                )
            ]
        else:
            if direction:
                return [
                    ProcessorState(
                        processor_id=processor_id,
                        state_id=f"test {direction} state id",
                        direction=direction
                )]
            else:
                return [
                    ProcessorState(
                        processor_id=processor_id,
                        state_id=str(uuid.uuid4()) if not state_id else state_id,
                        direction=direction if direction else ProcessorStateDirection.INPUT
                    ),
                    ProcessorState(
                        processor_id=processor_id,
                        state_id=str(uuid.uuid4()) if not state_id else state_id,
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
        processor_state_storage=MockProcessorStateStorage(),
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

