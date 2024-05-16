import pytest

from alethic_ism_core.core.processor_state import State, StateConfig
from alethic_ism_core.core.processor_state_storage import StateStorage, \
    StateMachineStorage


class TestStateStorageProvider(StateStorage):
    def save_state(self, state: State) -> State:
        state.id = "test state id"
        return state


def test_state_machine_storage_method_derive():

    test_state_machine = StateMachineStorage(
        state_storage=TestStateStorageProvider()
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

