import random
import pytest

from alethic_ism_core.core.processor_state import StateConfigLM, State

def create_mock_state_json():
    state = create_mock_state()
    state_json = state.model_dump()
    return state_json, state


def create_mock_state() -> State:
    state = State(
        config=StateConfigLM(
            name="Test Me",
            version="Test Version 1.0",
            model_name="Hello World Model",
            provider_name="Hello World Provider",
            output_path="./tmp/tmp_state_test.pickle",
            user_template_path="./tmp/tmp_user_template.json"
        )
    )

    for i in range(10):
        state.apply_columns({
            "State Key": "hello_world_state_key_a",
            "State Data": random.randbytes(64)
        })

    return state


def test_state_json_model():
    state_json, state = create_mock_state_json()
    deserialized_state = State(**state_json)

    assert isinstance(state.config, StateConfigLM)
    assert isinstance(deserialized_state.config, StateConfigLM)


def test_state():
    state = create_mock_state()
    assert state != None

    assert 'state_key' in state.columns
    assert 'state_data' in state.columns

    assert 'Test Me' in state.config.name
    assert 'Test Version 1.0' in state.config.version