import random
import pytest

from alethic_ism_core.core.processor_state import StateConfigLM, State, StateDataColumnDefinition


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


def test_state_callable_columns():
    state = create_mock_state()

    state.add_column(column=StateDataColumnDefinition(
        name="perspective_index",
        value="P1"
    ))
    state.add_columns(columns=[
        StateDataColumnDefinition(
            name="provider_name",
            value="callable:config.provider_name"
        ),
        StateDataColumnDefinition(
            name="model_name",
            value="callable:config.model_name"
        ),
        StateDataColumnDefinition(
            name="version",
            value="callable:config.version"
        )]
    )

    #
    for i in range(1, 5):
        state.apply_row_data(
            {
                "state_key": f"hello world #{i}",
                "state data": f"testing {i}"
            }
        )

    query_state0 = state.get_query_state_from_row_index(0)
    assert query_state0['provider_name'] == state.config.provider_name
    assert query_state0['model_name'] == state.config.model_name
    assert query_state0['version'] == state.config.version


def test_state():
    state = create_mock_state()
    assert state != None

    assert 'state_key' in state.columns
    assert 'state_data' in state.columns

    assert 'Test Me' in state.config.name
    assert 'Test Version 1.0' in state.config.version