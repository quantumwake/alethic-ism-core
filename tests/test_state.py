import random
from alethic_ism_core.core.processor_state import StateConfigLM, State, StateDataColumnDefinition


def create_mock_state_json():
    state = create_mock_state()
    state_json = state.model_dump()
    return state_json, state


def create_mock_state() -> State:
    state = State(
        # state_type="StateConfigLM",
        config=StateConfigLM(
            name="Test Me",
            storage_class="database",
            user_template_id="/tmp/tmp_user_template",
            system_template_id="/tmp/tmp_system_template"
        )
    )

    for i in range(10):
        state.process_and_add_columns({
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
        # StateDataColumnDefinition(
        #     name="provider_name",
        #     value="callable:config.provider_name"
        # ),
        # StateDataColumnDefinition(
        #     name="model_name",
        #     value="callable:config.model_name"
        # ),
        StateDataColumnDefinition(
            name="name",
            value="callable:config.name"
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

    query_state0 = state.build_query_state_from_row_data(0)
    assert query_state0['name'] == state.config.name
    # assert query_state0['storage_class'] == state.config.storage_class
    # assert query_state0['user_template_id'] == state.config.user_template_id
    # assert query_state0['system_template_id'] == state.config.system_template_id


def test_state():
    state = create_mock_state()
    assert state != None

    assert 'state_key' in state.columns
    assert 'state_data' in state.columns

    assert 'Test Me' in state.config.name