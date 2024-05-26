import random
from alethic_ism_core.core.processor_state import StateConfigLM, State, StateDataColumnDefinition, StateConfig, \
    StateDataKeyDefinition


def create_mock_state_json():
    state = create_mock_state()
    state_json = state.model_dump()
    return state_json, state


def create_mock_state(state_id: str = None) -> State:
    state = State(
        id=state_id,
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

def test_state_has_query_state():
    state = State(
        config=StateConfig(
            name="Test State Hash Query State Key",
            primary_key=[
                StateDataKeyDefinition(name="question"),
                StateDataKeyDefinition(name="animal")
            ],
            template_columns=[
                StateDataKeyDefinition(name="question")
            ]
        )
    )

    query_state = state.apply_query_state(query_state={
        "question": "what do you think about {animal}s?",
        "animal": "cat"
    })

    query_state = state.apply_query_state(query_state={
        "question": "what do you think about {animal}s?",
        "animal": "dog"
    })

    query_state_0 = state.build_query_state_from_row_data(0)
    query_state_1 = state.build_query_state_from_row_data(1)

    assert query_state_0["question"] == "what do you think about cats?"
    assert query_state_1["question"] == "what do you think about dogs?"

    query_state = state.apply_query_state(query_state={
        "question": "what do you think about {animal}s?",
        "animal": "dog"
    })

    assert state.count == 2

    query_state = state.apply_query_state(query_state={
        "question": "what do you think about {animal}s?",
        "animal": "pig"
    })

    assert state.count == 3


def test_state_json_model():
    state_json, state = create_mock_state_json()
    deserialized_state = State(**state_json)

    assert isinstance(state.config, StateConfigLM)
    assert isinstance(deserialized_state.config, StateConfigLM)


def test_state_primary_key_constant_and_dyanmic():
    state = create_mock_state(state_id="d16d45cc-a6e2-4b2e-a989-ebe6ffe9a434")
    state.config.primary_key = [
        StateDataKeyDefinition(name="provider"),
        StateDataKeyDefinition(name="question")
    ]

    # define some static columns
    state.columns = {
        "test_constant_column": StateDataColumnDefinition(
            name="test_constant_column",
            value="my test constant column value"
        ),
        "provider": StateDataColumnDefinition(
            name="provider",
            value="my test constant column provider name"
        ),
        "answer_length": StateDataColumnDefinition(
            name="answer_length",
            value="len(query_state['answer'])",
            callable=True
        ),
    }

    # dynamic state to apply, should create two columns in addition to the above columns
    to_apply_query_state = {
        "question": "what is the name of hello world?",
        "answer": "the name of hello world is worldly hellos."
    }

    updated_query_state = state.apply_query_state(query_state=to_apply_query_state)
    query_state_entry = state.build_query_state_from_row_data(0)

    assert "my test constant column provider name" == state.get_column_data_from_row_index("provider", index=0)
    assert 42 == state.get_column_data_from_row_index("answer_length", index=0)
    assert updated_query_state['answer_length'] == 42
    assert query_state_entry['state_key'] is not None



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
            value="config.name",
            callable=True
        )]
    )

    #
    for i in range(1, 5):
        state.process_and_add_row_data(
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