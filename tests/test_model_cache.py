from tests.test_state import create_mock_state


def test_state_cache_1():
    state = create_mock_state()
    # state_json = state.model_dump()
    x = state.hash()
    assert x is not None
    state.config.name = "Hello World"
    y = state.hash()
    assert y is not None
    assert x != y

