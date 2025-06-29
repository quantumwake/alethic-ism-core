from tests.test_state import create_mock_state


def test_state_cache_1():
    state = create_mock_state()
    # Get hash of the state by dumping to JSON and hashing
    import hashlib
    x = hashlib.sha256(state.model_dump_json().encode()).hexdigest()
    assert x is not None
    
    state.config.name = "Hello World"
    y = hashlib.sha256(state.model_dump_json().encode()).hexdigest()
    assert y is not None
    assert x != y

