from alethic_ism_core.core.base_model import ProcessorState

def test_processor_state_internal_id_binding():
    ps = ProcessorState(**{
        "internal_id": 12345,
        "id": "hello-world",
        "processor_id": "myproc-id",
        "state_id": "mystate-id",
        "direction": "INPUT",
        "status": "CREATED"})

    assert ps.internal_id == 12345

    ps_json = ps.json()
    assert 'internal_id' not in ps_json
