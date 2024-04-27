import random
import json
from alethic_ism_core.core.base_data_router import Router, MessageStatus
from alethic_ism_core.core.processor_state import StateConfigLM, State, StateDataColumnDefinition
from tests.test_state import create_mock_state


def test_route_1():
    router = Router(yaml_file="./test_routes/test_route.yaml")
    route = router.find_router(selector="language/models/openai/gpt-4-1106-preview")
    status = route.send_message(msg=json.dumps({
        "name": "hello",
        "hello": "world",
        "world": "goodbye"
    }))
    assert route is not None
    assert status.status == MessageStatus.QUEUED




