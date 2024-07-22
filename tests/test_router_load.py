from alethic_ism_core.core.messaging.base_message_router import Router
from alethic_ism_core.core.messaging.nats_message_provider import NATSMessageProvider

message_provider = NATSMessageProvider()
router = Router(
    yaml_file="./test_routes/test_nats_route.yaml",
    provider=message_provider
)

def test_basic_find_route():
    test_route = router.find_route('test/test')
    assert test_route

def test_basic_grouping():
    test_group = router.create_route_group_by_subject(subject="test.group")
    assert test_group.channel_name() == "test.group"
    print(test_group.channel_name())
    assert test_group
