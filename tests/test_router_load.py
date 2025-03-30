from ismcore.messaging.base_message_router import Router
from ismcore.messaging.nats_message_provider import NATSMessageProvider

message_provider = NATSMessageProvider()
router = Router(
    yaml_file="./test_routes/test_nats_route.yaml",
    provider=message_provider
)


def test_basic_find_route():
    test_route = router.find_route('test/test')
    assert test_route

def test_wildcard_find_route():
    test_route = router.find_route_wildcard('test/wildcard/hello_world')
    print(test_route)

def test_basic_grouping():
    subject_group = router.find_route_by_subject(subject="test.subject")
    assert subject_group.subject_group == "test.subject"
    print(subject_group.subject_group)
    assert subject_group

