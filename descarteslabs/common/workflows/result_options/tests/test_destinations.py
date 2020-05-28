import pytest

from descarteslabs.common.proto.destinations import destinations_pb2

from ..destinations import (
    user_destination_to_proto,
    destination_proto_to_user_facing_destination,
)


def test_user_destination_to_proto():
    proto = user_destination_to_proto({"type": "email", "to": "test@email.com"})
    assert isinstance(proto, destinations_pb2.Destination)
    assert proto.has_email and not proto.has_download
    assert proto.email.to == ["test@email.com"]


@pytest.mark.parametrize(
    "destination_, error_msg",
    [
        ({}, "The destination dictionary must include a destination type"),
        ({"type": "does-not-exist"}, "Unknown destination"),
        ({"type": "email", "foo": 1}, "Unsupported parameter 'foo' for destination"),
    ],
)
def test_user_destination_to_proto_invalid(destination_, error_msg):
    with pytest.raises(ValueError, match=error_msg):
        user_destination_to_proto(destination_)


@pytest.mark.parametrize(
    "destination",
    [
        {"type": "download"},
        {
            "type": "email",
            "to": ["test@email.com", "other@email.com"],
            "cc": ["someone@email.com", "other_other@email.com"],
            "bcc": ["someone_else@email.com", "someone_else_else@email.com"],
            "subject": "This is a test",
            "body": "Testing",
        },
    ],
)
def test_destination_proto_to_user_facing_destination(destination):
    proto = user_destination_to_proto(destination)
    assert destination_proto_to_user_facing_destination(proto) == destination
