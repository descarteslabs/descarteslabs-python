import pytest

from descarteslabs.common.proto.formats import formats_pb2
from descarteslabs.common.proto.destinations import destinations_pb2

from ..proto_to_user_dict import has_proto_to_user_dict
from ..user_dict_to_proto import user_dict_to_has_proto


@pytest.mark.parametrize(
    "user_dict, msg_type",
    [
        ({"type": "json"}, formats_pb2.Format),
        ({"type": "pyarrow", "compression": "lz4"}, formats_pb2.Format),
        (
            {
                "type": "geotiff",
                "overviews": False,
                "compression": "jpeg",
                "overview_resampler": "nearest",
            },
            formats_pb2.Format,
        ),
        ({"type": "download", "result_url": ""}, destinations_pb2.Destination),
        (
            {
                "type": "email",
                "subject": "This is a test",
                "body": "Testing",
                "result_url": "",
            },
            destinations_pb2.Destination,
        ),
    ],
)
def test_roundtrip(user_dict, msg_type):
    proto = user_dict_to_has_proto(user_dict, msg_type, {})
    assert has_proto_to_user_dict(proto) == user_dict
