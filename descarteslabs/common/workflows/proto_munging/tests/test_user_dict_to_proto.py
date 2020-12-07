import re

import pytest

from descarteslabs.common.proto.formats import formats_pb2
from descarteslabs.common.proto.destinations import destinations_pb2
from descarteslabs.common.proto.typespec import typespec_pb2
from ..user_dict_to_proto import user_dict_to_has_proto, user_dict_to_proto


def test_user_dict_to_has_proto():
    proto = user_dict_to_has_proto(
        {"type": "pyarrow", "compression": "lz4"}, formats_pb2.Format, {}
    )
    assert isinstance(proto, formats_pb2.Format)
    assert proto.has_pyarrow and not proto.has_geotiff
    assert (
        proto.pyarrow.compression
        == formats_pb2.Pyarrow.PyarrowCompression.PYARROWCOMPRESSION_LZ4
    )

    proto = user_dict_to_has_proto(
        {"type": "email", "subject": 1234}, destinations_pb2.Destination, {}
    )
    assert isinstance(proto, destinations_pb2.Destination)
    assert proto.has_email and not proto.has_download
    assert proto.email.subject == "1234"
    assert proto.email.body == ""


def test_user_dict_to_has_proto_defaults():
    defaults = {"body": "job is done"}
    proto = user_dict_to_has_proto(
        {"type": "email"},
        destinations_pb2.Destination,
        {destinations_pb2.Email: defaults},
    )
    assert isinstance(proto, destinations_pb2.Destination)
    assert proto.has_email and not proto.has_download
    assert proto.email.subject == ""
    assert proto.email.body == defaults["body"]


def test_user_dict_to_has_proto_more_options():
    proto = user_dict_to_has_proto(
        {"type": "geotiff", "overviews": False, "compression": "JPEG"},
        formats_pb2.Format,
        {},
    )
    assert isinstance(proto, formats_pb2.Format)
    assert proto.has_geotiff and not proto.has_pyarrow
    assert proto.geotiff.not_overviews
    assert (
        proto.geotiff.compression
        == formats_pb2.Geotiff.GeotiffCompression.GEOTIFFCOMPRESSION_JPEG
    )


@pytest.mark.parametrize(
    "format_, error_msg",
    [
        ({"type": "does-not-exist"}, "Unknown Format"),
        ({"type": "pyarrow", "foo": 1}, "Unknown field 'foo' for Pyarrow"),
        (
            {"type": "geotiff", "overviews": 13},
            re.escape("Parameter 'overviews' (13) must be castable to bool"),
        ),
        (
            {"type": "geotiff", "compression": "foo"},
            re.escape("Must be one of: ['lzw', 'none', 'jpeg']"),
        ),
    ],
)
def test_user_dict_to_has_proto_invalid(format_, error_msg):
    with pytest.raises(ValueError, match=error_msg):
        user_dict_to_has_proto(format_, formats_pb2.Format, {})


def test_user_dict_to_proto_recursive():
    proto = user_dict_to_proto(
        {
            "geotiff": {"compression": "none"},
            "pyarrow": {"compression": "brotli"},
            "json": {},
            "has_geotiff": True,
            "has_pyarrow": True,
            "has_json": True,
        },
        formats_pb2.Format(),
    )

    assert isinstance(proto, formats_pb2.Format)
    assert (
        proto.has_geotiff
        and proto.has_pyarrow
        and proto.has_json
        and not proto.has_geojson
        and not proto.has_csv
        and not proto.has_png
        and not proto.has_msgpack
    )
    assert (
        proto.geotiff.compression
        == proto.geotiff.GeotiffCompression.GEOTIFFCOMPRESSION_NONE
    )

    assert (
        proto.pyarrow.compression
        == proto.pyarrow.PyarrowCompression.PYARROWCOMPRESSION_BROTLI
    )


def test_user_dict_to_proto_recursive_repeated():
    proto = user_dict_to_proto(
        {
            "type": "foo",
            "params": [
                {"type": "bar"},
                {"primitive": {"int_": 9}},
            ],
        },
        typespec_pb2.CompositeType(),
    )
    assert isinstance(proto, typespec_pb2.CompositeType)
    assert proto == typespec_pb2.CompositeType(
        type="foo",
        params=[
            typespec_pb2.Typespec(type="bar"),
            typespec_pb2.Typespec(primitive=typespec_pb2.Primitive(int_=9)),
        ],
    )
