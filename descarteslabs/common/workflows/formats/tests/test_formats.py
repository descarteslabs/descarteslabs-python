import pytest

from descarteslabs.common.proto.formats import formats_pb2

from ..formats import (
    mimetype_to_proto,
    user_format_to_proto,
    format_proto_to_user_facing_format,
)


def test_mimetype_to_proto():
    proto = mimetype_to_proto("application/vnd.pyarrow; compression=lz4")
    assert isinstance(proto, formats_pb2.Format)
    assert proto.has_pyarrow and not proto.has_geotiff
    assert proto.pyarrow.compression == formats_pb2.PyArrow.Compression.lz4


def test_mimetype_to_proto_more_options():
    proto = mimetype_to_proto(
        "image/tiff; overviews=False; tiled=False; compression=JPEG"
    )
    assert isinstance(proto, formats_pb2.Format)
    assert proto.has_geotiff and not proto.has_pyarrow
    assert proto.geotiff.not_overviews
    assert proto.geotiff.not_tiled
    assert proto.geotiff.compression == formats_pb2.GeoTIFF.Compression.JPEG


@pytest.mark.parametrize(
    "mimetype, error_msg",
    [
        ("application/does-not-exist", "Unknown MIME type"),
        ("application/vnd.pyarrow; compression=lz4;", "Invalid MIME type"),
        ("application/vnd.pyarrow; foo=1", "Unsupported parameter 'foo' for format"),
        ("image/tiff; overviews=13", "Parameter 'overviews' must be castable"),
    ],
)
def test_mimetype_to_proto_invalid(mimetype, error_msg):
    with pytest.raises(ValueError, match=error_msg):
        mimetype_to_proto(mimetype)


def test_user_format_to_proto():
    proto = user_format_to_proto({"type": "pyarrow", "compression": "lz4"})
    assert isinstance(proto, formats_pb2.Format)
    assert proto.has_pyarrow and not proto.has_geotiff
    assert proto.pyarrow.compression == formats_pb2.PyArrow.Compression.lz4


def test_user_format_to_proto_more_options():
    proto = user_format_to_proto(
        {"type": "geotiff", "overviews": False, "tiled": False, "compression": "JPEG"}
    )
    assert isinstance(proto, formats_pb2.Format)
    assert proto.has_geotiff and not proto.has_pyarrow
    assert proto.geotiff.not_overviews
    assert proto.geotiff.not_tiled
    assert proto.geotiff.compression == formats_pb2.GeoTIFF.Compression.JPEG


@pytest.mark.parametrize(
    "format_, error_msg",
    [
        ({}, "The format dictionary must include a serialization type"),
        ({"type": "does-not-exist"}, "Unknown format"),
        ({"type": "pyarrow", "foo": 1}, "Unsupported parameter 'foo' for format"),
        (
            {"type": "geotiff", "overviews": 13},
            "Parameter 'overviews' must be castable",
        ),
    ],
)
def test_user_format_to_proto_invalid(format_, error_msg):
    with pytest.raises(ValueError, match=error_msg):
        user_format_to_proto(format_)


@pytest.mark.parametrize(
    "format_",
    [
        {"type": "json"},
        {"type": "pyarrow", "compression": "lz4"},
        {"type": "geotiff", "overviews": False, "tiled": False, "compression": "JPEG"},
    ],
)
def test_format_proto_to_user_facing_format(format_):
    proto = user_format_to_proto(format_)
    assert format_proto_to_user_facing_format(proto) == format_
