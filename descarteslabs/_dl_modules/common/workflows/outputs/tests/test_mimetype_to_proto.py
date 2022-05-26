import pytest

from ....proto.formats import formats_pb2
from .. import mimetype_to_proto


def test_mimetype_to_proto():
    proto = mimetype_to_proto("application/vnd.pyarrow; compression=LZ4")
    assert isinstance(proto, formats_pb2.Format)
    assert proto.has_pyarrow and not proto.has_geotiff
    assert (
        proto.pyarrow.compression
        == formats_pb2.Pyarrow.PyarrowCompression.PYARROWCOMPRESSION_LZ4
    )


def test_mimetype_to_proto_more_options():
    proto = mimetype_to_proto("image/tiff; overviews=False; compression=JPEG")
    assert isinstance(proto, formats_pb2.Format)
    assert proto.has_geotiff and not proto.has_pyarrow
    assert proto.geotiff.not_overviews
    assert (
        proto.geotiff.compression
        == formats_pb2.Geotiff.GeotiffCompression.GEOTIFFCOMPRESSION_JPEG
    )


@pytest.mark.parametrize(
    "mimetype, error_msg",
    [
        ("application/does-not-exist", "Unknown MIME type"),
        ("application/vnd.pyarrow; compression=LZ4;", "Invalid MIME type"),
        ("application/vnd.pyarrow; foo=1", "Unknown field 'foo' for Pyarrow"),
        ("image/tiff; overviews=13", "must be castable to bool"),
    ],
)
def test_mimetype_to_proto_invalid(mimetype, error_msg):
    with pytest.raises(ValueError, match=error_msg):
        mimetype_to_proto(mimetype)
