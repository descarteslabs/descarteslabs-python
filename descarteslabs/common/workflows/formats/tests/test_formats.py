import pytest

from descarteslabs.common.proto.formats import formats_pb2

from ..formats import mimetype_to_proto


def test_mimetype_to_proto():
    proto = mimetype_to_proto("application/vnd.pyarrow; compression=lz4")
    assert isinstance(proto, formats_pb2.Format)
    assert proto.has_pyarrow and not proto.has_geotiff
    assert proto.pyarrow.compression == formats_pb2.PyArrow.Compression.lz4


def test_mimetype_to_proto_with_sequence():
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
        ("application/vnd.pyarrow; foo=1", "Unsupported parameter foo for format"),
        ("image/tiff; overviews=13", "Parameter overviews must be castable"),
    ],
)
def test_mimetype_to_proto_invalid(mimetype, error_msg):
    with pytest.raises(ValueError, match=error_msg):
        mimetype_to_proto(mimetype)
