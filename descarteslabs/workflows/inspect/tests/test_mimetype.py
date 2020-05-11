import pytest

from collections import OrderedDict

from ..mimetype import format_to_mimetype


@pytest.mark.parametrize(
    "format, expected",
    [
        ("pyarrow", "application/vnd.pyarrow"),
        ("json", "application/json"),
        ("geotiff", "image/tiff"),
    ],
)
def test_format_to_mimetype_no_options(format, expected):
    mimetype = format_to_mimetype(format)
    assert mimetype == expected


def test_format_to_mimetype_with_options():
    mimetype = format_to_mimetype(
        "pyarrow", OrderedDict([("compression", "lz4"), ("other_param", 1)])
    )
    # TODO: Remove OrderedDict from this test once we drop support for py3.5
    assert mimetype == "application/vnd.pyarrow; compression=lz4; other_param=1"


def test_format_to_mimetype_with_not_options():
    mimetype = format_to_mimetype("geotiff", {"overviews": False})
    assert mimetype == "image/tiff; overviews=False"


def test_format_to_mimetype_invalid():
    with pytest.raises(ValueError, match="Output format for inspect"):
        format_to_mimetype("foo")

    with pytest.raises(AssertionError, match="Format options keys must be"):
        format_to_mimetype("pyarrow", {1: "lz4"})

    with pytest.raises(AssertionError, match="Format options values must be"):
        format_to_mimetype("pyarrow", {"compression": [1, 2, 3]})
