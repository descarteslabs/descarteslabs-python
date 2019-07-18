import pytest

from ... import Image, ImageCollection
from .. import cos, log, log2, log10, normalized_difference, sin, sqrt, tan


@pytest.mark.parametrize("func", [log, log2, log10, sqrt, cos, sin, tan])
@pytest.mark.parametrize(
    "obj", [1, 1.2, Image.from_id("foo"), ImageCollection.from_id("bar")]
)
def test_arithmetic_functions(func, obj):

    result = func(obj)

    assert result is not obj


def test_normalized_difference():
    assert normalized_difference(2.0, 1.0) == 1.0 / 3
