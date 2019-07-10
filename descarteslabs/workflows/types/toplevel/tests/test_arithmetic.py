import pytest

from .. import log, log2, log10, sqrt, cos, sin, tan
from ... import Image, ImageCollection


@pytest.mark.parametrize("func", [log, log2, log10, sqrt, cos, sin, tan])
@pytest.mark.parametrize(
    "obj", [1, 1.2, Image.from_id("foo"), ImageCollection.from_id("bar")]
)
def test_arithmetic_functions(func, obj):

    result = func(obj)

    assert result is not obj
