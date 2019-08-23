import itertools
import pytest

from ... import Image, ImageCollection
from .. import where


@pytest.mark.parametrize("a", [Image.from_id("foo"), ImageCollection.from_id("bar")])
@pytest.mark.parametrize("b", [Image.from_id("bar"), ImageCollection.from_id("foo")])
def test_where_imagecollection(a, b):
    col = ImageCollection.from_id("baz")
    args = [col, a, b]
    for args_ in itertools.permutations(args):
        assert isinstance(where(*args_), ImageCollection)

    for args_ in itertools.permutations([1, 1.2]):
        assert isinstance(where(col, *args_), ImageCollection)

    assert isinstance(where(True, col, a), ImageCollection)
    assert isinstance(where(False, a, col), ImageCollection)


@pytest.mark.parametrize("x", [1, 1.2, Image.from_id("foo")])
@pytest.mark.parametrize("y", [1, 1.2, Image.from_id("bar")])
def test_where_image(x, y):
    im = Image.from_id("baz")
    assert isinstance(where(im, x, y), Image)
    assert isinstance(where(True, im, y), Image)
    assert isinstance(where(True, x, im), Image)


# TODO(Clark): Support these cases.
# def test_where_float():
#     assert isinstance(where(True, 1, 1.3), Float)
#     assert isinstance(where(True, 1.3, 1), Float)


# def test_where_int():
#     assert isinstance(where(True, 1, 2), Int)
