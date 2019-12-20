from ... import Image, ImageCollection
from .. import concat


def test_concat():
    img = Image.from_id("foo")
    col = ImageCollection.from_id("bar")
    assert isinstance(concat(img, img), ImageCollection)
    assert isinstance(concat(col, img), ImageCollection)
    assert isinstance(concat(img, col), ImageCollection)
    assert isinstance(concat(col, col), ImageCollection)
