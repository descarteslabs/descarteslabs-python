from ...containers import Dict, Tuple
from ...primitives import Int, Float, Str
from ...identifier import parameter
from .. import ImageCollection, ImageCollectionGroupby


def test_all_methods():
    x = parameter("x", Int)
    y = parameter("y", Int)
    col = ImageCollection.from_id("foo")

    groupby1 = ImageCollectionGroupby[Int](
        col, lambda img: img.properties["date"].year + x
    )
    assert isinstance(groupby1, ImageCollectionGroupby[Int])
    assert groupby1.params == (x,)

    groupby2 = ImageCollectionGroupby[Tuple[Str, Int]](
        col + x, lambda img: (img.properties["id"], img.properties["date"].year + x + y)
    )
    assert isinstance(groupby2, ImageCollectionGroupby[Tuple[Str, Int]])
    assert groupby2.params == (x, y)

    assert isinstance(groupby1.groups, Dict[groupby1.key_type, ImageCollection])
    assert isinstance(groupby1.one(), Tuple[groupby1.key_type, ImageCollection])
    assert isinstance(groupby1[2017], ImageCollection)
    assert isinstance(groupby1.map(lambda group, imgs: imgs.log()), ImageCollection)
    assert isinstance(
        groupby1.map(lambda group, imgs: imgs.sum(axis=("images", "bands"))),
        ImageCollection,
    )
    assert isinstance(
        groupby1.map(lambda group, imgs: imgs.sum(axis=("images", "pixels"))),
        Dict[Int, Dict[Str, Float]],
    )
    assert isinstance(groupby1.count(axis="bands"), ImageCollection)
    assert isinstance(groupby1.sum(axis="bands"), ImageCollection)
    assert isinstance(groupby1.min(axis="bands"), ImageCollection)
    assert isinstance(groupby1.max(axis="bands"), ImageCollection)
    assert isinstance(groupby1.mean(axis="bands"), ImageCollection)
    assert isinstance(groupby1.median(axis="bands"), ImageCollection)
    assert isinstance(groupby1.std(axis="bands"), ImageCollection)
    assert isinstance(groupby1.mosaic(), ImageCollection)
