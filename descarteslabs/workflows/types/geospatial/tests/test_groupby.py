from ...containers import Dict, Tuple
from ...primitives import Int, Float, Str
from .. import ImageCollection, ImageCollectionGroupby


def test_all_methods():
    col = ImageCollection.from_id("foo")
    groupby = ImageCollectionGroupby[Int](col, lambda img: img.properties["date"].year)
    assert isinstance(groupby, ImageCollectionGroupby[Int])
    assert isinstance(groupby.groups, Dict[groupby.key_type, ImageCollection])
    assert isinstance(groupby.one(), Tuple[groupby.key_type, ImageCollection])
    assert isinstance(groupby[2017], ImageCollection)
    assert isinstance(groupby.map(lambda group, imgs: imgs.log()), ImageCollection)
    assert isinstance(
        groupby.map(lambda group, imgs: imgs.sum(axis=("images", "bands"))),
        ImageCollection,
    )
    assert isinstance(
        groupby.map(lambda group, imgs: imgs.sum(axis=("images", "pixels"))),
        Dict[Int, Dict[Str, Float]],
    )
    assert isinstance(groupby.count(axis="bands"), ImageCollection)
    assert isinstance(groupby.sum(axis="bands"), ImageCollection)
    assert isinstance(groupby.min(axis="bands"), ImageCollection)
    assert isinstance(groupby.max(axis="bands"), ImageCollection)
    assert isinstance(groupby.mean(axis="bands"), ImageCollection)
    assert isinstance(groupby.median(axis="bands"), ImageCollection)
    assert isinstance(groupby.std(axis="bands"), ImageCollection)
