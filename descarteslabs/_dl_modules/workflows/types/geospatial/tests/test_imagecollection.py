import pytest

from ...core import _resolve_lambdas
from ...core.tests import utils

from ...containers import Dict, List, Slice, Tuple
from ...primitives import Float, Int, Str, Bool, NoneType, Any
from ...identifier import parameter

from .. import (
    Image,
    ImageCollection,
    ImageCollectionGroupby,
    Geometry,
    Feature,
    FeatureCollection,
)


def thaw_axis(frozen_axis):
    axis = tuple(frozen_axis)
    if len(axis) == 1:
        axis = axis[0]
    return axis


def test_init():
    x = parameter("x", Image)
    ic = ImageCollection([Image.from_id("foo"), x])
    assert ic.params == (x,)

    with pytest.raises(TypeError):
        ImageCollection("", "")


def test_from_id():
    assert isinstance(ImageCollection.from_id("foo"), ImageCollection)
    assert isinstance(
        ImageCollection.from_id(
            "foo",
            start_datetime="2020-01-01",
            end_datetime="2021-01-01",
            limit=1000,
            resampler="near",
            processing_level="toa",
        ),
        ImageCollection,
    )

    with pytest.raises(ValueError, match="Unknown resampler type: 'foo'"):
        ImageCollection.from_id("foo", resampler="foo")


def test_stats_return_type():
    for axis, return_type in _resolve_lambdas(
        ImageCollection._STATS_RETURN_TYPES
    ).items():
        assert ImageCollection._stats_return_type(thaw_axis(axis)) == return_type

    with pytest.raises(ValueError):
        Image._stats_return_type(5)

    with pytest.raises(ValueError):
        Image._stats_return_type("foo")


def test_all_methods_nonstats():
    col = ImageCollection.from_id("foo")
    col2 = ImageCollection.from_id("bar")
    img = Image.from_id("baz")
    geom = Geometry(type="point", coordinates=[1, 2])
    feature = Feature(geometry=geom, properties={})
    fc = FeatureCollection([feature, feature])

    assert isinstance(col.nbands, Int)
    assert isinstance(col.with_bandinfo("red", foo="bar", baz="qux"), ImageCollection)
    assert isinstance(col.without_bandinfo("red", "foo", "baz"), ImageCollection)
    assert isinstance(col.rename_bands("foo", "bar"), ImageCollection)
    assert isinstance(col.rename_bands(baz="quiz"), ImageCollection)
    assert isinstance(col.rename_bands("foo", "bar"), ImageCollection)
    assert isinstance(col.rename_bands(red="green", blue="yellow"), ImageCollection)
    assert isinstance(col.clip_values(0.1, 0.5), ImageCollection)
    assert isinstance(col.clip_values([0.1, 0.4], [0.5, 0.8]), ImageCollection)
    assert isinstance(col.scale_values(0.1, 0.5), ImageCollection)
    assert isinstance(
        col.replace_empty_with(0.1, bandinfo={"red": {}}), ImageCollection
    )
    assert isinstance(
        col.replace_empty_with(0.1, bandinfo=col.bandinfo), ImageCollection
    )
    assert isinstance(col.replace_empty_with(col), ImageCollection)
    assert isinstance(col.value_at(0.1, 0.1), List[Dict[Str, Float]])
    assert isinstance(col.index_to_coords(0, 0), Tuple[Float, Float])
    assert isinstance(col.coords_to_index(0.0, 0.0), Tuple[Int, Int])
    assert isinstance(col.mosaic(), Image)
    assert isinstance(col.reduction("mosaic", axis="images"), Image)
    assert isinstance(col.concat(img), ImageCollection)
    assert isinstance(col.mask(img), ImageCollection)
    assert isinstance(col.mask(img, replace=True), ImageCollection)
    assert isinstance(col.mask(col2), ImageCollection)
    assert isinstance(col.mask(col2, replace=True), ImageCollection)
    assert isinstance(col.mask(geom), ImageCollection)
    assert isinstance(col.mask(feature), ImageCollection)
    assert isinstance(col.mask(fc), ImageCollection)
    assert isinstance(col.getmask(), ImageCollection)
    assert isinstance(col.colormap(), ImageCollection)
    assert isinstance(col.groupby(dates="year"), ImageCollectionGroupby)
    assert isinstance(col.head(0), ImageCollection)
    assert isinstance(col.tail(0), ImageCollection)
    assert isinstance(col.partition(0), Tuple[ImageCollection, ImageCollection])
    assert isinstance(
        col.map_window(
            lambda back, img, fwd: back.min(axis="images")
            + img
            + fwd.min(axis="images"),
            back=1,
            fwd=1,
        ),
        ImageCollection,
    )
    assert isinstance(
        col.map_window(lambda back, img, fwd: back.concat(fwd), back=1, fwd=1),
        ImageCollection,
    )
    assert isinstance(
        col.map_window(lambda back, img, fwd: img.properties["id"], back=1, fwd=1),
        List[Str],
    )


@pytest.mark.parametrize(
    "stats_func_name", ["min", "max", "mean", "median", "sum", "std", "count"]
)
def test_all_stats_methods_and_reduction(stats_func_name):
    col = ImageCollection.from_id("foo")
    stats_func = getattr(col, stats_func_name)

    # NOTE: This assumes the correct construction of
    #       `ImageCollection._RESOLVED_STATS_RETURN_TYPES`.
    for axis, return_type in ImageCollection._RESOLVED_STATS_RETURN_TYPES.items():
        assert isinstance(stats_func(axis=thaw_axis(axis)), return_type)
        assert isinstance(
            col.reduction(stats_func_name, axis=thaw_axis(axis)), return_type
        )


def test_properties():
    col = ImageCollection.from_id("foo")

    assert isinstance(col.properties, List)
    assert isinstance(col.bandinfo, Dict)


all_values_to_try = [
    ImageCollection([]),
    Image.from_id("foo"),
    Int(0),
    Float(1.1),
    Bool(True),
    NoneType(None),
    Any(0),
]

base_types = [ImageCollection, Image, Bool, Int, Any]


@pytest.mark.parametrize(
    "operator, accepted_types, return_type",
    [
        ["log", (), ImageCollection],
        ["log2", (), ImageCollection],
        ["log10", (), ImageCollection],
        ["log1p", (), ImageCollection],
        ["sqrt", (), ImageCollection],
        ["cos", (), ImageCollection],
        ["arccos", (), ImageCollection],
        ["sin", (), ImageCollection],
        ["arcsin", (), ImageCollection],
        ["tan", (), ImageCollection],
        ["arctan", (), ImageCollection],
        ["exp", (), ImageCollection],
        ["square", (), ImageCollection],
        ["__reversed__", (), ImageCollection],
        ["__getitem__", [Any, Bool, Int, Slice], (Image, ImageCollection)],
        ["__lt__", base_types + [Float], ImageCollection],
        ["__le__", base_types + [Float], ImageCollection],
        ["__eq__", base_types + [Float], ImageCollection],
        ["__ne__", base_types + [Float], ImageCollection],
        ["__gt__", base_types + [Float], ImageCollection],
        ["__ge__", base_types + [Float], ImageCollection],
        ["__invert__", (), ImageCollection],
        ["__and__", base_types, ImageCollection],
        ["__or__", base_types, ImageCollection],
        ["__xor__", base_types, ImageCollection],
        ["__lshift__", base_types, ImageCollection],
        ["__rshift__", base_types, ImageCollection],
        ["__rand__", base_types, ImageCollection],
        ["__ror__", base_types, ImageCollection],
        ["__rxor__", base_types, ImageCollection],
        ["__rlshift__", base_types, ImageCollection],
        ["__rrshift__", base_types, ImageCollection],
        ["__neg__", (), ImageCollection],
        ["__pos__", (), ImageCollection],
        ["__abs__", (), ImageCollection],
        ["__add__", base_types + [Float], ImageCollection],
        ["__sub__", base_types + [Float], ImageCollection],
        ["__mul__", base_types + [Float], ImageCollection],
        ["__div__", base_types + [Float], ImageCollection],
        ["__truediv__", base_types + [Float], ImageCollection],
        ["__floordiv__", base_types + [Float], ImageCollection],
        ["__mod__", base_types + [Float], ImageCollection],
        ["__pow__", base_types + [Float], ImageCollection],
        ["__radd__", base_types + [Float], ImageCollection],
        ["__rsub__", base_types + [Float], ImageCollection],
        ["__rmul__", base_types + [Float], ImageCollection],
        ["__rdiv__", base_types + [Float], ImageCollection],
        ["__rtruediv__", base_types + [Float], ImageCollection],
        ["__rfloordiv__", base_types + [Float], ImageCollection],
        ["__rmod__", base_types + [Float], ImageCollection],
        ["__rpow__", base_types + [Float], ImageCollection],
    ],
)
def test_all_operators(operator, accepted_types, return_type):
    utils.operator_test(
        ImageCollection([]), all_values_to_try, operator, accepted_types, return_type
    )
