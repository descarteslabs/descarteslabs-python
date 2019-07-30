import pytest
import mock

from ...core.tests import utils

from .... import env
from ...primitives import Float, Int, Bool, NoneType, Any
from ...containers import List, Dict

from .. import Image, ImageCollection, Geometry, Feature, FeatureCollection


def test_init():
    ImageCollection([Image.from_id("foo")])

    with pytest.raises(TypeError):
        ImageCollection("", "")


@mock.patch.object(env, "geoctx")
@mock.patch.object(env, "_token")
def test_from_id(mock_geoctx, mock_token):
    mock_geoctx_graft = mock.PropertyMock(return_value={"returns": "geoctx"})
    mock_token_graft = mock.PropertyMock(return_value={"returns": "_token_"})
    type(mock_geoctx).graft = mock_geoctx_graft
    type(mock_token).graft = mock_token_graft
    # "Because of the way mock attributes are stored you can't directly attach a PropertyMock to a mock object.
    # Instead you can attach it to the mock type object."
    ImageCollection.from_id("foo")
    mock_geoctx_graft.assert_called()
    mock_token_graft.assert_called()


@mock.patch.object(env, "geoctx")
@mock.patch.object(env, "_token")
def test_from_id_with_datetime(mock_geoctx, mock_token):
    mock_geoctx_graft = mock.PropertyMock(return_value={"returns": "geoctx"})
    mock_token_graft = mock.PropertyMock(return_value={"returns": "_token_"})
    type(mock_geoctx).graft = mock_geoctx_graft
    type(mock_token).graft = mock_token_graft
    # "Because of the way mock attributes are stored you can't directly attach a PropertyMock to a mock object.
    # Instead you can attach it to the mock type object."
    ImageCollection.from_id(
        "foo",
        start_datetime="2018-05-17T00:00:00+00:00",
        end_datetime="2019-05-17T00:00:00+00:00",
    )
    mock_geoctx_graft.assert_called()
    mock_token_graft.assert_called()


@mock.patch.object(env, "geoctx")
@mock.patch.object(env, "_token")
def test_from_id_with_limit(mock_geoctx, mock_token):
    mock_geoctx_graft = mock.PropertyMock(return_value={"returns": "geoctx"})
    mock_token_graft = mock.PropertyMock(return_value={"returns": "_token_"})
    type(mock_geoctx).graft = mock_geoctx_graft
    type(mock_token).graft = mock_token_graft
    # "Because of the way mock attributes are stored you can't directly attach a PropertyMock to a mock object.
    # Instead you can attach it to the mock type object."
    ImageCollection.from_id("foo", limit=10)
    mock_geoctx_graft.assert_called()
    mock_token_graft.assert_called()


def test_all_methods():
    col = ImageCollection.from_id("foo")
    col2 = ImageCollection.from_id("bar")
    img = Image.from_id("baz")
    geom = Geometry(type="point", coordinates=[1, 2])
    feature = Feature(geometry=geom, properties={})
    fc = FeatureCollection([feature, feature])

    assert isinstance(col.rename_bands("foo", "bar"), ImageCollection)
    assert isinstance(col.rename_bands(baz="quiz"), ImageCollection)
    assert isinstance(col.rename_bands("foo", "bar"), ImageCollection)
    assert isinstance(col.rename_bands(red="green", blue="yellow"), ImageCollection)
    assert isinstance(col.clip_values(0.1, 0.5), ImageCollection)
    assert isinstance(col.clip_values([0.1, 0.4], [0.5, 0.8]), ImageCollection)
    assert isinstance(col.scale_values(0.1, 0.5), ImageCollection)
    assert isinstance(col.mask(img), ImageCollection)
    assert isinstance(col.mask(img, replace=True), ImageCollection)
    assert isinstance(col.mask(col2), ImageCollection)
    assert isinstance(col.mask(col2, replace=True), ImageCollection)
    assert isinstance(col.mask(geom), ImageCollection)
    assert isinstance(col.mask(feature), ImageCollection)
    assert isinstance(col.mask(fc), ImageCollection)
    assert isinstance(col.getmask(), ImageCollection)
    assert isinstance(col.colormap(), ImageCollection)
    assert isinstance(col.min(), Image)
    assert isinstance(col.max(), Image)
    assert isinstance(col.mean(), Image)
    assert isinstance(col.median(), Image)
    assert isinstance(col.sum(), Image)
    assert isinstance(col.count(), Image)


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

base_types = [ImageCollection, Image, Int, Any]


@pytest.mark.parametrize(
    "operator, accepted_types, return_type",
    [
        ["log", (), ImageCollection],
        ["log2", (), ImageCollection],
        ["log10", (), ImageCollection],
        ["sqrt", (), ImageCollection],
        ["cos", (), ImageCollection],
        ["sin", (), ImageCollection],
        ["tan", (), ImageCollection],
        ["__lt__", base_types + [Float], ImageCollection],
        ["__le__", base_types + [Float], ImageCollection],
        ["__eq__", base_types + [Float, Bool], ImageCollection],
        ["__ne__", base_types + [Float, Bool], ImageCollection],
        ["__gt__", base_types + [Float], ImageCollection],
        ["__ge__", base_types + [Float], ImageCollection],
        ["__invert__", (), ImageCollection],
        ["__and__", base_types + [Bool], ImageCollection],
        ["__or__", base_types + [Bool], ImageCollection],
        ["__xor__", base_types + [Bool], ImageCollection],
        ["__lshift__", base_types, ImageCollection],
        ["__rshift__", base_types, ImageCollection],
        ["__rand__", base_types + [Bool], ImageCollection],
        ["__ror__", base_types + [Bool], ImageCollection],
        ["__rxor__", base_types + [Bool], ImageCollection],
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
