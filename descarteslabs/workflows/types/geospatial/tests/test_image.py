import pytest
import mock
import six

from descarteslabs import scenes
from ...core.tests import utils

from .... import env
from ...core import ProxyTypeError, _resolve_lambdas
from ...primitives import Str, Float, Int, Bool, NoneType, Any
from ...containers import Dict, Tuple

from .. import Image, ImageCollection, Geometry, Feature, FeatureCollection


def test_init_raises():
    with pytest.raises(TypeError, match="Please use a classmethod"):
        Image()


@mock.patch.object(env, "geoctx")
@mock.patch.object(env, "_token")
def test_from_id(mock_geoctx, mock_token):
    mock_geoctx_graft = mock.PropertyMock(return_value={"returns": "geoctx"})
    mock_token_graft = mock.PropertyMock(return_value={"returns": "_token_"})
    type(mock_geoctx).graft = mock_geoctx_graft
    type(mock_token).graft = mock_token_graft
    # "Because of the way mock attributes are stored you can't directly attach a PropertyMock to a mock object.
    # Instead you can attach it to the mock type object."
    Image.from_id("foo")
    mock_geoctx_graft.assert_called()
    mock_token_graft.assert_called()


@mock.patch.object(Image, "from_id", wraps=Image.from_id)
def test_promote(from_id_wrapper):
    scene = scenes.Scene.__new__(scenes.Scene)
    # ^ easier than making fake metadata for the constructor
    scene.properties = {"id": "foo"}

    img = Image._promote(scene)
    from_id_wrapper.assert_called_once_with("foo")

    assert Image._promote(img) is img

    with pytest.raises(ProxyTypeError):
        Image._promote("")


def test_stats_return_type():
    for axis, return_type in six.iteritems(_resolve_lambdas(Image._STATS_RETURN_TYPES)):
        assert Image._stats_return_type(axis) == return_type

    with pytest.raises(ValueError):
        Image._stats_return_type(5)

    with pytest.raises(ValueError):
        Image._stats_return_type("foo")


def test_all_methods():
    img = Image.from_id("foo")
    img2 = Image.from_id("bar")
    geom = Geometry(type="point", coordinates=[1, 2])
    feature = Feature(geometry=geom, properties={})
    fc = FeatureCollection([feature, feature])

    assert isinstance(img.nbands, Int)
    assert isinstance(img.with_properties(foo="bar", baz="qux"), Image)
    assert isinstance(img.without_properties("foo", "baz"), Image)
    assert isinstance(img.with_bandinfo("red", foo="bar", baz="qux"), Image)
    assert isinstance(img.without_bandinfo("red", "foo", "baz"), Image)
    assert isinstance(img.rename_bands("foo", "bar"), Image)
    assert isinstance(img.rename_bands(baz="quiz"), Image)
    assert isinstance(img.rename_bands("foo", "bar"), Image)
    assert isinstance(img.rename_bands(red="green", blue="yellow"), Image)
    assert isinstance(img.pick_bands("red green blue"), Image)
    assert isinstance(img.pick_bands(["red", "green", "blue"]), Image)
    assert isinstance(img.unpack_bands("red green blue"), tuple)
    assert isinstance(img.unpack_bands(["red", "green", "blue"]), tuple)
    assert isinstance(img.concat(img), ImageCollection)
    assert isinstance(img.concat_bands(img2), Image)
    assert isinstance(img.clip_values(0.1, 0.5), Image)
    assert isinstance(img.clip_values([0.1, 0.4], [0.5, 0.8]), Image)
    assert isinstance(img.scale_values(0.1, 0.5), Image)
    assert isinstance(img.value_at(0.1, 0.1), Dict[Str, Float])
    assert isinstance(img.index_to_coords(0, 0), Tuple[Float, Float])
    assert isinstance(img.coords_to_index(0.0, 0.0), Tuple[Int, Int])
    assert isinstance(img.mask(img2), Image)
    assert isinstance(img.mask(img2, replace=True), Image)
    assert isinstance(img.mask(geom), Image)
    assert isinstance(img.mask(feature), Image)
    assert isinstance(img.mask(fc), Image)
    assert isinstance(img.getmask(), Image)
    assert isinstance(img.colormap(), Image)
    assert isinstance(img.min(axis="pixels"), Dict[Str, Float])
    assert isinstance(img.min(axis="bands"), Image)
    assert isinstance(img.min(axis=None), Float)
    assert isinstance(img.min(), Float)
    assert isinstance(img.max(axis="pixels"), Dict[Str, Float])
    assert isinstance(img.max(axis="bands"), Image)
    assert isinstance(img.max(axis=None), Float)
    assert isinstance(img.max(), Float)
    assert isinstance(img.mean(axis="pixels"), Dict[Str, Float])
    assert isinstance(img.mean(axis="bands"), Image)
    assert isinstance(img.mean(axis=None), Float)
    assert isinstance(img.mean(), Float)
    assert isinstance(img.median(axis="pixels"), Dict[Str, Float])
    assert isinstance(img.median(axis="bands"), Image)
    assert isinstance(img.median(axis=None), Float)
    assert isinstance(img.median(), Float)
    assert isinstance(img.sum(axis="pixels"), Dict[Str, Float])
    assert isinstance(img.sum(axis="bands"), Image)
    assert isinstance(img.sum(axis=None), Float)
    assert isinstance(img.sum(), Float)
    assert isinstance(img.std(axis="pixels"), Dict[Str, Float])
    assert isinstance(img.std(axis="bands"), Image)
    assert isinstance(img.std(axis=None), Float)
    assert isinstance(img.std(), Float)
    assert isinstance(img.count(axis="pixels"), Dict[Str, Float])
    assert isinstance(img.count(axis="bands"), Image)
    assert isinstance(img.count(axis=None), Float)
    assert isinstance(img.count(), Float)


img = Image.from_id("bar")
all_values_to_try = [
    Image.from_id("foo"),
    ImageCollection([]),
    Int(0),
    Float(1.1),
    Bool(True),
    NoneType(None),
    Any(0),
]

base_types = [Image, ImageCollection, Bool, Int, Any]
returns_img_or_coll = {ImageCollection: ImageCollection, "default": Image}


@pytest.mark.parametrize(
    "operator, accepted_types, return_type",
    [
        ["log", (), Image],
        ["log2", (), Image],
        ["log10", (), Image],
        ["log1p", (), Image],
        ["sqrt", (), Image],
        ["cos", (), Image],
        ["arccos", (), Image],
        ["sin", (), Image],
        ["arcsin", (), Image],
        ["tan", (), Image],
        ["arctan", (), Image],
        ["exp", (), Image],
        ["square", (), Image],
        ["__lt__", base_types + [Float], returns_img_or_coll],
        ["__le__", base_types + [Float], returns_img_or_coll],
        ["__eq__", base_types + [Float], returns_img_or_coll],
        ["__ne__", base_types + [Float], returns_img_or_coll],
        ["__gt__", base_types + [Float], returns_img_or_coll],
        ["__ge__", base_types + [Float], returns_img_or_coll],
        ["__invert__", (), Image],
        ["__and__", base_types, returns_img_or_coll],
        ["__or__", base_types, returns_img_or_coll],
        ["__xor__", base_types, returns_img_or_coll],
        ["__lshift__", base_types, returns_img_or_coll],
        ["__rshift__", base_types, returns_img_or_coll],
        ["__rand__", base_types, returns_img_or_coll],
        ["__ror__", base_types, returns_img_or_coll],
        ["__rxor__", base_types, returns_img_or_coll],
        ["__rlshift__", base_types, returns_img_or_coll],
        ["__rrshift__", base_types, returns_img_or_coll],
        ["__neg__", (), Image],
        ["__pos__", (), Image],
        ["__abs__", (), Image],
        ["__add__", base_types + [Float], returns_img_or_coll],
        ["__sub__", base_types + [Float], returns_img_or_coll],
        ["__mul__", base_types + [Float], returns_img_or_coll],
        ["__div__", base_types + [Float], returns_img_or_coll],
        ["__truediv__", base_types + [Float], returns_img_or_coll],
        ["__floordiv__", base_types + [Float], returns_img_or_coll],
        ["__mod__", base_types + [Float], returns_img_or_coll],
        ["__pow__", base_types + [Float], returns_img_or_coll],
        ["__radd__", base_types + [Float], returns_img_or_coll],
        ["__rsub__", base_types + [Float], returns_img_or_coll],
        ["__rmul__", base_types + [Float], returns_img_or_coll],
        ["__rdiv__", base_types + [Float], returns_img_or_coll],
        ["__rtruediv__", base_types + [Float], returns_img_or_coll],
        ["__rfloordiv__", base_types + [Float], returns_img_or_coll],
        ["__rmod__", base_types + [Float], returns_img_or_coll],
        ["__rpow__", base_types + [Float], returns_img_or_coll],
    ],
)
def test_all_operators(operator, accepted_types, return_type):
    utils.operator_test(img, all_values_to_try, operator, accepted_types, return_type)
