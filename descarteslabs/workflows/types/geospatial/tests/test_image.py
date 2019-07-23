import pytest
import mock

from descarteslabs import scenes
from ...core.tests import utils

from .... import env
from ...core import ProxyTypeError
from ...primitives import Str, Float, Int, Bool, NoneType, Any
from ...containers import Dict

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


def test_all_methods():
    img = Image.from_id("foo")
    img2 = Image.from_id("bar")
    geom = Geometry(type="point", coordinates=[1, 2])
    feature = Feature(geometry=geom, properties={})
    fc = FeatureCollection([feature, feature])

    assert isinstance(img.rename_bands("foo", "bar"), Image)
    assert isinstance(img.rename_bands(baz="quiz"), Image)
    assert isinstance(img.rename_bands("foo", "bar"), Image)
    assert isinstance(img.rename_bands(red="green", blue="yellow"), Image)
    assert isinstance(img.pick_bands("red green blue"), Image)
    assert isinstance(img.pick_bands(["red", "green", "blue"]), Image)
    assert isinstance(img.unpack_bands("red green blue"), tuple)
    assert isinstance(img.unpack_bands(["red", "green", "blue"]), tuple)
    assert isinstance(img.concat_bands(img2), Image)
    assert isinstance(img.mask(img2), Image)
    assert isinstance(img.mask(img2, replace=True), Image)
    assert isinstance(img.mask(geom), Image)
    assert isinstance(img.mask(feature), Image)
    assert isinstance(img.mask(fc), Image)
    assert isinstance(img.getmask(), Image)
    assert isinstance(img.colormap(), Image)
    assert isinstance(img.minpixels(), Dict[Str, Float])
    assert isinstance(img.maxpixels(), Dict[Str, Float])
    assert isinstance(img.meanpixels(), Dict[Str, Float])
    assert isinstance(img.medianpixels(), Dict[Str, Float])
    assert isinstance(img.sumpixels(), Dict[Str, Float])
    assert isinstance(img.stdpixels(), Dict[Str, Float])
    assert isinstance(img.countpixels(), Dict[Str, Float])
    # assert isinstance(img.minbands(), Image)
    # assert isinstance(img.maxbands(), Image)
    # assert isinstance(img.meanbands(), Image)
    # assert isinstance(img.sumbands(), Image)
    # assert isinstance(img.stdbands(), Image)


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

base_types = [Image, ImageCollection, Int, Any]
returns_img_or_coll = {ImageCollection: ImageCollection, "default": Image}


@pytest.mark.parametrize(
    "operator, accepted_types, return_type",
    [
        ["log", (), Image],
        ["log2", (), Image],
        ["log10", (), Image],
        ["sqrt", (), Image],
        ["cos", (), Image],
        ["sin", (), Image],
        ["tan", (), Image],
        ["__lt__", base_types + [Float], returns_img_or_coll],
        ["__le__", base_types + [Float], returns_img_or_coll],
        ["__eq__", base_types + [Float, Bool], returns_img_or_coll],
        ["__ne__", base_types + [Float, Bool], returns_img_or_coll],
        ["__gt__", base_types + [Float], returns_img_or_coll],
        ["__ge__", base_types + [Float], returns_img_or_coll],
        ["__invert__", (), Image],
        ["__and__", base_types + [Bool], returns_img_or_coll],
        ["__or__", base_types + [Bool], returns_img_or_coll],
        ["__xor__", base_types + [Bool], returns_img_or_coll],
        ["__lshift__", base_types, returns_img_or_coll],
        ["__rshift__", base_types, returns_img_or_coll],
        ["__rand__", base_types + [Bool], returns_img_or_coll],
        ["__ror__", base_types + [Bool], returns_img_or_coll],
        ["__rxor__", base_types + [Bool], returns_img_or_coll],
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
