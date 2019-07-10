import pytest
import mock

from ... import Proxytype, List, Dict, Tuple, Str, Int
from .. import Image, ImageCollection


@mock.patch.object(Proxytype, "_from_apply", wraps=Proxytype._from_apply)
@pytest.mark.parametrize(
    "obj, namespace",
    [[Image.from_id(""), "Image"], [ImageCollection.from_id(""), "ImageCollection"]],
)
def test_pick_bands(_from_apply, obj, namespace):
    prefix = namespace + "."

    obj.pick_bands(["red", "green", "blue"])
    _from_apply.assert_called_once_with(
        prefix + "pick_bands", obj, "red", "green", "blue"
    )
    _from_apply.reset_mock()

    proxy_str = Str("green")
    obj.pick_bands(["red", proxy_str, "blue"])
    _from_apply.assert_called_once_with(
        prefix + "pick_bands", obj, "red", proxy_str, "blue"
    )
    _from_apply.reset_mock()

    obj.pick_bands("red green blue")
    _from_apply.assert_called_once_with(
        prefix + "pick_bands", obj, "red", "green", "blue"
    )
    _from_apply.reset_mock()

    obj.pick_bands("red")
    _from_apply.assert_called_once_with(prefix + "pick_bands", obj, "red")
    _from_apply.reset_mock()

    proxy_list = List[Str](["red", "nir"])
    obj.pick_bands(proxy_list)
    _from_apply.assert_called_once_with(prefix + "pick_bands_list", obj, proxy_list)

    with pytest.raises(TypeError, match="Band names must all be strings"):
        obj.pick_bands(["red", 2])

    with pytest.raises(TypeError, match=r"expected List\[Str\]"):
        obj.pick_bands(List[Int]([0, 1]))


@pytest.mark.parametrize("obj", [Image.from_id(""), ImageCollection.from_id("")])
def test_cant_rename_bands_pos_and_kwarg(obj):
    with pytest.raises(TypeError):
        obj.rename_bands("foo", bar="baz")


@pytest.mark.parametrize("cls", [Image, ImageCollection])
def test_map_bands_to_image_type(cls):
    obj = cls.from_id("")

    mapped_obj = obj.map_bands(lambda name, band_obj: band_obj / 2)
    assert isinstance(mapped_obj, cls)

    mapped_non_obj = obj.map_bands(lambda name, band_obj: (name, band_obj + 1))
    assert isinstance(mapped_non_obj, Dict[Str, Tuple[Str, cls]])
