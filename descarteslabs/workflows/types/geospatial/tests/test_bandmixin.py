import pytest
import mock

from ... import Proxytype, List, Dict, Tuple, Str, Int, Bool
from .. import Image, ImageCollection


@mock.patch.object(Proxytype, "_from_apply", wraps=Proxytype._from_apply)
@pytest.mark.parametrize("obj", [Image.from_id(""), ImageCollection.from_id("")])
@pytest.mark.parametrize("allow_missing", [True, False])
def test_pick_bands(_from_apply, obj, allow_missing):
    obj.pick_bands(["red", "green", "blue"], allow_missing=allow_missing)
    _from_apply.assert_called_once_with(
        "wf.pick_bands", obj, "red", "green", "blue", allow_missing=allow_missing
    )
    _from_apply.reset_mock()

    proxy_str = Str("green")
    obj.pick_bands(["red", proxy_str, "blue"], allow_missing=allow_missing)
    _from_apply.assert_called_once_with(
        "wf.pick_bands", obj, "red", proxy_str, "blue", allow_missing=allow_missing
    )
    _from_apply.reset_mock()

    obj.pick_bands("red green blue", allow_missing=allow_missing)
    _from_apply.assert_called_once_with(
        "wf.pick_bands", obj, "red", "green", "blue", allow_missing=allow_missing
    )
    _from_apply.reset_mock()

    obj.pick_bands("red", allow_missing=allow_missing)
    _from_apply.assert_called_once_with(
        "wf.pick_bands", obj, "red", allow_missing=allow_missing
    )
    _from_apply.reset_mock()

    proxy_list = List[Str](["red", "nir"])
    proxy_bool = Bool(allow_missing)
    obj.pick_bands(proxy_list, proxy_bool)
    _from_apply.assert_called_once_with(
        "wf.pick_bands_list", obj, proxy_list, allow_missing=proxy_bool
    )

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
