import pytest

from ...primitives import Int, Float
from .. import MaskedArray, Array


arr_fixture = [[1, 2], [3, 4]]
mask_fixture = [[True, False], [False, True]]


def test_init():
    ma = MaskedArray[Int, 2](arr_fixture, mask_fixture)
    assert isinstance(ma, MaskedArray[Int, 2])
    assert ma.dtype is Int
    assert ma.ndim == 2


def test_init_bool_mask():
    ma = MaskedArray[Int, 2](arr_fixture, False)
    assert isinstance(ma, MaskedArray[Int, 2])
    assert ma.dtype is Int
    assert ma.ndim == 2
    assert ma.graft[ma.graft[ma.graft["returns"]][2]] is False


def test_init_fill_value():
    fill_value = 5
    ma = MaskedArray[Int, 2](arr_fixture, mask_fixture, fill_value=fill_value)
    assert isinstance(ma, MaskedArray[Int, 2])
    assert ma.dtype is Int
    assert ma.ndim == 2
    assert ma.graft[ma.graft[ma.graft["returns"]][3]] == fill_value

    fill_value = Array[Int, 1]([5, 6])
    ma = MaskedArray[Int, 2](arr_fixture, mask_fixture, fill_value=fill_value)
    assert isinstance(ma, MaskedArray[Int, 2])
    assert ma.dtype is Int
    assert ma.ndim == 2
    assert ma.graft[ma.graft[ma.graft["returns"]][3]][0] == "array.create"


def test_init_fill_value_wrong_type():
    with pytest.raises(ValueError):
        fill_value = 3.4
        MaskedArray[Int, 2](arr_fixture, mask_fixture, fill_value=fill_value)

    with pytest.raises(ValueError):
        fill_value = Array[Float, 1]([1.2, 3.4])
        MaskedArray[Int, 2](arr_fixture, mask_fixture, fill_value=fill_value)


def test_getdata():
    ma = MaskedArray[Int, 2](arr_fixture, mask_fixture)
    arr = ma.getdata()
    assert isinstance(arr, Array)


def test_getmaskarray():
    ma = MaskedArray[Int, 2](arr_fixture, mask_fixture)
    arr = ma.getmaskarray()
    assert isinstance(arr, Array)


def test_filled():
    ma = MaskedArray[Int, 2](arr_fixture, mask_fixture)
    fill_value = 5
    arr = ma.filled(fill_value)
    assert isinstance(arr, Array)

    fill_value = Array[Int, 1]([5, 6])
    arr = ma.filled(fill_value)
    assert isinstance(arr, Array)


def test_filled_wrong_type():
    ma = MaskedArray[Int, 2](arr_fixture, mask_fixture)
    with pytest.raises(ValueError):
        fill_value = 3.4
        ma.filled(fill_value)

    with pytest.raises(ValueError):
        fill_value = Array[Float, 1]([1.2, 3.4])
        ma.filled(fill_value)


@pytest.mark.parametrize(
    "axis, return_type",
    [(1, MaskedArray[Int, 2]), ((1, 2), MaskedArray[Int, 1]), ((1, 2, 3), Int)],
)
def test_stats(axis, return_type):
    ma = MaskedArray[Int, 3]([[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]], False)

    assert isinstance(ma.min(axis=axis), return_type)
    assert isinstance(ma.max(axis=axis), return_type)
    assert isinstance(ma.mean(axis=axis), return_type)
    assert isinstance(ma.median(axis=axis), return_type)
    assert isinstance(ma.sum(axis=axis), return_type)
    assert isinstance(ma.std(axis=axis), return_type)
    assert isinstance(ma.count(axis=axis), return_type)
