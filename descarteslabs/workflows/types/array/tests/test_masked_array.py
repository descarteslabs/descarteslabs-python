import pytest

from ...primitives import Float
from .. import MaskedArray, Array


arr_fixture = [[1, 2], [3, 4]]
mask_fixture = [[True, False], [False, True]]


def test_init():
    ma = MaskedArray(arr_fixture, mask_fixture)
    assert isinstance(ma, MaskedArray)


def test_init_bool_mask():
    ma = MaskedArray(arr_fixture, False)
    assert isinstance(ma, MaskedArray)


def test_init_fill_value():
    fill_value = 5
    ma = MaskedArray(arr_fixture, mask_fixture, fill_value=fill_value)
    assert isinstance(ma, MaskedArray)

    fill_value = Array([5, 6])
    ma = MaskedArray(arr_fixture, mask_fixture, fill_value=fill_value)
    assert isinstance(ma, MaskedArray)


def test_getdata():
    ma = MaskedArray(arr_fixture, mask_fixture)
    arr = ma.getdata()
    assert isinstance(arr, Array)


def test_getmaskarray():
    ma = MaskedArray(arr_fixture, mask_fixture)
    arr = ma.getmaskarray()
    assert isinstance(arr, Array)


def test_filled():
    ma = MaskedArray(arr_fixture, mask_fixture)
    fill_value = 5
    arr = ma.filled(fill_value)
    assert isinstance(arr, Array)


@pytest.mark.parametrize(
    "axis, return_type", [(1, MaskedArray), ((1, 2), MaskedArray), (None, Float)]
)
def test_stats(axis, return_type):
    ma = MaskedArray([[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]], False)

    assert isinstance(ma.min(axis=axis), return_type)
    assert isinstance(ma.max(axis=axis), return_type)
    assert isinstance(ma.mean(axis=axis), return_type)
    assert isinstance(ma.median(axis=axis), return_type)
    assert isinstance(ma.sum(axis=axis), return_type)
    assert isinstance(ma.std(axis=axis), return_type)
    assert isinstance(ma.count(axis=axis), return_type)
