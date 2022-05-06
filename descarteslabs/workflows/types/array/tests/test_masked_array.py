import operator

import pytest

from ...primitives import Int, Str, Any
from ...containers import List
from ...geospatial import ImageCollection
from ...identifier import parameter
from .. import MaskedArray, Array, DType, Scalar

import numpy as np

from ...core import ProxyTypeError


arr_fixture = [[1, 2], [3, 4]]
mask_fixture = [[True, False], [False, True]]
ma = MaskedArray([[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]], False)


def test_init():
    ma = MaskedArray(arr_fixture, mask_fixture)
    assert isinstance(ma, MaskedArray)
    assert ma.params == ()


def test_init_bool_mask():
    ma = MaskedArray(arr_fixture, False)
    assert isinstance(ma, MaskedArray)
    assert ma.params == ()


def test_init_fill_value():
    fill_value = 5
    ma = MaskedArray(arr_fixture, mask_fixture, fill_value=fill_value)
    assert isinstance(ma, MaskedArray)
    assert ma.params == ()

    fill_value = Array([5, 6])
    ma = MaskedArray(arr_fixture, mask_fixture, fill_value=fill_value)


def test_from_numpy():
    np_ma = np.ma.masked_array([1, 2, 3], [True, True, False])
    ma = MaskedArray.from_numpy(np_ma)
    assert isinstance(ma, MaskedArray)
    assert isinstance(ma, MaskedArray)
    assert ma.params == ()


def test_init_params():
    x = parameter("x", Int)
    y = parameter("y", Int)

    ma = MaskedArray(data=x, mask=mask_fixture, fill_value=y)
    assert isinstance(ma, MaskedArray)
    assert ma.params == (x, y)


@pytest.mark.parametrize(
    "val",
    [
        [1, 2, 3],
        np.array([1, 2, 3]),
        np.ma.masked_array([1, 2, 3], [True, True, False]),
        Any([1, 2]),
    ],
)
def test_promote(val):
    ma = MaskedArray._promote(val)
    assert isinstance(ma, MaskedArray)


@pytest.mark.parametrize(
    "val", ["foo", Str("foo"), List[Int]([1, 2, 3]), np.array([1, 2], dtype=object)]
)
def test_promote_invalid(val):
    with pytest.raises((TypeError, ProxyTypeError)):
        MaskedArray._promote(val)


def test_dtype():
    assert isinstance(ma.dtype, DType)


def test_ndim():
    assert isinstance(ma.ndim, Int)


def test_shape():
    assert isinstance(ma.shape, List[Int])


def test_size():
    assert isinstance(ma.size, Int)


def test_astype():
    assert isinstance(ma.astype("int"), MaskedArray)
    assert isinstance(ma.astype(DType(int)), MaskedArray)


def test_flatten():
    assert isinstance(ma.flatten(), MaskedArray)


@pytest.mark.parametrize("shape", [(-1,), (1, 2)])
def test_reshape(shape):
    assert isinstance(ma.reshape(*shape), MaskedArray)


@pytest.mark.parametrize(
    "idx",
    [
        None,
        1,
        slice(2),
        (0, 0, 0),
        (None, 0, 0, 0),
        (1, None),
        [1, 2],
        Array([1, 2]),
        Array([True, False]),
        Array([[[]]]),
        (Array([[[]]]), None),
        (0, Array([[]])),
        (0, Array([[]]), None),
    ],
)
def test_getitem(idx):
    result = ma[idx]
    assert isinstance(result, MaskedArray)


@pytest.mark.parametrize(
    "idx, err_type, msg",
    [
        ([1, 2.2], TypeError, r"Invalid types in \[1, 2.2\]"),
        (
            List[Str]([]),
            TypeError,
            r"only be sliced with 1D List\[Int\] or List\[Bool\], not List\[Str\]",
        ),
        (
            (Array([]), Array([])),
            ValueError,
            "cannot slice an Array with lists or Arrays in multiple axes",
        ),
        (
            ([1], [2]),
            ValueError,
            "cannot slice an Array with lists or Arrays in multiple axes",
        ),
        (
            ([1], Array([])),
            ValueError,
            "cannot slice an Array with lists or Arrays in multiple axes",
        ),
    ],
)
def test_getitem_error(idx, err_type, msg):
    with pytest.raises(err_type, match=msg):
        ma[idx]


def test_to_imagery():
    assert isinstance(ma.to_imagery(), ImageCollection)


def test_to_imagery_error():
    with pytest.raises(TypeError):
        ma.to_imagery(properties="foo")
        ma.to_imagery(bandinfo=[1, 2, 3])


@pytest.mark.parametrize(
    "method",
    [
        operator.lt,
        operator.le,
        operator.gt,
        operator.ge,
        operator.eq,
        operator.ne,
        operator.add,
        operator.sub,
        operator.mul,
        operator.floordiv,
        operator.truediv,
        operator.mod,
        operator.pow,
    ],
)
@pytest.mark.parametrize("other", [Array([[1, 2, 3], [4, 5, 6]]), Array(1), 1, 0.5])
def test_container_methods(method, other):
    result = method(ma, other)
    r_result = method(other, ma)
    assert isinstance(result, MaskedArray)
    assert isinstance(r_result, MaskedArray)


@pytest.mark.parametrize(
    "axis, return_type", [(1, MaskedArray), ((1, 2), MaskedArray), (None, Scalar)]
)
def test_stats(axis, return_type):
    assert isinstance(ma.min(axis=axis), return_type)
    assert isinstance(ma.max(axis=axis), return_type)
    assert isinstance(ma.mean(axis=axis), return_type)
    assert isinstance(ma.median(axis=axis), return_type)
    assert isinstance(ma.sum(axis=axis), return_type)
    assert isinstance(ma.std(axis=axis), return_type)
    assert isinstance(ma.count(axis=axis), return_type)


def test_getdata():
    arr = ma.getdata()
    assert isinstance(arr, Array)


def test_getmaskarray():
    arr = ma.getmaskarray()
    assert isinstance(arr, Array)


@pytest.mark.parametrize("fill_value", [5, 5.0, True, np.int32(5), np.float64(5), None])
def test_filled(fill_value):
    arr = ma.filled(fill_value)
    assert isinstance(arr, Array)


def test_compressed():
    arr = ma.compressed()
    assert isinstance(arr, Array)
