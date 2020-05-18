import operator

import pytest

import numpy as np

from ...core import ProxyTypeError
from ...primitives import Float, Bool, Str, Int, Any
from ...geospatial import ImageCollection
from ...containers import List
from .. import Array, DType, Scalar


arr = Array([[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]])


@pytest.mark.parametrize(
    "val", [1, Int(1), [1, 2, 3, 4], np.ones((1, 2, 3)), List[Int]([1, 2, 3])]
)
def test_init(val):
    arr = Array(val)
    assert isinstance(arr, Array)


@pytest.mark.parametrize(
    "val",
    [
        1,
        Int(1),
        1.0,
        Float(1.0),
        True,
        Bool(True),
        [1, 2, 3, 4],
        List[Int]([1, 2, 3]),
        np.ones((1, 2, 3)),
        Any([1, 2]),
    ],
)
def test_promote(val):
    arr = Array._promote(val)
    assert isinstance(arr, Array)


@pytest.mark.parametrize("val", ["foo", Str("foo"), np.array([1, 2], dtype=np.object)])
def test_promote_invalid(val):
    with pytest.raises((TypeError, ProxyTypeError)):
        Array._promote(val)


def test_dtype():
    assert isinstance(arr.dtype, DType)


def test_ndim():
    assert isinstance(arr.ndim, Int)


def test_shape():
    assert isinstance(arr.shape, List[Int])


def test_size():
    assert isinstance(arr.size, Int)


def test_astype():
    assert isinstance(arr.astype("int"), Array)
    assert isinstance(arr.astype(int), Array)
    assert isinstance(arr.astype(Int), Array)
    assert isinstance(arr.astype(DType(int)), Array)


def test_flatten():
    assert isinstance(arr.flatten(), Array)


@pytest.mark.parametrize("shape", [(-1,), (1, 2)])
def test_reshape(shape):
    assert isinstance(arr.reshape(*shape), Array)


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
    result = arr[idx]
    assert isinstance(result, Array)


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
        arr[idx]


def test_to_imagery():
    assert isinstance(arr.to_imagery(), ImageCollection)


def test_to_imagery_error():
    with pytest.raises(TypeError):
        arr.to_imagery(properties="foo")
        arr.to_imagery(bandinfo=[1, 2, 3])


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
    result = method(arr, other)
    r_result = method(other, arr)
    assert isinstance(result, Array)
    assert isinstance(r_result, Array)


@pytest.mark.parametrize(
    "axis, return_type", [(1, Array), ((1, 2), Array), (None, Scalar)]
)
def test_stats(axis, return_type):
    assert isinstance(arr.min(axis=axis), return_type)
    assert isinstance(arr.max(axis=axis), return_type)
    assert isinstance(arr.mean(axis=axis), return_type)
    assert isinstance(arr.median(axis=axis), return_type)
    assert isinstance(arr.sum(axis=axis), return_type)
    assert isinstance(arr.std(axis=axis), return_type)
