import operator

import pytest

import numpy as np

from ...primitives import Float, Str
from ...geospatial import ImageCollection
from ...containers import List
from .. import Array


def test_init():
    arr = Array([1, 2, 3, 4])
    assert isinstance(arr, Array)

    arr = Array(np.ones((1, 2, 3)))
    assert isinstance(arr, Array)


@pytest.mark.parametrize(
    "idx",
    [
        None,
        (0, 0, 0),
        (None, 0, 0, 0),
        1,
        (1, None),
        [1, 2],
        Array([1, 2]),
        Array([True, False]),
        Array([[[]]]),
        (Array([[[]]]), None),
        (0, Array([[]])),
        (0, Array([[]]), None),
        slice(2),
    ],
)
def test_getitem(idx):
    arr = Array([[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]])
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
    arr = Array([[[[]]]])
    with pytest.raises(err_type, match=msg):
        arr[idx]


def test_to_imagery():
    arr = Array([[10, 11, 12], [13, 14, 15]])

    arr = Array([[[]]])
    assert isinstance(arr.to_imagery({}, {}), ImageCollection)


@pytest.mark.parametrize(
    "method",
    [operator.lt, operator.le, operator.gt, operator.ge, operator.eq, operator.ne],
)
@pytest.mark.parametrize("other", [Array([[1, 2, 3], [4, 5, 6]]), 1, 0.5])
def test_container_bool_methods(method, other):
    arr = Array([[10, 11, 12], [13, 14, 15]])
    result = method(arr, other)
    r_result = method(other, arr)
    assert isinstance(result, Array)
    assert isinstance(r_result, Array)


@pytest.mark.parametrize(
    "method",
    [
        operator.add,
        operator.sub,
        operator.mul,
        operator.floordiv,
        operator.truediv,
        operator.mod,
        operator.pow,
    ],
)
@pytest.mark.parametrize("other", [Array([[1, 2, 3], [4, 5, 6]]), 1, 0.5])
def test_container_arithmetic(method, other):
    arr = Array([[10.0, 11.0, 12.0], [13.0, 14.0, 15.0]])
    result = method(arr, other)
    r_result = method(other, arr)
    assert isinstance(result, Array)
    assert isinstance(r_result, Array)


@pytest.mark.parametrize(
    "axis, return_type", [(1, Array), ((1, 2), Array), (None, Float)]
)
def test_stats(axis, return_type):
    arr = Array([[[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]])

    assert isinstance(arr.min(axis=axis), return_type)
    assert isinstance(arr.max(axis=axis), return_type)
    assert isinstance(arr.mean(axis=axis), return_type)
    assert isinstance(arr.median(axis=axis), return_type)
    assert isinstance(arr.sum(axis=axis), return_type)
    assert isinstance(arr.std(axis=axis), return_type)
