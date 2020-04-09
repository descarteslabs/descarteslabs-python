import pytest
import numpy as np

from descarteslabs.workflows.types.array import Array, MaskedArray
from descarteslabs.workflows.types.containers import Tuple
from descarteslabs.workflows.types.primitives import Int, Float, Bool
import descarteslabs.workflows.types.numpy as wf_np


img_arr = Array(np.ones((3, 3, 3)))
col_arr = Array(np.ones((2, 3, 3, 3)))
data = np.ones((3, 3, 3))
mask = (np.arange(data.size) % 3 == 0).reshape(data.shape)
img_arr_masked = MaskedArray(data, mask)
data = np.ones((2, 3, 3, 3))
mask = (np.arange(data.size) % 3 == 0).reshape(data.shape)
col_arr_masked = MaskedArray(data, mask)


@pytest.mark.parametrize(
    "bins, range, bins_return_type",
    [
        [5, (0, 10), Array],
        [5, (0.1, 10.5), Array],
        [[1, 2, 3], None, Array],
        [[0.5, 1.0, 1.5], None, Array],
    ],
)
@pytest.mark.parametrize(
    "density, hist_return_type", [[None, Array], [False, Array], [True, Array]]
)
def test_histogram(bins, range, bins_return_type, density, hist_return_type):
    result = wf_np.histogram(img_arr, bins=bins, range=range, density=density)
    assert isinstance(result, Tuple[hist_return_type, bins_return_type])


@pytest.mark.parametrize("density", ["foo", 5, object()])
def test_histogram_manual_density_typecheck(density):
    with pytest.raises(TypeError):
        wf_np.histogram(img_arr, bins=5, range=(0, 1), density=density)


@pytest.mark.parametrize(
    "arr, return_type", [(img_arr, Array), (img_arr_masked, MaskedArray)]
)
@pytest.mark.parametrize(
    "newshape", [(1,), (2, 4), (3, 3, 3), [2, 2], Tuple[Int, Int]((5, 5))]
)
def test_reshape(arr, return_type, newshape):
    result = wf_np.reshape(arr, newshape)
    assert isinstance(result, return_type)


@pytest.mark.parametrize("newshape", ["foo", 5, Tuple[Float]((1.0,))])
def test_reshape_newshape_raises(newshape):
    with pytest.raises(TypeError):
        wf_np.reshape(img_arr, newshape)


@pytest.mark.parametrize(
    "base_arr, return_type", [(img_arr, Array), (img_arr_masked, MaskedArray)]
)
@pytest.mark.parametrize("axis", [0, 1, 2])
@pytest.mark.parametrize("num_arr", [2, 4, 6])
def test_stack(base_arr, return_type, num_arr, axis):
    result = wf_np.stack([base_arr] * num_arr, axis=axis)
    assert isinstance(result, return_type)


@pytest.mark.parametrize("func", [(wf_np.argmin), (wf_np.argmax)])
@pytest.mark.parametrize(
    "arr, axis", [(img_arr, None), (col_arr, None), (img_arr, 0), (col_arr, 1)]
)
def test_argmin_argmax(func, arr, axis):
    result = func(arr, axis=axis)
    if axis is None:
        assert isinstance(result, Int)
    else:
        assert isinstance(result, Array)


@pytest.mark.parametrize("func", [(wf_np.all), (wf_np.any)])
@pytest.mark.parametrize(
    "arr, axis", [(img_arr, None), (col_arr, 0), (col_arr, [0, 2])]
)
def test_all_any(func, arr, axis):
    result = func(arr, axis=axis)
    if axis is None:
        assert isinstance(result, Bool)
    elif isinstance(axis, (int, tuple, list)):
        assert isinstance(result, Array)


@pytest.mark.parametrize("func", [(wf_np.all), (wf_np.any)])
def test_all_any_incorrect_axis(func):
    with pytest.raises(TypeError, match="Cannot call function"):
        func(img_arr, axis=(1, "foo", 3))
        func(img_arr, axis="foo")
