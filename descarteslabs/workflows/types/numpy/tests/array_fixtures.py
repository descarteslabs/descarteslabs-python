import numpy as np
import pytest
from pytest_cases import fixture_ref

from descarteslabs.workflows.types.array import Array, MaskedArray
from descarteslabs.workflows.types.primitives import Float


@pytest.fixture()
def img_arr():
    return Array[Float, 3](np.ones((3, 3, 3)))


@pytest.fixture()
def col_arr():
    return Array[Float, 4](np.ones((2, 3, 3, 3)))


@pytest.fixture()
def img_arr_masked():
    data = np.ones((3, 3, 3))
    mask = (np.arange(data.size) % 3 == 0).reshape(data.shape)
    return MaskedArray[Float, 3](data, mask)


@pytest.fixture()
def col_arr_masked():
    data = np.ones((2, 3, 3, 3))
    mask = (np.arange(data.size) % 3 == 0).reshape(data.shape)
    return MaskedArray[Float, 4](data, mask)


unmasked_arrays = [fixture_ref("img_arr"), fixture_ref("col_arr")]
masked_arrays = [fixture_ref("img_arr_masked"), fixture_ref("col_arr_masked")]
img_arrays = [fixture_ref("img_arr"), fixture_ref("img_arr_masked")]
col_arrays = [fixture_ref("col_arr"), fixture_ref("col_arr_masked")]
all_arrays = unmasked_arrays + masked_arrays
