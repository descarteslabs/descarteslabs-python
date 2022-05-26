import numpy as np
import pyarrow as pa

from ..context import serialization_context


def test_numpy_masked_array_serialization():
    arr = np.array([1, 2, 3, 4])
    arr_mask = np.array([True, False, True, False])
    masked_arr = np.ma.masked_array(arr, arr_mask)
    serialized_masked_arr = pa.serialize(masked_arr, context=serialization_context)
    deserialized_masked_arr = pa.deserialize(
        serialized_masked_arr.to_buffer(), context=serialization_context
    )
    np.testing.assert_array_equal(deserialized_masked_arr, masked_arr)


def test_numpy_masked_array_serialization_nomask():
    arr = np.array([1, 2, 3, 4])
    arr_mask = np.ma.nomask
    masked_arr = np.ma.masked_array(arr, arr_mask)
    serialized_masked_arr = pa.serialize(masked_arr, context=serialization_context)
    deserialized_masked_arr = pa.deserialize(
        serialized_masked_arr.to_buffer(), context=serialization_context
    )
    np.testing.assert_array_equal(deserialized_masked_arr, masked_arr)


def test_numpy_masked_constant_serialization():
    constant = np.ma.masked

    serialized = pa.serialize(constant, context=serialization_context)
    deserialized = pa.deserialize(serialized.to_buffer(), context=serialization_context)

    assert deserialized is np.ma.masked


def test_python_slice_serialization():
    s = slice(1, 2, 3)

    serialized = pa.serialize(s, context=serialization_context)
    deserialized = pa.deserialize(serialized.to_buffer(), context=serialization_context)

    assert deserialized == s
