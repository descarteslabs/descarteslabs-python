import numpy as np
import pyarrow as pa

from ..context import serialization_context


class ContextTestCase(object):
    def test_numpy_masked_array_serialization(self):
        arr = np.array([1, 2, 3, 4])
        arr_mask = np.array([True, False, True, False])
        masked_arr = np.ma.masked_array(arr, arr_mask)
        serialized_masked_arr = pa.serialize(masked_arr, context=serialization_context)
        deserialized_masked_arr = pa.deserialize(
            serialized_masked_arr.to_buffer(), context=serialization_context
        )
        np.testing.assert_array_equal(deserialized_masked_arr, masked_arr)

    def test_numpy_masked_array_serialization_nomask(self):
        arr = np.array([1, 2, 3, 4])
        arr_mask = np.ma.nomask
        masked_arr = np.ma.masked_array(arr, arr_mask)
        serialized_masked_arr = pa.serialize(masked_arr, context=serialization_context)
        deserialized_masked_arr = pa.deserialize(
            serialized_masked_arr.to_buffer(), context=serialization_context
        )
        np.testing.assert_array_equal(deserialized_masked_arr, masked_arr)
