import pytest
from hypothesis import given
import hypothesis.strategies as st
import hypothesis.extra.numpy as st_np

import numpy as np
import pyarrow as pa

from ..context import serialization_context


# Fails, most concerningly, on:
# array([1], dtype=uint16)
# array([256], dtype=uint16)


@pytest.mark.xfail
@given(
    st_np.arrays(
        dtype=st_np.scalar_dtypes(), shape=st.one_of(st.just(0), st_np.array_shapes())
    )
)
def test_plain_roundtrip(arr):
    cereal = pa.serialize(arr, context=serialization_context)
    buf = cereal.to_buffer()
    decereal = pa.deserialize(buf, context=serialization_context)

    assert arr.dtype == decereal.dtype.newbyteorder(
        arr.dtype.byteorder
    )  # arrow may change endianness
    np.testing.assert_array_equal(arr, decereal)
