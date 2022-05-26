from hypothesis import given, note, assume
import hypothesis.strategies as st
import hypothesis.extra.numpy as st_np

import numpy as np
import pyarrow as pa

from ..context import serialization_context


@given(
    st_np.arrays(
        dtype=st_np.scalar_dtypes(), shape=st.one_of(st.just(0), st_np.array_shapes())
    )
)
def test_plain_roundtrip(arr):
    assume(arr.dtype.byteorder in ("=", "<"))
    # https://issues.apache.org/jira/browse/ARROW-4677
    # Otherwise this fails on endianness changes, such as:
    # array([1], dtype=">i2") -> array([256], dtype="int16")

    cereal = pa.serialize(arr, context=serialization_context)
    buf = cereal.to_buffer()
    decereal = pa.deserialize(buf, context=serialization_context)

    assert arr.dtype == decereal.dtype.newbyteorder(
        arr.dtype.byteorder
    )  # arrow may change endianness
    note((arr.dtype, decereal.dtype))
    np.testing.assert_array_equal(arr, decereal)
