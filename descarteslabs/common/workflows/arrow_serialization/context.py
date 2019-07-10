import numbers
import numpy as np
import pyarrow as pa

from pyarrow.serialization import (
    _serialize_numpy_array_list,
    _deserialize_numpy_array_list,
)


def _serialize_numpy_masked_array(obj):
    return (
        _serialize_numpy_array_list(np.ma.getdata(obj)),
        _serialize_numpy_array_list(np.ma.getmaskarray(obj)),
        obj.fill_value,
        obj.hardmask,
    )


def _deserialize_numpy_masked_array(obj):
    serialized_data, serialized_mask, fill_value, hardmask = obj
    data = _deserialize_numpy_array_list(serialized_data)
    # Handle the np.ma.nomask case, where the serialized mask will be the
    # the integer 0.
    mask = (
        _deserialize_numpy_array_list(serialized_mask)
        if not isinstance(serialized_mask[0], numbers.Number)
        else np.ma.nomask
    )
    return np.ma.MaskedArray(data, mask=mask, fill_value=fill_value, hard_mask=hardmask)


serialization_context = pa.SerializationContext()
pa.register_default_serialization_handlers(serialization_context)
serialization_context.register_type(
    np.ma.MaskedArray,
    "numpy.ma.core.MaskedArray",
    custom_serializer=_serialize_numpy_masked_array,
    custom_deserializer=_deserialize_numpy_masked_array,
)
