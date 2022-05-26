import numpy as np
import pyarrow as pa

from pyarrow.serialization import (
    _serialize_numpy_array_list,
    _deserialize_numpy_array_list,
)


def _serialize_numpy_masked_array(obj):
    data = np.ma.getdata(obj)
    mask = np.ma.getmask(obj)
    return (
        _serialize_numpy_array_list(data),
        _serialize_numpy_array_mask(mask),
        obj.fill_value,
        obj.hardmask,
    )


def _serialize_numpy_array_mask(obj):
    # mask is either a boolean array or np.ma.nomask
    # we will represent np.ma.nomask as None
    return None if obj is np.ma.nomask else _serialize_numpy_array_list(obj)


def _serialize_numpy_masked_constant(obj):
    # Workaround for "Changing the dtype of a 0d array is only supported if the itemsize is unchanged" error
    return None


def _serialize_python_slice(obj):
    return (obj.start, obj.stop, obj.step)


def _deserialize_numpy_masked_array(obj):
    serialized_data, serialized_mask, fill_value, hardmask = obj
    data = _deserialize_numpy_array_list(serialized_data)
    mask = _deserialize_numpy_array_mask(serialized_mask)
    return np.ma.MaskedArray(data, mask=mask, fill_value=fill_value, hard_mask=hardmask)


def _deserialize_numpy_array_mask(obj):
    # mask is either a boolean array or np.ma.nomask
    return _deserialize_numpy_array_list(obj) if obj is not None else np.ma.nomask


def _deserialize_numpy_masked_constant(obj):
    return np.ma.masked


def _deserialize_python_slice(obj):
    start, stop, step = obj
    return slice(start, stop, step)


serialization_context = pa.SerializationContext()
pa.register_default_serialization_handlers(serialization_context)

serialization_context.register_type(
    np.ma.MaskedArray,
    "numpy.ma.core.MaskedArray",
    custom_serializer=_serialize_numpy_masked_array,
    custom_deserializer=_deserialize_numpy_masked_array,
)

serialization_context.register_type(
    np.ma.core.MaskedConstant,
    "numpy.ma.core.MaskedConstant",
    custom_serializer=_serialize_numpy_masked_constant,
    custom_deserializer=_deserialize_numpy_masked_constant,
)

serialization_context.register_type(
    slice,
    "slice",
    custom_serializer=_serialize_python_slice,
    custom_deserializer=_deserialize_python_slice,
)
