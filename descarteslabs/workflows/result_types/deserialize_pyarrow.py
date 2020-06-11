import pyarrow as pa

from descarteslabs.common.workflows.arrow_serialization import serialization_context
from . import unmarshal
from . import types  # noqa: F401 - must import to register unmarshallers


def deserialize_pyarrow(
    data: bytes, codec: str, decompressed_size: int, result_type: str
):
    buffer = pa.decompress(data, codec=codec, decompressed_size=decompressed_size)
    marshalled = pa.deserialize(buffer, context=serialization_context)
    return unmarshal.unmarshal(result_type, marshalled)
    # ^ TODO use typespec for unmarshalling!!
