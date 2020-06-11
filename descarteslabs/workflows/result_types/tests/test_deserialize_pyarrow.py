import datetime

import pyarrow as pa

from descarteslabs.common.workflows.arrow_serialization import serialization_context

from ..deserialize_pyarrow import deserialize_pyarrow


def test_deserialize_pyarrow_basic():
    result = 2
    result_type = "Int"

    codec = "lz4"
    buffer = pa.serialize(result, context=serialization_context).to_buffer()
    serialized = pa.compress(buffer, codec=codec, asbytes=True)

    assert deserialize_pyarrow(serialized, codec, len(buffer), result_type) == result


def test_deserialize_pyarrow_unmarshal():
    result = datetime.datetime.now()
    result_type = "Datetime"

    codec = "lz4"
    marshalled = result.isoformat()
    buffer = pa.serialize(marshalled, context=serialization_context).to_buffer()
    serialized = pa.compress(buffer, codec=codec, asbytes=True)

    assert deserialize_pyarrow(serialized, codec, len(buffer), result_type) == result
