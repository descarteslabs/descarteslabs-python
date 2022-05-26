from ..serialization import serialize_pyarrow, deserialize_pyarrow


def test_pyarrow_serialization_basic():
    result = 2
    codec = "lz4"

    serialized = serialize_pyarrow(result, codec)
    deserialized = deserialize_pyarrow(serialized, codec)

    assert deserialized == result
