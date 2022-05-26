import pyarrow as pa

from .context import serialization_context


def serialize_pyarrow(data: bytes, codec: str):
    """
    Serialize an object and compress with a specific codec. Returns the
    serialized, compressed bytes in a pyarrow.Buffer. The caller is
    responsible for reading the returned bytes into a file, if necessary.

    Should be used in conjunction with `deserialize_pyarrow`.
    """
    raw = pa.BufferOutputStream()
    with pa.CompressedOutputStream(raw, compression=codec) as compressed:
        pa.serialize_to(data, compressed, context=serialization_context)
    return raw.getvalue()


def deserialize_pyarrow(data: bytes, codec: str):
    """
    Deserialize and decompress an object with a specific codec. The caller is
    responsible for unmarshalling the results, if neccessary.

    Should be used in conjunction with `serialize_pyarrow`.
    """
    reader = pa.BufferReader(data)
    with pa.CompressedInputStream(reader, compression=codec) as compressed:
        deserialized = pa.deserialize(compressed.read(), context=serialization_context)
    return deserialized
