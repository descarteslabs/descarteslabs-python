class IbisDeserializationError(Exception):
    """Base class for errors that occur during deserialization."""

    pass


class LiteralDeserializationError(IbisDeserializationError):
    """Error deserializing a literal value."""

    pass


class DataTypeError(IbisDeserializationError):
    """Error deserializing because an invalid data type was specified """

    pass


class OperationError(IbisDeserializationError):
    """Error deserializing due to an invalid operaion."""

    pass
