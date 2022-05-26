"""Serializer public API."""

import ibis.common.exceptions as com
from ibis.config import options  # noqa: F401

from ..serialization import AstSerializer

from .client import SerializerClient


__all__ = ["compile", "connect", "verify"]


def compile(expr, params=None):
    """Compile an expression for the Serializer.

    Returns
    -------
    compiled : a ``common.proto.ibis`` message

    See Also
    --------
    ibis.expr.types.Expr.compile

    """

    # TODO need to handle params

    return AstSerializer(expr).serialize()


def verify(expr, params=None) -> bool:
    """Check if an expression can be compiled using the Serializer."""
    try:
        compile(expr, params=params)
        return True
    except com.TranslationError:
        return False


def connect(database=None, **grpc_client_kwargs) -> SerializerClient:
    """Create a SerializerClient for use with Ibis.

    Returns
    -------
    SerializerClient

    """
    return SerializerClient(database=database, **grpc_client_kwargs)
