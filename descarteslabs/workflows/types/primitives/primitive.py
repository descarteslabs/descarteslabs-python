from descarteslabs.common.graft import client
from ..core import Proxytype, ProxyTypeError


class Primitive(Proxytype):
    """
    Proxy wrapper around a Python primitive type.

    Do not use Primitive directly; instead, use one of the built-in subtypes (Int, Str, etc.)
    """

    _pytype = None

    def __init__(self, obj):
        if self._is_generic():
            raise ProxyTypeError(
                "Cannot instantiate a generic {}; use a concrete subclass".format(
                    type(self).__name__
                )
            )

        from .any_ import Any  # TODO circular import

        if isinstance(obj, (type(self), Any)):
            self.graft = obj.graft
        else:
            if not isinstance(obj, self._pytype):
                raise ProxyTypeError(
                    "Cannot promote {} to {}".format(type(obj), type(self))
                )
            self.graft = client.value_graft(obj)

    @classmethod
    def _promote(cls, obj):
        return cls(obj)

    def _is_generic(self):
        return self._pytype is None
