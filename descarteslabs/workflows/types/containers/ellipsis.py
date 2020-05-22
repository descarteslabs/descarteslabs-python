from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import Proxytype, ProxyTypeError


@serializable()
class Ellipsis(Proxytype):
    """
    Proxy Ellipsis object.
    You shouldn't use this directly; Python ellipsis can be used to achieve the same result.
    """

    def __init__(self):
        self.graft = client.keyref_graft("wf.Ellipsis")

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj
        elif isinstance(obj, type(Ellipsis)):
            return cls()
        else:
            raise ProxyTypeError("Cannot promote {!r} to {}".format(obj, cls.__name__))

    def compute(self, *args, **kwargs):
        raise TypeError("{} cannot be computed directly.".format(type(self).__name__))
