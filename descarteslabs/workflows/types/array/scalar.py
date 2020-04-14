from ..core import Proxytype, ProxyTypeError


class Scalar(Proxytype):
    """
    Proxy Scalar object

    Internal class used as the return type for Array operations that return
    scalar values

    You shouldn't use this class directly. Instead, use the Int, Float, or
    Bool classes.
    """

    def __init__(self, obj):
        raise ProxyTypeError(
            "Cannot instantiate a Scalar directly. Use the Int, Float, or Bool classes instead."
        )
