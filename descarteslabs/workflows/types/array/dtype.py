import numpy as np

from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import Proxytype, ProxyTypeError
from ..primitives import Int, Float, Bool


@serializable()
class DType(Proxytype):
    """
    A Proxy object for representing the data type of an Array/MaskedArray.

    Should not be worked with directly.
    """

    def __init__(self, type_):
        if isinstance(type_, np.dtype):
            val = type_.char
        elif type_ in (
            int,
            np.int,
            np.int_,
            np.int8,
            np.int16,
            np.int32,
            np.int64,
            Int,
            float,
            np.float,
            np.float_,
            np.float32,
            np.float64,
            Float,
            bool,
            np.bool,
            np.bool_,
            Bool,
        ):
            val = type_.__name__.lower()
        else:
            raise ValueError(
                "Cannot construct a DType object from {}. Must be a "
                "NumPy dtype, NumPy type, Python type, or proxy type.".format(type_)
            )
        self.graft = client.apply_graft("wf.dtype.create", val)

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj

        try:
            return obj.cast(cls)
        except Exception:
            try:
                return DType(obj)
            except Exception:
                raise ProxyTypeError("Cannot promote {} to DType".format(obj))
