import numpy as np

from ..core import Proxytype, ProxyTypeError, allow_reflect
from ..mixins import NumPyMixin
from ...cereal import serializable

from descarteslabs.common.graft import client


def _delayed_numpy_ufuncs():
    # avoid circular imports
    from descarteslabs.workflows.types.numpy import numpy_ufuncs

    return numpy_ufuncs


PY_TYPE = {np.int64: int, np.float64: float, np.bool: bool, np.bool_: bool}


@serializable()
class Scalar(Proxytype, NumPyMixin):
    """
    Proxy Scalar object

    Internal class used as the return type for Array operations that return
    scalar values

    You shouldn't use this class directly. Instead, use the Int, Float, or
    Bool classes.
    """

    def __init__(self, obj):
        if isinstance(obj, type(self)):
            self.graft = obj.graft
        elif isinstance(obj, (np.int64, np.float64, np.bool, np.bool_)):
            cast = PY_TYPE[type(obj)](obj)
            self.graft = client.apply_graft("wf.scalar.create", cast)
        else:
            raise ProxyTypeError("Cannot instantiate a Scalar from {}.".format(obj))

    def __neg__(self):
        return _delayed_numpy_ufuncs().negative(self)

    def __pos__(self):
        return self._from_apply("wf.pos", self)

    def __abs__(self):
        return _delayed_numpy_ufuncs().absolute(self)

    @allow_reflect
    def __lt__(self, other):
        return _delayed_numpy_ufuncs().less(self, other)

    @allow_reflect
    def __le__(self, other):
        return _delayed_numpy_ufuncs().less_equal(self, other)

    @allow_reflect
    def __gt__(self, other):
        return _delayed_numpy_ufuncs().greater(self, other)

    @allow_reflect
    def __ge__(self, other):
        return _delayed_numpy_ufuncs().greater_equal(self, other)

    @allow_reflect
    def __eq__(self, other):
        return _delayed_numpy_ufuncs().equal(self, other)

    @allow_reflect
    def __ne__(self, other):
        return _delayed_numpy_ufuncs().not_equal(self, other)

    @allow_reflect
    def __add__(self, other):
        return _delayed_numpy_ufuncs().add(self, other)

    @allow_reflect
    def __sub__(self, other):
        return _delayed_numpy_ufuncs().subtract(self, other)

    @allow_reflect
    def __mul__(self, other):
        return _delayed_numpy_ufuncs().multiply(self, other)

    @allow_reflect
    def __div__(self, other):
        return _delayed_numpy_ufuncs().divide(self, other)

    @allow_reflect
    def __floordiv__(self, other):
        return _delayed_numpy_ufuncs().floor_divide(self, other)

    @allow_reflect
    def __truediv__(self, other):
        return _delayed_numpy_ufuncs().true_divide(self, other)

    @allow_reflect
    def __mod__(self, other):
        return _delayed_numpy_ufuncs().mod(self, other)

    @allow_reflect
    def __pow__(self, other):
        return _delayed_numpy_ufuncs().power(self, other)

    @allow_reflect
    def __radd__(self, other):
        return _delayed_numpy_ufuncs().add(other, self)

    @allow_reflect
    def __rsub__(self, other):
        return _delayed_numpy_ufuncs().subtract(other, self)

    @allow_reflect
    def __rmul__(self, other):
        return _delayed_numpy_ufuncs().multiply(other, self)

    @allow_reflect
    def __rdiv__(self, other):
        return _delayed_numpy_ufuncs().divide(other, self)

    @allow_reflect
    def __rfloordiv__(self, other):
        return _delayed_numpy_ufuncs().floor_divide(other, self)

    @allow_reflect
    def __rtruediv__(self, other):
        return _delayed_numpy_ufuncs().true_divide(other, self)

    @allow_reflect
    def __rmod__(self, other):
        return _delayed_numpy_ufuncs().mod(other, self)

    @allow_reflect
    def __rpow__(self, other):
        return _delayed_numpy_ufuncs().power(other, self)
