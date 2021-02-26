import numpy as np

from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import ProxyTypeError, typecheck_promote, allow_reflect
from ..mixins import NumPyMixin
from .bool_ import Bool
from .primitive import Primitive


def _delayed_numpy_ufuncs():
    # avoid circular imports
    from descarteslabs.workflows.types.numpy import numpy_ufuncs

    return numpy_ufuncs


def _binop_result(a, b):
    return Float if isinstance(a, Float) or isinstance(b, Float) else Int


@serializable()
class Number(NumPyMixin, Primitive):
    """
    Abstract base class for numeric Proxytypes.

    Use the concrete subtypes `Int` and `Float` instead;
    `Number` cannot be instantiated and should only be used for
    ``isinstance()`` checks.

    You can explicitly construct one numeric type from another,
    performing a cast (``Int(Float(4.2))``), but one numeric
    type will not implicitly promote to another (``Int._promote(Float(4.2))``
    will fail).
    """

    def __init__(self, obj):
        from .string import Str

        if (
            isinstance(obj, Number)
            and not self._is_generic()
            or isinstance(obj, (Bool, Str))
        ):
            self.params = obj.params
            if isinstance(obj, type(self)):
                self.graft = obj.graft
            else:
                self.graft = client.apply_graft(
                    "wf.{}.cast".format(self.__class__.__name__), obj
                )
                self.params = obj.params
        else:
            if isinstance(obj, np.generic):
                obj = obj.tolist()
            super(Number, self).__init__(obj)

    @classmethod
    def _promote(cls, obj):
        from .string import Str

        if isinstance(obj, cls):
            return obj
        elif isinstance(obj, (Number, Str)):
            # Another Number that isn't our type, or a string:
            # we won't auto-convert it
            raise ProxyTypeError(
                "Cannot promote {} to {}. "
                "You need to convert it explicitly, like `{}(x)`".format(
                    type(obj), cls, cls.__name__
                )
            )
        else:
            return cls(obj)

    def __abs__(self):
        return _delayed_numpy_ufuncs().absolute(self)

    @allow_reflect
    def __add__(self, other):
        return _delayed_numpy_ufuncs().add(self, other)

    @allow_reflect
    def __div__(self, other):
        return _delayed_numpy_ufuncs().divide(self, other)

    @typecheck_promote(lambda: (Int, Float, Bool), _reflect=True)
    def __divmod__(self, other):
        from ..containers import Tuple

        restype = _binop_result(self, other)
        return Tuple[restype, restype]._from_apply("wf.divmod", self, other)

    @allow_reflect
    def __eq__(self, other):
        return _delayed_numpy_ufuncs().equal(self, other)

    @allow_reflect
    def __floordiv__(self, other):
        return _delayed_numpy_ufuncs().floor_divide(self, other)

    def __hex__(self):
        raise TypeError(
            ("Cannot convert {} to Python string.").format(self.__class__.__name__)
        )

    @allow_reflect
    def __ge__(self, other):
        return _delayed_numpy_ufuncs().greater_equal(self, other)

    @allow_reflect
    def __gt__(self, other):
        return _delayed_numpy_ufuncs().greater(self, other)

    def __index__(self):
        raise TypeError(
            (
                "Cannot convert {} to Python int. A proxy type can "
                "only be used to slice other proxy types containers."
            ).format(self.__class__.__name__)
        )

    def __invert__(self):
        return self._from_apply("wf.invert", self)

    @allow_reflect
    def __le__(self, other):
        return _delayed_numpy_ufuncs().less_equal(self, other)

    @allow_reflect
    def __lt__(self, other):
        return _delayed_numpy_ufuncs().less(self, other)

    @allow_reflect
    def __mod__(self, other):
        return _delayed_numpy_ufuncs().mod(self, other)

    @allow_reflect
    def __mul__(self, other):
        return _delayed_numpy_ufuncs().multiply(self, other)

    @allow_reflect
    def __ne__(self, other):
        return _delayed_numpy_ufuncs().not_equal(self, other)

    def __neg__(self):
        return _delayed_numpy_ufuncs().negative(self)

    def __pos__(self):
        return self._from_apply("wf.pos", self)

    @allow_reflect
    def __pow__(self, other):
        return _delayed_numpy_ufuncs().power(self, other)

    @allow_reflect
    def __radd__(self, other):
        return _delayed_numpy_ufuncs().add(other, self)

    @allow_reflect
    def __rdiv__(self, other):
        return _delayed_numpy_ufuncs().divide(other, self)

    @typecheck_promote(lambda: (Int, Float, Bool))
    def __rdivmod__(self, other):
        from ..containers import Tuple

        restype = _binop_result(self, other)
        return Tuple[restype, restype]._from_apply("wf.divmod", other, self)

    @allow_reflect
    def __rfloordiv__(self, other):
        return _delayed_numpy_ufuncs().floor_divide(other, self)

    @allow_reflect
    def __rmod__(self, other):
        return _delayed_numpy_ufuncs().mod(other, self)

    @allow_reflect
    def __rmul__(self, other):
        return _delayed_numpy_ufuncs().multiply(other, self)

    @allow_reflect
    def __rpow__(self, other):
        return _delayed_numpy_ufuncs().power(other, self)

    @allow_reflect
    def __rsub__(self, other):
        return _delayed_numpy_ufuncs().subtract(other, self)

    @allow_reflect
    def __rtruediv__(self, other):
        return _delayed_numpy_ufuncs().true_divide(other, self)

    @allow_reflect
    def __sub__(self, other):
        return _delayed_numpy_ufuncs().subtract(self, other)

    @allow_reflect
    def __truediv__(self, other):
        return _delayed_numpy_ufuncs().true_divide(self, other)


@serializable()
class Int(Number):
    """
    Proxy integer.

    Examples
    --------
    >>> from descarteslabs.workflows import Int
    >>> my_int = Int(2)
    >>> my_int
    <descarteslabs.workflows.types.primitives.number.Int object at 0x...>
    >>> other_int = Int(5)
    >>> val = my_int + other_int
    >>> val.compute() # doctest: +SKIP
    7
    >>> val = my_int < other_int
    >>> val.compute() # doctest: +SKIP
    True
    """

    _pytype = int

    @typecheck_promote(lambda: (Int, Bool), _reflect=True)
    def __and__(self, other):
        return self._from_apply("wf.and", self, other)

    @typecheck_promote(lambda: Int, _reflect=True)
    def __lshift__(self, other):
        return self._from_apply("wf.lshift", self, other)

    @typecheck_promote(lambda: (Int, Bool), _reflect=True)
    def __or__(self, other):
        return self._from_apply("wf.or", self, other)

    @typecheck_promote(lambda: (Int, Bool))
    def __rand__(self, other):
        return self._from_apply("wf.and", other, self)

    @typecheck_promote(lambda: Int)
    def __rlshift__(self, other):
        return self._from_apply("wf.lshift", other, self)

    @typecheck_promote(lambda: (Int, Bool))
    def __ror__(self, other):
        return self._from_apply("wf.or", other, self)

    @typecheck_promote(lambda: Int)
    def __rrshift__(self, other):
        return self._from_apply("wf.rshift", other, self)

    @typecheck_promote(lambda: Int)
    def __rshift__(self, other):
        return self._from_apply("wf.rshift", self, other)

    @typecheck_promote(lambda: (Int, Bool))
    def __rxor__(self, other):
        return self._from_apply("wf.xor", other, self)

    @typecheck_promote(lambda: (Int, Bool), _reflect=True)
    def __xor__(self, other):
        return self._from_apply("wf.xor", self, other)


@serializable()
class Float(Number):
    """
    Proxy float.

    Examples
    --------
    >>> from descarteslabs.workflows import Float
    >>> my_float = Float(2.3)
    >>> my_float
    <descarteslabs.workflows.types.primitives.number.Float object at 0x...>
    >>> other_float = Float(5.6)
    >>> val = my_float + other_float
    >>> val.compute() # doctest: +SKIP
    7.9
    >>> val = my_float > other_float
    >>> val.compute() # doctest: +SKIP
    False
    """

    _pytype = float

    def __init__(self, obj):
        # handle the special case of constructing from a native python int
        # as a convenience to the user. This includes being able to
        # Float._promote() from a native python int also.
        if type(obj) is int:
            obj = Int(obj)
        super(Float, self).__init__(obj)
