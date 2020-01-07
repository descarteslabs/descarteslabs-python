from ...cereal import serializable
from ..core import ProxyTypeError, typecheck_promote
from .bool_ import Bool  # noqa TODO remove noqa later
from .primitive import Primitive


def _binop_result(a, b):
    return Float if isinstance(a, Float) or isinstance(b, Float) else Int


@serializable()
class Number(Primitive):
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
        if isinstance(obj, Number) and not self._is_generic():
            if isinstance(obj, type(self)):
                self.graft = obj.graft
            else:
                self.graft = self._from_apply(
                    "{}._cast".format(self.__class__.__name__), obj
                ).graft
        else:
            super(Number, self).__init__(obj)

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj
        elif isinstance(obj, Number):
            # Another Number that isn't our type:
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
        return self._from_apply("abs", self)

    @typecheck_promote(lambda: (Int, Float))
    def __add__(self, other):
        return _binop_result(self, other)._from_apply("add", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __div__(self, other):
        return Float._from_apply("div", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __divmod__(self, other):
        from ..containers import Tuple

        restype = _binop_result(self, other)
        return Tuple[restype, restype]._from_apply("divmod", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __eq__(self, other):
        return Bool._from_apply("eq", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __floordiv__(self, other):
        return _binop_result(self, other)._from_apply("floordiv", self, other)

    def __hex__(self):
        raise TypeError(
            ("Cannot convert {} to Python string.").format(self.__class__.__name__)
        )

    @typecheck_promote(lambda: (Int, Float))
    def __ge__(self, other):
        return Bool._from_apply("ge", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __gt__(self, other):
        return Bool._from_apply("gt", self, other)

    def __index__(self):
        raise TypeError(
            (
                "Cannot convert {} to Python int. A proxy type can "
                "only be used to slice other proxy types containers."
            ).format(self.__class__.__name__)
        )

    def __invert__(self):
        return self._from_apply("invert", self)

    @typecheck_promote(lambda: (Int, Float))
    def __le__(self, other):
        return Bool._from_apply("le", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __lt__(self, other):
        return Bool._from_apply("lt", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __mod__(self, other):
        return _binop_result(self, other)._from_apply("mod", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __mul__(self, other):
        return _binop_result(self, other)._from_apply("mul", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __ne__(self, other):
        return Bool._from_apply("ne", self, other)

    def __neg__(self):
        return self._from_apply("neg", self)

    def __pos__(self):
        return self._from_apply("pos", self)

    @typecheck_promote(lambda: (Int, Float))
    def __pow__(self, other):
        return _binop_result(self, other)._from_apply("pow", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __radd__(self, other):
        return _binop_result(self, other)._from_apply("add", other, self)

    @typecheck_promote(lambda: (Int, Float))
    def __rdiv__(self, other):
        return Float._from_apply("div", other, self)

    @typecheck_promote(lambda: (Int, Float))
    def __rdivmod__(self, other):
        from ..containers import Tuple

        restype = _binop_result(self, other)
        return Tuple[restype, restype]._from_apply("divmod", other, self)

    @typecheck_promote(lambda: (Int, Float))
    def __rfloordiv__(self, other):
        return _binop_result(self, other)._from_apply("floordiv", other, self)

    @typecheck_promote(lambda: (Int, Float))
    def __rmod__(self, other):
        return _binop_result(self, other)._from_apply("mod", other, self)

    @typecheck_promote(lambda: (Int, Float))
    def __rmul__(self, other):
        return _binop_result(self, other)._from_apply("mul", other, self)

    @typecheck_promote(lambda: (Int, Float))
    def __rpow__(self, other):
        return _binop_result(self, other)._from_apply("pow", other, self)

    @typecheck_promote(lambda: (Int, Float))
    def __rsub__(self, other):
        return _binop_result(self, other)._from_apply("sub", other, self)

    @typecheck_promote(lambda: (Int, Float))
    def __rtruediv__(self, other):
        return Float._from_apply("div", other, self)

    @typecheck_promote(lambda: (Int, Float))
    def __sub__(self, other):
        return _binop_result(self, other)._from_apply("sub", self, other)

    @typecheck_promote(lambda: (Int, Float))
    def __truediv__(self, other):
        return Float._from_apply("div", self, other)


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

    @typecheck_promote(lambda: Int)
    def __and__(self, other):
        return self._from_apply("and", self, other)

    @typecheck_promote(lambda: Int)
    def __lshift__(self, other):
        return self._from_apply("lshift", self, other)

    @typecheck_promote(lambda: Int)
    def __or__(self, other):
        return self._from_apply("or", self, other)

    @typecheck_promote(lambda: Int)
    def __rand__(self, other):
        return self._from_apply("and", other, self)

    @typecheck_promote(lambda: Int)
    def __rlshift__(self, other):
        return self._from_apply("lshift", other, self)

    @typecheck_promote(lambda: Int)
    def __ror__(self, other):
        return self._from_apply("or", other, self)

    @typecheck_promote(lambda: Int)
    def __rrshift__(self, other):
        return self._from_apply("rshift", other, self)

    @typecheck_promote(lambda: Int)
    def __rshift__(self, other):
        return self._from_apply("rshift", self, other)

    @typecheck_promote(lambda: Int)
    def __rxor__(self, other):
        return self._from_apply("xor", other, self)

    @typecheck_promote(lambda: Int)
    def __xor__(self, other):
        return self._from_apply("xor", self, other)


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
