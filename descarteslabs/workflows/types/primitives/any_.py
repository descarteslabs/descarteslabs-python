from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import Proxytype, ProxyTypeError
from .bool_ import Bool
from .number import Int


@serializable()
class Any(Proxytype):
    "Generic Proxytype that supports almost all operations."

    def __init__(self, value):
        self.graft = client.value_graft(value)

    @classmethod
    def _promote(cls, obj):
        try:
            return cls(obj)
        except TypeError:  # pragma: no cover
            raise ProxyTypeError(
                "Can only promote delayed-like objects or compatible primitives to Any, "
                "not {!r}".format(obj)
            )

    def __call__(self, *args, **kwargs):
        raise TypeError(
            "'Any' is not a callable type. Try casting it to the type it should be via `.cast`."
        )

    def cast(self, cls):
        "Convert this `Any` to another type."
        return self._cast(cls)

    # Binary comparators
    def __lt__(self, other):
        return Bool._from_apply("lt", self, other)

    def __le__(self, other):
        return Bool._from_apply("le", self, other)

    def __eq__(self, other):
        return Bool._from_apply("eq", self, other)

    def __ne__(self, other):
        return Bool._from_apply("ne", self, other)

    def __gt__(self, other):
        return Bool._from_apply("gt", self, other)

    def __ge__(self, other):
        return Bool._from_apply("ge", self, other)

    # Bitwise operators
    def __invert__(self):
        return self._from_apply("invert", self)

    def __and__(self, other):
        return self._from_apply("and", self, other)

    def __or__(self, other):
        return self._from_apply("or", self, other)

    def __xor__(self, other):
        return self._from_apply("xor", self, other)

    def __lshift__(self, other):
        return self._from_apply("lshift", self, other)

    def __rshift__(self, other):
        return self._from_apply("rshift", self, other)

    # Reflected bitwise operators
    def __rand__(self, other):
        return self._from_apply("and", other, self)

    def __ror__(self, other):
        return self._from_apply("or", other, self)

    def __rxor__(self, other):
        return self._from_apply("xor", other, self)

    def __rlshift__(self, other):
        return self._from_apply("lshift", other, self)

    def __rrshift__(self, other):
        return self._from_apply("rshift", other, self)

    # Arithmetic operators
    def __neg__(self):
        return self._from_apply("neg", self)

    def __pos__(self):
        return self._from_apply("pos", self)

    def __abs__(self):
        return self._from_apply("abs", self)

    def __add__(self, other):
        return self._from_apply("add", self, other)

    def __sub__(self, other):
        return self._from_apply("sub", self, other)

    def __mul__(self, other):
        return self._from_apply("mul", self, other)

    def __div__(self, other):
        return self._from_apply("div", self, other)

    def __truediv__(self, other):
        return self._from_apply("div", self, other)

    def __floordiv__(self, other):
        return self._from_apply("floordiv", self, other)

    def __mod__(self, other):
        return self._from_apply("mod", self, other)

    def __pow__(self, other):
        return self._from_apply("pow", self, other)

    def __divmod__(self, other):
        from ..containers import Tuple

        return Tuple[Any, Any]._from_apply("divmod", self, other)

    # Reflected arithmetic operators
    def __radd__(self, other):
        return self._from_apply("add", other, self)

    def __rsub__(self, other):
        return self._from_apply("sub", other, self)

    def __rmul__(self, other):
        return self._from_apply("mul", other, self)

    def __rdiv__(self, other):
        return self._from_apply("div", other, self)

    def __rtruediv__(self, other):
        return self._from_apply("truediv", other, self)

    def __rfloordiv__(self, other):
        return self._from_apply("floordiv", other, self)

    def __rmod__(self, other):
        return self._from_apply("mod", other, self)

    def __rpow__(self, other):
        return self._from_apply("pow", other, self)

    def __rdivmod__(self, other):
        from ..containers import Tuple

        return Tuple[Any, Any]._from_apply("divmod", other, self)

    # Mapping operators
    def __getitem__(self, item):
        return self._from_apply("getitem", self, item=item)

    def __getattr__(self, attr):
        return self._from_apply("getattr", self, attr=attr)

    # Sequence operators
    def __reversed__(self):
        return self._from_apply("reversed", self)

    def contains(self, other):
        """
        Contains is equivalient to the Python ``in`` operator.

        Parameters
        ----------
        other : Proxytype
            A Proxytype or type that can be promoted to a Proxytype

        Returns
        -------
        Boolean
            A Boolean ProxyType
        """
        return Bool._from_apply("contains", self, other)

    def length(self):
        """Length is equivalent to the Python ``len`` operator.

        Returns
        -------
        Int
            An Int proxytype
        """
        return Int._from_apply("length", self)
