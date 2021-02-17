from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import Proxytype, ProxyTypeError
from .bool_ import Bool
from .number import Int


@serializable()
class Any(Proxytype):
    """
    Represents a proxy object of an unknown type.

    `Any` is most commonly encountered when accessing unknown fields from metadata
    (for example, ``image.properties["foobar"]``). `Any` should generally be converted
    to the correct type with the `cast` method. However for convenience,
    `Any` also supports binary operators.

    .. code-block:: python

        x = image.properties["x"]  # Any
        y = image.properties["y"]  # Any
        # either of these work; the first is better practice
        result = x.cast(Int) + y.cast(Float)
        result = x + y

    Note that cast is simply changing the type representation in your client;
    the actual object on the backend isn't changed. It's up to you to cast to the correct Proxytype.
    At compute time, there is no check that you cast to the actual type that exists on the backend--
    though if you cast to the wrong type, subsequent operations will probably fail.

    `Any` can also be constructed with any JSON-serializable value, or other ProxyType, though there is
    little reason to do this.

    Examples
    --------
    >>> from descarteslabs.workflows import Any, Int
    >>> my_any = Any(2)
    >>> my_int = my_any.cast(Int) # cast to an Int
    >>> my_int
    <descarteslabs.workflows.types.primitives.number.Int object at 0x...>
    >>> my_int.compute() # doctest: +SKIP
    2

    >>> from descarteslabs.workflows import Any, List, Int
    >>> my_any = Any([1, 2, 3])
    >>> my_list = my_any.cast(List[Int]) # cast to a list of ints
    >>> my_list
    <descarteslabs.workflows.types.containers.list_.List[Int] object at 0x...>
    >>> my_list[0].compute() # doctest: +SKIP
    1

    >>> from descarteslabs.workflows import Any, Int
    >>> my_any = Any(2)
    >>> other_any = Any(3)
    >>> val = my_any * other_any
    >>> val.cast(Int).compute() # doctest: +SKIP
    6
    """

    def __init__(self, value):
        self.graft = client.value_graft(value)
        self.params = getattr(value, "params", ())

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
        return Bool._from_apply("wf.lt", self, other)

    def __le__(self, other):
        return Bool._from_apply("wf.le", self, other)

    def __eq__(self, other):
        return Bool._from_apply("wf.eq", self, other)

    def __ne__(self, other):
        return Bool._from_apply("wf.ne", self, other)

    def __gt__(self, other):
        return Bool._from_apply("wf.gt", self, other)

    def __ge__(self, other):
        return Bool._from_apply("wf.ge", self, other)

    # Bitwise operators
    def __invert__(self):
        return self._from_apply("wf.invert", self)

    def __and__(self, other):
        return self._from_apply("wf.and", self, other)

    def __or__(self, other):
        return self._from_apply("wf.or", self, other)

    def __xor__(self, other):
        return self._from_apply("wf.xor", self, other)

    def __lshift__(self, other):
        return self._from_apply("wf.lshift", self, other)

    def __rshift__(self, other):
        return self._from_apply("wf.rshift", self, other)

    # Reflected bitwise operators
    def __rand__(self, other):
        return self._from_apply("wf.and", other, self)

    def __ror__(self, other):
        return self._from_apply("wf.or", other, self)

    def __rxor__(self, other):
        return self._from_apply("wf.xor", other, self)

    def __rlshift__(self, other):
        return self._from_apply("wf.lshift", other, self)

    def __rrshift__(self, other):
        return self._from_apply("wf.rshift", other, self)

    # Arithmetic operators
    def __neg__(self):
        return self._from_apply("wf.neg", self)

    def __pos__(self):
        return self._from_apply("wf.pos", self)

    def __abs__(self):
        return self._from_apply("wf.abs", self)

    def __add__(self, other):
        return self._from_apply("wf.add", self, other)

    def __sub__(self, other):
        return self._from_apply("wf.sub", self, other)

    def __mul__(self, other):
        return self._from_apply("wf.mul", self, other)

    def __div__(self, other):
        return self._from_apply("wf.div", self, other)

    def __truediv__(self, other):
        return self._from_apply("wf.div", self, other)

    def __floordiv__(self, other):
        return self._from_apply("wf.floordiv", self, other)

    def __mod__(self, other):
        return self._from_apply("wf.mod", self, other)

    def __pow__(self, other):
        return self._from_apply("wf.pow", self, other)

    def __divmod__(self, other):
        from ..containers import Tuple

        return Tuple[Any, Any]._from_apply("wf.divmod", self, other)

    # Reflected arithmetic operators
    def __radd__(self, other):
        return self._from_apply("wf.add", other, self)

    def __rsub__(self, other):
        return self._from_apply("wf.sub", other, self)

    def __rmul__(self, other):
        return self._from_apply("wf.mul", other, self)

    def __rdiv__(self, other):
        return self._from_apply("wf.div", other, self)

    def __rtruediv__(self, other):
        return self._from_apply("wf.truediv", other, self)

    def __rfloordiv__(self, other):
        return self._from_apply("wf.floordiv", other, self)

    def __rmod__(self, other):
        return self._from_apply("wf.mod", other, self)

    def __rpow__(self, other):
        return self._from_apply("wf.pow", other, self)

    def __rdivmod__(self, other):
        from ..containers import Tuple

        return Tuple[Any, Any]._from_apply("wf.divmod", other, self)

    # Mapping operators
    def __getitem__(self, item):
        return self._from_apply("wf.get", self, item)

    def __getattr__(self, attr):
        if attr[0] == "_":
            raise AttributeError(
                "'Any' object has no attribute {!r}. Perhaps you need "
                "to change it to the correct type via `.cast`?".format(attr)
            )
        return self._from_apply("wf.getattr", self, attr)

    # Sequence operators
    def __reversed__(self):
        return self._from_apply("wf.reversed", self)

    def __iter__(self):
        raise TypeError("Any object is not iterable")

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

        Example
        -------
        >>> from descarteslabs.workflows import Any
        >>> Any([1, 2, 3]).contains(3).compute() # doctest: +SKIP
        True
        >>> Any([1, 2, 3]).contains(5).compute() # doctest: +SKIP
        False
        """
        return Bool._from_apply("wf.contains", self, other)

    def length(self):
        """Length is equivalent to the Python ``len`` operator.

        Returns
        -------
        Int
            An Int proxytype

        Example
        -------
        >>> from descarteslabs.workflows import Any
        >>> Any([5, 4, 3, 2, 1]).length().compute() # doctest: +SKIP
        5
        """
        return Int._from_apply("wf.length", self)
