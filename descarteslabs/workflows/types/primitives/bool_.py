from ...cereal import serializable
from ..core import allow_reflect
from .primitive import Primitive
from ..mixins import NumPyMixin


def _delayed_numpy_overrides():
    # avoid circular imports
    from descarteslabs.workflows.types.numpy import numpy_overrides

    return numpy_overrides


@serializable()
class Bool(NumPyMixin, Primitive):
    """
    Proxy boolean.

    Note that this cannot be compared with Python's ``and`` and ``or`` operators;
    you must use the bitwise operators ``&`` and ``|``. Also note that more parenthesis are needed
    with bitwise operators than with ``and`` and ``or``.

    Examples
    --------
    >>> from descarteslabs.workflows import Bool
    >>> my_bool = Bool(True)
    >>> my_bool
    <descarteslabs.workflows.types.primitives.bool_.Bool object at 0x...>
    >>> other_bool = Bool(False)
    >>> val = my_bool | other_bool
    >>> val.compute() # doctest: +SKIP
    True
    """

    _pytype = bool

    def __bool__(self):
        raise TypeError(
            "Conditionals and Python binary operators (like `and` and `or`) "
            "are not supported on Proxytype {} objects. "
            "Instead, use bitwise operators (like & and |). "
            "Don't forget extra parenthesis around the expressions you're comparing, "
            "since the precedence of bitwise operators "
            "is lower than that of `and` and `or`.".format(type(self).__name__)
        )

    def __invert__(self):
        return _delayed_numpy_overrides().logical_not(self)

    @allow_reflect
    def __eq__(self, other):
        return _delayed_numpy_overrides().equal(self, other)

    @allow_reflect
    def __ne__(self, other):
        return _delayed_numpy_overrides().not_equal(self, other)

    @allow_reflect
    def __and__(self, other):
        return _delayed_numpy_overrides().logical_and(self, other)

    @allow_reflect
    def __or__(self, other):
        return _delayed_numpy_overrides().logical_or(self, other)

    @allow_reflect
    def __xor__(self, other):
        return _delayed_numpy_overrides().logical_xor(self, other)

    @allow_reflect
    def __rand__(self, other):
        return _delayed_numpy_overrides().logical_and(other, self)

    @allow_reflect
    def __ror__(self, other):
        return _delayed_numpy_overrides().logical_or(other, self)

    @allow_reflect
    def __rxor__(self, other):
        return _delayed_numpy_overrides().logical_xor(other, self)
