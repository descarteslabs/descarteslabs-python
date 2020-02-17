from ...cereal import serializable
from ..core import typecheck_promote
from .primitive import Primitive


@serializable()
class Bool(Primitive):
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
        return self._from_apply("not", self)

    @typecheck_promote(lambda: Bool)
    def __eq__(self, other):
        return self._from_apply("eq", self, other)

    @typecheck_promote(lambda: Bool)
    def __ne__(self, other):
        return self._from_apply("ne", self, other)

    @typecheck_promote(lambda: Bool)
    def __and__(self, other):
        return self._from_apply("and", self, other)

    @typecheck_promote(lambda: Bool)
    def __or__(self, other):
        return self._from_apply("or", self, other)

    @typecheck_promote(lambda: Bool)
    def __xor__(self, other):
        return self._from_apply("xor", self, other)

    @typecheck_promote(lambda: Bool)
    def __rand__(self, other):
        return self._from_apply("and", other, self)

    @typecheck_promote(lambda: Bool)
    def __ror__(self, other):
        return self._from_apply("or", other, self)

    @typecheck_promote(lambda: Bool)
    def __rxor__(self, other):
        return self._from_apply("xor", other, self)
