from ...cereal import serializable
from ..core import typecheck_promote
from .primitive import Primitive


@serializable()
class Bool(Primitive):
    "Proxy bool"
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

    __nonzero__ = __bool__  # for python 2

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
        return self._from_apply("rand", self, other)

    @typecheck_promote(lambda: Bool)
    def __ror__(self, other):
        return self._from_apply("ror", self, other)

    @typecheck_promote(lambda: Bool)
    def __rxor__(self, other):
        return self._from_apply("rxor", self, other)
