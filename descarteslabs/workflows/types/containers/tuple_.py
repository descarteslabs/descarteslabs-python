import operator

from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import ProxyTypeError, GenericProxytype, typecheck_promote
from ..proxify import proxify
from ..primitives import Any, Int, Bool

from ._check_valid_binop import check_valid_binop_for

try:
    # only after py3.4
    from collections import abc
except ImportError:
    import collections as abc


@serializable()
class Tuple(GenericProxytype):
    """
    ``Tuple[item1_type, item2_type, ...]``: Proxy sequence of a fixed number of elements of specific types.
    A type must be specified for each item in the tuple.

    Can be instantiated from any Python iterable.

    Examples
    --------
    >>> from descarteslabs.workflows import Tuple, Int, Float, Str, Bool
    >>> Tuple[Int, Float]([1, 2.2]) # 2-tuple of Int and Float
    <descarteslabs.workflows.types.containers.tuple_.Tuple[Int, Float] object at 0x...>
    >>> Tuple[Int]([1]) # 1-tuple of Int (NOT a variable length tuple of Ints)
    <descarteslabs.workflows.types.containers.tuple_.Tuple[Int] object at 0x...>
    >>> Tuple[Float, Float, Float]([1.1, 2.2, 3.3]) # 3-tuple of Floats
    <descarteslabs.workflows.types.containers.tuple_.Tuple[Float, Float, Float] object at 0x...>
    >>> Tuple[Str, Tuple[Int, Bool]](["foo", (1, True)]) # 2-tuple of Str and 2-tuple of Int and Bool
    <descarteslabs.workflows.types.containers.tuple_.Tuple[Str, Tuple[Int, Bool]] object at 0x...>

    >>> from descarteslabs.workflows import Tuple, Str
    >>> my_tuple = Tuple[Str, Str](["hello", "world"])
    >>> my_tuple
    <descarteslabs.workflows.types.containers.tuple_.Tuple[Str, Str] object at 0x...>
    >>> my_tuple.compute() # doctest: +SKIP
    ('hello', 'world')
    >>> my_tuple[0].compute() # doctest: +SKIP
    'hello'

    >>> from descarteslabs.workflows import Tuple, Int, Float, Str
    >>> tuple_a = Tuple[Int, Float]([1, 2.2])
    >>> tuple_b = Tuple[Str, Float](["foo", 3.3])
    >>> tuple_a + tuple_b
    <descarteslabs.workflows.types.containers.tuple_.Tuple[Int, Float, Str, Float] object at 0x...>
    >>> (tuple_a + ("x", False)).compute() # doctest: +SKIP
    (1, 2.2, "x", False)
    """

    def __init__(self, iterable):
        value_types = self._type_params
        if value_types is None:
            raise TypeError(
                "Cannot instantiate a generic Tuple; the item types must be specified (like `Tuple[Int, Float]`)"
            )
        # TODO: copy constructor
        if not isinstance(iterable, abc.Iterable):
            raise ProxyTypeError("Expected an iterable, got {}".format(iterable))

        try:
            length = len(iterable)
        except TypeError:
            iterable = tuple(iterable)
            length = len(iterable)

        if length != len(self._type_params):
            raise ProxyTypeError(
                "To construct {}, expected an iterable of {} items, "
                "but got {} items".format(
                    type(self).__name__, len(self._type_params), length
                )
            )

        def checker_promoter(i, x):
            cls = value_types[i]
            try:
                return cls._promote(x)
            except ProxyTypeError:
                raise ProxyTypeError(
                    "While constructing {}, expected {} for tuple element {}, but got {!r}".format(
                        type(self).__name__, cls, i, x
                    )
                )

        iterable = tuple(checker_promoter(i, x) for i, x in enumerate(iterable))
        self.graft = client.apply_graft("tuple", *iterable)

    def __getitem__(self, item):
        # TODO(gabe): slices
        # TODO(gabe): cache
        # TODO(gabe): no nested traceback on out-of-bounds index?
        try:
            promoted_item = Int._promote(item)
        except ProxyTypeError:
            raise ProxyTypeError(
                "Tuple indicies must be integers, not {}".format(type(item))
            )
        item_type = self._type_params[item] if isinstance(item, int) else Any
        # ^ if `idx` is a proxy Int, we don't know which index we're selecting and therefore the return type
        return item_type._from_apply("getitem", self, promoted_item)

    def __len__(self):
        try:
            return len(self._type_params)
        except TypeError:
            raise TypeError(
                "Generic Tuple has no length; must be parameterized with item types"
            )

    def length(self):
        """Length is equivalent to the Python ``len`` operator.

        Returns
        -------
        Int
            An Int Proxytype

        Example
        -------
        >>> from descarteslabs.workflows import Tuple, Str
        >>> my_tuple = Tuple[Str, Str, Str](("foo", "bar", "baz"))
        >>> my_tuple.length().compute() # doctest: +SKIP
        3
        """
        return Int._from_apply("length", self)

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    @typecheck_promote(lambda self: type(self))
    def __lt__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.lt,
                elemtype,
                f"Operator `<` invalid for element {elemtype.__name__} in {type(self).__name__}",
            )
        return Bool._from_apply("lt", self, other)

    @typecheck_promote(lambda self: type(self))
    def __le__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.le,
                elemtype,
                f"Operator `<=` invalid for element {elemtype.__name__} in {type(self).__name__}",
            )
        return Bool._from_apply("le", self, other)

    @typecheck_promote(lambda self: type(self))
    def __gt__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.gt,
                elemtype,
                f"Operator `>` invalid for element {elemtype.__name__} in {type(self).__name__}",
            )
        return Bool._from_apply("gt", self, other)

    @typecheck_promote(lambda self: type(self))
    def __ge__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.ge,
                elemtype,
                f"Operator `>=` invalid for element {elemtype.__name__} in {type(self).__name__}",
            )
        return Bool._from_apply("ge", self, other)

    @typecheck_promote(lambda self: type(self))
    def __eq__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.eq,
                elemtype,
                f"Operator `==` invalid for element {elemtype.__name__} in {type(self).__name__}",
            )
        return Bool._from_apply("eq", self, other)

    @typecheck_promote(lambda self: type(self))
    def __ne__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.ne,
                elemtype,
                f"Operator `!=` invalid for element {elemtype.__name__} in {type(self).__name__}",
            )
        return Bool._from_apply("ne", self, other)

    def __add__(self, other):
        if isinstance(other, Tuple):
            concat_type = Tuple[self._type_params + other._type_params]
        elif isinstance(other, tuple):
            try:
                proxified = proxify(other)
            except NotImplementedError:
                return NotImplemented
            return self + proxified
        else:
            return NotImplemented

        return concat_type._from_apply("add", self, other)

    def __radd__(self, other):
        if isinstance(other, Tuple):
            concat_type = Tuple[other._type_params + self._type_params]
        elif isinstance(other, tuple):
            try:
                proxified = proxify(other)
            except NotImplementedError:
                return NotImplemented
            return proxified + self
        else:
            return NotImplemented

        return concat_type._from_apply("add", other, self)
