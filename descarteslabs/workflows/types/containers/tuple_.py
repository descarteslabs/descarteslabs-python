import operator

from collections import abc

from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import (
    ProxyTypeError,
    GenericProxytype,
    typecheck_promote,
    assert_is_proxytype,
)
from ..proxify import proxify
from ..primitives import Any, Int, Bool
from .slice import Slice

from ._check_valid_binop import check_valid_binop_for


@serializable()
class Tuple(GenericProxytype):
    """
    ``Tuple[item1_type, item2_type, ...]``: Proxy sequence of a fixed number of elements of specific types.
    A type must be specified for each item in the tuple.

    Can be instantiated from any Python iterable.

    When indexing with a modified proxy integer, the return type will be `Tuple[Any]` because the type of
    the value at the index is unknown (the index is unknown), but we know the length is 1. When slicing a
    Tuple with modified proxy integers, an `Any` will be returned because the types of the values at the
    indices is unknown, and the length of the resulting Tuple is unknown.

    Note: A modified proxy integer is a proxy integer that has been changed through other operations
    (`wf.Int(1) + 1` is a proxy Int that has been modified with an addition, `wf.Int(2)` has not been modified)

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
        self.graft = client.apply_graft("wf.tuple", *iterable)

    @classmethod
    def _validate_params(cls, type_params):
        for i, type_param in enumerate(type_params):
            error_message = "Tuple type parameters must be Proxytypes but for parameter {}, got {}".format(
                i, type_param
            )
            assert_is_proxytype(type_param, error_message=error_message)

    @typecheck_promote((Int, Slice))
    def __getitem__(self, item):
        # TODO(gabe): cache
        # TODO(gabe): no nested traceback on out-of-bounds index?
        type_slice = item.literal_value
        if isinstance(item, Int):
            item_type = (
                self._type_params[type_slice] if isinstance(type_slice, int) else Any
            )
        else:
            item_type = (
                Any if type_slice is None else Tuple[self._type_params[type_slice]]
            )
        return item_type._from_apply("wf.getitem", self, item)

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
        return Int._from_apply("wf.length", self)

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    @typecheck_promote(lambda self: type(self))
    def __lt__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.lt,
                elemtype,
                "Operator `<` invalid for element {} in {}".format(
                    elemtype.__name__, type(self).__name__
                ),
            )
        return Bool._from_apply("wf.lt", self, other)

    @typecheck_promote(lambda self: type(self))
    def __le__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.le,
                elemtype,
                "Operator `<=` invalid for element {} in {}".format(
                    elemtype.__name__, type(self).__name__
                ),
            )
        return Bool._from_apply("wf.le", self, other)

    @typecheck_promote(lambda self: type(self))
    def __gt__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.gt,
                elemtype,
                "Operator `>` invalid for element {} in {}".format(
                    elemtype.__name__, type(self).__name__
                ),
            )
        return Bool._from_apply("wf.gt", self, other)

    @typecheck_promote(lambda self: type(self))
    def __ge__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.ge,
                elemtype,
                "Operator `>=` invalid for element {} in {}".format(
                    elemtype.__name__, type(self).__name__
                ),
            )
        return Bool._from_apply("wf.ge", self, other)

    @typecheck_promote(lambda self: type(self))
    def __eq__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.eq,
                elemtype,
                "Operator `==` invalid for element {} in {}".format(
                    elemtype.__name__, type(self).__name__
                ),
            )
        return Bool._from_apply("wf.eq", self, other)

    @typecheck_promote(lambda self: type(self))
    def __ne__(self, other):
        for elemtype in set(self._type_params):
            check_valid_binop_for(
                operator.ne,
                elemtype,
                "Operator `!=` invalid for element {} in {}".format(
                    elemtype.__name__, type(self).__name__
                ),
            )
        return Bool._from_apply("wf.ne", self, other)

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

        return concat_type._from_apply("wf.add", self, other)

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

        return concat_type._from_apply("wf.add", other, self)
