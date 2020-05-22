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
from ..primitives import Int, Bool
from .collection import CollectionMixin
from .slice import Slice

from ._check_valid_binop import check_valid_binop_for


@serializable()
class List(GenericProxytype, CollectionMixin):
    """
    ``List[ValueType]``: Proxy sequence of any number of elements, all of the same type.

    Can be instantiated from any Python iterable, or another List of the same type.

    Examples
    --------
    >>> from descarteslabs.workflows import List, Str, Int
    >>> List[Str](["foo", "bar", "baz"]) # list of Strs
    <descarteslabs.workflows.types.containers.list_.List[Str] object at 0x...>
    >>> List[List[Int]]([[1, 2], [-1], [10, 11, 12]]) # list of lists of Ints
    <descarteslabs.workflows.types.containers.list_.List[List[Int]] object at 0x...>

    >>> from descarteslabs.workflows import List, Float
    >>> my_list = List[Float]([1.1, 2.2, 3.3, 4.4])
    >>> my_list
    <descarteslabs.workflows.types.containers.list_.List[Float] object at 0x...>
    >>> my_list.compute() # doctest: +SKIP
    [1.1, 2.2, 3.3, 4.4]
    >>> my_list[2].compute() # doctest: +SKIP
    3.3
    >>> (my_list * 2).compute() # doctest: +SKIP
    [1.1, 2.2, 3.3, 4.4, 1.1, 2.2, 3.3, 4.4]
    >>> (my_list == my_list).compute() # doctest: +SKIP
    True
    """

    def __init__(self, iterable):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic List; the item type must be specified (like `List[Int]`)"
            )

        if isinstance(iterable, type(self)):
            self.graft = client.apply_graft("wf.list.copy", iterable)
        elif isinstance(iterable, List):
            raise ProxyTypeError(
                "Cannot convert {} to {}, since they have different value types".format(
                    type(iterable).__name__, type(self).__name__
                )
            )
        else:
            if not isinstance(iterable, abc.Iterable):
                raise ProxyTypeError("Expected an iterable, got {}".format(iterable))
            value_type = self._type_params[0]

            def checker_promoter(i, x):
                try:
                    return value_type._promote(x)
                except ProxyTypeError:
                    raise ProxyTypeError(
                        "{}: Expected iterable values of type {}, but for item {}, got {!r}".format(
                            type(self).__name__, value_type, i, x
                        )
                    )

            iterable = tuple(checker_promoter(i, x) for i, x in enumerate(iterable))
            self.graft = client.apply_graft("wf.list", *iterable)

    @classmethod
    def _validate_params(cls, type_params):
        assert len(type_params) == 1, "List can only have one element type specified"
        type_param = type_params[0]
        error_message = "List type must be a Proxytype, got {}".format(type_param)
        assert_is_proxytype(type_param, error_message=error_message)

    @typecheck_promote((Int, Slice))
    def __getitem__(self, item):
        # TODO(gabe): cache
        return_type = self._type_params[0] if isinstance(item, Int) else type(self)
        return return_type._from_apply("wf.getitem", self, item)

    def __iter__(self):
        raise TypeError(
            "Proxy List object is not iterable, since it contains an unknown number of elements"
        )

    @property
    def _element_type(self):
        return self._type_params[0]

    @typecheck_promote(lambda self: type(self))
    def __lt__(self, other):
        check_valid_binop_for(
            operator.lt,
            self._type_params[0],
            "Operator `<` invalid for {}".format(type(self).__name__),
        )
        return Bool._from_apply("wf.lt", self, other)

    @typecheck_promote(lambda self: type(self))
    def __le__(self, other):
        check_valid_binop_for(
            operator.le,
            self._type_params[0],
            "Operator `<=` invalid for {}".format(type(self).__name__),
        )
        return Bool._from_apply("wf.le", self, other)

    @typecheck_promote(lambda self: type(self))
    def __gt__(self, other):
        check_valid_binop_for(
            operator.gt,
            self._type_params[0],
            "Operator `>` invalid for {}".format(type(self).__name__),
        )
        return Bool._from_apply("wf.gt", self, other)

    @typecheck_promote(lambda self: type(self))
    def __ge__(self, other):
        check_valid_binop_for(
            operator.ge,
            self._type_params[0],
            "Operator `>=` invalid for {}".format(type(self).__name__),
        )
        return Bool._from_apply("wf.ge", self, other)

    @typecheck_promote(lambda self: type(self))
    def __eq__(self, other):
        check_valid_binop_for(
            operator.eq,
            self._type_params[0],
            "Operator `==` invalid for {}".format(type(self).__name__),
        )
        return Bool._from_apply("wf.eq", self, other)

    @typecheck_promote(lambda self: type(self))
    def __ne__(self, other):
        check_valid_binop_for(
            operator.ne,
            self._type_params[0],
            "Operator `!=` invalid for {}".format(type(self).__name__),
        )
        return Bool._from_apply("wf.ne", self, other)

    @typecheck_promote(lambda self: type(self))
    def __add__(self, other):
        return self._from_apply("wf.add", self, other)

    @typecheck_promote(lambda self: type(self))
    def __radd__(self, other):
        return self._from_apply("wf.add", other, self)

    @typecheck_promote(Int)
    def __mul__(self, times):
        return self._from_apply("wf.mul", self, times)

    @typecheck_promote(Int)
    def __rmul__(self, times):
        return self._from_apply("wf.mul", times, self)
