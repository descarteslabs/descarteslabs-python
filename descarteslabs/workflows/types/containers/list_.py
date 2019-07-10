from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import ProxyTypeError, GenericProxytype
from ..primitives import Int
from .collection import CollectionMixin

try:
    # only after py3.4
    from collections import abc
except ImportError:
    import collections as abc


@serializable()
class List(GenericProxytype, CollectionMixin):
    """
    Proxy sequence of any number of elements, all of the same type.

    Can be instantiated from any Python iterable, or another List of the same type.

    Examples
    --------
    >>> List[Str](["foo", "bar", "baz"])
    >>> List[List[Int]]([[1, 2], [-1], [10, 11, 12]])
    """

    def __init__(self, iterable):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic List; the item type must be specified (like `List[Int]`)"
            )

        if isinstance(iterable, type(self)):
            self.graft = client.apply_graft("list.copy", iterable)
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
            self.graft = client.apply_graft("list", *iterable)

    def __getitem__(self, item):
        # TODO(gabe): slices
        # TODO(gabe): cache
        try:
            item = Int._promote(item)
        except ProxyTypeError:
            raise ProxyTypeError(
                "List indicies must be integers, not {}".format(type(item))
            )
        return self._type_params[0]._from_apply("getitem", self, item)
