from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import ProxyTypeError, GenericProxytype
from ..primitives import Any, Int

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
        return Int._from_apply("length", self)

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]
