import six

from collections import abc

from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import (
    ProxyTypeError,
    GenericProxytype,
    typecheck_promote,
    assert_is_proxytype,
)
from ..primitives import Str
from .list_ import List
from .tuple_ import Tuple


@serializable()
class Dict(GenericProxytype):
    """
    ``Dict[KeyType, ValueType]``: Proxy mapping, from keys of a specific type to values of a specific type.

    Can be instantiated from a Python mapping and/or keyword arguments.

    Examples
    --------
    >>> from descarteslabs.workflows import Dict, List, Str, Int, Float
    >>> Dict[Str, Int](a=1, b=2) # dict of Str to Int
    <descarteslabs.workflows.types.containers.dict_.Dict[Str, Int] object at 0x...>
    >>> Dict[Str, Int]({'a': 1, 'b': 2}, b=100, c=3) # dict of Str to Int
    <descarteslabs.workflows.types.containers.dict_.Dict[Str, Int] object at 0x...>
    >>> Dict[Str, List[Float]](a=[1.1, 2.2], b=[3.3]) # dict of Str to List of Floats
    <descarteslabs.workflows.types.containers.dict_.Dict[Str, List[Float]] object at 0x...>

    >>> from descarteslabs.workflows import Dict, Str, Float
    >>> my_dict = Dict[Str, Float]({"red": 100.5, "blue": 67.6})
    >>> my_dict
    <descarteslabs.workflows.types.containers.dict_.Dict[Str, Float] object at 0x...>
    >>> my_dict.compute() # doctest: +SKIP
    {"red": 100.5, "blue": 67.6}
    >>> my_dict.keys().compute() # doctest: +SKIP
    ['red', 'blue']
    >>> my_dict["red"].compute() # doctest: +SKIP
    100.5
    """

    def __init__(self, *dct, **kwargs):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic Dict; the key and value types must be specified (like `Dict[Str, Bool]`)"
            )

        if len(dct) > 1:
            raise TypeError(
                "Dict expected at most 1 arguments, got {}".format(len(dct))
            )
        if len(dct) == 0:
            dct = kwargs
            kwargs = {}
        else:
            dct = dct[0]

        kt, vt = self._type_params
        if isinstance(dct, Dict):
            other_kt, other_vt = dct._type_params
            if not (issubclass(other_kt, kt) and issubclass(other_vt, vt)):
                raise ProxyTypeError(
                    "Cannot convert {} to {}, their element types are different".format(
                        type(dct).__name__, type(self).__name__
                    )
                )
            self.graft = dct.graft
            if len(kwargs) > 0:
                raise NotImplementedError(
                    "Don't have key merging onto a proxy dict yet."
                )
        else:
            if not isinstance(dct, abc.Mapping):
                raise ProxyTypeError("Expected a mapping, got {}".format(dct))

            dct = dct.copy()
            dct.update(kwargs)
            # TODO(gabe): numer of copies of source dict could definitely be reduced here

            is_str_dict = issubclass(kt, Str)
            promoted = {} if is_str_dict else []
            for key, val in six.iteritems(dct):
                try:
                    promoted_key = kt._promote(key)
                except ProxyTypeError:
                    raise ProxyTypeError(
                        "Expected Dict keys of type {}, but got {}".format(kt, key)
                    )
                try:
                    promoted_val = vt._promote(val)
                except ProxyTypeError:
                    raise ProxyTypeError(
                        "Expected Dict values of type {}, but got {}".format(vt, val)
                    )
                if is_str_dict:
                    promoted[key] = promoted_val
                    # note we use the unpromoted key, which should be a string
                    # this is an optimization that produces a cleaner graph for the case of string-keyed dicts
                else:
                    promoted += [promoted_key, promoted_val]
                    # for non-string dicts, we just give varargs of key, value, key, value, ...
                    # since that's a much simpler graft representation than constructing a list
                    # of tuples

            if is_str_dict:
                self.graft = client.apply_graft("dict.create", **promoted)
            else:
                self.graft = client.apply_graft("dict.create", *promoted)

    @classmethod
    def _validate_params(cls, type_params):
        assert len(type_params) == 2, "Both Dict key and value types must be specified"
        for i, type_param in enumerate(type_params):
            error_message = "Dict key and value types must be Proxytypes but for parameter {}, got {}".format(
                i, type_param
            )
            assert_is_proxytype(type_param, error_message=error_message)

    def __getitem__(self, item):
        # TODO: cache
        # requires figuring out how to make Delayed objects hashable
        kt, vt = self._type_params
        try:
            item = kt._promote(item)
        except ProxyTypeError:
            raise ProxyTypeError(
                "Dict keys are of type {}, but indexed with {}".format(kt, item)
            )
        return vt._from_apply("getitem", self, item)

    def __iter__(self):
        raise TypeError(
            "Proxy {} is not iterable. Consider .keys().map(...) "
            "to achieve something similar.".format(type(self).__name__)
        )

    @classmethod
    @typecheck_promote(lambda cls: List[Tuple[cls._type_params]])
    def from_pairs(cls, pairs):
        """
        Construct a Dict from a list of key-value pairs.

        Example
        -------
        >>> from descarteslabs.workflows import Dict, Str, Int
        >>> pairs = [("foo", 1), ("bar", 2), ("baz", 3)]
        >>> my_dict = Dict[Str, Int].from_pairs(pairs)
        >>> my_dict
        <descarteslabs.workflows.types.containers.dict_.Dict[Str, Int] object at 0x...>
        >>> my_dict.compute() # doctest: +SKIP
        {'foo': 1, 'bar': 2, 'baz': 3}
        """
        return cls._from_apply("dict.from_pairs", pairs)

    def keys(self):
        """Get a list of all the dictionary keys.

        Returns
        -------
        List

        Example
        -------
        >>> from descarteslabs.workflows import Dict, Str, Int
        >>> my_dict = Dict[Str, Int]({"foo": 1, "bar": 2, "baz": 3})
        >>> my_dict.keys().compute() # doctest: +SKIP
        ['foo', 'bar', 'baz']
        """
        return List[self._type_params[0]]._from_apply("dict.keys", self)

    def values(self):
        """Get a list of all the dictionary values.

        Returns
        -------
        List

        Example
        -------
        >>> from descarteslabs.workflows import Dict, Str, Int
        >>> my_dict = Dict[Str, Int]({"foo": 1, "bar": 2, "baz": 3})
        >>> my_dict.values().compute() # doctest: +SKIP
        [1, 2, 3]
        """
        return List[self._type_params[1]]._from_apply("dict.values", self)

    def items(self):
        """Get a list of tuples of key-value pairs in the dictionary.

        Returns
        -------
        List[Tuple[KeyType, ValueType]]

        Example
        -------
        >>> from descarteslabs.workflows import Dict, Str, Int
        >>> my_dict = Dict[Str, Int]({"foo": 1, "bar": 2, "baz": 3})
        >>> my_dict.items().compute() # doctest: +SKIP
        [('foo', 1), ('bar', 2), ('baz', 3)]
        """
        return List[Tuple[self._type_params[0], self._type_params[1]]]._from_apply(
            "dict.items", self
        )
