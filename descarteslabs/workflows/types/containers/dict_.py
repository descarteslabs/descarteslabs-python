import six

from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import ProxyTypeError, GenericProxytype
from ..primitives import Str
from .list_ import List
from .tuple_ import Tuple

try:
    # only after py3.4
    from collections import abc
except ImportError:
    import collections as abc


@serializable()
class Dict(GenericProxytype):
    """
    Proxy mapping, from keys of a specific type to values of a specific type.

    Can be instantiated from a Python mapping and/or keyword arguments.

    Examples
    --------
    >>> Dict[Str, Int](a=1, b=2)
    >>> Dict[Str, Int]({'a': 1, 'b': 2}, b=100, c=3)
    >>> Dict[Str, List[Float]](a=[1.1, 2.2], b=[3.3])
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

    def keys(self):
        # TODO(gabe): need a Set type, since these should be unordered
        return List[self._type_params[0]]._from_apply("dict.keys", self)

    def values(self):
        # TODO(gabe): need a Set type, since these should be unordered
        return List[self._type_params[1]]._from_apply("dict.values", self)

    def items(self):
        # TODO(gabe): need a Set type, since these should be unordered
        return List[Tuple[self._type_params[0], self._type_params[1]]]._from_apply(
            "dict.items", self
        )
