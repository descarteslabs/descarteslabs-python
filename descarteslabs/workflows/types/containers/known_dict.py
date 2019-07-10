from ...cereal import serializable
from ..core import GenericProxytype, ProxyTypeError


@serializable()
class KnownDict(GenericProxytype):
    """
    Mapping from specific keys to specific value types,
    with default type for unknown values.

    Examples
    --------
    >>> kd = KnownDict[{'x': Float, 'y': Bool}, Str, Int]()
    >>> kd['x']  # Float
    >>> kd['foo']  # Int
    """

    def __init__(self):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic Dict; the key and value types must be specified "
                "(like `KnownDict[Str, Bool]`)"
            )
        super(KnownDict, self).__init__()

    def __getitem__(self, item):
        items, kt, vt = self._type_params
        try:
            result_cls = items[item]
        except (TypeError, KeyError):
            result_cls = vt

        try:
            item = kt._promote(item)
        except ProxyTypeError:
            raise ProxyTypeError(
                "Dict keys are of type {}, but indexed with {}".format(kt, item)
            )

        return result_cls._from_apply("getitem", self, item)
