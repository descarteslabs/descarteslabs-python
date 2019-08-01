from ...cereal import serializable
from ..core import GenericProxytype, ProxyTypeError


@serializable()
class KnownDict(GenericProxytype):
    """
    Mapping from specific keys to specific value types,
    with default type for unknown values.

    Cannot be instantiated directly; meant to be used as an element type
    in a container, or ``._cast`` to.

    Examples
    --------
    >>> from descarteslabs.workflows import Float, Bool, Str, Int, Any
    >>> from descarteslabs.workflows.types.containers import KnownDict
    >>> kd_type = KnownDict[{'x': Float, 'y': Bool}, Str, Int]
    >>> kd = Any({'x': 1, 'y': 2.2})._cast(kd_type)
    >>> kd['x']  # Float
    <descarteslabs.workflows.types.primitives.number.Float object at 0x...>
    >>> kd['foo']  # Int
    <descarteslabs.workflows.types.primitives.number.Int object at 0x...>
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
