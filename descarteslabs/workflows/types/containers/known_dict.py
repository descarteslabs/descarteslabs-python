import six

from ...cereal import serializable
from ..core import assert_is_proxytype
from .dict_ import BaseDict


@serializable()
class KnownDict(BaseDict):
    """
    ``KnownDict[<{key: KnownType, ...}>, KeyType, ValueType]``: Proxy mapping from specific keys to specific value
    types, with default type for unknown values.

    Cannot be instantiated directly; meant to be used as an element type
    in a container, or ``._cast`` to.

    Examples
    --------
    >>> from descarteslabs.workflows import Float, Bool, Str, Int, Any
    >>> from descarteslabs.workflows.types.containers import KnownDict, Tuple
    >>> kd_type = KnownDict[Str, Int] # same as Dict[Str, Int]: no known keys given
    >>> kd_type = KnownDict[Str, Tuple[Int, Float]] # known dict of Str to 2-tuple of Int and Float
    >>> kd_type = KnownDict[{'x': Float, 'y': Bool}, Str, Int] # known dict where 'x' is Float, 'y' is Bool
    >>> # all other keys are Str, and all other values are Int
    >>> kd = Any({'x': 1, 'y': 2.2}).cast(kd_type)
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

    @classmethod
    def _validate_params(cls, type_params):
        if len(type_params) not in (2, 3):
            raise TypeError(
                "KnownDict takes 2 or 3 type parameters, not {}".format(
                    len(type_params)
                )
            )
        if len(type_params) == 3:
            assert isinstance(type_params[0], dict)
            for key, param_cls in six.iteritems(type_params[0]):
                error_message = "KnownDict item type parameters must be Proxytypes but for key '{}', got {!r}".format(
                    key, param_cls
                )
                assert_is_proxytype(param_cls, error_message=error_message)

        assert_is_proxytype(
            type_params[-2],
            error_message="KnownDict key type parameter must be a Proxytype, not a value",
        )
        assert_is_proxytype(
            type_params[-1],
            error_message="KnownDict value type parameter must be a Proxytype, not a value",
        )

    def __getitem__(self, key):
        try:
            result_cls = self._type_params[0][key]
        except (TypeError, KeyError):
            result_cls = self.value_type

        return result_cls._from_apply("wf.get", self, self._promote_key(key))

    def get(self, key, default):
        try:
            result_cls = self._type_params[0][key]
        except (TypeError, KeyError):
            result_cls = self.value_type

        return result_cls._from_apply(
            "wf.get", self, self._promote_key(key), self._promote_default(default)
        )

    get.__doc__ = BaseDict.get.__doc__
