from ...cereal import serializable
from .primitive import Primitive
from .bool_ import Bool  # noqa TODO remove noqa later


@serializable()
class NoneType(Primitive):
    """
    Proxy type(None).

    Cannot be computed directly.

    Examples
    --------
    >>> from descarteslabs.workflows import NoneType
    >>> NoneType(None)
    <descarteslabs.workflows.types.primitives.none.NoneType object at 0x...>
    """

    _pytype = type(None)
