from ...cereal import serializable
from .primitive import Primitive
from .bool_ import Bool  # noqa TODO remove noqa later


@serializable()
class NoneType(Primitive):
    "Proxy type(None)"
    _pytype = type(None)
