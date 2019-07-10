def proxify(obj):
    from ..core import Proxytype
    from ..function import Function
    from ..containers import Tuple
    from ..primitives import Int, Float, Bool, Str, NoneType

    if isinstance(obj, Proxytype):
        return obj
    elif callable(obj):
        return Function.from_callable(obj)
    elif isinstance(obj, (tuple, list)):
        contents = [proxify(x) for x in obj]
        types = tuple(type(x) for x in contents)
        return Tuple[types](contents)
    elif isinstance(obj, int):
        return Int(obj)
    elif isinstance(obj, float):
        return Float(obj)
    elif isinstance(obj, bool):
        return Bool(obj)
    elif isinstance(obj, str):
        return Str(obj)
    elif obj is None:
        return NoneType(obj)
    else:
        raise NotImplementedError(
            "Cannot automatically convert to a Proxytype. "
            "Please manually construct the appropriate container type "
            "and initialize it with your object. Value: {}".format(obj)
        )
