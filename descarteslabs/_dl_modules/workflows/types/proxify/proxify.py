import numpy as np
import datetime


def proxify(obj):
    from descarteslabs import scenes
    from ..geospatial import GeoContext
    from ..core import Proxytype, ProxyTypeError
    from ..function import Function
    from ..containers import Tuple, List
    from ..primitives import Int, Float, Bool, Str, NoneType, Any
    from ..datetimes import Datetime, Timedelta
    from ..array import Array, MaskedArray, DType

    if isinstance(obj, Proxytype):
        return obj
    elif callable(obj):
        return Function.from_callable(obj)
    elif isinstance(obj, (tuple, list)):
        contents = [proxify(x) for x in obj]
        types = tuple(type(x) for x in contents)
        if (
            isinstance(obj, list)
            and len(types) > 0
            and all(t is types[0] for t in types[1:])
        ):
            return List[types[0]](contents)
        else:
            return Tuple[types](contents)
    elif isinstance(obj, (bool, np.bool_)):
        return Bool(obj)
    elif isinstance(obj, (int, np.integer)):
        return Int(obj)
    elif isinstance(obj, float):
        return Float(obj)
    elif isinstance(obj, str):
        return Str(obj)
    elif obj is None:
        return NoneType(obj)
    elif isinstance(obj, np.ma.MaskedArray):
        return MaskedArray.from_numpy(obj)
    elif isinstance(obj, np.ndarray):
        return Array(obj)
    elif isinstance(obj, np.dtype):
        return DType(obj)
    elif isinstance(obj, (datetime.datetime, datetime.date)):
        return Datetime._promote(obj)
    elif isinstance(obj, datetime.timedelta):
        return Timedelta._promote(obj)
    elif isinstance(obj, scenes.GeoContext):
        return GeoContext.from_scenes(obj)
    else:
        try:
            return Any._promote(obj)
        except ProxyTypeError:
            raise NotImplementedError(
                "Cannot automatically convert to a Proxytype. "
                "Please manually construct the appropriate container type "
                "and initialize it with your object. Value: {!r}".format(obj)
            )
