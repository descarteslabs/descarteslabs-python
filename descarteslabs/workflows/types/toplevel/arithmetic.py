from ..core import typecheck_promote
from ..primitives import Int, Float, Number
from ..geospatial import Image
from ..geospatial import ImageCollection


@typecheck_promote((Int, Float, Image, ImageCollection))
def log(obj):
    """
    Element-wise natural log of an `Image` or `ImageCollection`.

    Can also be used with `Int` and `Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("log", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def log2(obj):
    """
    Element-wise base 2 log of an `Image` or `ImageCollection`.

    Can also be used with `Int` and `Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("log2", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def log10(obj):
    """
    Element-wise base 10 log of an `Image` or `ImageCollection`.

    Can also be used with `Int` and `Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("log10", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def sqrt(obj):
    """
    Element-wise square root of an `Image` or `ImageCollection`.

    Can also be used with `Int` and `Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("sqrt", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def cos(obj):
    """
    Element-wise cosine of an `Image` or `ImageCollection`.

    Can also be used with `Int` and `Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("cos", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def sin(obj):
    """
    Element-wise sine of an `Image` or `ImageCollection`.

    Can also be used with `Int` and `Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("sin", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def tan(obj):
    """
    Element-wise tangent of an `Image` or `ImageCollection`.

    Can also be used with `Int` and `Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("tan", obj)
