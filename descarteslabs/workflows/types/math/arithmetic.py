from ..core import typecheck_promote
from ..primitives import Int, Float, Number
from ..geospatial import Image
from ..geospatial import ImageCollection


def _higher_precedence_type(t1, t2, to_float=True):
    order = (Int, Float, Image, ImageCollection)
    t1_i = order.index(t1)
    t2_i = order.index(t2)
    return order[max(t1_i, t2_i, 1 if to_float else 0)]


@typecheck_promote((Int, Float, Image, ImageCollection))
def log(obj):
    """
    Element-wise natural log of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("log", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def log2(obj):
    """
    Element-wise base 2 log of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("log2", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def log10(obj):
    """
    Element-wise base 10 log of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("log10", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def sqrt(obj):
    """
    Element-wise square root of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("sqrt", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def cos(obj):
    """
    Element-wise cosine of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("cos", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def sin(obj):
    """
    Element-wise sine of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("sin", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def tan(obj):
    """
    Element-wise tangent of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("tan", obj)


def normalized_difference(x, y):
    """
    Normalized difference helper function for computing an index such
    as NDVI.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
    >>> nir, red = col.unpack_bands(["nir", "red"])
    >>> ndvi = wf.normalized_difference(nir, red)
    """

    return (x - y) / (x + y)


@typecheck_promote(
    (Int, Float, Image, ImageCollection), (Int, Float, Image, ImageCollection)
)
def arctan2(y, x):
    """
    Element-wise arc tangent of ``y/x`` choosing the quadrant correctly.

    The quadrant (i.e., branch) is chosen so that ``arctan2(y, x)`` is
    the signed angle in radians between the ray ending at the origin and
    passing through the point (1,0), and the ray ending at the origin and
    passing through the point (x, y).  (Note the role reversal: the
    "y-coordinate" is the first function parameter, the "x-coordinate"
    is the second.)  By IEEE convention, this function is defined for
    x = +/-0 and for either or both of y and x = +/-inf (see
    Notes for specific values).

    Parameters
    ----------
    y: Int, Float, ~.geospatial.Image, ~.geospatial.ImageCollection
        y-coordinates
    x: Int, Float, ~.geospatial.Image, ~.geospatial.ImageCollection
        x-coordinates

    Returns
    -------
    x: Float, ~.geospatial.Image, ~.geospatial.ImageCollection
        Angle(s) in radians, in the range ``[-pi, pi]``,
        of the type that results from broadcasting ``y`` to ``x``,
        (except `.Int` is promoted to `.Float`)

    Notes
    -----
    *arctan2* is identical to the ``atan2`` function of the underlying
    C library.  The following special values are defined in the C
    standard: [1]_

    ====== ====== ================
    ``y``   ``x``   ``arctan2(y,x)``
    ====== ====== ================
    +/- 0  +0     +/- 0
    +/- 0  -0     +/- pi
     > 0   +/-inf +0 / +pi
     < 0   +/-inf -0 / -pi
    +/-inf +inf   +/- (pi/4)
    +/-inf -inf   +/- (3*pi/4)
    ====== ====== ================

    Note that +0 and -0 are distinct floating point numbers, as are +inf
    and -inf.

    References
    ----------
    .. [1] ISO/IEC standard 9899:1999, "Programming language C."
    """
    return_type = _higher_precedence_type(type(y), type(x))
    return return_type._from_apply("arctan2", y, x)
