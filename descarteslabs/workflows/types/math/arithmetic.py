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

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(1)
    >>> wf.log(my_int).compute() # doctest: +SKIP
    0.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.log", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def log2(obj):
    """
    Element-wise base 2 log of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(1)
    >>> wf.log2(my_int).compute() # doctest: +SKIP
    0.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.log2", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def log10(obj):
    """
    Element-wise base 10 log of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(1)
    >>> wf.log10(my_int).compute() # doctest: +SKIP
    0.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.log10", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def log1p(obj):
    """
    Element-wise log of 1 + an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(1)
    >>> wf.log1p(my_int).compute() # doctest: +SKIP
    0.6931471805599453
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.log1p", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def sqrt(obj):
    """
    Element-wise square root of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(4)
    >>> wf.sqrt(my_int).compute() # doctest: +SKIP
    2.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.sqrt", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def cos(obj):
    """
    Element-wise cosine of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(0)
    >>> wf.cos(my_int).compute() # doctest: +SKIP
    1.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.cos", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def arccos(obj):
    """
    Element-wise inverse cosine of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(0)
    >>> wf.arccos(my_int).compute() # doctest: +SKIP
    1.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.arccos", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def sin(obj):
    """
    Element-wise sine of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(0)
    >>> wf.sin(my_int).compute() # doctest: +SKIP
    0.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.sin", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def arcsin(obj):
    """
    Element-wise inverse sine of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(0)
    >>> wf.arcsin(my_int).compute() # doctest: +SKIP
    0.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.arcsin", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def tan(obj):
    """
    Element-wise tangent of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(0)
    >>> wf.tan(my_int).compute() # doctest: +SKIP
    0.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.tan", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def arctan(obj):
    """
    Element-wise inverse tangent of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(0)
    >>> wf.arctan(my_int).compute() # doctest: +SKIP
    0.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.arctan", obj)


def normalized_difference(x, y):
    """
    Normalized difference helper function for computing an index such
    as NDVI.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
    ...     start_datetime="2017-01-01",
    ...     end_datetime="2017-05-30")
    >>> nir, red = col.unpack_bands("nir red")
    >>> # geoctx is an arbitrary geocontext for 'col'
    >>> wf.normalized_difference(nir, red).compute(geoctx) # doctest: +SKIP
    ImageCollectionResult of length 2:
      * ndarray: MaskedArray<shape=(2, 1, 512, 512), dtype=float64>
      * properties: 2 items
      * bandinfo: 'nir_sub_red_div_nir_add_red'
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
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

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(1)
    >>> wf.arctan2(my_int, my_int).compute() # doctest: +SKIP
    0.7853981633974483

    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(1)
    >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
    >>> wf.arctan2(img, my_int).compute(geoctx) # geoctx is an arbitrary geocontext for 'img' # doctest: +SKIP
    ImageResult:
      * ndarray: MaskedArray<shape=(1, 512, 512), dtype=float64>
      * properties: 'acquired', 'area', 'bits_per_pixel', 'bright_fraction', ...
      * bandinfo: 'red'
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...

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
    return return_type._from_apply("wf.arctan2", y, x)


@typecheck_promote((Int, Float, Image, ImageCollection))
def exp(obj):
    """
    Element-wise exponential of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(1)
    >>> wf.exp(my_int).compute() # doctest: +SKIP
    2.718281828459045
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.exp", obj)


@typecheck_promote((Int, Float, Image, ImageCollection))
def square(obj):
    """
    Element-wise square of an `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Can also be used with `.Int` and `.Float` types.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> my_int = wf.Int(2)
    >>> wf.square(my_int).compute() # doctest: +SKIP
    4.0
    """
    return_type = Float if isinstance(obj, Number) else type(obj)
    return return_type._from_apply("wf.square", obj)
