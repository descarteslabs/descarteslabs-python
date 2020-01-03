from ..core import typecheck_promote
from ..primitives import Bool, Int, Float
from .image import Image
from .imagecollection import ImageCollection


@typecheck_promote(
    (Image, ImageCollection, Bool),
    (Image, ImageCollection, Int, Float),
    (Image, ImageCollection, Int, Float),
)
def where(condition, x, y):
    """
    Returns an `Image` or `ImageCollection` with values chosen from ``x`` or ``y``
    depending on ``condition``. The bandnames of the returned imagery will be
    of the format ``<band name>_where_<condition band name>`` for each band in
    ``condition``. Depending on the number of bands in ``x`` and ``y``, <band name>
    can be taken from ``x``, ``y``, ``x_or_y``, or ``condition``.

    Parameters
    ----------
    condition: `Image`, `ImageCollection`, `Bool`
        A `Bool`, or a boolean `Image` or `ImageCollection`.  Where True, yield ``x``;
        where False, yield ``y``.  If a non-boolean `Image` or `ImageCollection` is
        provided, its values will be coerced to booleans by taking nonzeroness.
    x: `Image`, `ImageCollection`, `Int`, `Float`
        The true `Image`, `ImageCollection`, or scalar.  Where ``condition`` is True, we
        yield values from ``x``.
    y: `Image`, `ImageCollection`, `Int`, `Float`
        The false `Image`, `ImageCollection`, or scalar.  Where ``condition`` is False, we
        yield values from ``y``.

    Returns
    -------
    `Image`, `ImageCollection`
        An `Image` or `ImageCollection` with values from ``x`` where ``condition`` is
        True, and values from ``y`` elsewhere.
    """
    if (
        isinstance(condition, Bool)
        and isinstance(x, (Int, Float))
        and isinstance(y, (Int, Float))
    ):
        raise ValueError(
            "Can't call workflows.where with a boolean condition and scalar x, y; currently not supported."
        )
    args = [condition, x, y]
    if any(isinstance(arg, ImageCollection) for arg in args):
        return_type = ImageCollection
    elif any(isinstance(arg, Image) for arg in args):
        return_type = Image
    elif isinstance(x, Float) or isinstance(y, Float):
        return_type = Float
    else:
        return_type = Int
    return return_type._from_apply("where", condition, x, y)
