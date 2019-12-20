from .image import Image
from .imagecollection import ImageCollection


# Once we support checking variadic positional args in typecheck_promote, we can use typecheck_promote instead
def concat(*imgs):
    """
    `ImageCollection` of ``imgs`` concatenated to one another, where
    ``imgs`` is a variable number of `Image` or `ImageCollection` objects.

    Images, properties, and bandinfo are concatenated. All collections
    must have the same number of bands with identical names. Any empty
    `Images` or `ImageCollections` will not be concatenated.

    Parameters
    ----------
    *imgs: variable number of `Image` or `ImageCollection` objects

    Returns
    -------
    concatenated: ImageCollection
    """
    if len(imgs) < 2:
        raise ValueError(
            "Must pass at least 2 imagery objects to concat() not {}.".format(len(imgs))
        )

    for img in imgs:
        if not isinstance(img, (Image, ImageCollection)):
            raise TypeError(
                "Argument 'imgs' to function concat(): expected Image/ImageCollection objects but got ({})".format(
                    ", ".join(type(i).__name__ for i in imgs)
                )
            )
    return ImageCollection._from_apply("concat", *imgs)
