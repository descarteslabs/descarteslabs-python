from .image import Image
from .imagecollection import ImageCollection


# Once we support checking variadic positional args in typecheck_promote, we can use typecheck_promote instead
def concat(*imgs):
    """
    `ImageCollection` of ``imgs`` concatenated to one another, where
    ``imgs`` is a variable number of `Image` or `ImageCollection` objects.

    Images and properties are concatenated. Bandinfos are intersected. All collections
    must have the same number of bands with identical names. Any empty
    `Image` or `ImageCollection` objects will not be concatenated.

    Example
    -------
    >>> from descarteslabs.workflows import Image, ImageCollection, concat
    >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
    >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
    ...        start_datetime="2017-01-01",
    ...        end_datetime="2017-05-30")
    >>> # imagery must have same bands to be concatenated
    >>> rgb_img = img.pick_bands("red green blue")
    >>> rgb_col = col.pick_bands("red green blue") # rgb_col has 2 images
    >>> # geoctx is an arbitrary geocontext for the imagery
    >>> concat(rgb_col, rgb_img).compute(geoctx) # doctest: +SKIP
    ImageCollectionResult of length 3:
      * ndarray: MaskedArray<shape=(3, 3, 512, 512), dtype=float64>
      * properties: 3 items
      * bandinfo: 'red', 'green', 'blue'
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...

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
    return ImageCollection._from_apply("wf.concat", *imgs)
