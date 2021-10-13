from descarteslabs.common.graft import client


from ..core import typecheck_promote, GenericProxytype, merge_params
from ..containers import Dict, Tuple
from ..function import Function

from .image import Image
from .imagecollection import ImageCollection


class ImageCollectionGroupby(GenericProxytype):
    """
    Dict-like object for a grouped `ImageCollection`.

    If constructed from an empty `ImageCollection` or ``func`` results in empty groups,
    `ImageCollectionGroupby.groups` will return an empty Dict. Other operations (like `map` and `count`)
    will also return an empty Dict or an empty `Image`/`ImageCollection`.

    Groups can be accessed with dict-like syntax.
    If indexing with a key that does not exist, an empty `ImageCollection` is returned.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> from descarteslabs.scenes import DLTile
    >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
    ...        start_datetime="2017-01-01",
    ...        end_datetime="2017-12-31")
    >>> # all Images from the same month, regardless of year
    >>> col.groupby(dates="month")
    <descarteslabs.workflows.types.geospatial.groupby.ImageCollectionGroupby[Int] object at 0x...>
    >>>
    >>> # group into 14-day bins
    >>> col.groupby(lambda img: img.properties["date"] // wf.Timedelta(days=14))
    <descarteslabs.workflows.types.geospatial.groupby.ImageCollectionGroupby[Datetime] object at 0x...>
    >>>
    >>> # group by "pass" ("ASCENDING" or "DESCENDING")
    >>> col.groupby(lambda img: img.properties["pass"])
    <descarteslabs.workflows.types.geospatial.groupby.ImageCollectionGroupby[Any] object at 0x...>
    >>>
    >>> # all Images from the same year and month
    >>> month_grouped = col.groupby(dates=("year", "month"))
    >>>
    >>> # .mean(), etc. are applied to each group, then combined into one ImageCollection
    >>> monthly = month_grouped.mean(axis="images")  # ImageCollection of monthly mean composites
    >>>
    >>> # a `group` field is added to each Image
    >>> monthly.map(lambda img: img.properties['group']).compute(geoctx) # doctest: +SKIP
    [(2018, 1), (2018, 2), ... ]
    >>>
    >>> # use .map() to apply a function to each group, then combine into a Dict or ImageCollection
    >>> monthly_l2_norm = month_grouped.map(lambda group, imgs: (imgs ** 2).sum(axis="images").sqrt())
    >>> # ^ ImageCollection of each month's L2 norm
    >>>
    >>> monthly_medians = month_grouped.map(lambda group, imgs: imgs.median())
    >>> # ^ Dict of (year, month) to median pixel value
    >>>
    >>> # you can select a single group with dict-like syntax
    >>> feb = month_grouped[(2018, 2)]
    >>> feb.compute(geoctx) # doctest: +SKIP
    ImageCollectionResult of length 2:
    ...
    >>> # selecting a non-existent group returns an empty ImageCollection
    >>> month_grouped[(1900, 1)].compute(geoctx) # doctest: +SKIP
    ImageCollectionResult of length 0:
    ...
    """

    @typecheck_promote(ImageCollection, lambda self: Function[Image, {}, self.key_type])
    def __init__(self, imgs, func):
        """
        You should construct `ImageCollectionGroupby` from `.ImageCollection.groupby` in most cases.

        Parameters
        ----------
        imgs: ImageCollection
            `ImageCollection` to group
        func: Function
            Key function which takes an `Image` and returns which group the `Image` belongs to.
            Must return an instance of ``self.key_type``.

        Returns
        -------
        grouped: ImageCollectionGroupby
        """
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic {}; the item type must be specified".format(
                    type(self).__name__
                )
            )

        self.graft = client.apply_graft("wf.ImageCollectionGroupby.create", imgs, func)
        self.params = merge_params(imgs, func)

        # store this once so that repeated calls to `.groups` refer to the same object in the graft
        self._groups = Dict[self.key_type, ImageCollection]._from_apply(
            "wf.ImageCollectionGroupby.groups", self
        )

    @property
    def key_type(self):
        "Proxytype: The type of the group keys"
        return self._type_params[0]

    @property
    def groups(self):
        "`Dict` of group key -> `ImageCollection`"
        return self._groups

    def one(self):
        """A `Tuple` of (group key, `ImageCollection`) for one random group. Helpful for debugging.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> groups = col.groupby(dates="month")
        >>> groups.one().compute(geoctx) # doctest: +SKIP
        (5, ImageCollection...)
        """
        return Tuple[self.key_type, ImageCollection]._from_apply(
            "wf.ImageCollectionGroupby.one", self
        )

    @typecheck_promote(lambda self: self.key_type)
    def __getitem__(self, idx):
        return ImageCollection._from_apply("wf.ImageCollectionGroupby.get", self, idx)

    def compute(self, *args, **kwargs):
        raise TypeError(
            "{} cannot be computed directly. "
            "Instead, compute `.groups`, use `.map` or `.mean`, etc. to composite the groups "
            "into a single Image/ImageCollection, or pick one group with [] syntax "
            "and compute that.".format(type(self).__name__)
        )

    def map(self, func):
        """
        Apply a function to each group as an `ImageCollection` and combine the results.

        ``func`` must take two arguments: the group, and an `ImageCollection` of all Images in that group.

        If it returns an `ImageCollection`, the ImageCollections for all groups will be concatenated together.

        If it returns an `Image`, those Images will be combined into a single ImageCollection.

        If it returns any other type, `map` will return a `Dict`, where keys are groups
        and values are results of ``func``.

        Note that every `Image` in every `ImageCollecion` gets a ``"group"`` field added to its `~.Image.properties`,
        so that field will be merged/dropped according to normal metadata broadcasting rules (i.e. will still be present
        on an `Image` composited from a group's `ImageCollection`, since it's the same for every `Image`.)

        Parameters
        ----------
        func: Python function
            Function that takes an Int group number and ImageCollection

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> groups = col.groupby(dates="month")
        >>> groups.map(lambda group, col: col.pick_bands("red green blue"))# pick rgb bands for each group
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> # since the mapper function returns an ImageCollection, they are combined back into a single collection
        >>> # each have a group key in their properties
        >>> groups.map(lambda group, col: col.mean()) # equivalent to groups.mean()
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Float] object at...>
        >>> # since the mapper function returns a Dict, map returns a Dict of group key to func results
        """
        proxy_func = Function.from_callable(func, self.key_type, ImageCollection)
        result_type = proxy_func._type_params[-1]

        if result_type in (ImageCollection, Image):
            out_type = ImageCollection
            func = "wf.ImageCollectionGroupby.map_ic"
        else:
            out_type = Dict[self.key_type, result_type]
            func = "wf.ImageCollectionGroupby.map"
        return out_type._from_apply(func, self, proxy_func)

    def count(self, axis=None):
        """Apply `.ImageCollection.count` to each group.

        Parameters
        ----------
        axis: Str or Tuple[Str], optional, default: None
            A string or tuple of strings of "pixels", "images", or "bands"

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> groups = col.groupby(lambda img: img.properties["date"].month)
        >>> groups.count(axis="images")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.count(axis="bands")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.count(axis="pixels")
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Dict[Str, Float]]] object at 0x...>
        >>> groups.count(axis=("bands", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Float]] object at 0x...>
        >>> groups.count(axis=("images", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Dict[Str, Float]] object at 0x...>
        >>> groups.count(axis=None)
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Float] object at 0x...>
        """
        return self.map(lambda group, imgs: imgs.count(axis=axis))

    def sum(self, axis=None):
        """Apply `.ImageCollection.sum` to each group.

        Parameters
        ----------
        axis: Str or Tuple[Str] or None
            A string or tuple of strings of "pixels", "images", or "bands"

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> groups = col.groupby(lambda img: img.properties["date"].month)
        >>> groups.sum(axis="images")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.sum(axis="bands")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.sum(axis="pixels")
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Dict[Str, Float]]] object at 0x...>
        >>> groups.sum(axis=("bands", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Float]] object at 0x...>
        >>> groups.sum(axis=("images", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Dict[Str, Float]] object at 0x...>
        >>> groups.sum(axis=None)
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Float] object at 0x...>
        """
        return self.map(lambda group, imgs: imgs.sum(axis=axis))

    def min(self, axis=None):
        """Apply `.ImageCollection.min` to each group.

        Parameters
        ----------
        axis: Str or Tuple[Str] or None
            A string or tuple strings of "pixels", "images", or "bands"

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> groups = col.groupby(lambda img: img.properties["date"].month)
        >>> groups.min(axis="images")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.min(axis="bands")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.min(axis="pixels")
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Dict[Str, Float]]] object at 0x...>
        >>> groups.min(axis=("bands", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Float]] object at 0x...>
        >>> groups.min(axis=("images", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Dict[Str, Float]] object at 0x...>
        >>> groups.min(axis=None)
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Float] object at 0x...>
        """
        return self.map(lambda group, imgs: imgs.min(axis=axis))

    def max(self, axis=None):
        """Apply `.ImageCollection.max` to each group.

        Parameters
        ----------
        axis: Str or Tuple[Str] or None
            A string or tuple of strings of "pixels", "images", or "bands"

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> groups = col.groupby(lambda img: img.properties["date"].month)
        >>> groups.max(axis="images")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.max(axis="bands")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.max(axis="pixels")
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Dict[Str, Float]]] object at 0x...>
        >>> groups.max(axis=("bands", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Float]] object at 0x...>
        >>> groups.max(axis=("images", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Dict[Str, Float]] object at 0x...>
        >>> groups.max(axis=None)
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Float] object at 0x...>
        """
        return self.map(lambda group, imgs: imgs.max(axis=axis))

    def mean(self, axis=None):
        """Apply `.ImageCollection.mean` to each group.

        Parameters
        ----------
        axis: Str or Tuple[Str] or None
            A string or tuple of strings of "pixels", "images", or "bands"

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> groups = col.groupby(lambda img: img.properties["date"].month)
        >>> groups.mean(axis="images")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.mean(axis="bands")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.mean(axis="pixels")
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Dict[Str, Float]]] object at 0x...>
        >>> groups.mean(axis=("bands", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Float]] object at 0x...>
        >>> groups.mean(axis=("images", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Dict[Str, Float]] object at 0x...>
        >>> groups.mean(axis=None)
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Float] object at 0x...>
        """
        return self.map(lambda group, imgs: imgs.mean(axis=axis))

    def median(self, axis=None):
        """Apply `.ImageCollection.median` to each group.

        Parameters
        ----------
        axis: Str or Tuple[Str] or None
            A string or tuple of strings of "pixels", "images", or "bands"

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> groups = col.groupby(lambda img: img.properties["date"].month)
        >>> groups.median(axis="images")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.median(axis="bands")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.median(axis="pixels")
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Dict[Str, Float]]] object at 0x...>
        >>> groups.median(axis=("bands", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Float]] object at 0x...>
        >>> groups.median(axis=("images", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Dict[Str, Float]] object at 0x...>
        >>> groups.median(axis=None)
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Float] object at 0x...>
        """
        return self.map(lambda group, imgs: imgs.median(axis=axis))

    def std(self, axis=None):
        """Apply `.ImageCollection.std` to each group.

        Parameters
        ----------
        axis: Str or Tuple[Str] or None
            A string or tuple of strings of "pixels", "images", or "bands"

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> groups = col.groupby(lambda img: img.properties["date"].month)
        >>> groups.std(axis="images")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.std(axis="bands")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> groups.std(axis="pixels")
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Dict[Str, Float]]] object at 0x...>
        >>> groups.std(axis=("bands", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, List[Float]] object at 0x...>
        >>> groups.std(axis=("images", "pixels"))
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Dict[Str, Float]] object at 0x...>
        >>> groups.std(axis=None)
        <descarteslabs.workflows.types.containers.dict_.Dict[Int, Float] object at 0x...>
        """
        return self.map(lambda group, imgs: imgs.std(axis=axis))

    def mosaic(self, reverse=False):
        """Apply `.ImageCollection.mosaic` to each group.

        Always returns an `.ImageCollection`.

        Parameters
        ----------
        axis: Str or Tuple[Str] or None
            A string or tuple of strings of "pixels", "images", or "bands"

        Returns
        -------
        mosaic: ImageCollection
            The order of mosaicing is from last to first per group, meaning
            the last `Image` in the group's `ImageCollection` is on top.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> groups = col.groupby(lambda img: img.properties["date"].month)
        >>> groups.mosaic()
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        """
        return self.map(lambda group, imgs: imgs.mosaic(reverse=reverse))
