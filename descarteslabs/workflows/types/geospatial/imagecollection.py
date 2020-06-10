import six

from descarteslabs.common.graft import client

from ... import env
from ...cereal import serializable
from ..array import MaskedArray
from ..containers import CollectionMixin, Dict, KnownDict, List, Slice, Struct, Tuple
from ..core import _resolve_lambdas, typecheck_promote
from ..datetimes import Datetime
from ..function import Function
from ..primitives import Any, Bool, Float, Int, NoneType, Str
from .feature import Feature
from .featurecollection import FeatureCollection
from .geometry import Geometry
from .image import Image
from .mixins import BandsMixin

ImageCollectionBase = Struct[
    {
        "ndarray": MaskedArray,
        "properties": List[
            KnownDict[
                {
                    "id": Str,
                    "product": Str,
                    "date": Datetime,
                    "crs": Str,
                    "geotrans": Tuple[Float, Float, Float, Float, Float, Float],
                },
                Str,
                Any,
            ]
        ],
        "bandinfo": Dict[
            Str,
            KnownDict[
                {"id": Str, "name": Str, "data_range": Tuple[Float, Float]}, Str, Any
            ],
        ],
    }
]


@serializable(is_named_concrete_type=True)
class ImageCollection(BandsMixin, CollectionMixin, ImageCollectionBase):
    """Proxy object representing a stack of Images; typically construct with `~.ImageCollection.from_id`.

    An ImageCollection is a proxy object holding multiple images, each with the same (ordered) bands of raster data,
    plus metadata about each Image.

    ImageCollections don't have a set spatial extent, CRS, resolution, etc:
    that's determined at computation time by the `~.geospatial.GeoContext` passsed in.

    Supports unary operations such as negation and absolute value, as well as arithmetic and comparison
    operators such as ``>``, ``+``, and ``//``.

    Examples
    --------
    >>> from descarteslabs.workflows import ImageCollection
    >>> from descarteslabs.scenes import DLTile
    >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
    ...        start_datetime="2017-01-01",
    ...        end_datetime="2017-12-31")
    >>> col
    <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
    >>> # create a geocontext for our computation, using DLTile
    >>> geoctx = DLTile.from_latlon(10, 30, resolution=10, tilesize=512, pad=0)
    >>> col.compute(geoctx) # doctest: +SKIP
    ImageCollectionResult of length 14:
      * ndarray: MaskedArray<shape=(14, 27, 512, 512), dtype=float64>
      * properties: 14 items
      * bandinfo: 'coastal-aerosol', 'blue', 'green', 'red', ...
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
    >>>
    >>> rgb = col.pick_bands("red green blue") # an ImageCollection with the red, green, and blue bands only
    >>> rgb.compute(geoctx) # doctest: +SKIP
    ImageCollectionResult of length 14:
      * ndarray: MaskedArray<shape=(14, 3, 512, 512), dtype=float64>
      * properties: 14 items
      * bandinfo: 'red', 'green', 'blue'
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
    >>> rgb.max(axis=("images", "pixels")).compute(geoctx) # max along the images then pixels axis # doctest: +SKIP
    {'red': 0.8074, 'green': 0.7857000000000001, 'blue': 0.8261000000000001}
    """

    _doc = {
        "properties": """Metadata for each `Image` in the `ImageCollection`.

            ``properties`` is a `List` of `Dict`, each of which always contains these fields:

            * ``id`` (`.Str`): the Descartes Labs ID of the Image
            * ``product`` (`.Str`): the Descartes Labs ID of the product the Image belogs to
            * ``date`` (`.Datetime`): the UTC date the Image was acquired
            * ``crs`` (`.Str`): the original Coordinate Reference System of the Image
            * ``geotrans`` (`.Tuple`): The original 6-tuple GDAL geotrans for the Image.

            Accessing other fields in the `Dict` will return instances of `.Any`.
            Accessing fields that don't actually exist on the data is a compute-time error.

            Note that you can call `.compute` on ``properties`` as a quick way to just retrieve metadata.
            Since ``properties`` is a `List`, you can also call `List.map`, `List.filter`, etc. on it.

            Example
            -------
            >>> from descarteslabs.workflows import ImageCollection
            >>> imgs = ImageCollection.from_id("landsat:LC08:PRE:TOAR")
            >>> result = imgs.properties.compute(ctx)  # doctest: +SKIP
            >>> type(result)  # doctest: +SKIP
            list
            >>> imgs.properties[0]['date']
            <descarteslabs.workflows.types.datetimes.datetime_.Datetime object at 0x...>
            >>> imgs.properties.map(lambda p: p['crs'])
            <descarteslabs.workflows.types.containers.list_.List[Str] object at 0x...>
            >>> imgs.properties[-1]['foobar']  # almost certainly a compute-time error
            <descarteslabs.workflows.types.primitives.any_.Any object at 0x...>
            """,
        "bandinfo": """\
            Metadata about the bands of the `ImageCollection`.

            ``bandinfo`` is a `Dict`, where keys are band names and values are Dicts
            which always contain these fields:

            * ``id`` (`.Str`): the Descartes Labs ID of the band
            * ``name`` (`.Str`): the name of the band. Equal to the key the Dict
              is stored under in ``bandinfo``
            * ``data_range`` (`.Tuple`): The ``(min, max)`` values the original data had.
              However, data in Images is automatically rescaled to physical range,
              or ``[0, 1]`` if physical range is undefined, so it won't be in ``data_range``
              anymore.

            Accessing other fields will return instances of `.Any`.
            Accessing fields that don't actually exist on the data is a compute-time error.

            Example
            -------
            >>> from descarteslabs.workflows import ImageCollection
            >>> imgs = ImageCollection.from_id("landsat:LC08:PRE:TOAR")
            >>> imgs.bandinfo['red']['data_range']
            <descarteslabs.workflows.types.containers.tuple_.Tuple[Float, Float] object at 0x...>
            >>> imgs.bandinfo['red']['foobar']  # almost certainly a compute-time error
            <descarteslabs.workflows.types.primitives.any_.Any object at 0x...>
            >>> imgs.bandinfo['foobar']['id']  # also likely a compute-time error
            <descarteslabs.workflows.types.primitives.string.Str object at 0x...>
            """,
    }
    _element_type = Image

    @typecheck_promote(List[Image])
    def __init__(self, images):
        """
        Construct an ImageCollection from a sequence of Images.

        Will return an empty `ImageCollection` if given an empty list or a list
        of empty images.
        If given a list of some non-empty and some empty images, the empties will be dropped.
        """

        self.graft = client.apply_graft(
            "wf.ImageCollection.from_images", images, env.geoctx
        )

    @classmethod
    @typecheck_promote(
        Str,
        start_datetime=(Datetime, NoneType),
        end_datetime=(Datetime, NoneType),
        limit=(Int, NoneType),
        resampler=(Str, NoneType),
        processing_level=(Str, NoneType),
    )
    def from_id(
        cls,
        product_id,
        start_datetime=None,
        end_datetime=None,
        limit=None,
        resampler=None,
        processing_level=None,
    ):
        """
        Create a proxy `ImageCollection` representing an entire product in the Descartes Labs catalog.

        This `ImageCollection` represents every `Image` in the product, everywhere.
        But when a `~.geospatial.GeoContext` is supplied at computation time
        (either by passing one into `compute`, or implicitly from the map view area
        when using `.Image.visualize`), the actual metadata lookup and computation
        only happens within that area of interest.

        Note there are two ways of filtering dates: using the ``start_datetime`` and ``end_datetime`` arguments here,
        and calling `filter` (like ``imgs.filter(lambda img: img.properties['date'].month > 5)``).
        We recommend using ``start_datetime`` and ``end_datetime`` for giving a coarse date window
        (at the year level, for example), then using `filter` to do more sophisticated filtering
        within that subset if necessary.

        The `ImageCollection` is sorted by date, in ascending order (older Images come first).

        If no imagery is found to satisfy the constraints, an empty `ImageCollection` is returned.

        Parameters
        ----------
        product_id: Str
            ID of the product
        start_datetime: Datetime or None, optional, default None
            Restrict the `ImageCollection` to Images acquired after this timestamp.
        end_datetime: Datetime or None, optional, default None
            Restrict the `ImageCollection` to Images before after this timestamp.
        limit: Int or None, optional, default None
            Maximum number of Images to include. If None (default),
            uses the Workflows default of 10,000. Note that specifying no limit
            is not supported.
        resampler: Str, optional, default None
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to the resolution and CRS eventually defined by a `~.geospatial.GeoContext`.
            Possible values are ``near`` (nearest-neighbor), ``bilinear``, ``cubic``, ``cubicspline``,
            ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``, ``q3``.
        processing_level : Str, optional
            Reflectance processing level. Possible values are ``'toa'`` (top of atmosphere)
            and ``'surface'``. For products that support it, ``'surface'`` applies
            Descartes Labs' general surface reflectance algorithm to the output.
            Any Images that have not been processed for surface reflectance are filtered out.
            If no Images support surface reflectance, returns an empty `ImageCollection`.

        Returns
        -------
        imgs: ImageCollection

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30", resampler="min")
        >>> col.compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        if resampler.literal_value is not None and resampler.literal_value not in [
            "near",
            "bilinear",
            "cubic",
            "cubicspline",
            "lanczos",
            "average",
            "mode",
            "max",
            "min",
            "med",
            "q1",
            "q3",
        ]:
            raise ValueError(
                "Unknown resampler type: {}".format(resampler.literal_value)
            )
        if (
            processing_level.literal_value is not None
            and processing_level.literal_value not in ("toa", "surface")
        ):
            raise ValueError(
                "Unknown processing level: {!r}. Must be None, 'toa', or 'surface'.".format(
                    processing_level.literal_value
                )
            )
        return cls._from_apply(
            "wf.ImageCollection.from_id",
            product_id,
            geocontext=env.geoctx,
            token=env._token,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            limit=limit,
            resampler=resampler,
            processing_level=processing_level,
            ruster=env._ruster,
        )

    @property
    def nbands(self):
        """The number of bands in the `ImageCollection`.

        If the `ImageCollection` is empty, returns 0.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.nbands.compute(geoctx) # doctest: +SKIP
        27
        """
        return Int._from_apply("wf.nbands", self)

    def with_bandinfo(self, band, **bandinfo):
        """
        New `ImageCollection`, with the given ``**bandinfo`` fields added to the specified band's `bandinfo`.

        If a given field already exists on the band's `bandinfo`, it will be overwritten.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Parameters
        ----------
        band: Str
            The name of the band whose bandinfo will be added to.
        **bandinfo: dict
            Fields that will be added to the band's bandinfo

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> with_foo = col.with_bandinfo("red", foo="baz")
        >>> with_foo.bandinfo["red"]["foo"].compute(geoctx) # doctest: +SKIP
        'baz'
        """
        return super(ImageCollection, self).with_bandinfo(band, **bandinfo)

    # @typecheck_promote(Str, VarArgs[Str])
    # Once we support checking variadic positional args in typecheck_promote, we can use
    # typecheck_promote instead.
    def without_bandinfo(self, band, *bandinfo_keys):
        """
        New `ImageCollection`, with each given ``*bandinfo_keys`` field dropped from the specified band's `bandinfo`.

        If a given field doesn't exists on the band's `bandinfo`, it will be a no-op.

        Parameters
        ----------
        band: Str
            The name of the band whose bandinfo will be pruned.
        *bandinfo_keys: Str
            Fields that will be dropped from the band's bandinfo

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.bandinfo["red"].compute(geoctx) # doctest: +SKIP
        {'color': 'Red',
         'data_description': 'TOAR, 0-10000 is 0 - 100% reflective',
        ...
        >>> without_desc = col.without_bandinfo("red", "data_description")
        >>> without_desc.bandinfo["red"].compute(geoctx) # doctest: +SKIP
        {'color': 'Red',
         'data_range': [0, 10000],
        ...
        """
        return super(ImageCollection, self).without_bandinfo(band, *bandinfo_keys)

    def pick_bands(self, bands, allow_missing=False):
        """
        New `ImageCollection`, containing Images with only the given bands.

        Bands can be given as a sequence of strings,
        or a single space-separated string (like ``"red green blue"``).

        Bands on the new `ImageCollection` will be in the order given.

        If names are duplicated, repeated names will be suffixed with ``_N``,
        with N incrementing from 1 for each duplication (``pick_bands("red red red")``
        returns bands named ``red red_1 red_2``).

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        If ``allow_missing`` is False (default), raises an error if given band
        names that don't exist in the `ImageCollection`. If ``allow_missing``
        is True, any missing names are dropped, and if none of the names exist,
        returns an empty `ImageCollection`.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> rgb = col.pick_bands("red green blue")
        >>> rgb.bandinfo.keys().inspect(ctx)  # doctest: +SKIP
        ["red", "green", "blue"]

        >>> red = col.pick_bands(["red", "nonexistent_band_name"], allow_missing=True)
        >>> red.bandinfo.keys().inspect(ctx)  # doctest: +SKIP
        ["red"]

        >>> s1_col = ImageCollection.from_id("sentinel-1:GRD")
        >>> vv_vh_vv = s1_col.pick_bands("vv vh vv")
        >>> vv_vh_vv.bandinfo.keys().inspect(ctx)  # doctest: +SKIP
        ["vv", "vh", "vv_1"]
        """
        return super(ImageCollection, self).pick_bands(
            bands, allow_missing=allow_missing
        )

    def rename_bands(self, *new_positional_names, **new_names):
        """
        New `ImageCollection`, with bands renamed by position or name.

        New names can be given positionally (like ``rename_bands('new_red', 'new_green')``),
        which renames the i-th band to the i-th argument.

        Or, new names can be given by keywords (like ``rename_bands(red="new_red")``)
        mapping from old band names to new ones.

        To eliminate ambiguity, names cannot be given both ways.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> renamed = col.rename_bands(red="new_red", blue="new_blue", green="new_green")
        """
        return super(ImageCollection, self).rename_bands(
            *new_positional_names, **new_names
        )

    def map(self, func):
        """
        Map a function over the Images in an `ImageCollection`.

        If the `ImageCollection` is empty, it will return an empty `ImageCollection` or `List`,
        according to the return type of ``func``.
        If ``func`` returns some empty and some non-empty `Image` objects, the empties are dropped
        so only the non-empties are included in the resulting `ImageCollection`.
        If ``func`` returns all empty `Image` objects, an empty `ImageCollection` is returned.

        Parameters
        ----------
        func : Python callable
            A function that takes a single `Image` and returns another
            proxytype.

        Returns
        -------
        mapped: ImageCollection or List
            `ImageCollection` if ``func`` returns `Image`,
            otherwise ``List[T]``, where ``T`` is the return type of ``func``.

            For example:

            * ``ic.map(lambda img: img + 1)`` returns an `ImageCollection`
            * ``ic.map(lambda img: img.properties["date"])`` returns a ``List[Datetime]``.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("sentinel-2:L1C")
        >>> dates = col.map(lambda img: img.properties["date"])
        >>> type(dates).__name__
        'List[Datetime]'
        >>> means = col.map(lambda img: img.mean(axis="pixels"))
        >>> type(means).__name__
        'List[Dict[Str, Float]]'
        >>> mean_col = col.map(lambda img: img.mean(axis="bands"))
        >>> type(mean_col).__name__
        'ImageCollection'
        """
        delayed_func = Function._delay(func, None, self._element_type)

        result_type = type(delayed_func)

        container_type, func = (
            (type(self), "wf.map_imagery")
            if result_type is self._element_type
            else (List[result_type], "wf.map")
        )
        return container_type._from_apply(func, self, delayed_func)

    @typecheck_promote(None, reverse=Bool)
    def sorted(self, key, reverse=False):
        """
        Copy of this `ImageCollection`, sorted by a key function.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Parameters
        ----------
        key: Function
            Function which takes an `Image` and returns a value to sort by.
        reverse: Bool, default False
            Sorts in ascending order if False (default), descending if True.

        Returns
        -------
        sorted: ImageCollection

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> month_reverse_sort = col.sorted(key=lambda img: img.properties["date"].month, reverse=True)
        """
        key = self._make_sort_key(key)
        return self._from_apply("wf.sorted", self, key, reverse=reverse)
        # NOTE(gabe): `key` is a required arg for the "sorted" function when given an ImageCollection,
        # hence why we don't give it as a kwarg like we do for Collection.sorted

    @typecheck_promote(None, back=Int, fwd=Int)
    def map_window(self, func, back=0, fwd=0):
        """
        Map a function over a sliding window of this `ImageCollection`.

        The function must take 3 arguments:

        * ``back``: `ImageCollection` of N prior images
        * ``current``: current `Image`
        * ``fwd``: `ImageCollection` of N subsequent images

        The window slides over the `ImageCollection`, starting at
        index ``back`` and ending ``fwd`` images before the end.

        Note that the total length of the window is ``back + 1 + fwd``.
        Specifying a window longer than the `ImageCollection` will cause an error.

        If the `ImageCollection` is empty, it will return an empty `ImageCollection` or `List`,
        according to the return type of ``func``.
        If ``func`` returns some empty and some non-empty imagery, the empties are dropped
        so only the non-empties are included in the resulting `ImageCollection`.
        If ``func`` returns all empty imagery, an empty `ImageCollection` is returned.

        Parameters
        ----------
        back: Int, optional, default 0
            Number of previous Images to pass as ``back`` to the function.
        fwd: Int, optional, default 0
            Number of subsequent Images to pass as ``fwd`` to the function.

        Returns
        -------
        mapped: ImageCollection or List
            If ``func`` returns an `ImageCollection` or `Image`,
            all of them are concatenated together and returned as one `ImageCollection`.

            Otherwise, returns a `List` of the values returned by ``func``.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection, concat
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-06-30")
        >>> new_col = col.map_window(lambda back, img, fwd:
        ...     concat(back, img, fwd).mean(axis="images").with_properties(date=img.properties['date']), back=1, fwd=1)
        >>> new_col.compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        delayed_func = Function.from_callable(
            func, ImageCollection, Image, ImageCollection
        )
        return_type = delayed_func._type_params[-1]

        out_type, func = (
            (ImageCollection, "wf.ImageCollection.map_window_ic")
            if return_type in (Image, ImageCollection)
            else (List[return_type], "wf.ImageCollection.map_window")
        )
        return out_type._from_apply(func, self, delayed_func, back, fwd)

    def map_bands(self, func):
        """
        Map a function over each band in ``self``.

        The function must take 2 arguments:

            1. `.Str`: the band name
            2. `ImageCollection`: 1-band `ImageCollection`

        If the function returns an `ImageCollection`, `map_bands` will also
        return one `ImageCollection`, containing the bands from all ImageCollections
        returned by ``func`` concatenated together.

        Otherwise, `map_bands` will return a `Dict` of the results
        of each call to ``func``, where the keys are the band names.

        Note that ``func`` can return ImageCollections with more than 1 band,
        but the band names must be unique across all of its results.

        If the `ImageCollection` is empty, it will return an empty `ImageCollection` or `Dict`,
        according to the return type of ``func``.
        If ``func`` produces an empty `Image` or `ImageCollection` for any band,
        that empty is returned.
        (Any band being empty propagates to all of them, because it is impossible to have some
        bands empty and some not.)

        Parameters
        ----------
        func: Python function
            Function that takes a `.Str` and an `ImageCollection`.

        Returns
        -------
        `ImageCollection` if ``func`` returns `ImageCollection`,
        otherwise ``Dict[Str, T]``, where ``T`` is the return type of ``func``.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> band_means = col.mean(axis=("images", "pixels"))
        >>> deviations = col.map_bands(lambda name, imgs: imgs - band_means[name])
        """
        return super(ImageCollection, self).map_bands(func)

    @typecheck_promote((lambda: ImageCollection, Image))
    def concat_bands(self, other):
        """
        New `ImageCollection`, with the bands in ``other`` appended to this one.

        If band names overlap, the band name from ``other`` will be suffixed with "_1".

        If the `ImageCollection` is empty, or ``other`` is empty, an empty is returned (following broadcasting rules).

        Parameters
        ----------
        other: `.Image`, ImageCollection
            If ``other`` is a single `.Image`, its bands will be added to
            every image in this `ImageCollection`.

            If ``other`` is an `ImageCollection`, it must be the same length as ``self``.

        Returns
        -------
        concatenated: ImageCollection

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> red = col.pick_bands("red")
        >>> green = col.pick_bands("green")
        >>> rg = red.concat_bands(green).compute(geoctx) # doctest: +SKIP
        >>> rg.bandinfo.keys() # doctest: +SKIP
        ['red', 'green']
        """
        return self._from_apply("wf.concat_bands", self, other)

    # @typecheck_promote(lambda: Tuple[ImageCollection])
    # Once we support checking variadic positional args in typecheck_promote, we can use typecheck_promote instead
    def concat(self, *imgs):
        """
        `ImageCollection` with ``imgs`` concatenated onto this one, where
        ``imgs`` is a variable number of `Image` or `ImageCollection` objects.

        Images, properties, and bandinfo are concatenated. All imagery
        must have the same number of bands with identical names. Any empty
        `Images` or `ImageCollections` will not be concatenated.

        Parameters
        ----------
        *imgs: variable number of `Image` or `ImageCollection` objects

        Returns
        -------
        concatenated: ImageCollection

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.concat(col, col).compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 6:
        ...
        """
        from .concat import concat as wf_concat

        if len(imgs) < 1:
            raise ValueError(
                "Must pass at least one imagery object to ImageCollection.concat()."
            )
        return wf_concat(self, *imgs)

    @typecheck_promote(Int)
    def head(self, n):
        """
        `ImageCollection` of the first ``n`` Images.

        If the `ImageCollection` is empty, an empty `ImageCollection` is returned (all values of n are valid).

        Parameters
        ----------
        n: Int
            Can be longer than the `ImageCollection` without error,
            in which case the whole `ImageCollection` is returned.

        Returns
        -------
        imgs: ImageCollection

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.head(2).compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        return ImageCollection._from_apply("wf.ImageCollection.head", self, n)

    @typecheck_promote(Int)
    def tail(self, n):
        """
        `ImageCollection` of the last ``n`` Images.

        If the `ImageCollection` is empty, an empty `ImageCollection` is returned (all values of n are valid).

        Parameters
        ----------
        n: Int
            Can be longer than the `ImageCollection` without error,
            in which case the whole `ImageCollection` is returned.

        Returns
        -------
        imgs: ImageCollection

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.tail(1).compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 1:
        ...
        """
        return ImageCollection._from_apply("wf.ImageCollection.tail", self, n)

    @typecheck_promote(Int)
    def partition(self, i):
        """
        Split this `ImageCollection` into two collections at index ``i``.

        If the `ImageCollection` is empty, a 2-tuple of empty `ImageCollection` objects
        is returned (all indices are valid).

        Parameters
        ----------
        i: Int
            The first `ImageCollection` will contain all Images up to but not including index ``i``.
            The second will contain the `Image` at index ``i`` and all subsequent ones.

        Returns
        -------
        Tuple[ImageCollection, ImageCollection]

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> head, tail = col.partition(1)
        >>> head.compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 1:
        ...
        >>> tail.compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 1:
        ...
        """
        return Tuple[ImageCollection, ImageCollection]._from_apply(
            "wf.ImageCollection.partition", self, i
        )

    @typecheck_promote(
        (lambda: ImageCollection, Image, Geometry, Feature, FeatureCollection),
        replace=Bool,
    )
    def mask(self, mask, replace=False):
        """
        New `ImageCollection`, masked with a boolean `ImageCollection`, `Image`, or vector object.

        If the mask is empty, the original `ImageCollection` is returned unchanged.
        (If the `ImageCollection` was already empty, it is still empty even if the mask is non-empty.)

        Parameters
        ----------
        mask: `ImageCollection`, `Image`, `Geometry`, `~.workflows.types.geospatial.Feature`, `~.workflows.types.geospatial.FeatureCollection`
            A single-band `ImageCollection` of boolean values,
            (such as produced by ``col > 2``, for example)
            where True means masked (invalid).

            Or, single-band `Image` of boolean values,
            (such as produced by ``img > 2``, for example)
            where True means masked (invalid).

            Or, a vector (`Geometry`, `~.workflows.types.geospatial.Feature`,
            or `~.workflows.types.geospatial.FeatureCollection`),
            in which case pixels *outside* the vector are masked.
        replace: Bool, default False
            If False (default), adds this mask to the current one,
            so already-masked pixels remain masked,
            or replaces the current mask with this new one if True.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> red = col.pick_bands("red")
        >>> masked = red.mask(red < 0.2) # mask all the bands where the red band is low
        """  # noqa
        if isinstance(mask, (Geometry, Feature, FeatureCollection)):
            mask = mask.rasterize().getmask()
        return self._from_apply("wf.mask", self, mask, replace=replace)

    def getmask(self):
        """
        Mask of this `ImageCollection`, as a new `ImageCollection` with one boolean band named 'mask'.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> mask = col.getmask().compute(geoctx) # doctest: +SKIP
        >>> mask.ndarray # doctest: +SKIP
        masked_array(
          data=[[[[0, 0, 0, ..., 1, 1, 1],
        ...
        >>> mask.bandinfo # doctest: +SKIP
        {'mask': {}}
        """
        return self._from_apply("wf.getmask", self)

    @typecheck_promote((Str, NoneType))
    def colormap(self, named_colormap="viridis", vmin=None, vmax=None):
        """
        Apply a colormap to an `ImageCollection`. Each image must have a single band.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Parameters
        ----------
        named_colormap: Str, default "viridis"
            The name of the Colormap registered with matplotlib.
            See https://matplotlib.org/users/colormaps.html for colormap options.
        vmin: float, default None
            The minimum value of the range to normalize the bands within.
            If specified, vmax must be specified as well.
        vmax: float, default None
            The maximum value of the range to normalize the bands within.
            If specified, vmin must be specified as well.

        Note: If neither vmin nor vmax are specified, the min and max values in each `Image` will be used.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.pick_bands("red").colormap("magma", vmin=0.1, vmax=0.8).compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
          * ndarray: MaskedArray<shape=(2, 3, 512, 512), dtype=float64>
          * properties: 2 items
          * bandinfo: 'red', 'green', 'blue'
          * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
        """
        if (vmin is not None and vmax is None) or (vmin is None and vmax is not None):
            raise ValueError("Must specify both vmin and vmax, or neither.")
        if (
            named_colormap.literal_value is not None
            and named_colormap.literal_value not in Image._colormaps
        ):
            raise ValueError(
                "Unknown colormap type: {}".format(named_colormap.literal_value)
            )
        return self._from_apply("wf.colormap", self, named_colormap, vmin, vmax)

    def groupby(self, func=None, dates=None):
        """
        Group the `ImageCollection` by a key value for each `Image`.

        ``func`` must take an `Image`, and return which group that `Image` belongs to.
        (The return type of ``func`` can be anything; the unique values returned by ``func``
        over every `Image` become the groups.)

        For convenience, ``dates`` can be given instead as a field name, or tuple of field names,
        to pull from the ``"date"`` field of `.Image.properties`.

        Creates an `ImageCollectionGroupby` object, which can be used
        to aggregate the groups.

        Within the `ImageCollectionGroupby`, every `Image` in every `ImageCollection` group
        also gets the field ``"group"`` added to its `~.Image.properties`, which identifies which group
        it belongs to.

        Type-theoretically: ``groupby(func: Function[Image, {}, T]) -> ImageCollectionGroupby[T]``

        If the `ImageCollection` is empty, or ``func`` results in empty groups,
        `ImageCollectionGroupby.groups` will return an empty `Dict`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> # group all Images from the same year and month, then take the mean (along the 'images' axis) of each group
        >>> col.groupby(dates=("year", "month")).mean(axis="images")
        <descarteslabs.workflows.types.geospatial.imagecollection.ImageCollection object at 0x...>
        >>> # group all Images from the same month, get Images from April, and take the median along the 'images' axis
        >>> col.groupby(dates="month")[4].median(axis="images")
        <descarteslabs.workflows.types.geospatial.image.Image object at 0x...>
        >>> # place images in 14 day bins, and take the min for each group (using a mapper function)
        >>> col.groupby(lambda img: img.properties['date'] // wf.Timedelta(days=14)).map(lambda group, img: img.min())
        <descarteslabs.workflows.types.containers.dict_.Dict[Datetime, Float] object at 0x...>
        """
        from .groupby import ImageCollectionGroupby

        if func and dates:
            raise TypeError("Only one of `func` or `dates` may be given")

        if dates:
            if isinstance(dates, six.string_types):
                dates = (dates,)

            # consider implementing this on the backend instead; may be more performant
            def func(img):
                date = img.properties["date"]
                fields = tuple(getattr(date, field) for field in dates)
                return fields[0] if len(fields) == 1 else fields

        delayed_func = Function.from_callable(func, Image)
        key_type = delayed_func._type_params[-1]

        return ImageCollectionGroupby[key_type](self, delayed_func)

    # Axis tuple to return type mapping.
    _STATS_RETURN_TYPES = {
        frozenset((None,)): Float,
        frozenset(("images",)): lambda: Image,
        frozenset(("bands",)): lambda: ImageCollection,
        frozenset(("pixels",)): List[Dict[Str, Float]],
        frozenset(("images", "pixels")): Dict[Str, Float],
        frozenset(("bands", "pixels")): List[Float],
        frozenset(("images", "bands")): lambda: Image,
        frozenset(("images", "bands", "pixels")): Float,
    }

    # To be resolved on first call.
    _RESOLVED_STATS_RETURN_TYPES = None

    @classmethod
    def _stats_return_type(cls, axis):
        if cls._RESOLVED_STATS_RETURN_TYPES is None:
            cls._RESOLVED_STATS_RETURN_TYPES = _resolve_lambdas(cls._STATS_RETURN_TYPES)

        axis_ = (
            axis
            if isinstance(axis, tuple)
            else tuple(axis)
            if isinstance(axis, list)
            else (axis,)
        )

        try:
            return cls._RESOLVED_STATS_RETURN_TYPES[frozenset(axis_)]
        except (KeyError, TypeError):
            raise ValueError(
                "Invalid axis argument {!r}, should be None, one of the strings "
                "'images', 'bands', or 'pixels', or a tuple containing some "
                "combination of 'images', 'bands', and 'pixels'.".format(axis)
            )

    def min(self, axis=None):
        """
        Minimum pixel value across the provided ``axis``, or across all pixels in the image
        collection if no ``axis`` argument is provided.

        If the `ImageCollection` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "images", "bands", "pixels", ("images", "pixels"), ("bands", "pixels"), ("images", "bands")}
            A Python string indicating the axis along which to take the minimum.

            Options:

            * ``"images"``: Returns an `.Image`
              containing the minimum value for each pixel in each band, across all
              scenes (i.e., a temporal minimum composite.)
            * ``"bands"``: Returns a new `ImageCollection` with one band, ``"min"``,
              containing the minimum value for each pixel across all bands in each
              scene.
            * ``"pixels"`` Returns a ``List[Dict[Str, Float]]`` containing each band's
              minimum pixel value for each scene in the collection.
            * ``None``: Returns a `.Float` that represents the minimum pixel value of the
              entire `ImageCollection`, across all scenes, bands, and pixels.
            * ``("images", "pixels")``: Returns a ``Dict[Str, Float]`` of the minimum pixel value for
              each band across all scenes, keyed by band name.
            * ``("bands", "pixels")``: Returns a ``List[Float]`` of the minimum pixel value for
              each scene across all bands.
            * ``("images", "bands")``: Returns an `.Image` containing the minimum value
              across all scenes and bands.

        Returns
        -------
        ``Dict[Str, Float]``, ``List[Float]``, ``List[Dict[Str, Float]]``, `.ImageCollection`, `.Image` or `.Float`
            Minimum pixel values across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> min_composite = col.min(axis="images")
        >>> min_col = col.min(axis="bands")
        >>> band_mins_per_scene = col.min(axis="pixels")
        >>> scene_mins = col.min(axis=("bands", "pixels"))
        >>> band_mins = col.min(axis=("images", "pixels"))
        >>> min_pixel = col.min(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.min", self, axis)

    def max(self, axis=None):
        """
        Maximum pixel value across the provided ``axis``, or across all pixels in the image
        collection if no ``axis`` argument is provided.

        If the `ImageCollection` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "images", "bands", "pixels", ("images", "pixels"), ("bands", "pixels"), ("images", "bands")}
            A Python string indicating the axis along which to take the maximum.

            Options:

            * ``"images"``: Returns an `.Image`
              containing the maximum value for each pixel in each band, across all
              scenes (i.e., a temporal maximum composite.)
            * ``"bands"``: Returns a new `ImageCollection` with one band, ``"max"``,
              containing the maximum value for each pixel across all bands in each scene.
            * ``"pixels"`` Returns a ``List[Dict[Str, Float]]`` containing each band's
              maximum pixel value for each scene in the collection.
            * ``None``: Returns a `.Float` that represents the maximum pixel value of the
              entire `ImageCollection`, across all scenes, bands, and pixels.
            * ``("images", "pixels")``: Returns a ``Dict[Str, Float]`` of the maximum pixel value for
              each band across all scenes, keyed by band name.
            * ``("bands", "pixels")``: Returns a ``List[Float]`` of the maximum pixel value for
              each scene across all bands.
            * ``("images", "bands")``: Returns an `.Image` containing the maximum value
              across all scenes and bands.

        Returns
        -------
        ``Dict[Str, Float]``, ``List[Float]``, ``List[Dict[Str, Float]]``, `.ImageCollection`, `.Image` or `.Float`
            Maximum pixel values across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> max_composite = col.max(axis="images")
        >>> max_col = col.max(axis="bands")
        >>> band_maxs_per_scene = col.max(axis="pixels")
        >>> scene_maxs = col.max(axis=("bands", "pixels"))
        >>> band_maxs = col.max(axis=("images", "pixels"))
        >>> max_pixel = col.max(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.max", self, axis)

    def mean(self, axis=None):
        """
        Mean pixel value across the provided ``axis``, or across all pixels in the image
        collection if no ``axis`` argument is provided.

        If the `ImageCollection` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "images", "bands", "pixels", ("images", "pixels"), ("bands", "pixels"), ("images", "bands")}
            A Python string indicating the axis along which to take the mean.

            Options:

            * ``"images"``: Returns an `.Image`
              containing the mean value for each pixel in each band, across all scenes
            * ``"bands"``: Returns a new `ImageCollection` with one band, ``"mean"``,
              containing the mean value for each pixel across all bands in each scene.
            * ``"pixels"`` Returns a ``List[Dict[Str, Float]]`` containing each band's
              mean pixel value for each scene in the collection.
              (i.e., a temporal mean composite.)
            * ``None``: Returns a `.Float` that represents the mean pixel value of the entire
              `ImageCollection`, across all scenes, bands, and pixels.
            * ``("images", "pixels")``: Returns a ``Dict[Str, Float]`` of the mean pixel value for
              each band across all scenes, keyed by band name.
            * ``("bands", "pixels")``: Returns a ``List[Float]`` of the mean pixel value for
              each scene across all bands.
            * ``("images", "bands")``: Returns an `.Image` containing the mean value
              across all scenes and bands.

        Returns
        -------
        ``Dict[Str, Float]``, ``List[Float]``, ``List[Dict[Str, Float]]``, `.ImageCollection`, `.Image` or `.Float`
            Mean pixel value across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> mean_composite = col.mean(axis="images")
        >>> mean_col = col.mean(axis="bands")
        >>> band_means_per_scene = col.mean(axis="pixels")
        >>> scene_means = col.mean(axis=("bands", "pixels"))
        >>> band_means = col.mean(axis=("images", "pixels"))
        >>> mean_pixel = col.mean(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.mean", self, axis)

    def median(self, axis=None):
        """
        Median pixel value across the provided ``axis``, or across all pixels in the image
        collection if no ``axis`` argument is provided.

        If the `ImageCollection` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "images", "bands", "pixels", ("images", "pixels"), ("bands", "pixels"), ("images", "bands")}
            A Python string indicating the axis along which to take the median.

            Options:

            * ``"images"``: Returns an `.Image`
              containing the median value for each pixel in each band, across all
              scenes (i.e., a temporal median composite.)
            * ``"bands"``: Returns a new `ImageCollection` with one band, ``"median"``,
              containing the median value for each pixel across all bands in each scene.
            * ``"pixels"`` Returns a ``List[Dict[Str, Float]]`` containing each band's
              median pixel value for each scene in the collection.
            * ``None``: Returns a `.Float` that represents the median pixel value of the
              entire `ImageCollection`, across all scenes, bands, and pixels.
            * ``("images", "pixels")``: Returns a ``Dict[Str, Float]`` of the median pixel value for
              each band across all scenes, keyed by band name.
            * ``("bands", "pixels")``: Returns a ``List[Float]`` of the median pixel value for
              each scene across all bands.
            * ``("images", "bands")``: Returns an `.Image` containing the median value
              across all scenes and bands.

        Returns
        -------
        ``Dict[Str, Float]``, ``List[Float]``, ``List[Dict[Str, Float]]``, `.ImageCollection`, `.Image` or `.Float`
            Median pixel value across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> median_composite = col.median(axis="images")
        >>> median_col = col.median(axis="bands")
        >>> band_medians_per_scene = col.median(axis="pixels")
        >>> scene_medians = col.median(axis=("bands", "pixels"))
        >>> band_medians = col.median(axis=("images", "pixels"))
        >>> median_pixel = col.median(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.median", self, axis)

    @typecheck_promote(band=(lambda: ImageCollection, Str), operation=Str)
    def sortby_composite(self, band, operation="argmax"):
        """
        Sort-by composite of an `ImageCollection`
        Creates a composite of an `ImageCollection` using the argmin or argmax of
        a specified band as the per-pixel ordering.

        Parameters
        ----------
        band: Str or `ImageCollection`
            If Str, the name of the band in ``self`` to use as the sorting band.
            If `ImageCollection`, use this single-band `ImageCollection`
            as the sorting band.

        operation: {"argmin", "argmax"}, default "argmax"
            A string indicating whether to use the minimum or maximum from ``band``
            when computing the sort-by composite.

        Returns
        -------
        composite: `.Image`

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> rgb = col.pick_bands("red green blue")
        >>> min_red_composite = rgb.sortby_composite("red", operation="argmin")
        >>> # ^ compute a minimum sort-by composite with an existing band
        >>> quality_band = col.pick_bands("swir1")
        >>> max_swir_composite = rgb.sortby_composite(quality_band)
        >>> # ^ compute a maximum sort-by composite with a provided band
        """
        if operation.literal_value is not None and operation.literal_value not in [
            "argmin",
            "argmax",
        ]:
            raise ValueError(
                "Invalid operation {!r}, must be 'argmin' or 'argmax'.".format(
                    operation.literal_value
                )
            )

        return Image._from_apply(
            "wf.ImageCollection.sortby_composite", self, band, operation=operation
        )

    def sum(self, axis=None):
        """
        Sum of pixel values across the provided ``axis``, or across all pixels in the image
        collection if no ``axis`` argument is provided.

        If the `ImageCollection` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "images", "bands", "pixels", ("images", "pixels"), ("bands", "pixels"), ("images", "bands")}
            A Python string indicating the axis along which to take the sum.

            Options:

            * ``"images"``: Returns an `.Image`
              containing the sum across all scenes for each pixel in each band (i.e., a
              temporal sum composite.)
            * ``"bands"``: Returns a new `ImageCollection` with one band, ``"sum"``,
              containing the sum across all bands for each pixel and each scene.
            * ``"pixels"`` Returns a ``List[Dict[Str, Float]]`` containing the sum of
              the pixel values for each band in each scene.
            * ``None``: Returns a `.Float` that represents the sum of all pixel values in the
              `ImageCollection`, across all scenes, bands, and pixels.
            * ``("images", "pixels")``: Returns a ``Dict[Str, Float]`` containing the sum of the
              pixel values for each band across all scenes, keyed by band name.
            * ``("bands", "pixels")``: Returns a ``List[Float]`` contianing the sum of the
              pixel values for each scene across all bands.
            * ``("images", "bands")``: Returns an `.Image` containing the sum across
              all scenes and bands.

        Returns
        -------
        ``Dict[Str, Float]``, ``List[Float]``, ``List[Dict[Str, Float]]``, `.ImageCollection`, `.Image` or `.Float`
            Sum of pixel values across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> sum_composite = col.sum(axis="images")
        >>> sum_col = col.sum(axis="bands")
        >>> band_sums_per_scene = col.sum(axis="pixels")
        >>> scene_sums = col.sum(axis=("bands", "pixels"))
        >>> band_sums = col.sum(axis=("images", "pixels"))
        >>> sum_pixel = col.sum(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.sum", self, axis)

    def std(self, axis=None):
        """
        Standard deviation along the provided ``axis``, or across all pixels in the image
        collection if no ``axis`` argument is provided.

        If the `ImageCollection` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "images", "bands", "pixels", ("images", "pixels"), ("bands", "pixels"), ("images", "bands")}
            A Python string indicating the axis along which to take the standard deviation.

            Options:

            * ``"images"``: Returns an `.Image`
              containing standard deviation across all scenes, for each pixel in each
              band (i.e., a temporal standard deviation composite.)
            * ``"bands"``: Returns a new `ImageCollection` with one band, ``"std"``,
              containing the standard deviation across all bands, for each pixel in each
              scene.
            * ``"pixels"`` Returns a ``List[Dict[Str, Float]]`` containing each band's
              standard deviation, for each scene in the collection.
            * ``None``: Returns a `.Float` that represents the standard deviation of the
              entire `ImageCollection`, across all scenes, bands, and pixels.
            * ``("images", "pixels")``: Returns a ``Dict[Str, Float]`` containing the standard
              deviation across all scenes, for each band, keyed by band name.
            * ``("bands", "pixels")``: Returns a ``List[Float]`` containing the standard
              deviation across all bands, for each scene.
            * ``("images", "bands")``: Returns an `.Image` containing the standard
              deviation across all scenes and bands.

        Returns
        -------
        ``Dict[Str, Float]``, ``List[Float]``, ``List[Dict[Str, Float]]``, `.ImageCollection`, `.Image` or `.Float`
            Standard deviation along the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> std_composite = col.std(axis="images")
        >>> std_col = col.std(axis="bands")
        >>> band_stds_per_scene = col.std(axis="pixels")
        >>> scene_stds = col.std(axis=("bands", "pixels"))
        >>> band_stds = col.std(axis=("images", "pixels"))
        >>> std = col.std(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.std", self, axis)

    def count(self, axis=None):
        """
        Count of valid (unmasked) pixels across the provided ``axis``, or across all pixels
        in the `ImageCollection` if no ``axis`` argument is provided.

        If the `ImageCollection` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "images", "bands", "pixels", ("images", "pixels"), ("bands", "pixels"), ("images", "bands")}
            A Python string indicating the axis along which to take the valid pixel count.

            Options:

            * ``"images"``: Returns an `.Image`
              containing the count of valid pixels across all scenes for each pixel in
              each band (i.e., a temporal count composite.)
            * ``"bands"``: Returns a new `ImageCollection` with one band, ``"count"``,
              containing the count of valid pixels across all bands for each pixel and
              each scene.
            * ``"pixels"``: Returns a ``List[Dict[Str, Float]]`` containing the count of
              valid pixels in each band in each scene.
            * ``None``: Returns a `.Float` that represents the valid pixel count for the
              entire `ImageCollection`, across all scenes, bands, and pixels.
            * ``("images", "pixels")``: Returns a ``Dict[Str, Float]`` containing the count of valid
              pixels in each band across all scenes, keyed by band name.
            * ``("bands", "pixels")``: Returns a ``List[Float]`` contianing the count of  valid
              pixels in each scene across all bands.
            * ``("images", "bands")``: Returns an `.Image` containing the count of
              valid pixels across all scenes and bands.

        Returns
        -------
        ``Dict[Str, Float]``, ``List[Float]``, ``List[Dict[Str, Float]]``, `.ImageCollection`, `.Image` or `.Float`
            Count of valid pixels across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> count_composite = col.count(axis="images")
        >>> count_col = col.count(axis="bands")
        >>> band_counts_per_scene = col.count(axis="pixels")
        >>> scene_counts = col.count(axis=("bands", "pixels"))
        >>> band_counts = col.count(axis=("images", "pixels"))
        >>> count = col.count(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.count", self, axis)

    @typecheck_promote(Bool)
    def mosaic(self, reverse=False):
        """
        Composite the `ImageCollection` into a single `~.geospatial.Image` by picking each first-unmasked pixel.

        The order of mosaicing is from last to first, meaning the last `Image` in the `ImageCollection` is on top.

        Parameters
        ----------
        reverse: Bool, default False
            The order of mosaicing. If False (default), the last `Image` in the `ImageCollection is on top. If True,
            the first `Image` in the `ImageCollection` is on top.

        Returns
        -------
        mosaicked: ~.geospatial.Image
            The mosaicked Image

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> col.mosaic()
        <descarteslabs.workflows.types.geospatial.image.Image object at 0x...>
        >>> date_mosaic = col.sorted(lambda img: img.properties['date']).mosaic()
        >>> cloudfree_mosaic = col.sorted(lambda img: img.properties['cloud_fraction']).mosaic()
        """
        return Image._from_apply("wf.mosaic", self, reverse=reverse)

    @typecheck_promote((Int, Slice))
    def __getitem__(self, item):
        return_type = Image if isinstance(item, Int) else self
        return return_type._from_apply("wf.getitem", self, item)

    # Binary comparators
    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __lt__(self, other):
        return self._from_apply("wf.lt", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __le__(self, other):
        return self._from_apply("wf.le", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float, Bool))
    def __eq__(self, other):
        return self._from_apply("wf.eq", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float, Bool))
    def __ne__(self, other):
        return self._from_apply("wf.ne", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __gt__(self, other):
        return self._from_apply("wf.gt", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __ge__(self, other):
        return self._from_apply("wf.ge", self, other)

    # Bitwise operators
    def __invert__(self):
        return self._from_apply("wf.invert", self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __and__(self, other):
        return self._from_apply("wf.and", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __or__(self, other):
        return self._from_apply("wf.or", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __xor__(self, other):
        return self._from_apply("wf.xor", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int))
    def __lshift__(self, other):
        return self._from_apply("wf.lshift", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int))
    def __rshift__(self, other):
        return self._from_apply("wf.rshift", self, other)

    # Reflected bitwise operators
    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __rand__(self, other):
        return self._from_apply("wf.and", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __ror__(self, other):
        return self._from_apply("wf.or", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __rxor__(self, other):
        return self._from_apply("wf.xor", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int))
    def __rlshift__(self, other):
        return self._from_apply("wf.lshift", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int))
    def __rrshift__(self, other):
        return self._from_apply("wf.rshift", other, self)

    # Arithmetic operators
    def log(ic):
        """
        Element-wise natural log of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.log().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.log(ic)

    def log2(ic):
        """
        Element-wise base 2 log of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.log2().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.log2(ic)

    def log10(ic):
        """
        Element-wise base 10 log of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.log10().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.log10(ic)

    def log1p(ic):
        """
        Element-wise log of 1 + an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.log1p().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.log1p(ic)

    def sqrt(self):
        """
        Element-wise square root of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.sqrt().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.sqrt(self)

    def cos(self):
        """
        Element-wise cosine of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.cos().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.cos(self)

    def arccos(self):
        """
        Element-wise inverse cosine of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.arccos().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.arccos(self)

    def sin(self):
        """
        Element-wise sine of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.sin().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.sin(self)

    def arcsin(self):
        """
        Element-wise inverse sine of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.arcsin().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.arcsin(self)

    def tan(self):
        """
        Element-wise tangent of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.tan().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.tan(self)

    def arctan(self):
        """
        Element-wise inverse tangent of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.arctan().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.arctan(self)

    def exp(self):
        """
        Element-wise exponential of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.exp().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.exp(self)

    def square(self):
        """
        Element-wise square of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.square().compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 2:
        ...
        """
        from ..math import arithmetic

        return arithmetic.square(self)

    @typecheck_promote(
        (Int, Float, NoneType, List[Int], List[Float], List[NoneType]),
        (Int, Float, NoneType, List[Int], List[Float], List[NoneType]),
    )
    def clip_values(self, min=None, max=None):
        """
        Given an interval, band values outside the interval are clipped to the interval edge.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Parameters
        ----------
        min: float or list, default None
            Minimum value of clipping interval. If None, clipping is not performed on the lower interval edge.
        max: float or list, default None
            Maximum value of clipping interval. If None, clipping is not performed on the upper interval edge.
            Different per-band clip values can be given by using lists for ``min`` or ``max``,
            in which case they must be the same length as the number of bands.

        Note: ``min`` and ``max`` cannot both be None. At least one must be specified.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.compute(geoctx).ndarray # doctest: +SKIP
        masked_array(
          data=[[[[0.1578, 0.1578, 0.1578, ..., 0.13920000000000002, 0.1376,
                         0.1376],
        ...
        >>> clipped = col.clip_values(0.14, 0.3).compute(geoctx) # doctest: +SKIP
        >>> clipped.ndarray # doctest: +SKIP
        masked_array(
          data=[[[[0.1578, 0.1578, 0.1578, ..., 0.14, 0.14, 0.14],
        ...
        """
        if min is None and max is None:
            raise ValueError(
                "min and max cannot both be None. At least one must be specified."
            )
        return self._from_apply("wf.clip_values", self, min, max)

    def scale_values(self, range_min, range_max, domain_min=None, domain_max=None):
        """
        Given an interval, band values will be scaled to the interval.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Parameters
        ----------
        range_min: float
            Minimum value of output range.
        range_max: float
            Maximum value of output range.
        domain_min: float, default None
            Minimum value of the domain. If None, the band minimum is used.
        domain_max: float, default None
            Maximum value of the domain. If None, the band maximum is used.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.compute(geoctx).ndarray # doctest: +SKIP
        masked_array(
          data=[[[[0.1578, 0.1578, 0.1578, ..., 0.13920000000000002, 0.1376,
                         0.1376],
        ...
        >>> scaled = col.scale_values(0.1, 0.5).compute(geoctx) # doctest: +SKIP
        >>> scaled.ndarray # doctest: +SKIP
        masked_array(
          data=[[[[0.10000706665039064, 0.10000706665039064,
        ...
        """
        return self._from_apply(
            "wf.scale_values", self, range_min, range_max, domain_min, domain_max
        )

    @typecheck_promote(
        (lambda: ImageCollection, Int, Float),
        mask=Bool,
        bandinfo=(NoneType, Dict[Str, Dict[Str, Any]]),
    )
    def replace_empty_with(self, fill, mask=True, bandinfo=None):
        """
        Replace `ImageCollection`, if empty, with fill value.

        Parameters
        ----------
        fill: int, float, `ImageCollection`
            The value to fill the `ImageCollection` with. If int or float, the fill value will be broadcasted to
            a 1 image collection, with band dimensions as determined by the geocontext and provided bandinfo.
        mask: bool, default True
            Whether to mask the band data. If ``mask`` is True and ``fill`` is an `ImageCollection`,
            the original `ImageCollection` mask will be overridden and all underlying data will be masked.
            If ``mask`` is False and ``fill`` is an `ImageCollection`, the original mask is left as is.
            If ``fill`` is scalar, the `ImageCollection` constructed will be fully masked or fully un-masked
            data if ``mask`` is True and False respectively.
        bandinfo: dict, default None
            Bandinfo used in constructing new `ImageCollection`. If ``fill`` is an `ImageCollection`,
            bandinfo is optional, and will be ignored if provided. If ``fill`` is a scalar,
            the bandinfo will be used to determine the number of bands on the new `ImageCollection`,
            as well as become the bandinfo for it.

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> # no imagery exists for this product within the date range
        >>> empty_col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-02-28")
        >>> empty_col.compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 0:
        ...
        >>> non_empty = empty_col.replace_empty_with(9999, bandinfo={"red":{}, "green":{}, "blue":{}})
        >>> non_empty.compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 0:
          * ndarray: MaskedArray<shape=(1, 3, 512, 512), dtype=int64>
          * properties: 0 items
          * bandinfo: 'red', 'green', 'blue'
          * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
        """
        if isinstance(fill, (Int, Float)) and isinstance(bandinfo, NoneType):
            # filling with scalar requires bandinfo to be provided
            raise ValueError(
                "To replace empty ImageCollection with an int or float, bandinfo must be provided."
            )
        return self._from_apply(
            "wf.ImageCollection.replace_empty_with", self, fill, mask, bandinfo
        )

    def value_at(self, x, y):
        """
        Given coordinates x, y, returns the pixel values from an ImageCollection in a `List[Dict]` by bandname.

        Coordinates must be given in the same coordinate reference system as the `~.geospatial.GeoContext`
        you call `.compute` with. For example, if your `~.geospatial.GeoContext` uses ``"EPSG:4326"``, you'd
        give ``x`` and ``y`` in lon, lat degrees. If your `~.geospatial.GeoContext` uses UTM, you'd give ``x``
        and ``y`` in UTM coordinates.

        When using `.visualize` to view the Image on a map, ``x`` and ``y`` must always be given in web-mercator
        (``"EPSG:3857"``) coordinates (with units of meters, not degrees).

        If the `ImageCollection` is empty, returns an empty `List[Dict]`.

        Parameters
        ----------
        x: float
           The x coordinate, in the same CRS as the `~.geospatial.GeoContext`
        y: float
            The y coordinate, in the same CRS as the `~.geospatial.GeoContext`

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.compute(geoctx).ndarray # doctest: +SKIP
        >>> rgb = col.pick_bands("red green blue") # an Image with the red, green, and blue bands only
        >>> rgb.value_at(459040.0, 3942400.0).compute(ctx) # doctest: +SKIP
        [{'red': 0.3569,
        'green': 0.33890000000000003,
        'blue': 0.37010000000000004},
        {'red': 0.2373,
        'green': 0.24480000000000002,
        'blue': 0.2505}]
        """
        return List[Dict[Str, Float]]._from_apply("wf.value_at", self, x, y)

    @typecheck_promote(Int, Int)
    def index_to_coords(self, row, col):
        """
        Convert pixel coordinates (row, col) in the `ImageCollection` into spatial coordinates (x, y).

        Parameters
        ----------
        row: int
           The row
        col: int
            The col

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.index_to_coords(0, 0).compute(ctx) # doctest: +SKIP
        (459040.0, 3942400.0)
        """
        return Tuple[Float, Float]._from_apply("wf.index_to_coords", self, row, col)

    @typecheck_promote(Float, Float)
    def coords_to_index(self, x, y):
        """
        Convert spatial coordinates (x, y) to pixel coordinates (row, col) in the `ImageCollection`.

        Parameters
        ----------
        row: Float
            The x coordinate, in the same CRS as the `~.geospatial.GeoContext`
        col: Float
             The y coordinate, in the same CRS as the `~.geospatial.GeoContext`

        Example
        -------
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-05-30")
        >>> col.coords_to_index(459040.0, 3942400.0).compute(ctx) # doctest: +SKIP
        (0, 0)
        """
        return Tuple[Int, Int]._from_apply("wf.coords_to_index", self, x, y)

    def __neg__(self):
        return self._from_apply("wf.neg", self)

    def __pos__(self):
        return self._from_apply("wf.pos", self)

    def __abs__(self):
        return self._from_apply("wf.abs", self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __add__(self, other):
        return self._from_apply("wf.add", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __sub__(self, other):
        return self._from_apply("wf.sub", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __mul__(self, other):
        return self._from_apply("wf.mul", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __div__(self, other):
        return self._from_apply("wf.div", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __truediv__(self, other):
        return self._from_apply("wf.div", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __floordiv__(self, other):
        return self._from_apply("wf.floordiv", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __mod__(self, other):
        return self._from_apply("wf.mod", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __pow__(self, other):
        return self._from_apply("wf.pow", self, other)

    # Reflected arithmetic operators
    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __radd__(self, other):
        return self._from_apply("wf.add", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rsub__(self, other):
        return self._from_apply("wf.sub", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rmul__(self, other):
        return self._from_apply("wf.mul", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rdiv__(self, other):
        return self._from_apply("wf.div", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rtruediv__(self, other):
        return self._from_apply("wf.truediv", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rfloordiv__(self, other):
        return self._from_apply("wf.floordiv", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rmod__(self, other):
        return self._from_apply("wf.mod", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rpow__(self, other):
        return self._from_apply("wf.pow", other, self)
