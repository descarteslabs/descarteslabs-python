import six

from descarteslabs.common.graft import client

from ... import env
from ...cereal import serializable
from ..containers import CollectionMixin, Dict, KnownDict, List, Struct, Tuple
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
    "Proxy object representing a stack of Images; typically construct with `~.ImageCollection.from_id`"
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
            >>> import descarteslabs.workflows as wf
            >>> imgs = wf.ImageCollection.from_id("landsat:LC08:PRE:TOAR")
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
            >>> import descarteslabs.workflows as wf
            >>> imgs = wf.ImageCollection.from_id("landsat:LC08:PRE:TOAR")
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
            "ImageCollection.from_images", images, env.geoctx
        )

    @classmethod
    @typecheck_promote(
        Str,
        start_datetime=(Datetime, NoneType),
        end_datetime=(Datetime, NoneType),
        limit=(Int, NoneType),
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
        resampler: str, optional, default None
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to the resolution and CRS eventually defined by a `~.geospatial.GeoContext`.
            Possible values are ``near`` (nearest-neighbor), ``bilinear``, ``cubic``, ``cubicsplice``,
            ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``, ``q3``.
        processing_level : str, optional
            Reflectance processing level. Possible values are ``'toa'`` (top of atmosphere)
            and ``'surface'``. For products that support it, ``'surface'`` applies
            Descartes Labs' general surface reflectance algorithm to the output.

        Returns
        -------
        imgs: ImageCollection
        """
        if resampler is not None and resampler not in [
            "near",
            "bilinear",
            "cubic",
            "cubicsplice",
            "lanczos",
            "average",
            "mode",
            "max",
            "min",
            "med",
            "q1",
            "q3",
        ]:
            raise ValueError("Unknown resampler type: {}".format(resampler))
        if processing_level is not None and processing_level not in ("toa", "surface"):
            raise ValueError(
                "Unknown processing level: {!r}. Must be None, 'toa', or 'surface'.".format(
                    processing_level
                )
            )
        return cls._from_apply(
            "ImageCollection.from_id",
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
        """
        return super(ImageCollection, self).without_bandinfo(band, *bandinfo_keys)

    def pick_bands(self, bands):
        """
        New `ImageCollection`, containing Images with only the given bands.

        Bands can be given as a sequence of strings,
        or a single space-separated string (like ``"red green blue"``).

        Bands on the Images will be in the order given.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.
        """
        return super(ImageCollection, self).pick_bands(bands)

    def rename_bands(self, *new_positional_names, **new_names):
        """
        New `ImageCollection`, with bands renamed by position or name.

        New names can be given positionally (like ``rename_bands('new_red', 'new_green')``),
        which renames the i-th band to the i-th argument.

        Or, new names can be given by keywords (like ``rename_bands(red="new_red")``)
        mapping from old band names to new ones.

        To eliminate ambiguity, names cannot be given both ways.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.
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
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("sentinel-2:L1C")
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
            (type(self), "map_imagery")
            if result_type is self._element_type
            else (List[result_type], "map")
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
        """
        key = self._make_sort_key(key)
        return self._from_apply("sorted", self, key, reverse=reverse)
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
        """
        delayed_func = Function.from_callable(
            func, ImageCollection, Image, ImageCollection
        )
        return_type = delayed_func._type_params[-1]

        out_type, func = (
            (ImageCollection, "ImageCollection.map_window_ic")
            if return_type in (Image, ImageCollection)
            else (List[return_type], "ImageCollection.map_window")
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
        """
        return self._from_apply("ImageCollection.concat_bands", self, other)

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
        """
        return ImageCollection._from_apply("ImageCollection.head", self, n)

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
        """
        return ImageCollection._from_apply("ImageCollection.tail", self, n)

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
        """
        return Tuple[ImageCollection, ImageCollection]._from_apply(
            "ImageCollection.partition", self, i
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
        """  # noqa
        if isinstance(mask, (Geometry, Feature, FeatureCollection)):
            mask = mask.rasterize().getmask()
        return self._from_apply("mask", self, mask, replace=replace)

    def getmask(self):
        """
        Mask of this `ImageCollection`, as a new `ImageCollection` with one boolean band named 'mask'.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.
        """
        return self._from_apply("getmask", self)

    def colormap(self, named_colormap="viridis", vmin=None, vmax=None):
        """
        Apply a colormap to an `ImageCollection`. Each image must have a single band.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.

        Parameters
        ----------
        named_colormap: str, default "viridis"
            The name of the Colormap registered with matplotlib.
            See https://matplotlib.org/users/colormaps.html for colormap options.
        vmin: float, default None
            The minimum value of the range to normalize the bands within.
            If specified, vmax must be specified as well.
        vmax: float, default None
            The maximum value of the range to normalize the bands within.
            If specified, vmin must be specified as well.

        Note: If neither vmin nor vmax are specified, the min and max values in each `Image` will be used.
        """
        if (vmin is not None and vmax is None) or (vmin is None and vmax is not None):
            raise ValueError("Must specify both vmin and vmax, or neither.")
        if named_colormap not in [
            "viridis",
            "plasma",
            "inferno",
            "magma",
            "cividis",
            "Greys",
            "Purples",
            "Blues",
            "Greens",
            "Oranges",
            "Reds",
            "YlOrBr",
            "YlOrRd",
            "OrRd",
            "PuRd",
            "RdPu",
            "BuPu",
            "GnBu",
            "PuBu",
            "YlGnBu",
            "PuBuGn",
            "BuGn",
            "YlGn",
            "binary",
            "gist_yarg",
            "gist_gray",
            "gray",
            "bone",
            "pink",
            "spring",
            "summer",
            "autumn",
            "winter",
            "cool",
            "Wistia",
            "hot",
            "afmhot",
            "gist_heat",
            "copper",
            "PiYG",
            "PRGn",
            "BrBG",
            "PuOr",
            "RdGy",
            "RdBu",
            "RdYlBu",
            "RdYlGn",
            "Spectral",
            "coolwarm",
            "bwr",
            "seismic",
            "twilight",
            "twilight_shifted",
            "hsv",
            "Pastel1",
            "Pastel2",
            "Paired",
            "Accent",
            "Dark2",
            "Set1",
            "Set2",
            "Set3",
            "tab10",
            "tab20",
            "tab20b",
            "tab20c",
            "flag",
            "prism",
            "ocean",
            "gist_earth",
            "terrain",
            "gist_stern",
            "gnuplot",
            "gnuplot2",
            "CMRmap",
            "cubehelix",
            "brg",
            "gist_rainbow",
            "rainbow",
            "jet",
            "nipy_spectral",
            "gist_ncar",
        ]:
            raise ValueError("Unknown colormap type: {}".format(named_colormap))
        return self._from_apply("colormap", self, named_colormap, vmin, vmax)

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
        return return_type._from_apply("min", self, axis)

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
        return return_type._from_apply("max", self, axis)

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
        return return_type._from_apply("mean", self, axis)

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
        return return_type._from_apply("median", self, axis)

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
        return return_type._from_apply("sum", self, axis)

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
        return return_type._from_apply("std", self, axis)

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
        return return_type._from_apply("count", self, axis)

    @typecheck_promote(Int)
    def __getitem__(self, item):
        # TODO(gabe): slices
        return Image._from_apply("getitem", self, item)

    # Binary comparators
    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __lt__(self, other):
        return self._from_apply("lt", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __le__(self, other):
        return self._from_apply("le", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float, Bool))
    def __eq__(self, other):
        return self._from_apply("eq", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float, Bool))
    def __ne__(self, other):
        return self._from_apply("ne", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __gt__(self, other):
        return self._from_apply("gt", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __ge__(self, other):
        return self._from_apply("ge", self, other)

    # Bitwise operators
    def __invert__(self):
        return self._from_apply("invert", self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __and__(self, other):
        return self._from_apply("and", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __or__(self, other):
        return self._from_apply("or", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __xor__(self, other):
        return self._from_apply("xor", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int))
    def __lshift__(self, other):
        return self._from_apply("lshift", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int))
    def __rshift__(self, other):
        return self._from_apply("rshift", self, other)

    # Reflected bitwise operators
    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __rand__(self, other):
        return self._from_apply("and", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __ror__(self, other):
        return self._from_apply("or", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Bool))
    def __rxor__(self, other):
        return self._from_apply("xor", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int))
    def __rlshift__(self, other):
        return self._from_apply("lshift", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int))
    def __rrshift__(self, other):
        return self._from_apply("rshift", other, self)

    # Arithmetic operators
    def log(ic):
        """
        Element-wise natural log of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.
        """
        from ..math import arithmetic

        return arithmetic.log(ic)

    def log2(ic):
        """
        Element-wise base 2 log of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.
        """
        from ..math import arithmetic

        return arithmetic.log2(ic)

    def log10(ic):
        """
        Element-wise base 10 log of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.
        """
        from ..math import arithmetic

        return arithmetic.log10(ic)

    def sqrt(self):
        """
        Element-wise square root of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.
        """
        from ..math import arithmetic

        return arithmetic.sqrt(self)

    def cos(self):
        """
        Element-wise cosine of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.
        """
        from ..math import arithmetic

        return arithmetic.cos(self)

    def sin(self):
        """
        Element-wise sine of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.
        """
        from ..math import arithmetic

        return arithmetic.sin(self)

    def tan(self):
        """
        Element-wise tangent of an `ImageCollection`.

        If the `ImageCollection` is empty, returns the empty `ImageCollection`.
        """
        from ..math import arithmetic

        return arithmetic.tan(self)

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
        """
        if min is None and max is None:
            raise ValueError(
                "min and max cannot both be None. At least one must be specified."
            )
        return self._from_apply("clip_values", self, min, max)

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
        """
        return self._from_apply(
            "scale_values", self, range_min, range_max, domain_min, domain_max
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
        """
        if isinstance(fill, (Int, Float)) and isinstance(bandinfo, NoneType):
            # filling with scalar requires bandinfo to be provided
            raise ValueError(
                "To replace empty ImageCollection with an int or float, bandinfo must be provided."
            )
        return self._from_apply(
            "ImageCollection.replace_empty_with", self, fill, mask, bandinfo
        )

    def __neg__(self):
        return self._from_apply("neg", self)

    def __pos__(self):
        return self._from_apply("pos", self)

    def __abs__(self):
        return self._from_apply("abs", self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __add__(self, other):
        return self._from_apply("add", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __sub__(self, other):
        return self._from_apply("sub", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __mul__(self, other):
        return self._from_apply("mul", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __div__(self, other):
        return self._from_apply("div", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __truediv__(self, other):
        return self._from_apply("div", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __floordiv__(self, other):
        return self._from_apply("floordiv", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __mod__(self, other):
        return self._from_apply("mod", self, other)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __pow__(self, other):
        return self._from_apply("pow", self, other)

    # Reflected arithmetic operators
    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __radd__(self, other):
        return self._from_apply("add", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rsub__(self, other):
        return self._from_apply("sub", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rmul__(self, other):
        return self._from_apply("mul", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rdiv__(self, other):
        return self._from_apply("div", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rtruediv__(self, other):
        return self._from_apply("truediv", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rfloordiv__(self, other):
        return self._from_apply("floordiv", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rmod__(self, other):
        return self._from_apply("mod", other, self)

    @typecheck_promote((Image, lambda: ImageCollection, Int, Float))
    def __rpow__(self, other):
        return self._from_apply("pow", other, self)
