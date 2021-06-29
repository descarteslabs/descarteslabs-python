import six
import logging

from descarteslabs import scenes

from ...cereal import serializable
from ..array import MaskedArray
from ..containers import Dict, KnownDict, Struct, Tuple, List
from ..core import typecheck_promote, _resolve_lambdas
from ..datetimes import Datetime
from ..primitives import Any, Bool, Float, Int, Str, NoneType
from ..proxify import proxify
from .feature import Feature
from .featurecollection import FeatureCollection
from .geometry import Geometry
from .mixins import BandsMixin


def _DelayedImageCollection():
    from .imagecollection import ImageCollection

    return ImageCollection


ImageBase = Struct[
    {
        "ndarray": MaskedArray,
        "properties": KnownDict[
            {
                "id": Str,
                "date": Datetime,
                "product": Str,
                "crs": Str,
                "geotrans": Tuple[Float, Float, Float, Float, Float, Float],
            },
            Str,
            Any,
        ],
        "bandinfo": Dict[
            Str,
            KnownDict[
                {
                    "id": Str,
                    "name": Str,
                    # "unit": Str,
                    "data_range": Tuple[Float, Float],
                    # "physical_range": Tuple[Float, Float],
                },
                Str,
                Any,
            ],
        ],
    }
]


@serializable(is_named_concrete_type=True)
class Image(ImageBase, BandsMixin):
    """
    Proxy Image; construct with `~.Image.from_id` or `~.Image.from_scenes`.

    An Image is a proxy object holding multiple (ordered) bands of raster data,
    plus some metadata.

    Images don't have a set spatial extent, CRS, resolution, etc:
    that's determined at computation time by the `~.geospatial.GeoContext` passsed in.

    Supports unary operations such as negation and absolute value, as well as arithmetic and comparison
    operators such as ``>``, ``+``, and ``//``.

    Examples
    --------
    >>> from descarteslabs.workflows import Image
    >>> from descarteslabs.scenes import DLTile
    >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
    >>> img
    <descarteslabs.workflows.types.geospatial.image.Image object at 0x...>
    >>> # create a geocontext for our computation, using DLTile
    >>> geoctx = DLTile.from_latlon(35.6, -105.4, resolution=10, tilesize=512, pad=0)
    >>> img.compute(geoctx) # doctest: +SKIP
    ImageResult:
      * ndarray: MaskedArray<shape=(27, 512, 512), dtype=float64>
      * properties: 'absolute_orbit', 'acquired', 'archived', 'area', ...
      * bandinfo: 'coastal-aerosol', 'blue', 'green', 'red', ...
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
    >>>
    >>> rgb = img.pick_bands("red green blue") # an Image with the red, green, and blue bands only
    >>> rgb.compute(geoctx) # doctest: +SKIP
    ImageResult:
      * ndarray: MaskedArray<shape=(3, 512, 512), dtype=float64>
      * properties: 'absolute_orbit', 'acquired', 'archived', 'area', ...
      * bandinfo: 'red', 'green', 'blue'
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
    >>> rgb.min(axis="pixels").compute(geoctx) # min along the pixels axis # doctest: +SKIP
    {'red': 0.0329, 'green': 0.0461, 'blue': 0.0629}
    """

    _doc = {
        "properties": """\
            Metadata for the `Image`.

            ``properties`` is a `Dict` which always contains these fields:

            * ``id`` (`.Str`): the Descartes Labs ID of the Image
            * ``product`` (`.Str`): the Descartes Labs ID of the product the Image belogs to
            * ``date`` (`.Datetime`): the UTC date the Image was acquired
            * ``crs`` (`.Str`): the original Coordinate Reference System of the Image
            * ``geotrans`` (`.Tuple`): The original 6-tuple GDAL geotrans for the Image.

            Accessing other fields will return instances of `.Any`.
            Accessing fields that don't actually exist on the data is a compute-time error.

            Example
            -------
            >>> from descarteslabs.workflows import Image
            >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
            >>> img.properties['date']
            <descarteslabs.workflows.types.datetimes.datetime_.Datetime object at 0x...>
            >>> img.properties['date'].year
            <descarteslabs.workflows.types.primitives.number.Int object at 0x...>
            >>> img.properties['id']
            <descarteslabs.workflows.types.primitives.string.Str object at 0x...>
            >>> img.properties['foobar']  # almost certainly a compute-time error
            <descarteslabs.workflows.types.primitives.any_.Any object at 0x...>
            """,
        "bandinfo": """\
            Metadata about the bands of the `Image`.

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
            >>> from descarteslabs.workflows import Image
            >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
            >>> img.bandinfo['red']['data_range']
            <descarteslabs.workflows.types.containers.tuple_.Tuple[Float, Float] object at 0x...>
            >>> img.bandinfo['red']['foobar']  # almost certainly a compute-time error
            <descarteslabs.workflows.types.primitives.any_.Any object at 0x...>
            >>> img.bandinfo['foobar']['id']  # also likely a compute-time error
            <descarteslabs.workflows.types.primitives.string.Str object at 0x...>
            """,
    }

    def __init__(self):
        raise TypeError(
            "Please use a classmethod such as `Image.from_id` or `Image.from_scene` to instantiate an Image."
        )

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, scenes.Scene):
            return cls.from_scene(obj)
        return super(Image, cls)._promote(obj)

    @classmethod
    @typecheck_promote(Str, (Str, NoneType), (Str, NoneType))
    def from_id(cls, image_id, resampler=None, processing_level=None):
        """
        Create a proxy `Image` from an ID in the Descartes Labs catalog.

        Parameters
        ----------
        image_id: Str
            ID of the image
        resampler: Str, optional, default None
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to the resolution and CRS eventually defined by a `~.geospatial.GeoContext`.
            Possible values are ``near`` (nearest-neighbor), ``bilinear``, ``cubic``, ``cubicspline``,
            ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``, ``q3``.
        processing_level : Str, optional
            Image processing level. Possible values depend on the particular product and bands. Some
            examples include ``'toa'``, ``'surface'``, ``'toa_refectance'``, ``'toa_radiance'``.

        Returns
        -------
        img: ~.geospatial.Image

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", processing_level="surface")
        >>> img.compute(geoctx) # doctest: +SKIP
        ImageResult:
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
            raise ValueError(f"Unknown resampler type: {resampler.literal_value!r}")
        return cls._from_apply(
            "wf.Image.load",
            image_id,
            resampler=resampler,
            processing_level=processing_level,
        )

    @classmethod
    def from_scene(cls, scene):
        "Create a proxy image from a `~descarteslabs.scenes.scene.Scene` object"
        return cls.from_id(scene.properties["id"])

    @property
    def nbands(self):
        """The number of bands in the `Image`.

        If the `Image` is empty, returns 0.

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> img.nbands.compute(geoctx) # doctest: +SKIP
        25
        """
        return Int._from_apply("wf.nbands", self)

    def with_properties(self, **properties):
        """
        New `Image`, with the given ``**properties`` fields added to the Image's `properties`.

        If a given field already exists on the Image's properties, it will be overwritten.

        If the `Image` is empty, returns the empty `Image`.

        Parameters
        ----------
        **properties: Proxytype, or any JSON-serializable value
            Fields that will be added to the image's properties

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> with_foo = img.with_properties(foo="baz").compute(geoctx) # doctest: +SKIP
        >>> with_foo.properties["foo"] # doctest: +SKIP
        'baz'
        """
        properties_promoted = {}
        for name, value in six.iteritems(properties):
            try:
                properties_promoted[name] = proxify(value)
            except NotImplementedError as e:
                raise ValueError(
                    "Invalid value {!r} for property {!r}.\n{}".format(
                        value, name, str(e)
                    )
                )

        return self._from_apply("wf.with_properties", self, **properties_promoted)

    # @typecheck_promote(VarArgs[Str])
    # Once we support checking variadic positional args in typecheck_promote, we can use
    # typecheck_promote instead.
    def without_properties(self, *property_keys):
        """
        New `Image`, with each given property field name dropped from the Image's `properties` field.

        If a given field doesn't exist on the Image's properties, it will be a no-op.

        Parameters
        ----------
        *properties_keys: Str
            Fields that will be dropped from the image's properties

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> img.compute(geoctx).properties # doctest: +SKIP
        {'acquired': '2016-07-06T16:59:42.753476+00:00',
         'area': 35619.4,
        ...
        >>> without_acq = img.without_properties("acquired").compute(geoctx) # doctest: +SKIP
        >>> without_acq.properties # doctest: +SKIP
        {'area': 35619.4,
         'bits_per_pixel': [0.836, 1.767, 0.804],
        ...
        """
        for property_key in property_keys:
            if not isinstance(property_key, (Str, six.string_types)):
                raise TypeError(
                    "Invalid type {!r} for property key, must be a string.".format(
                        type(property_key).__name__
                    )
                )
        return self._from_apply("wf.without_properties", self, *property_keys)

    def with_bandinfo(self, band, **bandinfo):
        """
        New `Image`, with the given ``**bandinfo`` fields added to the specified band's `bandinfo`.

        If a given field already exists on the band's bandinfo, it will be overwritten.

        If the `Image` is empty, returns the empty `Image`.

        Parameters
        ----------
        band: Str
            The name of the band whose bandinfo will be added to.
        **bandinfo: dict
            Fields that will be added to the band's bandinfo

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> with_foo = img.with_bandinfo("red", foo="baz").compute(geoctx) # doctest: +SKIP
        >>> with_foo.bandinfo["red"]["foo"] # doctest: +SKIP
        'baz'
        """
        return super(Image, self).with_bandinfo(band, **bandinfo)

    # @typecheck_promote(Str, VarArgs[Str])
    # Once we support checking variadic positional args in typecheck_promote, we can use
    # typecheck_promote instead.
    def without_bandinfo(self, band, *bandinfo_keys):
        """
        New `Image`, with each given ``*bandinfo_keys`` field dropped from the specified band's `bandinfo`.

        If a given field doesn't exists on the band's `bandinfo`, it will be a no-op.

        Parameters
        ----------
        band: Str
            The name of the band whose bandinfo will be pruned.
        *bandinfo_keys: Str
            Fields that will be dropped from the band's bandinfo

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> img.compute(geoctx).bandinfo["red"] # doctest: +SKIP
        {'color': 'Red',
         'data_description': 'TOAR, 0-10000 is 0 - 100% reflective',
        ...
        >>> without_desc = img.without_bandinfo("red", "data_description").compute(geoctx) # doctest: +SKIP
        >>> without_desc.bandinfo["red"] # doctest: +SKIP
        {'color': 'Red',
         'data_range': [0, 10000],
        ...
        """
        return super(Image, self).without_bandinfo(band, *bandinfo_keys)

    @typecheck_promote(lambda: Image)
    def concat_bands(self, other_image):
        """
        New `Image`, with the bands in ``other_image`` appended to this one.

        If band names overlap, the band from the *other* `Image` will be suffixed with "_1".

        If either `Image` is empty, returns another empty `Image`.

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> red = img.pick_bands("red")
        >>> green = img.pick_bands("green")
        >>> rg = red.concat_bands(green).compute(geoctx) # doctest: +SKIP
        >>> rg.bandinfo.keys() # doctest: +SKIP
        ['red', 'green']
        """
        return self._from_apply("wf.concat_bands", self, other_image)

    # @typecheck_promote(lambda: Tuple[ImageCollection])
    # Once we support checking variadic positional args in typecheck_promote, we can use typecheck_promote instead
    def concat(self, *imgs):
        """
        `ImageCollection` with ``imgs`` concatenated onto this `Image`, where
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
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> img.concat(img, img).compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 3:
        ...
        """
        from .concat import concat as wf_concat

        if len(imgs) < 1:
            raise ValueError("Must pass at least one imagery object to Image.concat().")
        return wf_concat(self, *imgs)

    @typecheck_promote(
        (lambda: Image, Geometry, Feature, FeatureCollection), replace=Bool
    )
    def mask(self, mask, replace=False):
        """
        New `Image`, masked with a boolean `Image` or vector object.

        If the mask is empty, the original `Image` is returned unchanged.
        (If the `Image` was already empty, it is still empty even if the mask is non-empty.)

        Parameters
        ----------
        mask: `Image`, `Geometry`, `~.workflows.types.geospatial.Feature`, `~.workflows.types.geospatial.FeatureCollection`
            A single-band `Image` of boolean values,
            (such as produced by ``img > 2``, for example)
            where True means masked (invalid).

            Or, a vector (`Geometry`, `~.workflows.types.geospatial.Feature`,
            or `~.workflows.types.geospatial.FeatureCollection`),
            in which case pixels *outside* the vector are masked.
        replace: Bool, default False
            Whether to add to the Image's current mask, or replace it.

            If False (default):

            * Adds this mask to the current one, so already-masked pixels remain masked.
            * Masked-out pixels in the `mask` are ignored (considered False).

            If True:

            * Replaces the current mask with this new one.
            * Masked-out pixels in the `mask` are also masked in the result.

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> red = img.pick_bands("red")
        >>> masked = red.mask(red < 0.2) # mask all bands where red band is low
        """  # noqa
        if isinstance(mask, (Geometry, Feature, FeatureCollection)):
            mask = mask.rasterize().getmask()
        return self._from_apply("wf.mask", self, mask, replace=replace)

    def getmask(self):
        """
        Mask of this `Image`, as a new `Image` with one boolean band named ``'mask'``.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> mask = img.getmask().compute(geoctx) # doctest: +SKIP
        >>> mask.ndarray # doctest: +SKIP
        masked_array(
          data=[[[0, 0, 0, ..., 1, 1, 1],
        ...
        >>> mask.bandinfo # doctest: +SKIP
        {'mask': {}}
        """
        return self._from_apply("wf.getmask", self)

    @typecheck_promote((Str, NoneType))
    def colormap(self, named_colormap="viridis", vmin=None, vmax=None):
        """
        Apply a colormap to an `Image`. Image must have a single band.

        If the `Image` is empty, returns the empty `Image`.

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

        Note: If neither vmin nor vmax are specified, the min and max values in the `Image` will be used.

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> img.pick_bands("red").colormap("magma", vmin=0.1, vmax=0.8).compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        if (vmin is not None and vmax is None) or (vmin is None and vmax is not None):
            raise ValueError("Must specify both vmin and vmax, or neither.")
        if (
            named_colormap.literal_value is not None
            and named_colormap.literal_value not in self._colormaps
        ):
            raise ValueError(
                "Unknown colormap type: {}".format(named_colormap.literal_value)
            )
        return self._from_apply("wf.colormap", self, named_colormap, vmin, vmax)

    _STATS_RETURN_TYPES = {
        None: Float,
        "pixels": Dict[Str, Float],
        "bands": lambda: Image,
        ("pixels", "bands"): Float,
        ("bands", "pixels"): Float,
    }
    _RESOLVED_STATS_RETURN_TYPES = None

    @classmethod
    def _stats_return_type(cls, axis):
        if cls._RESOLVED_STATS_RETURN_TYPES is None:
            cls._RESOLVED_STATS_RETURN_TYPES = _resolve_lambdas(cls._STATS_RETURN_TYPES)

        if isinstance(axis, list):
            axis = tuple(axis)
        if isinstance(axis, tuple) and len(axis) == 1:
            axis = axis[0]

        try:
            return cls._RESOLVED_STATS_RETURN_TYPES[axis]
        except KeyError:
            raise ValueError(
                "Invalid axis argument {!r}, should be one of {}.".format(
                    axis,
                    ", ".join(
                        map(repr, six.viewkeys(cls._RESOLVED_STATS_RETURN_TYPES))
                    ),
                )
            )

    def min(self, axis=None):
        """
        Minimum pixel value across the provided ``axis``, or across all pixels in the image
        if no ``axis`` argument is provided.

        If the `Image` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "pixels", "bands"}
            A Python string indicating the axis along which to take the minimum.

            Options:

            * ``"pixels"``: Returns a ``Dict[Str, Float]`` of each band's minimum pixel value.
            * ``"bands"``: Returns a new `.Image` with
              one band, ``"min"``, containing the minimum value for each pixel across
              all bands.
            * ``None``: Returns a `.Float` that represents the minimum pixel value of the
              entire image.

        Returns
        -------
        ``Dict[Str, Float]`` or `.Image` or `.Float`
            Minimum pixel values across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:01:RT:TOAR:meta_LC08_L1TP_033035_20170516_20170516_01_RT_v1")
        >>> min_img = img.min(axis="bands")
        >>> band_mins = img.min(axis="pixels")
        >>> min_pixel = img.min(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.min", self, axis)

    def max(self, axis=None):
        """
        Maximum pixel value across the provided ``axis``, or across all pixels in the image
        if no ``axis`` argument is provided.

        If the `Image` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "pixels", "bands"}
            A Python string indicating the axis along which to take the maximum.

            Options:

            * ``"pixels"``: Returns a ``Dict[Str, Float]`` of each band's maximum pixel value.
            * ``"bands"``: Returns a new `.Image` with
              one band, ``"max"``, containing the maximum value for each pixel across
              all bands.
            * ``None``: Returns a `.Float` that represents the maximum pixel value of the
              entire image.

        Returns
        -------
        ``Dict[Str, Float]`` or `.Image` or `.Float`
            Maximum pixel values across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:01:RT:TOAR:meta_LC08_L1TP_033035_20170516_20170516_01_RT_v1")
        >>> max_img = img.max(axis="bands")
        >>> band_maxs = img.max(axis="pixels")
        >>> max_pixel = img.max(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.max", self, axis)

    def mean(self, axis=None):
        """
        Mean pixel value across the provided ``axis``, or across all pixels in the image
        if no ``axis`` argument is provided.

        If the `Image` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "pixels", "bands"}
            A Python string indicating the axis along which to take the mean.

            Options:

            * ``"pixels"``: Returns a ``Dict[Str, Float]`` of each band's mean pixel value.
            * ``"bands"``: Returns a new `.Image` with
              one band, ``"mean"``, containing the mean value for each pixel across all
              bands.
            * ``None``: Returns a `.Float` that represents the mean pixel value of the entire
              image.

        Returns
        -------
        ``Dict[Str, Float]`` or `.Image` or `.Float`
            Mean pixel values across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:01:RT:TOAR:meta_LC08_L1TP_033035_20170516_20170516_01_RT_v1")
        >>> mean_img = img.mean(axis="bands")
        >>> band_means = img.mean(axis="pixels")
        >>> mean_pixel = img.mean(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.mean", self, axis)

    def median(self, axis=None):
        """
        Median pixel value across the provided ``axis``, or across all pixels in the image
        if no ``axis`` argument is provided.

        If the `Image` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "pixels", "bands"}
            A Python string indicating the axis along which to take the median.

            Options:

            * ``"pixels"``: Returns a ``Dict[Str, Float]`` of each band's median pixel value.
            * ``"bands"``: Returns a new `.Image` with
              one band, ``"median"``, containing the median value for each pixel across
              all bands.
            * ``None``: Returns a `.Float` that represents the median pixel value of the
              entire image.

        Returns
        -------
        ``Dict[Str, Float]`` or `.Image` or `.Float`
            Median pixel values across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:01:RT:TOAR:meta_LC08_L1TP_033035_20170516_20170516_01_RT_v1")
        >>> median_img = img.median(axis="bands")
        >>> band_medians = img.median(axis="pixels")
        >>> median_pixel = img.median(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.median", self, axis)

    def sum(self, axis=None):
        """
        Sum of pixel values across the provided ``axis``, or across all pixels in the image
        if no ``axis`` argument is provided.

        If the `Image` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "pixels", "bands"}
            A Python string indicating the axis along which to take the sum.

            Options:

            * ``"pixels"``: Returns a ``Dict[Str, Float]`` containing the sum of the pixel
              values for each band.
            * ``"bands"``: Returns a new `.Image` with
              one band, ``"sum"``, containing the sum across all bands for each pixel.
            * ``None``: Returns a `.Float` that represents the sum of all pixels in the
              image.

        Returns
        -------
        ``Dict[Str, Float]`` or `.Image` or `.Float`
            Sum of pixel values across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:01:RT:TOAR:meta_LC08_L1TP_033035_20170516_20170516_01_RT_v1")
        >>> sum_img = img.sum(axis="bands")
        >>> band_sums = img.sum(axis="pixels")
        >>> sum_pixels = img.sum(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.sum", self, axis)

    def std(self, axis=None):
        """
        Standard deviation along the provided ``axis``, or across all pixels in the image
        if no ``axis`` argument is provided.

        If the `Image` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "pixels", "bands"}
            A Python string indicating the axis along which to take the standard deviation.

            Options:

            * ``"pixels"``: Returns a ``Dict[Str, Float]`` containing the standard deviation
              across each band.
            * ``"bands"``: Returns a new `.Image` with
              one band, ``"std"``, containing the standard deviation across all bands
              for each pixel.
            * ``None``: Returns a `.Float` that represents the standard deviation of the
              entire image.

        Returns
        -------
        ``Dict[Str, Float]`` or `.Image` or `.Float`
            Standard deviation along the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:01:RT:TOAR:meta_LC08_L1TP_033035_20170516_20170516_01_RT_v1")
        >>> std_img = img.std(axis="bands")
        >>> band_stds = img.std(axis="pixels")
        >>> std = img.std(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.std", self, axis)

    def count(self, axis=None):
        """
        Count of valid (unmasked) pixels across the provided ``axis``, or across all pixels
        in the image if no ``axis`` argument is provided.

        If the `Image` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        axis: {None, "pixels", "bands"}
            A Python string indicating the axis along which to take the valid pixel count.

            Options:

            * ``"pixels"``: Returns a ``Dict[Str, Float]`` containing the count of valid
              pixels in each band.
            * ``"bands"``: Returns a new `.Image` with
              one band, ``"count"``, containing the count of valid pixels across all
              bands, for each pixel.
            * ``None``: Returns a `.Float` that represents the count of valid pixels in the
              image.

        Returns
        -------
        ``Dict[Str, Float]`` or `.Image` or `.Float`
            Count of valid pixels across the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:01:RT:TOAR:meta_LC08_L1TP_033035_20170516_20170516_01_RT_v1")
        >>> count_img = img.count(axis="bands")
        >>> band_counts = img.count(axis="pixels")
        >>> count = img.count(axis=None)
        """
        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.count", self, axis)

    @typecheck_promote(operation=Str)
    def reduction(self, operation, axis=None):
        """
        Reduction along the provided ``axis``, or across all pixels in the image
        if no ``axis`` argument is provided.

        If the `Image` is empty, an empty (of the type determined by ``axis``) will be returned.

        Parameters
        ----------
        operation: {"min", "max", "mean", "median", "sum", "std", "count"}
            A string indicating the reduction method to apply along the specified axis.
        axis: {None, "pixels", "bands"}
            A Python string indicating the axis along which to perform the reduction.

            Options:

            * ``"pixels"``: Returns a ``Dict[Str, Float]`` containing the reduction
              across each band.
            * ``"bands"``: Returns a new `.Image` with
              one band, ``"std"``, containing the reduction across all bands
              for each pixel.
            * ``None``: Returns a `.Float` that represents the reduction of the
              entire image.

        Returns
        -------
        ``Dict[Str, Float]`` or `.Image` or `.Float`
            Reduction along the provided ``axis``.  See the options for the ``axis``
            argument for details.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:01:RT:TOAR:meta_LC08_L1TP_033035_20170516_20170516_01_RT_v1")
        >>> std_img = img.reduction("std", axis="bands")
        >>> band_stds = img.reduction("std", axis="pixels")
        >>> std = img.reduction("std", axis=None)
        """
        if operation.literal_value is not None and operation.literal_value not in [
            "min",
            "max",
            "mean",
            "median",
            "sum",
            "std",
            "count",
        ]:
            raise ValueError(
                "Invalid operation {!r}, must be 'min', 'max', 'mean', 'median', 'sum', 'std', or 'count'.".format(
                    operation.literal_value
                )
            )

        return_type = self._stats_return_type(axis)
        axis = list(axis) if isinstance(axis, tuple) else axis
        return return_type._from_apply("wf.reduction", self, operation, axis=axis)

    # Binary comparators
    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __lt__(self, other):
        return _result_type(other)._from_apply("wf.lt", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __le__(self, other):
        return _result_type(other)._from_apply("wf.le", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float, Bool)
    )
    def __eq__(self, other):
        return _result_type(other)._from_apply("wf.eq", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float, Bool)
    )
    def __ne__(self, other):
        return _result_type(other)._from_apply("wf.ne", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __gt__(self, other):
        return _result_type(other)._from_apply("wf.gt", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __ge__(self, other):
        return _result_type(other)._from_apply("wf.ge", self, other)

    # Bitwise operators
    def __invert__(self):
        return self._from_apply("wf.invert", self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __and__(self, other):
        return _result_type(other)._from_apply("wf.and", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __or__(self, other):
        return _result_type(other)._from_apply("wf.or", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __xor__(self, other):
        return _result_type(other)._from_apply("wf.xor", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int))
    def __lshift__(self, other):
        return _result_type(other)._from_apply("wf.lshift", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int))
    def __rshift__(self, other):
        return _result_type(other)._from_apply("wf.rshift", self, other)

    # Reflected bitwise operators
    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __rand__(self, other):
        return _result_type(other)._from_apply("wf.and", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __ror__(self, other):
        return _result_type(other)._from_apply("wf.or", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __rxor__(self, other):
        return _result_type(other)._from_apply("wf.xor", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int))
    def __rlshift__(self, other):
        return _result_type(other)._from_apply("wf.lshift", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int))
    def __rrshift__(self, other):
        return _result_type(other)._from_apply("wf.rshift", other, self)

    # Arithmetic operators
    def log(img):
        """
        Element-wise natural log of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.log().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.log(img)

    def log2(img):
        """
        Element-wise base 2 log of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.log2().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.log2(img)

    def log10(img):
        """
        Element-wise base 10 log of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.log10().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.log10(img)

    def log1p(img):
        """
        Element-wise log of 1 + an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.log1p().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.log1p(img)

    def sqrt(self):
        """
        Element-wise square root of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.sqrt().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.sqrt(self)

    def cos(self):
        """
        Element-wise cosine of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.cos().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.cos(self)

    def arccos(self):
        """
        Element-wise inverse cosine of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.arccos().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.arccos(self)

    def sin(self):
        """
        Element-wise sine of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.sin().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.sin(self)

    def arcsin(self):
        """
        Element-wise inverse sine of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.arcsin().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.arcsin(self)

    def tan(self):
        """
        Element-wise tangent of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.tan().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.tan(self)

    def arctan(self):
        """
        Element-wise inverse tangent of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.arctan().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.arctan(self)

    def exp(self):
        """
        Element-wise exponential of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.exp().compute(geoctx) # doctest: +SKIP
        ImageResult:
        ...
        """
        from ..math import arithmetic

        return arithmetic.exp(self)

    def square(self):
        """
        Element-wise square of an `Image`.

        If the `Image` is empty, returns the empty `Image`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1").pick_bands("red")
        >>> img.square().compute(geoctx) # doctest: +SKIP
        ImageResult:
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

        If the `Image` is empty, returns the empty `Image`.

        Parameters
        ----------
        min: float or list, default None
            Minimum value of clipping interval. If None, clipping is not performed on the lower interval edge.
        max: float or list, default None
            Maximum value of clipping interval. If None, clipping is not performed on the upper interval edge.
            Different per-band clip values can by given by using lists for ``min`` or ``max``,
            in which case they must be the same length as the number of bands.

        Note: ``min`` and ``max`` cannot both be None. At least one must be specified.

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> img.compute(geoctx).ndarray # doctest: +SKIP
        masked_array(
          data=[[[0.34290000000000004, 0.34290000000000004, 0.34290000000000004,
                        ..., 0.0952, 0.0952, 0.0952],
        ...
        >>> clipped = img.clip_values(0.08, 0.3).compute(geoctx) # doctest: +SKIP
        >>> clipped.ndarray # doctest: +SKIP
        masked_array(
          data=[[[0.3, 0.3, 0.3, ..., 0.0952, 0.0952, 0.0952],
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

        If the `Image` is empty, returns the empty `Image`.

        Parameters
        ----------
        range_min: float
            Minimum value of output range.
        range_max: float
            Maximum value of output range.
        domain_min: float, default None
            Minimum value of the domain. If None, the band minimim is used.
        domain_max: float, default None
            Maximum value of the domain. If None, the band maximum is used.

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> img.compute(geoctx).ndarray # doctest: +SKIP
        masked_array(
          data=[[[0.34290000000000004, 0.34290000000000004, 0.34290000000000004, ...,
        ...
        >>> scaled = img.scale_values(0.5, 1).compute(geoctx) # doctest: +SKIP
        >>> scaled.ndarray # doctest: +SKIP
        masked_array(
          data=[[[0.500010245513916, 0.500010245513916, 0.500010245513916, ...,
        ...
        """
        return self._from_apply(
            "wf.scale_values", self, range_min, range_max, domain_min, domain_max
        )

    @typecheck_promote(
        (lambda: Image, Int, Float),
        mask=Bool,
        bandinfo=lambda self: (
            NoneType,
            Dict[Str, Dict[Str, Any]],
            type(self.bandinfo),
        ),
    )
    def replace_empty_with(self, fill, mask=True, bandinfo=None):
        """
        Replace `Image`, if empty, with fill value.

        Parameters
        ----------
        fill: int, float, `Image`
            The value to fill the `Image` with. If int or float, the fill value will be broadcasted to
            band dimensions as determined by the geocontext and provided bandinfo.
        mask: bool, default True
            Whether to mask the band data. If ``mask`` is True and ``fill`` is an `Image`,
            the original `Image` mask will be overridden and all underlying data will be masked.
            If ``mask`` is False and ``fill`` is an `Image`, the original mask is left as is.
            If ``fill`` is scalar, the `Image` constructed will be fully masked or fully un-masked
            data if ``mask`` is True and False respectively.
        bandinfo: dict, default None
            Bandinfo used in constructing new `Image`. If ``fill`` is an `Image`, bandinfo is optional, and
            will be ignored if provided. If ``fill`` is a scalar, the bandinfo will be used to determine
            the number of bands on the new `Image`, as well as become the bandinfo for it.

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> empty_img = Image.from_id("id_without_surface_reflectance", processing_level="surface") # doctest: +SKIP
        >>> empty_img.compute(geoctx) # doctest: +SKIP
        EmptyImage
        >>> non_empty = empty_img.replace_empty_with(9999, bandinfo={"red":{}, "green":{}, "blue":{}}) # doctest: +SKIP
        >>> non_empty.compute(geoctx) # doctest: +SKIP
        ImageResult:
          * ndarray: MaskedArray<shape=(3, 512, 512), dtype=float64>
          * properties:
          * bandinfo: 'red', 'green', 'blue'
          * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
        """
        if isinstance(fill, (Int, Float)) and isinstance(bandinfo, NoneType):
            # filling with scalar requires bandinfo to be provided
            raise ValueError(
                "To replace empty Image with an int or float, bandinfo must be provided."
            )
        return self._from_apply(
            "wf.Image.replace_empty_with", self, fill, mask, bandinfo
        )

    @typecheck_promote(Float, Float)
    def value_at(self, x, y):
        """
        Given coordinates x, y, returns the pixel values from an Image in a `Dict` by bandname.

        Coordinates must be given in the same coordinate reference system as the `~.geospatial.GeoContext`
        you call `.compute` with. For example, if your `~.geospatial.GeoContext` uses ``"EPSG:4326"``, you'd
        give ``x`` and ``y`` in lon, lat degrees. If your `~.geospatial.GeoContext` uses UTM, you'd give ``x``
        and ``y`` in UTM coordinates.

        When using `.visualize` to view the Image on a map, ``x`` and ``y`` must always be given in web-mercator
        (``"EPSG:3857"``) coordinates (with units of meters, not degrees).

        If the `Image` is empty, returns an empty `Dict`.

        Parameters
        ----------
        x: Float
           The x coordinate, in the same CRS as the `~.geospatial.GeoContext`
        y: Float
            The y coordinate, in the same CRS as the `~.geospatial.GeoContext`

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue") # an Image with the red, green, and blue bands only
        >>> rgb.value_at(459040.0, 3942400.0) # doctest: +SKIP
        '{red': 0.39380000000000004
        'green': 0.40950000000000003,
        'blue': 0.44870000000000004}
        """
        return Dict[Str, Float]._from_apply("wf.value_at", self, x, y)

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
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue") # an Image with the red, green, and blue bands only
        >>> rgb.index(0, 0).compute(ctx) # doctest: +SKIP
        (459040.0, 3942400.0)
        """
        return Tuple[Float, Float]._from_apply("wf.index_to_coords", self, row, col)

    @typecheck_promote(Float, Float)
    def coords_to_index(self, x, y):
        """
        Convert spatial coordinates (x, y) to pixel coordinates (row, col) in the `Image`.

        Parameters
        ----------
        row: Float
            The x coordinate, in the same CRS as the `~.geospatial.GeoContext`
        col: Float
             The y coordinate, in the same CRS as the `~.geospatial.GeoContext`

        Example
        -------
        >>> from descarteslabs.workflows import Image
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue") # an Image with the red, green, and blue bands only
        >>> rgb.index(0, 0).compute(ctx) # doctest: +SKIP
        (459040.0, 3942400.0)
        """
        return Tuple[Int, Int]._from_apply("wf.coords_to_index", self, x, y)

    def __neg__(self):
        return self._from_apply("wf.neg", self)

    def __pos__(self):
        return self._from_apply("wf.pos", self)

    def __abs__(self):
        return self._from_apply("wf.abs", self)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float), _reflect=True
    )
    def __add__(self, other):
        return _result_type(other)._from_apply("wf.add", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float), _reflect=True
    )
    def __sub__(self, other):
        return _result_type(other)._from_apply("wf.sub", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float), _reflect=True
    )
    def __mul__(self, other):
        return _result_type(other)._from_apply("wf.mul", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float), _reflect=True
    )
    def __div__(self, other):
        return _result_type(other)._from_apply("wf.div", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float), _reflect=True
    )
    def __truediv__(self, other):
        return _result_type(other)._from_apply("wf.div", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float), _reflect=True
    )
    def __floordiv__(self, other):
        return _result_type(other)._from_apply("wf.floordiv", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float), _reflect=True
    )
    def __mod__(self, other):
        return _result_type(other)._from_apply("wf.mod", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float), _reflect=True
    )
    def __pow__(self, other):
        return _result_type(other)._from_apply("wf.pow", self, other)

    # Reflected arithmetic operators
    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __radd__(self, other):
        return _result_type(other)._from_apply("wf.add", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rsub__(self, other):
        return _result_type(other)._from_apply("wf.sub", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rmul__(self, other):
        return _result_type(other)._from_apply("wf.mul", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rdiv__(self, other):
        return _result_type(other)._from_apply("wf.div", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rtruediv__(self, other):
        return _result_type(other)._from_apply("wf.truediv", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rfloordiv__(self, other):
        return _result_type(other)._from_apply("wf.floordiv", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rmod__(self, other):
        return _result_type(other)._from_apply("wf.mod", other, self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rpow__(self, other):
        return _result_type(other)._from_apply("wf.pow", other, self)

    def tile_layer(
        self,
        name=None,
        scales=None,
        colormap=None,
        checkerboard=True,
        log_level=logging.DEBUG,
        **parameter_overrides,
    ):
        """
        A `.WorkflowsLayer` for this `Image`.

        Generally, use `Image.visualize` for displaying on map.
        Only use this method if you're managing your own ipyleaflet Map instances,
        and creating more custom visualizations.

        An empty  `Image` will be rendered as a checkerboard (default) or blank tile.

        Parameters
        ----------
        name: str
            The name of the layer.
        scales: list of lists, default None
            The scaling to apply to each band in the `Image`.

            If `Image` contains 3 bands, ``scales`` must be a list like ``[(0, 1), (0, 1), (-1, 1)]``.

            If `Image` contains 1 band, ``scales`` must be a list like ``[(0, 1)]``,
            or just ``(0, 1)`` for convenience

            If None, each 256x256 tile will be scaled independently.
            based on the min and max values of its data.
        colormap: str, default None
            The name of the colormap to apply to the `Image`. Only valid if the `Image` has a single band.
        checkerboard: bool, default True
            Whether to display a checkerboarded background for missing or masked data.
        log_level: int, default logging.DEBUG
            Only listen for log records at or above this log level during tile computation.
            See https://docs.python.org/3/library/logging.html#logging-levels for valid
            log levels.
        **parameter_overrides: JSON-serializable value, Proxytype, or ipywidgets.Widget
            Values---or ipywidgets---for any parameters that this `Image` depends on.

            If this `Image` depends on ``wf.widgets``, you don't have to pass anything for those---any
            widgets it depends on are automatically linked to the layer. However, you can override
            their current values (or widgets) by passing new values (or ipywidget instances) here.

            Values can be given as Proxytypes, or as Python objects like numbers,
            lists, and dicts that can be promoted to them.
            These arguments cannot depend on any parameters.

            If an ``ipywidgets.Widget`` is given, it's automatically linked, so updating the widget causes
            the argument value to change, and the layer to update.

            Once these initial argument values are set, they can be modified by assigning to
            `~.WorkflowsLayer.parameters` on the returned `WorkflowsLayer`.

            For more information, see the docstring to `ParameterSet`.

        Returns
        -------
        layer: `.WorkflowsLayer`
        """
        from ... import interactive

        return interactive.WorkflowsLayer(
            self,
            name=name,
            scales=scales,
            colormap=colormap,
            checkerboard=checkerboard,
            log_level=log_level,
            parameter_overrides=parameter_overrides,
        )

    def visualize(
        self,
        name,
        scales=None,
        colormap=None,
        checkerboard=True,
        log_level=logging.DEBUG,
        map=None,
        **parameter_overrides,
    ):
        """
        Add this `Image` to `wf.map <.interactive.map>`, or replace a layer with the same name.

        An empty `Image` will be rendered as a checkerboard (default) or blank tile.

        Parameters
        ----------
        name: str
            The name of the layer.

            If a layer with this name already exists on `wf.map <.interactive.map>`,
            it will be replaced with this `Image`, scales, and colormap.
            This allows you to re-run cells in Jupyter calling `visualize`
            without adding duplicate layers to the map.
        scales: list of lists, default None
            The scaling to apply to each band in the `Image`.

            If `Image` contains 3 bands, ``scales`` must be a list like ``[(0, 1), (0, 1), (-1, 1)]``.

            If `Image` contains 1 band, ``scales`` must be a list like ``[(0, 1)]``,
            or just ``(0, 1)`` for convenience

            If None, each 256x256 tile will be scaled independently.
            based on the min and max values of its data.
        colormap: str, default None
            The name of the colormap to apply to the `Image`. Only valid if the `Image` has a single band.
        checkerboard: bool, default True
            Whether to display a checkerboarded background for missing or masked data.
        log_level: int, default logging.DEBUG
            Only listen for log records at or above this log level during tile computation.
            See https://docs.python.org/3/library/logging.html#logging-levels for valid
            log levels.
        map: `.Map` or `.MapApp`, optional, default None
            The `.Map` (or plain ipyleaflet Map) instance on which to show the `Image`.
            If None (default), uses `wf.map <.interactive.map>`, the singleton Workflows `.MapApp` object.
        **parameter_overrides: JSON-serializable value, Proxytype, or ipywidgets.Widget
            Values---or ipywidgets---for any parameters that this `Image` depends on.

            If this `Image` depends on ``wf.widgets``, you don't have to pass anything for those---any
            widgets it depends on are automatically linked to the layer. However, you can override
            their current values (or widgets) by passing new values (or ipywidget instances) here.

            Values can be given as Proxytypes, or as Python objects like numbers,
            lists, and dicts that can be promoted to them.
            These arguments cannot depend on any parameters.

            If an ``ipywidgets.Widget`` is given, it's automatically linked, so updating the widget causes
            the argument value to change, and the map to update. Running `visualize` again and passing in
            a different widget instance will un-link the old one automatically.

            Once these initial argument values are set, they can be modified by assigning to
            `~.WorkflowsLayer.parameters` on the returned `WorkflowsLayer`.

            For more information, see the docstring to `ParameterSet`.

        Returns
        -------
        layer: WorkflowsLayer
            The layer displaying this `Image`. Either a new `WorkflowsLayer` if one was created,
            or the layer with the same ``name`` that was already on the map.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> nir, red = col.unpack_bands(["nir", "red"])
        >>> ndvi = wf.normalized_difference(nir, red)
        >>> max_ndvi = ndvi.max()
        >>> highest_ndvi = max_ndvi > wf.parameter("threshold", wf.Float)
        >>> lyr = highest_ndvi.visualize(
        ...     name="My Cool Max NDVI",
        ...     scales=[0, 1],
        ...     colormap="viridis",
        ...     threshold=0.4,
        ... )  # doctest: +SKIP
        >>> wf.map  # doctest: +SKIP
        >>> # `wf.map` actually displays the map; right click and open in new view in JupyterLab
        >>> lyr.parameters.threshold = 0.3  # doctest: +SKIP
        >>> # update map with a new value for the "threshold" parameter
        """
        from ... import interactive

        if map is None:
            map = interactive.map

        for layer in map.layers:
            if layer.name == name:
                with layer.hold_url_updates():
                    layer.set_imagery(self, **parameter_overrides)
                    layer.set_scales(scales, new_colormap=colormap)
                    layer.checkerboard = checkerboard
                    if log_level is not None:
                        layer.log_level = log_level
                return layer
        else:
            layer = self.tile_layer(
                name=name,
                scales=scales,
                colormap=colormap,
                checkerboard=checkerboard,
                log_level=log_level,
                **parameter_overrides,
            )
            map.add_layer(layer)
            return layer

    _colormaps = [
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
    ]


def _result_type(other):
    ImageCollection = _DelayedImageCollection()
    return ImageCollection if isinstance(other, ImageCollection) else Image
