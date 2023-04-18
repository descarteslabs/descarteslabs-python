# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
The Scene class holds metadata about a single scene in the Descartes Labs catalog.

Example
-------
>>> import descarteslabs as dl
>>> scene, ctx = dl.scenes.Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")  # doctest: +SKIP
>>> ctx  # a default GeoContext to use when loading raster data from this Scene  # doctest: +SKIP
AOI(geometry=None,
    resolution=15.0,
    crs='EPSG:32615',
    align_pixels=False,
    bounds=(258292.5, 4503907.5, 493732.5, 4743307.5),
    bounds_crs='EPSG:32615',
    shape=None,
    all_touched=False)
>>> scene.properties.id  # doctest: +SKIP
'landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1'
>>> scene.properties.date  # doctest: +SKIP
datetime.datetime(2016, 7, 6, 16, 59, 42, 753476, tzinfo=<UTC>)
>>> scene.properties.bands.red.resolution  # doctest: +SKIP
15
>>> arr = scene.ndarray("red green blue", ctx.assign(resolution=120.))  # doctest: +SKIP
>>> type(arr)  # doctest: +SKIP
<class 'numpy.ma.core.MaskedArray'>
>>> arr.shape  # doctest: +SKIP
(3, 1995, 1962)
"""

from descarteslabs.exceptions import NotFoundError
from ..client.deprecation import deprecate
from ..catalog import Image
from ..common.dotdict import DotDict

from .helpers import REQUEST_PARAMS, cached_bands_by_product


# more or less like a DotDict but delegates everything to the underlying image
# most importantly, it is lazy about retrieving bands
class _PropertiesAccessor(object):
    def __init__(self, image):
        self.__dict__["_image"] = image

    # The following three properties are not part of the metadata model,
    # but instead were computed and added to the metadata dict by the old
    # Scenes constructor. We implement them via @property here instead
    # as all but `bands` are lightweight, and `bands` is best deferred until
    # actually needed.
    @property
    def bands(self):
        return DotDict(
            cached_bands_by_product(self._image.product_id, self._image._client)
        )

    @property
    def crs(self):
        return self._image.cs_code or self._image.projection

    @property
    def date(self):
        return self._image.acquired

    def get(self, key, default=None):
        try:
            return getattr(self, key)
        except AttributeError:
            return default

    def __getitem__(self, key):
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key) from None

    def __setitem__(self, key, value):
        raise TypeError("Properties are read-only")

    def __delitem__(self, key):
        raise TypeError("Properties are read-only")

    def __getattr__(self, attr):
        try:
            return DotDict._box(self._image.v1_properties[attr])
        except KeyError:
            raise AttributeError(attr) from None

    def __setattr__(self, attr, value):
        raise TypeError("Properties are read-only")

    def __delattr__(self, attr):
        raise TypeError("Properties are read-only")

    def __iter__(self):
        for k in self._image.v1_properties.keys():
            yield k
        yield "date"
        yield "crs"
        yield "bands"

    def __dir__(self):
        return super(_PropertiesAccessor, self).__dir__() + list(
            self._image.v1_properties.keys()
        )

    def keys(self):
        return self.__iter__()

    def values(self):
        for k, v in self.items():
            yield v

    def items(self):
        for kv in DotDict(self._image.v1_properties.items()):
            yield kv
        yield ("date", self.date)
        yield ("crs", self.crs)
        yield ("bands", self.bands)


class Scene(object):
    """
    Object holding metadata about a single scene in the Descartes Labs catalog.

    A Scene is structured like a GeoJSON Feature, with geometry and properties.

    Attributes
    ----------
    geometry : shapely.geometry.Polygon
        The region the scene's data covers, in WGS84 (lat-lon) coordinates,
        represented as a Shapely polygon.
    properties : DotDict-like
        Metadata about the scene. Some fields will vary between products,
        but these will be present:

        * ``id`` : str
            Descartes Labs ID of this scene
        * ``crs`` : str
            Native coordinate reference system of the scene,
            as an EPSG code or PROJ.4 definition
        * ``date`` : datetime.datetime
            ``'acquired'`` date parsed as a Python datetime if set,
            otherwise None
        * ``bands`` : DotDict[str, DotDict]
            Metadata about the bands available in the scene,
            as the mapping ``{band name: band metadata}``.

            Band names are either the band's ``name`` field (like "red"),
            or for derived bands, the band's ``id`` (like "derived:ndvi").

            Each band metadata dict should contain these fields:

            * ``id`` : str
                Descartes Labs ID of the band;
                unique to every band of every product
            * ``name`` : str
                Human-readable name of the band
            * ``dtype`` : str
                Native type in which the band's data is stored
            * ``data_range`` : list
                List of [min, max] values the band's data can have

            These fields are useful and available in most products,
            but may not always be available:

            * ``resolution`` : float
                Native resolution of the band, in ``resolution_unit``,
                that the edge of each pixel represents on the ground
            * ``resolution_unit`` : str
                Units of ``resolution`` field, such as ``"m"``
            * ``physical_range`` : list
                [min, max] range of values the band's data *represents*.
                Values of data have physical meaning
                (such as a reflectance fraction from 0-1), but often
                those values are remapped to a different numerical range
                for more efficient storage (since fixed-point integers require
                less space than floats). To return data to numbers
                with physical meaning, they should be mapped
                from ``data_range`` to ``physical_range``.
            * ``wavelength_min``
                Minimum wavelength captured by the sensor in this band
            * ``wavelength_center``
                Central wavelength captured by the sensor in this band
            * ``wavelength_max``
                Maximum wavelength captured by the sensor in this band
            * ``wavelength_unit``
                Units of the wavelength fields, such as ``"nm"``
    """

    def __init__(self, image: Image):
        """
        ``__init__`` instantiates a Scene by wrapping a `descarteslabs.catalog.Image` instead.

        It's preferred to use `Scene.from_id` or `scenes.search <scenes.search_api.search>` instead.
        """
        if not isinstance(image, Image):
            ValueError("image must be a descarteslabs.catalog.Image")
        self._image = image

    @property
    def geometry(self):
        return self._image.geometry

    @property
    def properties(self):
        return _PropertiesAccessor(self._image)

    @property
    def __geo_interface__(self):
        return self.geometry.__geo_interface__

    @classmethod
    @deprecate(removed=["metadata_client"])
    def from_id(cls, scene_id):
        """
        Return the metadata for a Descartes Labs scene ID as a Scene object.

        Also returns a :class:`~descarteslabs.common.geo.geocontext.GeoContext`
        for loading the Scene's original, unwarped data.

        Parameters
        ----------
        scene_id: str
            Descartes Labs scene ID,
            e.g. "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"

        Returns
        -------
        scene: Scene
            Scene instance with metadata loaded from the Descartes Labs catalog
        ctx: AOI
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` for loading this Scene's original data.
            The defaults used are described in `Scene.default_ctx`.

        Example
        -------
        >>> import descarteslabs as dl
        >>> scene, ctx = dl.scenes.Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1")  # doctest: +SKIP
        >>> ctx  # doctest: +SKIP
        AOI(geometry=None,
            resolution=15.0,
            crs='EPSG:32615',
            align_pixels=False,
            bounds=(348592.5, 4345567.5, 581632.5, 4582807.5),
            bounds_crs='EPSG:32615',
            shape=None.
            all_touched=True)
        >>> scene.properties.date  # doctest: +SKIP
        datetime.datetime(2016, 7, 15, 16, 53, 59, 495435, tzinfo=<UTC>)

        Raises
        ------
        NotFoundError
            If the ``scene_id`` cannot be found in the Descartes Labs catalog
        """

        image = Image.get(scene_id, request_params=REQUEST_PARAMS)
        if image is None:
            raise NotFoundError("Scene {scene_id} not found")
        return cls(image), image.geocontext

    def default_ctx(self):
        """
        Return an :class:`AOI GeoContext <descarteslabs.common.geo.geocontext.AOI>`
        for loading this Scene's original, unwarped data.

        These defaults are used:

        * resolution: resolution determined from the Scene's ``geotrans``
        * crs: native CRS of the Scene (often, a UTM CRS)
        * bounds: bounds determined from the Scene's ``geotrans`` and ``raster_size``
        * bounds_crs: native CRS of the Scene
        * align_pixels: False, to prevent interpolation snapping pixels to a new grid
        * geometry: None

        .. note::

            Using this :class:`~descarteslabs.common.geo.geocontext.GeoContext` will only
            return original, unwarped data if the Scene is axis-aligned ("north-up")
            within the CRS. If its ``geotrans`` applies a rotation, a warning will be raised.
            In that case, use `Raster.ndarray` or `Raster.raster` to retrieve
            original data. (The :class:`~descarteslabs.common.geo.geocontext.GeoContext`
            paradigm requires bounds for consistentcy, which are inherently axis-aligned.)

        Returns
        -------
        ctx: AOI
        """
        return self._image.geocontext

    def coverage(self, geom):
        """
        The fraction of a geometry-like object covered by this Scene's geometry.

        Parameters
        ----------
        geom : GeoJSON-like dict, :class:`~descarteslabs.common.geo.geocontext.GeoContext`, or object with __geo_interface__  # noqa: E501
            Geometry to which to compare this Scene's geometry

        Returns
        -------
        coverage: float
            The fraction of ``geom``'s area that overlaps with this Scene,
            between 0 and 1.

        Example
        -------
        >>> import descarteslabs as dl
        >>> scene, ctx = dl.scenes.Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")  # doctest: +SKIP
        >>> scene.coverage(scene.geometry.buffer(1))  # doctest: +SKIP
        0.258370644415335
        """
        return self._image.coverage(geom)

    @deprecate(removed=["raster_client"])
    def ndarray(
        self,
        bands,
        ctx,
        mask_nodata=True,
        mask_alpha=None,
        bands_axis=0,
        raster_info=False,
        resampler="near",
        processing_level=None,
        scaling=None,
        data_type=None,
        progress=None,
    ):
        """
        Load bands from this scene as an ndarray, optionally masking invalid data.

        If the selected bands have different data types the resulting ndarray
        has the most general of those data types. This table defines which data types
        can be cast to which more general data types:

        * ``Byte`` to: ``UInt16``, ``UInt32``, ``Int16``, ``Int32``, ``Float32``, ``Float64``
        * ``UInt16`` to: ``UInt32``, ``Int32``, ``Float32``, ``Float64``
        * ``UInt32`` to: ``Float64``
        * ``Int16`` to: ``Int32``, ``Float32``, ``Float64``
        * ``Int32`` to: ``Float32``, ``Float64``
        * ``Float32`` to: ``Float64``
        * ``Float64`` to: No possible casts

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue derived:ndvi"``),
            or a sequence of band names (``["red", "green", "blue", "derived:ndvi"]``).
            Names must be keys in ``self.properties.bands``.
            If the alpha band is requested, it must be last in the list
            to reduce rasterization errors.
        ctx : :class:`~descarteslabs.common.geo.geocontext.GeoContext`
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading this Scene
        mask_nodata : bool, default True
            Whether to mask out values in each band that equal
            that band's ``nodata`` sentinel value.
        mask_alpha : bool or str or None, default None
            Whether to mask pixels in all bands where the alpha band of the scene is 0.
            Provide a string to use an alternate band name for masking.
            If the alpha band is available and ``mask_alpha`` is None, ``mask_alpha``
            is set to True. If not, mask_alpha is set to False.
        bands_axis : int, default 0
            Axis along which bands should be located in the returned array.
            If 0, the array will have shape ``(band, y, x)``, if -1,
            it will have shape ``(y, x, band)``.

            It's usually easier to work with bands as the outermost axis,
            but when working with large arrays, or with many arrays concatenated
            together, NumPy operations aggregating each xy point across bands
            can be slightly faster with bands as the innermost axis.
        raster_info : bool, default False
            Whether to also return a dict of information about the rasterization
            of the scene, including the coordinate system WKT and geotransform matrix.
            Generally only useful if you plan to upload data derived
            from this scene back to the Descartes catalog, or use it with GDAL.
        resampler : str, default "near"
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to its new resolution or CRS. Possible values are
            ``near`` (nearest-neighbor), ``bilinear``, ``cubic``, ``cubicsplice``,
            ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``, ``q3``.
        processing_level : str, optional
            How the processing level of the underlying data should be adjusted. Possible
            values depend on the product and bands in use. Legacy products support
            ``toa`` (top of atmosphere) and in some cases ``surface``. Consult the
            available ``processing_levels`` in the product bands to understand what
            is available.
        scaling : None, str, list, dict
            Band scaling specification. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        data_type : None, str
            Output data type. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        progress : None, bool
            Controls display of a progress bar.
        raster_client : Raster, optional
            Unneeded in general use; lets you use a specific client instance
            with non-default auth and parameters.

        Returns
        -------
        arr : ndarray
            Returned array's shape will be ``(band, y, x)`` if bands_axis is 0,
            ``(y, x, band)`` if bands_axis is -1.
            If ``mask_nodata`` or ``mask_alpha`` is True, arr will be a masked array.
            The data type ("dtype") of the array is the most general of the data
            types among the bands being rastered.
        raster_info : dict
            If ``raster_info=True``, a raster information dict is also returned.

        Example
        -------
        >>> import descarteslabs as dl
        >>> scene, ctx = dl.scenes.Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")  # doctest: +SKIP
        >>> arr = scene.ndarray("red green blue", ctx.assign(resolution=120.))  # doctest: +SKIP
        >>> type(arr)  # doctest: +SKIP
        <class 'numpy.ma.core.MaskedArray'>
        >>> arr.shape  # doctest: +SKIP
        (3, 1995, 1962)
        >>> red_band = arr[0]  # doctest: +SKIP

        Raises
        ------
        ValueError
            If requested bands are unavailable.
            If band names are not given or are invalid.
            If the requested bands have incompatible dtypes.
        NotFoundError
            If a Scene's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given invalid parameters
        """
        return self._image.ndarray(
            bands,
            geocontext=ctx,
            mask_nodata=mask_nodata,
            mask_alpha=mask_alpha,
            bands_axis=bands_axis,
            raster_info=raster_info,
            resampler=resampler,
            processing_level=processing_level,
            scaling=scaling,
            data_type=data_type,
            progress=progress,
        )

    def has_alpha(self, alpha_band_name):
        return alpha_band_name in self.properties.bands

    @deprecate(removed=["raster_client"])
    def download(
        self,
        bands,
        ctx,
        dest=None,
        format="tif",
        resampler="near",
        processing_level=None,
        scaling=None,
        data_type=None,
        nodata=None,
        progress=None,
    ):
        """
        Save bands from this scene as a GeoTIFF, PNG, or JPEG, writing to a path.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue derived:ndvi"``),
            or a sequence of band names (``["red", "green", "blue", "derived:ndvi"]``).
            Names must be keys in ``self.properties.bands``.
        ctx : :class:`~descarteslabs.common.geo.geocontext.GeoContext`
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading this Scene
        dest : str or path-like object, default None
            Where to write the image file.

            * If None (default), it's written to an image file of the given ``format``
              in the current directory, named by the Scene's ID and requested bands,
              like ``"sentinel-2:L1C:2018-08-10_10TGK_68_S2A_v1-red-green-blue.tif"``
            * If a string or path-like object, it's written to that path.

              Any file already existing at that path will be overwritten.

              Any intermediate directories will be created if they don't exist.

              Note that path-like objects (such as pathlib.Path) are only supported
              in Python 3.6 or later.
        format : str, default "tif"
            If None is given as ``dest``: one of "tif", "png", or "jpg".

            If a str or path-like object is given as ``dest``, ``format`` is ignored
            and determined from the extension on the path (one of ".tif", ".png", or ".jpg").
        resampler : str, default "near"
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to its new resolution or SRS. Possible values are
            ``near`` (nearest-neighbor), ``bilinear``, ``cubic``, ``cubicsplice``,
            ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``, ``q3``.
        processing_level : str, optional
            How the processing level of the underlying data should be adjusted. Possible
            values depend on the product and bands in use. Legacy products support
            ``toa`` (top of atmosphere) and in some cases ``surface``. Consult the
            available ``processing_levels`` in the product bands to understand what
            is available.
        scaling : None, str, list, dict
            Band scaling specification. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        data_type : None, str
            Output data type. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        nodata : None, number
            NODATA value for a geotiff file. Will be assigned to any masked pixels.
        progress : None, bool
            Controls display of a progress bar.

        Returns
        -------
        path : str or None
            If ``dest`` is None or a path, the path where the image file was written is returned.
            If ``dest`` is file-like, nothing is returned.

        Example
        -------
        >>> import descarteslabs as dl
        >>> scene, ctx = dl.scenes.Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")  # doctest: +SKIP
        >>> scene.download("red green blue", ctx.assign(resolution=120.))  # doctest: +SKIP
        "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1_red-green-blue.tif"
        >>> import os
        >>> os.listdir(".")  # doctest: +SKIP
        ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1-red-green-blue.tif"]
        >>> scene.download(
        ...     "red green blue",
        ...     ctx,
        ...     "rasters/{ctx.resolution}-{scene.properties.id}.jpg".format(ctx=ctx, scene=scene)
        ... )  # doctest: +SKIP
        "rasters/15-landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1.tif"

        Raises
        ------
        ValueError
            If requested bands are unavailable.
            If band names are not given or are invalid.
            If the requested bands have incompatible dtypes.
            If ``format`` is invalid, or the path has an invalid extension.
        NotFoundError
            If a Scene's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given invalid parameters
        """
        return self._image.download(
            bands,
            geocontext=ctx,
            dest=dest,
            format=format,
            resampler=resampler,
            processing_level=processing_level,
            scaling=scaling,
            data_type=data_type,
            nodata=nodata,
            progress=progress,
        )

    def scaling_parameters(
        self, bands, processing_level=None, scaling=None, data_type=None
    ):
        """
        Computes fully defaulted scaling parameters and output data_type
        from provided specifications.

        This method makes accessible the scales and data_type parameters
        which will be generated and passed to the Raster API by methods
        such as :meth:`ndarray` and :meth:`download`. It is provided
        as a convenience to the user to aid in understanding how the
        ``scaling`` and ``data_type`` parameters will be handled by
        those methods. It would not usually be used in a normal workflow.

        Parameters
        ----------
        bands : list
            List of bands to be scaled.
        processing_level : str, optional
            How the processing level of the underlying data should be adjusted. Possible
            values depend on the product and bands in use. Legacy products support
            ``toa`` (top of atmosphere) and in some cases ``surface``. Consult the
            available ``processing_levels`` in the product bands to understand what
            is available.
        scaling : None or str or list or dict, default None
            Supplied scaling specification, see below.
        data_type : None or str, default None
            Result data type desired, as a standard data type string (e.g.
            ``"Byte"``, ``"Uint16"``, or ``"Float64"``). If not specified,
            will be deduced from the ``scaling`` specification. Typically
            this is left unset and the appropriate type will be determined
            automatically.

        Returns
        -------
        scales : list(tuple)
            The fully specified scaling parameter, compatible with the
            :class:`~descarteslabs.client.services.raster.Raster` API and the
            output data type.
        data_type : str
            The result data type as a standard GDAL type string.

        Raises
        ------
        ValueError
            If any invalid or incompatible value is passed to any of the
            three parameters.


        Scaling is determined on a band-by-band basis, incorporating the user
        provided specification, the output data_type, and properties for the
        band, such as the band type, the band data type, and the
        ``default_range``, ``data_range``, and ``physical_range`` properties.
        Ultimately the scaling for each band will be resolved to either
        ``None`` or a tuple of numeric values of length 0, 2, or 4, as
        accepted by the Raster API. The result is a list (with length equal
        to the number of bands) of one of these values, or may be a None
        value which is just a shorthand equivalent for a list of None values.

        A ``None`` indicates that no scaling should be performed.

        A 0-tuple ``()`` indicates that the band data should be automatically
        scaled from the minimum and maximum values present in the image data
        to the display range 0-255.

        A 2-tuple ``(input-min, input-max)`` indicates that the band data
        should be scaled from the specified input range to the display
        range of 0-255.

        A 4-tuple ``(input-min, input-max, output-min, output-max)``
        indicates that the band data should be scaled from the input range
        to the output range.

        In all cases, the scaling will be performed as a multiply and add,
        and the resulting values are only clipped as necessary to fit in
        the output data type. As such, if the input and output ranges are
        the same, it is effectively a no-op equivalent to ``None``.

        The support for scaling parameters in the ``Scenes`` API includes
        the concept of an automated scaling mode. The four supported modes
        are as follows.

        ``"raw"``:
            Equivalent to a ``None``, the data should not be scaled.
        ``"auto"``:
            Equivalent to a 0-tuple, the data should be scaled by
            the Raster service so that the actual range of data in the
            input is scaled up to the full display range (0-255). It
            is not possible to determine the bounds of this input range
            in the Scenes API as the actual band data is not accessible.
        ``"display"``:
            The data should be scaled from any specified input bounds,
            defaulting to the ``default_range`` property for the band,
            to the output range, defaulting to 0-255.
        ``"physical"``:
            The data should be scaled from the input range, defaulting
            to the ``data_range`` property for the band, to the output
            range, defaulting to the ``physical_range`` property for
            the band.

        The mode may be explicitly specified, or it may be determined
        implicitly from other characteristics such as the length
        and contents of the tuples for each band, or from the output
        data_type if this is explicitly specified (e.g. ``"Byte"``
        implies display mode, ``"Float64"`` implies physical mode).

        If it is not possible to infer the mode, and a mode is required
        in order to fully determine the results of this method, an
        error will be raised. It is also an error to explicitly
        specify more than one mode, with several exceptions: auto
        and display mode are compatible, while a raw display mode
        for a band which is of type "mask" or type "class" does
        not conflict with any other mode specification.

        Normally the ``data_type`` parameter is not provided by the
        user, and is instead determined from the mode as follows.

        ``"raw"``:
            The data type that best matches the data types of all
            the bands, preserving the precision and range of the
            original data.
        ``"auto"`` and ``"display"``:
            ``"Byte"``
        ``"physical"``:
            ``"Float64"``

        The ``scaling`` parameter passed to this method can be any
        of the following:

        None:
            No scaling for all bands. Equivalent to ``[None, ...]``.
        str:
            Any of the four supported automatic modes as
            described above.
        list or Iterable:
            A list or similar iterable must contain a number of
            elements equal to the number of bands specified. Each
            element must either be a None, a 0-, 2-, or 4-tuple, or
            one of the above four automatic mode strings. The
            elements of each tuple must either be a numeric value
            or a string containing a valid numerical string followed
            by a "%" character. The latter will be interpreted as a
            percentage of the appropriate range (e.g. ``default_range``,
            ``data_range``, or ``physical_range``) according to the mode.
            For example, a tuple of ``("25%", "75%")`` with a
            ``default_range`` of ``[0, 4000]`` will yield ``(1000, 3000)``.
        dict or Mapping:
            A dictionary or similar mapping with keys corresponding to
            band names and values as accepted as elements for each band
            as with a list described above. Each band name is used to
            lookup a value in the mapping. If none is found, and the
            band is not of type "mask" or "class", then the special
            key ``"default_"`` is looked up in the mapping if it exists.
            Otherwise a value of ``None`` will be used for the band.
            This is strictly a convenience for constructing a list of
            scale values, one for each band, but can be useful if a
            single general-purpose mapping is defined for all possible
            or relevant bands and then reused across many calls to the
            different methods in the Scenes API which accept a ``scaling``
            parameter.

        See Also
        --------
        :doc:`Scenes Guide </guides/scenes>` : This contains many examples of the use of
        the ``scaling`` and ``data_type`` parameters.
        """
        return self._image.scaling_parameters(
            bands, processing_level, scaling, data_type
        )

    # not sure this is even needed?
    # def _dict(self):
    #     return dict(geometry=self.__geo_interface__, properties=DotDict({k: v for k, v in self.properties})

    def __repr__(self):
        parts = [
            'Scene "{}"'.format(self.properties.get("id")),
            '  * Product: "{}"'.format(self.properties.get("product")),
            '  * CRS: "{}"'.format(self.properties.get("crs")),
        ]

        try:
            date = "  * Date: {:%c}".format(self.properties.get("date"))
            parts.append(date)
        except Exception:
            pass

        bands = self.properties.get("bands")
        if bands is not None:
            if len(bands) > 30:
                parts += ["  * Bands: {}".format(len(bands))]
            else:
                # strings will be formatted with a band dict as available fields
                part_format_strings = [
                    "{resolution}",
                    "{resolution_unit},",
                    "{dtype},",
                    "{data_range}",
                    "-> {physical_range}",
                    'in units "{data_unit}"',
                ]

                band_lines = []
                # QUESTION(gabe): should there be a canonical ordering to bands? (see GH #973)
                for bandname, band in bands.items():
                    band_line = "    * " + bandname
                    band_parts = []

                    for format_string in part_format_strings:
                        try:
                            # If the named field in `format_string` is missing from `band`,
                            # `format_string.format(**band)` will fail with a KeyError, which we catch.
                            band_parts.append(format_string.format(**band))
                        except (KeyError, ValueError):
                            pass

                    if len(band_parts) > 0:
                        band_line = band_line + ": " + " ".join(band_parts)
                    band_lines.append(band_line)

                if len(band_lines) > 0:
                    parts += ["  * Bands:"] + band_lines
        return "\n".join(parts)
