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

import io
import json
import os.path
import warnings
from tempfile import NamedTemporaryFile

try:
    import collections.abc as abc
except ImportError:
    import collections as abc

from affine import Affine
import numpy as np

from descarteslabs.exceptions import BadRequestError, NotFoundError

from ..client.services.raster import Raster
from ..client.services.service import ThirdPartyService
from ..common.geo import AOI, GeoContext
from ..common.property_filtering import Properties
from ..common.shapely_support import geometry_like_to_shapely
from .attributes import (
    EnumAttribute,
    File,
    GeometryAttribute,
    ListAttribute,
    StorageState,
    Timestamp,
    TupleAttribute,
    TypedAttribute,
    parse_iso_datetime,
)
from .catalog_base import DocumentState, check_deleted
from .helpers import bands_to_list, cached_bands_by_product, download
from .image_types import DownloadFileFormat, ResampleAlgorithm
from .named_catalog_base import NamedCatalogObject
from .scaling import scaling_parameters
from .search import AggregateDateField, GeoSearch, SummarySearchMixin

properties = Properties()


class ImageSummaryResult(object):
    """
    The readonly data returned by :py:meth:`SummaySearch.summary` or
    :py:meth:`SummaySearch.summary_interval`.

    Attributes
    ----------
    count : int
        Number of images in the summary.
    bytes : int
        Total number of bytes of data across all images in the summary.
    products : list(str)
        List of IDs for the products included in the summary.
    interval_start: datetime
        For interval summaries only, a datetime representing the start of the interval period.

    """

    def __init__(
        self, count=None, bytes=None, products=None, interval_start=None, **kwargs
    ):
        self.count = count
        self.bytes = bytes
        self.products = products
        self.interval_start = (
            parse_iso_datetime(interval_start) if interval_start else None
        )

    def __repr__(self):
        text = [
            "\nSummary for {} images:".format(self.count),
            " - Total bytes: {:,}".format(self.bytes),
        ]
        if self.products:
            text.append(" - Products: {}".format(", ".join(self.products)))
        if self.interval_start:
            text.append(" - Interval start: {}".format(self.interval_start))
        return "\n".join(text)


class ImageSearch(SummarySearchMixin, GeoSearch):
    # Be aware that the `|` characters below add whitespace.  The first one is needed
    # avoid the `Inheritance` section from appearing before the auto summary.
    """A search request that iterates over its search results for images.

    The `ImageSearch` is identical to `Search` but with a couple of summary methods:
    :py:meth:`summary` and :py:meth:`summary_interval`.
    """

    SummaryResult = ImageSummaryResult
    DEFAULT_AGGREGATE_DATE_FIELD = AggregateDateField.ACQUIRED

    def collect(self, geocontext=None, **kwargs):
        """
        Execute the search query and return the collection of the appropriate type.

        Parameters
        ----------
        geocontext : shapely.geometry.base.BaseGeometry, descarteslabs.common.geo.Geocontext, geojson-like, default None  # noqa: E501
            AOI for the ImageCollection.

        Returns
        -------
        ~descarteslabs.catalog.ImageCollection
            ImageCollection of Images returned from the search.

        Raises
        ------
        BadRequestError
            If any of the query parameters or filters are invalid
        ~descarteslabs.exceptions.ClientError or ~descarteslabs.exceptions.ServerError
            :ref:`Spurious exception <network_exceptions>` that can occur during a
            network request.
        """
        if geocontext is None:
            geocontext = self._intersects
        if geocontext is not None:
            kwargs["geocontext"] = geocontext

        return super(ImageSearch, self).collect(**kwargs)


class Image(NamedCatalogObject):
    """An image with raster data.

    Instantiating an image indicates that you want to create a *new* Descartes Labs
    catalog image.  If you instead want to retrieve an existing catalog image use
    `Image.get() <descarteslabs.catalog.Image.get>`, or if you're not sure use
    `Image.get_or_create() <~descarteslabs.catalog.Image.get_or_create>`.  You
    can also use `Image.search() <descarteslabs.catalog.Image.search>`.  Also
    see the example for :py:meth:`~descarteslabs.catalog.Image.save`.

    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict
        With the exception of readonly attributes (`created`, `modified`) and with the
        exception of properties (`ATTRIBUTES`, `is_modified`, and `state`), any
        attribute listed below can also be used as a keyword argument.  Also see
        `~Image.ATTRIBUTES`.
    """

    _doc_type = "image"
    _url = "/images"
    _default_includes = ["product"]
    # _collection_type set below due to circular import problems
    _upload_service = ThirdPartyService()

    # Geo referencing
    geometry = GeometryAttribute(
        doc="""str or shapely.geometry.base.BaseGeometry: Geometry representing the image coverage.

        *Filterable*

        (use :py:meth:`ImageSearch.intersects
        <descarteslabs.catalog.ImageSearch.intersects>` to search based on geometry)
        """
    )
    cs_code = TypedAttribute(
        str,
        doc="""str: The coordinate reference system used by the image as an EPSG or ESRI code.

        An example of a EPSG code is ``"EPSG:4326"``.  One of `cs_code` and `projection`
        is required.  If both are set and disagree, `cs_code` takes precedence.
        """,
    )
    projection = TypedAttribute(
        str,
        doc="""str: The spatial reference system used by the image.

        The projection can be specified as either a proj.4 string or a a WKT string.
        One of `cs_code` and `projection` is required.  If both are set and disagree,
        `cs_code` takes precedence.
        """,
    )
    geotrans = TupleAttribute(
        min_length=6,
        max_length=6,
        coerce=True,
        attribute_type=float,
        doc="""tuple of six float elements, optional if `~StorageState.REMOTE`: GDAL-style geotransform matrix.

        A GDAL-style `geotransform matrix
        <https://gdal.org/user/raster_data_model.html#affine-geotransform>`_ that
        transforms pixel coordinates into the spatial reference system defined by the
        `cs_code` or `projection` attributes.
        """,
    )
    x_pixels = TypedAttribute(
        int,
        doc="int, optional if `~StorageState.REMOTE`: X dimension of the image in pixels.",
    )
    y_pixels = TypedAttribute(
        int,
        doc="int, optional if `~StorageState.REMOTE`: Y dimension of the image in pixels.",
    )

    # Time dimensions
    acquired = Timestamp(
        doc="""str or datetime: Timestamp when the image was captured by its sensor or created.

        *Filterable, sortable*.
        """
    )
    acquired_end = Timestamp(
        doc="""str or datetime, optional: Timestamp when the image capture by its sensor was completed.

        *Filterable, sortable*.
        """
    )
    published = Timestamp(
        doc="""str or datetime, optional: Timestamp when the data provider published this image.

        *Filterable, sortable*.
        """
    )

    # Stored files
    storage_state = EnumAttribute(
        StorageState,
        doc="""str or StorageState: Storage state of the image.

        The state is `~StorageState.AVAILABLE` if the data is available and can be
        rastered, `~StorageState.REMOTE` if the data is not currently available.

        *Filterable, sortable*.
        """,
    )
    files = ListAttribute(
        File, doc="list(File): The list of files holding data for this image."
    )

    # Image properties
    area = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Surface area the image covers.

        *Filterable, sortable*.
        """,
    )
    azimuth_angle = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Sensor azimuth angle in degrees.

        *Filterable, sortable*.
        """,
    )
    bits_per_pixel = ListAttribute(
        TypedAttribute(float, coerce=True),
        doc="list(float), optional: Average bits of data per pixel per band.",
    )
    bright_fraction = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Fraction of the image that has reflectance greater than .4 in the blue band.

        *Filterable, sortable*.
        """,
    )
    brightness_temperature_k1_k2 = ListAttribute(
        ListAttribute(TypedAttribute(float, coerce=True)),
        doc="""list(list(float), optional: radiance to brightness temperature
        conversion coefficients.

        Outer list indexed by ``Band.vendor_order``, inner lists are ``[k1, k2]`` or
        empty if not applicable.
        """,
    )
    c6s_dlsr = ListAttribute(
        ListAttribute(TypedAttribute(float, coerce=True)),
        doc="list(list(float), optional: DLSR conversion coefficients.",
    )
    cloud_fraction = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Fraction of pixels which are obscured by clouds.

        *Filterable, sortable*.
        """,
    )
    confidence_dlsr = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Confidence value for DLSR coefficients.

        *Filterable, sortable*.
        """,
    )
    alt_cloud_fraction = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Fraction of pixels which are obscured by clouds.

        This is as per an alternative algorithm.  See the product documentation in the
        `Descartes Labs catalog <catalog.descarteslabs.com>`_ for more information.

        *Filterable, sortable*.
        """,
    )
    processing_pipeline_id = TypedAttribute(
        str,
        doc="""str, optional: Identifier for the pipeline that processed this image from raw data.

        *Filterable, sortable*.
        """,
    )
    fill_fraction = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Fraction of this image which has data.

        *Filterable, sortable*.
        """,
    )
    incidence_angle = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Sensor incidence angle in degrees.

        *Filterable, sortable*.
        """,
    )
    radiance_gain_bias = ListAttribute(
        ListAttribute(TypedAttribute(float, coerce=True)),
        doc="""list(list(float), optional: radiance conversion gain and bias.

        Outer list indexed by ``Band.vendor_order``, inner lists are ``[gain, bias]`` or
        empty if not applicable.
        """,
    )
    reflectance_gain_bias = ListAttribute(
        ListAttribute(TypedAttribute(float, coerce=True)),
        doc="""list(list(float), optional: reflectance conversion gain and bias.

        Outer list indexed by ``Band.vendor_order``, inner lists are ``[gain, bias]`` or
        empty if not applicable.
        """,
    )
    reflectance_scale = ListAttribute(
        TypedAttribute(float, coerce=True),
        doc="list(float), optional: Scale factors converting TOA radiances to TOA reflectances.",
    )
    roll_angle = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Applicable only to Landsat 8, roll angle in degrees.

        *Filterable, sortable*.
        """,
    )
    solar_azimuth_angle = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Solar azimuth angle at capture time.

        *Filterable, sortable*.
        """,
    )
    solar_elevation_angle = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Solar elevation angle at capture time.

        *Filterable, sortable*.
        """,
    )
    temperature_gain_bias = ListAttribute(
        ListAttribute(TypedAttribute(float, coerce=True)),
        doc="""list(list(float), optional: surface temperature conversion coefficients.

        Outer list indexed by ``Band.vendor_order``, inner lists are ``[gain, bias]`` or
        empty if not applicable.
        """,
    )
    view_angle = TypedAttribute(
        float,
        coerce=True,
        doc="""float, optional: Sensor view angle in degrees.

        *Filterable, sortable*.
        """,
    )
    satellite_id = TypedAttribute(
        str,
        doc="""str, optional: Id of the capturing satellite/sensor among a constellation of many satellites.

        *Filterable, sortable*.
        """,
    )

    # Provider info
    provider_id = TypedAttribute(
        str,
        doc="""str, optional: Id that uniquely ties this image to an entity as understood by the data provider.

        *Filterable, sortable*.
        """,
    )
    provider_url = TypedAttribute(
        str,
        doc="str, optional: An external (http) URL that has more details about the image",
    )
    preview_url = TypedAttribute(
        str,
        doc="""str, optional: An external (http) URL to a preview image.

        This image could be inlined in a UI to show a preview for the image.
        """,
    )
    preview_file = TypedAttribute(
        str,
        doc="""str, optional: A GCS URL with a georeferenced image.

        Use a GCS URL (``gs://...```) with appropriate access permissions.  This
        referenced image can be used to raster the image in a preview context, generally
        low resolution.  It should be a 3-band (RBG) or a 4-band (RGBA) image suitable
        for visual preview.  (It's not expected to conform to the bands of the
        products.)
        """,
    )

    SUPPORTED_DATATYPES = (
        "uint8",
        "int16",
        "uint16",
        "int32",
        "uint32",
        "float32",
        "float64",
    )

    def __init__(self, **kwargs):
        super(Image, self).__init__(**kwargs)
        self._geocontext = None

    @property
    def geocontext(self):
        """
        `~descarteslabs.common.geo.AOI`: A geocontext for loading this Image's original, unwarped data.

        These defaults are used:

        * resolution: resolution determined from the Image's ``geotrans``
        * crs: native CRS of the Image (often, a UTM CRS)
        * bounds: bounds determined from the Image's ``geotrans``, ``x_pixels`` and ``y_pixels``
        * bounds_crs: native CRS of the Image
        * align_pixels: False, to prevent interpolation snapping pixels to a new grid
        * geometry: None

        .. note::

            Using this :class:`~descarteslabs.common.geo.GeoContext` will only
            return original, unwarped data if the Image is axis-aligned ("north-up")
            within the CRS. If its ``geotrans`` applies a rotation, a warning will be raised.
            In that case, use `Raster.ndarray` or `Raster.raster` to retrieve
            original data. (The :class:`~descarteslabs.common.geo.GeoContext`
            paradigm requires bounds for consistency, which are inherently axis-aligned.)
        """
        if self._geocontext is None:
            shape = None
            bounds = None
            bounds_crs = None
            crs = self.cs_code or self.projection
            resolution = None

            geotrans = self.geotrans
            if geotrans is not None:
                geotrans = Affine.from_gdal(*geotrans)
                if not geotrans.is_rectilinear:
                    # NOTE: this may still be an insufficient check for some CRSs, i.e. polar stereographic?
                    warnings.warn(
                        "The GeoContext will *not* return this Image's original data, "
                        "since it's rotated compared to the grid of the CRS. "
                        "The array will be 'north-up', with the data rotated within it, "
                        "and extra empty pixels padded around the side(s). "
                        "To get the original, unrotated data, you must use the Raster API: "
                        "`descarteslabs.client.services.raster.Raster.ndarray(image.id, ...)`."
                    )

                if self.x_pixels is not None and self.y_pixels is not None:
                    # prefer to raster by image shape, to best preserve original data.
                    # upper-left, upper-right, lower-left, lower-right in pixel coordinates
                    pixel_corners = [
                        (0, 0),
                        (self.x_pixels, 0),
                        (0, self.y_pixels),
                        (self.x_pixels, self.y_pixels),
                    ]
                    geo_corners = [geotrans * corner for corner in pixel_corners]
                    xs, ys = zip(*geo_corners)
                    bounds = (min(xs), min(ys), max(xs), max(ys))
                    bounds_crs = crs
                    shape = (self.y_pixels, self.x_pixels)
                else:
                    x_res, y_res = geotrans._scaling
                    if x_res != y_res:
                        # if pixels aren't square (unlikely), we won't just pick a resolution,
                        # the user has to figure that out.
                        raise ValueError(
                            "Image has no size and non-square pixels, so resolution is ambiguous. "
                            "You must manually define a geocontext for this image."
                        )
                    resolution = x_res

            self._geocontext = AOI(
                geometry=self.geometry,
                resolution=resolution,
                bounds=bounds,
                bounds_crs=bounds_crs,
                crs=crs,
                shape=shape,
                align_pixels=False,
            )

        return self._geocontext

    @property
    def __geo_interface__(self):
        return self.geocontext.__geo_interface__

    # convenience property
    @property
    def date(self):
        return self.acquired

    @classmethod
    def search(cls, client=None, request_params=None):
        """A search query for all images.

        Return an `~descarteslabs.catalog.ImageSearch` instance for searching
        images in the Descartes Labs catalog.  This instance extends the
        :py:class:`~descarteslabs.catalog.Search` class with the
        :py:meth:`~descarteslabs.catalog.ImageSearch.summary` and
        :py:meth:`~descarteslabs.catalog.ImageSearch.summary_interval` methods
        which return summary statistics about the images that match the search query.

        Parameters
        ----------
        client : :class:`CatalogClient`, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.

        Returns
        -------
        :class:`~descarteslabs.catalog.ImageSearch`
            An instance of the `~descarteslabs.catalog.ImageSearch` class

        Example
        -------
        >>> from descarteslabs.catalog import Image
        >>> search = Image.search().limit(10)
        >>> for result in search: # doctest: +SKIP
        ...     print(result.name) # doctest: +SKIP

        """
        return ImageSearch(cls, client=client, request_params=request_params)

    @check_deleted
    def upload(self, files, upload_options=None, overwrite=False):
        """Uploads imagery from a file (or files).

        Uploads imagery from a file (or files) in GeoTIFF or JP2 format to be ingested
        as an Image.

        The Image must be in the state `~descarteslabs.catalog.DocumentState.UNSAVED`.
        The `product` or `product_id` attribute, the `name` attribute, and the
        `acquired` attribute must all be set. If either the `cs_code` or `projection`
        attributes is set (deprecated), it must agree with the projection defined in the file,
        otherwise an upload error will occur during processing.

        Parameters
        ----------
        files : str or io.IOBase or iterable of same
            File or files to be uploaded.  Can be string with path to the file in the
            local filesystem, or an opened file (``io.IOBase``), or an iterable of
            either of these when multiple files make up the image.
        upload_options : `~descarteslabs.catalog.ImageUploadOptions`, optional
            Control of the upload process.
        overwrite : bool, optional
            If True, then permit overwriting of an existing image with the same id
            in the catalog. Defaults to False. Note that in all cases, the image
            object must have a state of `~descarteslabs.catalog.DocumentState.UNSAVED`.
            USE WITH CAUTION: This can cause data cache inconsistencies in the platform,
            and should only be used for infrequent needs to update the image file
            contents. You can expect inconsistencies to endure for a period afterwards.

        Returns
        -------
        :py:class:`~descarteslabs.catalog.ImageUpload`
            An `~descarteslabs.catalog.ImageUpload` instance which can
            be used to check the status or wait on the asynchronous upload process to
            complete.

        Raises
        ------
        ValueError
            If any improper arguments are supplied.
        DeletedObjectError
            If this image was deleted.
        """
        from .image_upload import ImageUploadOptions, ImageUploadType

        if not self.id:
            raise ValueError("id field required")
        if not self.acquired:
            raise ValueError("acquired field required")
        if self.cs_code or self.projection:
            warnings.warn("cs_code and projection fields not permitted", FutureWarning)
            # raise ValueError("cs_code and projection fields not permitted")

        if self.state != DocumentState.UNSAVED:
            raise ValueError(
                "Image {} has been saved. Please use an unsaved image for uploading".format(
                    self.id
                )
            )

        if not overwrite and Image.exists(self.id, self._client):
            raise ValueError(
                "Image {} already exists in the catalog. Please either use a new image id or overwrite=True".format(
                    self.id
                )
            )

        if self.product.state != DocumentState.SAVED:
            raise ValueError(
                "Product {} has not been saved. Please save before uploading images".format(
                    self.product_id
                )
            )

        # convert file to a list, validating and extracting file names
        if isinstance(files, str) or isinstance(files, io.IOBase):
            files = [files]
        elif not isinstance(files, abc.Iterable):
            raise ValueError(
                "Invalid files value: must be string, IOBase, or iterable of the same"
            )
        filenames = []
        for f in files:
            if isinstance(f, str):
                filenames.append(f)
            elif isinstance(f, io.IOBase):
                filenames.append(f.name)
            else:
                raise ValueError(
                    "Invalid files value: must be string, IOBase, or iterable of the same"
                )
        if not filenames:
            raise ValueError("Invalid files value has zero length")

        if not upload_options:
            upload_options = ImageUploadOptions()

        upload_options.upload_type = ImageUploadType.FILE
        upload_options.image_files = filenames

        return self._do_upload(files, upload_options)

    @check_deleted
    def upload_ndarray(
        self,
        ndarray,
        upload_options=None,
        raster_meta=None,
        overviews=None,
        overview_resampler=None,
        overwrite=False,
    ):
        """Uploads imagery from an ndarray to be ingested as an Image.

        The Image must be in the state `~descarteslabs.catalog.DocumentState.UNSAVED`.
        The `product` or `product_id` attribute, the `name` attribute, and the
        `acquired` attribute must all be set. Either (but not both) the `cs_code`
        or `projection` attributes must be set, or the `raster_meta` parameter must be provided.
        Similarly, either the `geotrans` attribute must be set or `raster_meta` must be provided.

        Note that one of the spatial reference attributes (`cs_code` or
        `projection`), or the `geotrans` attribute can be
        specified explicitly in the image, or the `raster_meta` parameter can be
        specified.  Likewise, `overviews` and `overview_resampler` can be
        specified explicitly, or via the `upload_options` parameter.


        Parameters
        ----------
        ndarray : np.array, Iterable(np.array)
            A numpy array or list of numpy arrays with image data, either with 2
            dimensions of shape ``(x, y)`` for a single band or with 3 dimensions of
            shape ``(band, x, y)`` for any number of bands.  If providing a 3d array
            the first dimension must index the bands.  The ``dtype`` of the array must
            also be one of the following:
            [``uint8``, ``int8``, ``uint16``, ``int16``, ``uint32``, ``int32``,
            ``float32``, ``float64``]
        upload_options : :py:class:`~descarteslabs.catalog.ImageUploadOptions`, optional
            Control of the upload process.
        raster_meta : dict, optional
            Metadata returned from the :meth:`Raster.ndarray()
            <descarteslabs.client.services.raster.Raster.ndarray>` request which
            generated the initial data for the `ndarray` being uploaded.  Specifying
            `geotrans` and one of the spatial reference attributes (`cs_code` or
            `projection`) is unnecessary in this case but will take precedence over
            the value in `raster_meta`.
        overviews : list(int), optional
            Overview resolution magnification factors e.g.  [2, 4] would make two
            overviews at 2x and 4x the native resolution.  Maximum number of overviews
            allowed is 16.  Can also be set in the `upload_options` parameter.
        overview_resampler : `ResampleAlgorithm`, optional
            Resampler algorithm to use when building overviews.  Controls how pixels
            are combined to make lower res pixels in overviews. Can also be set in
            the `upload_options` parameter.
        overwrite : bool, optional
            If True, then permit overwriting of an existing image with the same id
            in the catalog. Defaults to False. Note that in all cases, the image
            object must have a state of `~descarteslabs.catalog.DocumentState.UNSAVED`.
            USE WITH CAUTION: This can cause data cache inconsistencies in the platform,
            and should only be used for infrequent needs to update the image file
            contents. You can expect inconsistencies to endure for a period afterwards.

        Raises
        ------
        ValueError
            If any improper arguments are supplied.
        DeletedObjectError
            If this image was deleted.

        Returns
        -------
        :py:class:`~descarteslabs.catalog.ImageUpload`
            An `~descarteslabs.catalog.ImageUpload` instance which can
            be used to check the status or wait on the asynchronous upload process to
            complete.
        """
        from .image_upload import ImageUploadOptions, ImageUploadType

        if not self.id:
            raise ValueError("id field required")
        if not self.acquired:
            raise ValueError("acquired field required")
        if self.cs_code and self.projection:
            warnings.warn(
                "Only one of cs_code and projection fields permitted",
                FutureWarning,
            )
            # raise ValueError("only one of cs_code and projection fields permitted")

        if self.state != DocumentState.UNSAVED:
            raise ValueError(
                "Image {} has been saved. Please use an unsaved image for uploading".format(
                    self.id
                )
            )

        if not overwrite and Image.exists(self.id, self._client):
            raise ValueError(
                "Image {} already exists in the catalog. Please either use a new image id or overwrite=True".format(
                    self.id
                )
            )

        if self.product.state != DocumentState.SAVED:
            raise ValueError(
                "Product {} has not been saved. Please save before uploading images".format(
                    self.product_id
                )
            )

        if isinstance(ndarray, (np.ndarray, np.generic)):
            ndarray = [ndarray]
        elif not isinstance(ndarray, abc.Iterable):
            raise ValueError(
                "The array must be an instance of ndarray or an Iterable of ndarrays"
                "such as a list."
            )

        # validate the shape of each ndarray
        # modify image data to shift axes to what ingest expects
        for idx, image_data in enumerate(ndarray):
            if not isinstance(image_data, (np.ndarray, np.generic)):
                raise ValueError(f"The item at index {idx} is not an ndarray")

            if len(image_data.shape) not in (2, 3):
                raise ValueError(
                    "The array must have 2 dimensions (shape '(x, y)') or 3 dimensions with the band "
                    "axis in the first dimension (shape '(band, x, y)'). The given array has shape "
                    "'{}' instead.".format(image_data.shape)
                )

            if image_data.dtype.name not in self.SUPPORTED_DATATYPES:
                raise ValueError(
                    "The array has an unsupported data type {}. Only the following data types are supported: {}".format(
                        image_data.dtype.name, ",".join(self.SUPPORTED_DATATYPES)
                    )
                )

            if len(image_data.shape) == 3:
                scale_factor = 5
                scaled_band_dim = image_data.shape[0] * scale_factor

                if (
                    scaled_band_dim > image_data.shape[1]
                    or scaled_band_dim > image_data.shape[2]
                ):
                    warnings.warn(
                        "The shape '{}' of the given 3d-array looks like it might not have the band "
                        "axis as the first dimension. Verify that your array conforms to the shape "
                        "'(band, x, y)'".format(image_data.shape)
                    )
                # v1 ingest expects (X,Y,bands)
                ndarray[idx] = np.moveaxis(image_data, 0, -1)

        # default to raster_meta fields if not explicitly provided
        if raster_meta:
            if not self.geotrans:
                self.geotrans = raster_meta.get("geoTransform")
            if not self.cs_code and not self.projection:
                # doesn't yet exist!
                self.projection = raster_meta.get("coordinateSystem", {}).get("proj4")

        if not self.geotrans:
            raise ValueError("geotrans field or raster_meta parameter is required")
        if not self.cs_code and not self.projection:
            raise ValueError(
                "cs_code or projection field is required if "
                + "raster_meta parameter is not given"
            )

        if not upload_options:
            upload_options = ImageUploadOptions()
        upload_options.upload_type = ImageUploadType.NDARRAY
        if overviews:
            upload_options.overviews = overviews
        if overview_resampler:
            upload_options.overview_resampler = overview_resampler

        # write all the ndarrays to files so that _do_upload can read them
        files = []
        upload_size = 0

        try:
            for image_data in ndarray:
                upload_size += image_data.nbytes
                tmp = NamedTemporaryFile(delete=False)
                files.append(tmp)
                np.save(tmp, image_data, allow_pickle=False)
                # From tempfile docs:
                # Whether the name can be used to open the file a second time,
                # while the named temporary file is still open, varies across
                # platforms (it can be so used on Unix; it cannot on Windows
                # NT or later)
                # We close the underlying file object so _do_upload can open
                # the path again in a cross platform compatible way.
                # Cleanup is manual in the finally block.
                tmp.close()

            file_names = [f.name for f in files]
            upload_options.upload_size = upload_size
            upload_options.image_files = file_names

            return self._do_upload(file_names, upload_options)
        finally:
            for file in files:
                try:
                    os.unlink(file.name)
                except OSError:
                    pass

    def image_uploads(self):
        """A search query for all uploads for this image created by this user.

        Returns
        -------
        :py:class:`~descarteslabs.catalog.Search`
            A :py:class:`~descarteslabs.catalog.Search` instance configured to
            find all uploads for this image.
        """
        from .image_upload import ImageUpload

        return ImageUpload.search(client=self._client).filter(
            (properties.product_id == self.product_id)
            & (properties.image_id == self.id)
        )

    # the upload implementation is broken out so it can be used from multiple methods
    def _do_upload(self, files, upload_options):
        from .image_upload import ImageUpload, ImageUploadStatus

        upload = ImageUpload(
            client=self._client, image=self, image_upload_options=upload_options
        )

        upload.save()

        headers = {"content-type": "application/octet-stream"}

        for file, upload_url in zip(files, upload.resumable_urls):
            if isinstance(file, io.IOBase):
                if "b" not in file.mode:
                    file.close()
                    file = io.open(file.name, "rb")
                f = file
            else:
                f = io.open(file, "rb")

            try:
                self._upload_service.session.put(upload_url, data=f, headers=headers)
            finally:
                f.close()

        upload.status = ImageUploadStatus.PENDING
        upload.save()

        return upload

    # Scenes functionality
    def coverage(self, geom):
        """
        The fraction of a geometry-like object covered by this Image's geometry.

        Parameters
        ----------
        geom : GeoJSON-like dict, :class:`~descarteslabs.common.geo.geocontext.GeoContext`, or object with __geo_interface__
            Geometry to which to compare this Image's geometry

        Returns
        -------
        coverage: float
            The fraction of ``geom``'s area that overlaps with this Image,
            between 0 and 1.

        Example
        -------
        >>> import descarteslabs as dl
        >>> image = dl.catalog.Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")  # doctest: +SKIP
        >>> image.coverage(image.geometry.buffer(1))  # doctest: +SKIP
        0.258370644415335
        """  # noqa: E501

        if isinstance(geom, GeoContext):
            shape = geom.geometry
        else:
            shape = geometry_like_to_shapely(geom)

        intersection = shape.intersection(self.geometry)
        return intersection.area / shape.area

    def ndarray(
        self,
        bands,
        geocontext=None,
        crs=None,
        resolution=None,
        all_touched=None,
        mask_nodata=True,
        mask_alpha=None,
        bands_axis=0,
        raster_info=False,
        resampler=ResampleAlgorithm.NEAR,
        processing_level=None,
        scaling=None,
        data_type=None,
        progress=None,
    ):
        """
        Load bands from this image as an ndarray, optionally masking invalid data.

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
        geocontext : :class:`~descarteslabs.common.geo.geocontext.GeoContext`, default None
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading this Image.
            If ``None`` then the default geocontext of the image will be used.
        crs : str, default None
            if not None, update the gecontext with this value to set the output CRS.
        resolution : float, default None
            if not None, update the geocontext with this value to set the output resolution
            in the units native to the specified or defaulted output CRS.
        all_touched : float, default None
            if not None, update the geocontext with this value to control rastering behavior.
        mask_nodata : bool, default True
            Whether to mask out values in each band that equal
            that band's ``nodata`` sentinel value.
        mask_alpha : bool or str or None, default None
            Whether to mask pixels in all bands where the alpha band of the image is 0.
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
            of the image, including the coordinate system WKT and geotransform matrix.
            Generally only useful if you plan to upload data derived
            from this image back to the Descartes catalog, or use it with GDAL.
        resampler : `ResampleAlgorithm`, default `ResampleAlgorithm.NEAR`
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to its new resolution or CRS.
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
        >>> image = dl.catalog.Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")  # doctest: +SKIP
        >>> arr = image.ndarray("red green blue", resolution=120.)  # doctest: +SKIP
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
            If a Image's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given invalid parameters
        """
        if geocontext is None:
            geocontext = self.geocontext
        if crs is not None or resolution is not None:
            try:
                params = {}
                if crs is not None:
                    params["crs"] = crs
                if resolution is not None:
                    params["resolution"] = resolution
                geocontext = geocontext.assign(**params)
            except TypeError:
                raise ValueError(
                    f"{type(geocontext)} geocontext does not support modifying crs or resolution"
                ) from None
        if all_touched is not None:
            geocontext = geocontext.assign(all_touched=all_touched)

        return self._ndarray(
            bands,
            geocontext,
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

    # the ndarray implementation is broken out so it can be used directly from ImageCollection
    def _ndarray(
        self,
        bands,
        geocontext,
        mask_nodata=True,
        mask_alpha=None,
        bands_axis=0,
        raster_info=False,
        resampler=ResampleAlgorithm.NEAR,
        processing_level=None,
        scaling=None,
        data_type=None,
        progress=None,
    ):
        if not (-3 < bands_axis < 3):
            raise ValueError(
                "Invalid bands_axis; axis {} would not exist in a 3D array".format(
                    bands_axis
                )
            )

        bands = bands_to_list(bands)
        product_bands = cached_bands_by_product(self.product_id, self._client)

        scales, data_type = scaling_parameters(
            product_bands, bands, processing_level, scaling, data_type
        )

        mask_nodata = bool(mask_nodata)

        alpha_band_name = "alpha"
        if isinstance(mask_alpha, str):
            alpha_band_name = mask_alpha
            mask_alpha = True
        elif mask_alpha is None:
            # if user does not set mask_alpha, only attempt to mask_alpha if
            # alpha band is exists in the image.
            mask_alpha = alpha_band_name in product_bands
        elif type(mask_alpha) is not bool:
            raise ValueError("'mask_alpha' must be None, a band name, or a bool.")

        drop_alpha = False
        if mask_alpha:
            if alpha_band_name not in product_bands:
                raise ValueError(
                    "Cannot mask alpha: no {} band for the product '{}'. "
                    "Try setting 'mask_alpha=False'.".format(
                        alpha_band_name, self.product_id
                    )
                )
            try:
                alpha_i = bands.index(alpha_band_name)
            except ValueError:
                bands.append(alpha_band_name)
                drop_alpha = True
            else:
                if alpha_i != len(bands) - 1:
                    raise ValueError(
                        "Alpha must be the last band in order to reduce rasterization errors"
                    )

        raster_params = geocontext.raster_params
        full_raster_args = dict(
            inputs=[self.id],
            order="gdal",
            bands=bands,
            scales=scales,
            data_type=data_type,
            resampler=resampler,
            processing_level=processing_level,
            masked=mask_nodata or mask_alpha,
            mask_nodata=mask_nodata,
            mask_alpha=mask_alpha,
            drop_alpha=drop_alpha,
            progress=progress,
            **raster_params,
        )

        try:
            arr, info = Raster.get_default_client().ndarray(**full_raster_args)

        except NotFoundError:
            raise NotFoundError(
                "'{}' does not exist in the Descartes catalog".format(self.id)
            ) from None
        except BadRequestError as e:
            msg = (
                "Error with request:\n"
                "{err}\n"
                "For reference, Raster.ndarray was called with these arguments:\n"
                "{args}"
            )
            msg = msg.format(err=e, args=json.dumps(full_raster_args, indent=2))
            raise BadRequestError(msg) from None

        if len(arr.shape) == 2:
            # if only 1 band requested, still return a 3d array
            arr = arr[np.newaxis]

        if bands_axis != 0:
            arr = np.moveaxis(arr, 0, bands_axis)
        if raster_info:
            return arr, info
        else:
            return arr

    def download(
        self,
        bands,
        geocontext=None,
        crs=None,
        resolution=None,
        all_touched=None,
        dest=None,
        format=DownloadFileFormat.TIF,
        resampler=ResampleAlgorithm.NEAR,
        processing_level=None,
        scaling=None,
        data_type=None,
        nodata=None,
        progress=None,
    ):
        """
        Save bands from this image as a GeoTIFF, PNG, or JPEG, writing to a path.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue derived:ndvi"``),
            or a sequence of band names (``["red", "green", "blue", "derived:ndvi"]``).
            Names must be keys in ``self.properties.bands``.
        geocontext : :class:`~descarteslabs.common.geo.geocontext.GeoContext`, default None
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading this image.
            If ``None`` then use the default context for the image.
        crs : str, default None
            if not None, update the gecontext with this value to set the output CRS.
        resolution : float, default None
            if not None, update the geocontext with this value to set the output resolution
            in the units native to the specified or defaulted output CRS.
        all_touched : float, default None
            if not None, update the geocontext with this value to control rastering behavior.
        dest : str or path-like object, default None
            Where to write the image file.

            * If None (default), it's written to an image file of the given ``format``
              in the current directory, named by the image's ID and requested bands,
              like ``"sentinel-2:L1C:2018-08-10_10TGK_68_S2A_v1-red-green-blue.tif"``
            * If a string or path-like object, it's written to that path.

              Any file already existing at that path will be overwritten.

              Any intermediate directories will be created if they don't exist.

              Note that path-like objects (such as pathlib.Path) are only supported
              in Python 3.6 or later.
        format : `DownloadFileFormat`, default `DownloadFileFormat.TIF`
            Output file format to use
            If a str or path-like object is given as ``dest``, ``format`` is ignored
            and determined from the extension on the path (one of ".tif", ".png", or ".jpg").
        resampler : `ResampleAlgorithm`, default `ResampleAlgorithm.NEAR`
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to its new resolution or SRS.
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
        >>> image = dl.catalog.Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")  # doctest: +SKIP
        >>> image.download("red green blue", resolution=120.)  # doctest: +SKIP
        "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1_red-green-blue.tif"
        >>> import os
        >>> os.listdir(".")  # doctest: +SKIP
        ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1_red-green-blue.tif"]
        >>> image.download(
        ...     "nir swir1",
        ...     "rasters/{geocontext.resolution}-{image_id}.jpg".format(geocontext=image.geocontext, image_id=image.id)
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
            If a image's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given invalid parameters
        """
        if geocontext is None:
            geocontext = self.geocontext
        if crs is not None or resolution is not None:
            try:
                params = {}
                if crs is not None:
                    params["crs"] = crs
                if resolution is not None:
                    params["resolution"] = resolution
                geocontext = geocontext.assign(**params)
            except TypeError:
                raise ValueError(
                    f"{type(geocontext)} geocontext does not support modifying crs or resolution"
                ) from None
        if all_touched is not None:
            geocontext = geocontext.assign(all_touched=all_touched)

        return self._download(
            bands,
            geocontext,
            dest=dest,
            format=format,
            resampler=resampler,
            processing_level=processing_level,
            scaling=scaling,
            data_type=data_type,
            nodata=nodata,
            progress=progress,
        )

    # the download implementation is broken out so it can be used directly from ImageCollection
    def _download(
        self,
        bands,
        geocontext,
        dest=None,
        format=DownloadFileFormat.TIF,
        resampler=ResampleAlgorithm.NEAR,
        processing_level=None,
        scaling=None,
        data_type=None,
        nodata=None,
        progress=None,
    ):
        bands = bands_to_list(bands)
        scales, data_type = scaling_parameters(
            cached_bands_by_product(self.product_id, self._client),
            bands,
            processing_level,
            scaling,
            data_type,
        )

        return download(
            inputs=[self.id],
            bands_list=bands,
            geocontext=geocontext,
            data_type=data_type,
            dest=dest,
            format=format,
            resampler=resampler,
            processing_level=processing_level,
            scales=scales,
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

        The support for scaling parameters in the Catalog API includes
        the concept of an automated scaling mode. The four supported modes
        are as follows.

        ``"raw"``:
            Equivalent to a ``None``, the data should not be scaled.
        ``"auto"``:
            Equivalent to a 0-tuple, the data should be scaled by
            the Raster service so that the actual range of data in the
            input is scaled up to the full display range (0-255). It
            is not possible to determine the bounds of this input range
            in the client as the actual band data is not accessible.
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
            different methods in the Catalog API which accept a ``scaling``
            parameter.

        See Also
        --------
        :doc:`Catalog Guide <guides/catalog>` : This contains many examples of the use of
        the ``scaling`` and ``data_type`` parameters.
        """
        bands = bands_to_list(bands)
        return scaling_parameters(
            cached_bands_by_product(self.product_id, self._client),
            bands,
            processing_level,
            scaling,
            data_type,
        )


# Deal with circular import problem
from .image_collection import ImageCollection  # noqa: E402

Image._collection_type = ImageCollection
