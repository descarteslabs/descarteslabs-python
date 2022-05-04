from enum import Enum
import os.path
import io
from tempfile import NamedTemporaryFile
import warnings

try:
    import collections.abc as abc
except ImportError:
    import collections as abc

import numpy as np

from ..common.property_filtering import GenericProperties
from ..client.services.service import ThirdPartyService

from .catalog_base import DocumentState, check_deleted
from .named_catalog_base import NamedCatalogObject
from .attributes import (
    EnumAttribute,
    GeometryAttribute,
    Timestamp,
    ListAttribute,
    File,
    TupleAttribute,
    TypedAttribute,
)

properties = GenericProperties()


class StorageState(str, Enum):
    """The storage state for an image.

    Attributes
    ----------
    AVAILABLE : enum
        The image data has been uploaded and can be rastered.
    REMOTE : enum
        The image data is has not been uploaded, but its location is known.
    """

    AVAILABLE = "available"
    REMOTE = "remote"


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
    _gcs_upload_service = ThirdPartyService()

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

    @classmethod
    def search(cls, client=None):
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
        from .search import ImageSearch

        return ImageSearch(cls, client=client)

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
        from .image_upload import ImageUploadType, ImageUploadOptions

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

        if not overwrite and Image.exists(self.id):
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
        ndarray : np.array
            A numpy array with image data, either with 2 dimensions of shape
            ``(x, y)`` for a single band or with 3 dimensions of shape
            ``(band, x, y)`` for any number of bands.  If providing a 3d array
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
        overview_resampler : str, optional
            Resampler algorithm to use when building overviews.  Controls how pixels
            are combined to make lower res pixels in overviews.  Allowed resampler
            algorithms are: [``nearest``, ``average``, ``gauss``, ``cubic``,
            ``cubicspline``, ``lanczos``, ``average_mp``, ``average_magphase``,
            ``mode``].  Can also be set in the `upload_options` parameter.
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
        from .image_upload import ImageUploadType, ImageUploadOptions

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

        if not overwrite and Image.exists(self.id):
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

        if len(ndarray.shape) not in (2, 3):
            raise ValueError(
                "The array must have 2 dimensions (shape '(x, y)') or 3 dimensions with the band "
                "axis in the first dimension (shape '(band, x, y)'). The given array has shape "
                "'{}' instead.".format(ndarray.shape)
            )

        if len(ndarray.shape) == 3:
            scale_factor = 5
            scaled_band_dim = ndarray.shape[0] * scale_factor
            if scaled_band_dim > ndarray.shape[1] or scaled_band_dim > ndarray.shape[2]:
                warnings.warn(
                    "The shape '{}' of the given 3d-array looks like it might not have the band "
                    "axis as the first dimension. Verify that your array conforms to the shape "
                    "'(band, x, y)'".format(ndarray.shape)
                )
            # v1 ingest expects (X,Y,bands)
            ndarray = np.moveaxis(ndarray, 0, -1)

        if ndarray.dtype.name not in self.SUPPORTED_DATATYPES:
            raise ValueError(
                "The array has an unsupported data type {}. Only the following data types are supported: {}".format(
                    ndarray.dtype.name, ",".join(self.SUPPORTED_DATATYPES)
                )
            )

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

        upload_options.upload_size = ndarray.nbytes

        with NamedTemporaryFile(delete=False) as tmp:
            try:
                np.save(tmp, ndarray, allow_pickle=False)

                # From tempfile docs:
                # Whether the name can be used to open the file a second time,
                # while the named temporary file is still open, varies across
                # platforms (it can be so used on Unix; it cannot on Windows
                # NT or later)
                # We close the underlying file object so _do_upload can open
                # the path again in a cross platform compatible way.
                # Cleanup is manual in the finally block.
                tmp.close()
                upload_options.image_files = [tmp.name]
                return self._do_upload([tmp.name], upload_options)
            finally:
                os.unlink(tmp.name)

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

    def _do_upload(self, files, upload_options):
        from .image_upload import ImageUpload, ImageUploadStatus

        upload = ImageUpload(
            client=self._client, image=self, image_upload_options=upload_options
        )

        upload.save()

        for file, upload_url in zip(files, upload.resumable_urls):
            if isinstance(file, io.IOBase):
                if "b" not in file.mode:
                    file.close()
                    file = io.open(file.name, "rb")
                f = file
            else:
                f = io.open(file, "rb")

            try:
                self._gcs_upload_service.session.put(upload_url, data=f)
            finally:
                f.close()

        upload.status = ImageUploadStatus.PENDING
        upload.save()

        return upload
