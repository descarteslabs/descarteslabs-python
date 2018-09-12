import os
import six
import io

from tempfile import NamedTemporaryFile
from requests.exceptions import RequestException

from descarteslabs.client.addons import numpy as np
from descarteslabs.client.auth import Auth
from descarteslabs.client.deprecation import check_deprecated_kwargs
from descarteslabs.client.exceptions import ServerError
from descarteslabs.client.services.metadata import Metadata
from descarteslabs.client.services.service import Service, ThirdPartyService


class Catalog(Service):
    """The Descartes Labs (DL) Catalog allows you to add georeferenced raster products
    into the Descartes Labs Platform. Catalog products can be used in other DL services
    like Raster and Tasks and Metadata.

    The entrypoint for using catalog is creating a Product using :meth:`Catalog.add_product`.
    After creating a product you should add band(s) to it using :meth:`Catalog.add_band`, and
    upload imagery using :meth:`Catalog.upload_image`.

    Bands define where and how data is encoded into imagery files, and how it should be displayed.
    Images define metadata about a specific groups of pixels (in the form of images), their georeferencing, geometry,
    coordinate system, and other pertinent information.
    """
    UPLOAD_NDARRAY_SUPPORTED_DTYPES = ['uint8', 'int8', 'uint16', 'int16', 'uint32', 'int32', 'float32', 'float64']
    TIMEOUT = (9.5, 30)

    def __init__(self, url=None, auth=None, metadata=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        if auth is None:
            auth = Auth()

        if metadata is None:
            self._metadata = Metadata(auth=auth)
        else:
            self._metadata = metadata

        self._gcs_upload_service = ThirdPartyService()

        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_CATALOG_URL",
                "https://platform.descarteslabs.com/metadata/v1/catalog"
            )

        super(Catalog, self).__init__(url, auth=auth)

    def namespace_product(self, product_id):
        namespace = self.auth.namespace

        if not product_id.startswith(namespace):
            product_id = '{}:{}'.format(namespace, product_id)

        return product_id

    def own_products(self):
        return self._metadata.products(owner=self.auth.payload['sub'])

    def own_bands(self):
        return self._metadata.bands(owner=self.auth.payload['sub'])

    def _add_core_product(self, product_id, **kwargs):
        kwargs['id'] = product_id
        check_deprecated_kwargs(kwargs, {
            "start_time": "start_datetime",
            "end_time": "end_datetime",
        })
        r = self.session.post('/core/products', json=kwargs)
        return r.json()

    def get_product(self, product_id, add_namespace=False):
        if add_namespace:
            product_id = self.namespace_product(product_id)

        r = self.session.get('/products/{}'.format(product_id))
        return r.json()

    def add_product(self, product_id, title, description, add_namespace=False, **kwargs):
        """Add a product to your catalog.

        :param str product_id: (Required) A unique name for this product. In the created
            product a namespace consisting of your user id (e.g.
            "ae60fc891312ggadc94ade8062213b0063335a3c:") or your organization id (e.g.,
            "yourcompany:") will be prefixed to this, if it doesn't already have one, in
            order to make the id globally unique.
        :param str title: (Required) Official product title.
        :param str description: (Required) Information about the product,
                                why it exists, and what it provides.

        :param bool add_namespace: Add your user namespace to the product_id. *Deprecated*
        :param list(str) read: A list of groups, or user hashes to give read access to.
        :param int spectral_bands: Number of spectral bands the product has.
        :param list(str) native_bands: A list of the names of the native bands of this product
                                        (most applicable to satellite sensors).
        :param str start_datetime: ISO8601 compliant date string, indicating start of product data.
        :param str end_datetime: ISO8601 compliant date string, indicating end of product data.
        :param str notes: Any notes to relay about the product.
        :param str orbit: Type of orbit (satellite only).
        :param str processing_level: Way in which raw data has been processed if any.
        :param str resolution: Pixel density of the data, provide units.
        :param str revisit: How often an AOI can expect updated data.
        :param str sensor: Name of the sensor used.
        :param str swath: How large an area the sensor captures at a given time.

        :return: JSON API representation of the product.
        :rtype: dict
        """
        for k, v in locals().items():
            if k in ['title', 'description']:
                if v is None:
                    raise TypeError("required arg `{}` not provided".format(k))
                kwargs[k] = v
        check_deprecated_kwargs(kwargs, {
            "start_time": "start_datetime",
            "end_time": "end_datetime",
        })

        kwargs['id'] = self.namespace_product(product_id) if add_namespace else product_id
        r = self.session.post('/products', json=kwargs)
        return r.json()

    def replace_product(
            self,
            product_id,
            title,
            description,
            add_namespace=False,
            set_global_permissions=False,
            **kwargs
    ):
        """Replace a product in your catalog with a new version.

        :param str product_id: (Required) A unique name for this product.
        :param str title: (Required) Official product title.
        :param str description: (Required) Information about the product,
                                why it exists, and what it provides.

        :param bool add_namespace: Add your user namespace to the product_id. *Deprecated*
        :param list(str) read: A list of groups, or user hashes to give read access to.
        :param int spectral_bands: Number of spectral bands the product has.
        :param list(str) native_bands: A list of the names of the native bands of this product
                                        (most applicable to satellite sensors).
        :param str start_datetime: ISO8601 compliant date string, indicating start of product data.
        :param str end_datetime: ISO8601 compliant date string, indicating end of product data.
        :param str notes: Any notes to relay about the product.
        :param str orbit: Type of orbit (satellite only).
        :param str processing_level: Way in which raw data has been processed if any.
        :param str resolution: Pixel density of the data, provide units.
        :param str revisit: How often an AOI can expect updated data.
        :param str sensor: Name of the sensor used.
        :param str swath: How large an area the sensor captures at a given time.

        :return: JSON API representation of the product.
        :rtype: dict
        """
        for k, v in locals().items():
            if k in ['title', 'description']:
                if v is None:
                    raise TypeError("required arg `{}` not provided".format(k))
                kwargs[k] = v
        check_deprecated_kwargs(kwargs, {
            "start_time": "start_datetime",
            "end_time": "end_datetime",
        })

        if add_namespace:
            product_id = self.namespace_product(product_id)

        params = None
        if set_global_permissions is True:
            params = {'set_global_permissions': 'true'}

        r = self.session.put('/products/{}'.format(product_id), json=kwargs, params=params)
        return r.json()

    def change_product(self, product_id, add_namespace=False, set_global_permissions=False, **kwargs):
        """Update a product to your catalog.

        :param str product_id: (Required) The ID of the product to change.
        :param list(str) read: A list of groups, or user hashes to give read access to.
        :param int spectral_bands: Number of spectral bands the product has.
        :param list(str) native_bands: A list of the names of the native bands of this product
                                        (most applicable to satellite sensors).

        :param bool add_namespace: Add your user namespace to the product_id. *Deprecated*
        :param bool set_global_permissions: Update permissions of all existing bands and products
                                            that belong to this product with the updated permission set
                                            specified in the `read` param. Default to false.

        :param str start_datetime: ISO8601 compliant date string, indicating start of product data.
        :param str end_datetime: ISO8601 compliant date string, indicating end of product data.
        :param str title: Official product title.
        :param str description: Information about the product,
                                why it exists, and what it provides.
        :param str notes: Any notes to relay about the product.
        :param str orbit: Type of orbit (satellite only).
        :param str processing_level: Way in which raw data has been processed if any.
        :param str resolution: Pixel density of the data, provide units.
        :param str revisit: How often an AOI can expect updated data.
        :param str sensor: Name of the sensor used.
        :param str swath: How large an area the sensor captures at a given time.

        :return: JSON API representation of the product.
        :rtype: dict
        """
        check_deprecated_kwargs(kwargs, {
            "start_time": "start_datetime",
            "end_time": "end_datetime",
        })

        if add_namespace:
            product_id = self.namespace_product(product_id)

        params = None
        if set_global_permissions is True:
            params = {'set_global_permissions': 'true'}

        r = self.session.patch('/products/{}'.format(product_id), json=kwargs, params=params)
        return r.json()

    def remove_product(self, product_id, add_namespace=False, cascade=False):
        """Remove a product from the catalog.

        :param str product_id: ID of the product to remove.
        :param bool cascade: Force deletion of all the associated bands and images. BEWARE this cannot be undone.
        """
        if add_namespace:
            product_id = self.namespace_product(product_id)
        params = {'cascade': cascade}
        r = self.session.delete('/products/{}'.format(product_id), params=params)
        if r.headers['content-type'] == 'application/json':
            return r.json()

    def product_deletion_status(self, deletion_task_id):
        """Get the status of a long running product deletion job.

        :param str deletion_task_id: deletion_task ID returned from a call to :meth:`Catalog.remove_product`
                                    with a deletion_token.
        :return: document with information about product deletion progress.
        :rtype: dict
        """
        r = self.session.get('/products/deletion_tasks/{}'.format(deletion_task_id))
        return r.json()

    def get_band(self, product_id, name, add_namespace=False):
        """Get a band by name.

        :return: JSON API representation of the band.
        :rtype: dict
        """
        if add_namespace:
            product_id = self.namespace_product(product_id)

        r = self.session.get('/products/{}/bands/{}'.format(product_id, name))
        return r.json()

    def _add_core_band(
            self,
            product_id,
            name,
            srcband=None,
            dtype=None,
            nbits=None,
            data_range=None,
            type=None,
            **kwargs
    ):
        for k, v in locals().items():
            if k in ['name', 'data_range', 'dtype', 'nbits', 'srcband', 'type']:
                if v is None:
                    raise TypeError("required arg `{}` not provided".format(k))
                kwargs[k] = v
        kwargs['id'] = name
        r = self.session.post('/core/products/{}/bands'.format(product_id), json=kwargs)
        return r.json()

    def add_band(
            self,
            product_id,
            name,
            add_namespace=False,
            srcband=None,
            dtype=None,
            nbits=None,
            data_range=None,
            type=None,
            **kwargs
    ):
        """Add a data band to an existing product.

        :param str product_id: (Required) Product to which this band will belong.
        :param str name: (Required) Name of this band.
        :param bool add_namespace: Add your user namespace to the product_id. *Deprecated*
        :param int jpx_layer: If data is processed to JPEG2000, which layer is the band in. Defaults to 0.
        :param int srcband: (Required) The 1-based index of the band in the jpx_layer specified.
        :param int srcfile: If the product was processed into multiple files,
                            which one is this in. Defaults to 0 (first file).
        :param str dtype: (Required) The data type used to store this band e.g Byte or Uint16 or Float32.
        :param int nbits: (Required) The number of bits of the dtype used to store this band.
        :param list(int) data_range: (Required) A list specifying the min and max values for the data in this band.
        :param str type: (Required) The data interpretation of the band. One of ['spectral', 'derived', 'mask', 'class']

        :param int nodata: Pixel value indicating no data available.
        :param list(str) read: A list of groups, or user hashes to give read access to.
                                     Defaults to the same as the parent product if not specified.
        :param str color: The color interpretation of this band.
                          One of ['Alpha', 'Black', 'Blue', 'Cyan', 'Gray', 'Green', 'Hue',
                          'Ligntness', 'Magenta', 'Palette', 'Red', 'Saturation',
                          'Undefined', 'YCbCr_Cb', 'YCbCr_Cr', 'YCbCr_Y', 'Yellow'].
                          Must be 'Alpha' if `type` is 'mask'.
                          Must be 'Palette' if `colormap_name` or `colormap` is set.
        :param str colormap_name: A named colormap to use for this band, one of
                                  ['plasma', 'magma', 'viridis', 'msw', 'inferno']
        :param list(list(str)) colormap: A custom colormap to use for this band. Takes a list of lists, where each
                                         nested list is a 4-tuple of rgba values to map pixels whose value is the index
                                         of the tuple. i.e the colormap [["100", "20", "200", "255"]] would map pixels
                                         whose value is 0 in the original band, to the rgba color vector at colormap[0].
                                         Less than 2^nbits 4-tuples may be provided, and omitted values
                                         will default map to black.

        :param str data_description: Description of band data.
        :param list(float) physical_range: If band represents a physical value e.g reflectance
                                           or radiance what are the possible range of those values.
        :param str data_unit: Units of the physical range e.g w/sr for radiance
        :param str data_unit_description: Description of the data unit.
        :param list(int) default_range: A good default scale for the band to use
                                        when rastering for display purposes.
        :param str description: Description of the band.
        :param str name_common: Standard name for band
        :param str name_vendor: What the vendor calls the band eg. B1
        :param int vendor_order: The index of the band in the vendors band tables.
                                 Useful for referencing the band to other processing
                                 properties like surface reflectance.
        :param str processing_level: Description of how the band was processed if at all.
        :param int res_factor: Scaling of this band relative to the native band resolution.
        :param int resolution: Resolution of this band.
        :param str resolution_unit: Unit of the resolution.
        :param float wavelength_center: Center position of wavelength.
        :param float wavelength_fwhm: Full width at half maximum value of the wavelength spread.
        :param float wavelength_min: Minimum wavelength this band is sensitive to.
        :param float wavelength_max: Maximum wavelength this band is sensitive to.
        :param str wavelength_unit: Units the wavelength is expressed in.

        :return: JSON API representation of the band.
        :rtype: dict
        """

        for k, v in locals().items():
            if k in ['name', 'data_range', 'dtype', 'nbits', 'srcband', 'type']:
                if v is None:
                    raise TypeError("required arg `{}` not provided".format(k))
                kwargs[k] = v
        if add_namespace:
            product_id = self.namespace_product(product_id)
        check_deprecated_kwargs(kwargs, {"id": "name"})

        r = self.session.post('/products/{}/bands'.format(product_id), json=kwargs)
        return r.json()

    def replace_band(
            self,
            product_id,
            name,
            srcband=None,
            dtype=None,
            nbits=None,
            data_range=None,
            add_namespace=False,
            type=None,
            **kwargs
    ):
        """Replaces an existing data band with a new document.

        :param str product_id: (Required) Product to which this band will belong.
        :param str name: (Required) Name of this band.
        :param bool add_namespace: Add your user namespace to the product_id. *Deprecated*
        :param int jpx_layer: If data is processed to JPEG2000, which layer is the band in. Defaults to 0.
        :param int srcband: (Required) The 1-based index of the band in the jpx_layer specified.
        :param int srcfile: If the product was processed into multiple files,
                            which one is this in. Defaults to 0 (first file).
        :param str dtype: (Required) The data type used to store this band e.g Byte or Uint16 or Float32.
        :param int nbits: (Required) The number of bits of the dtype used to store this band.
        :param list(int) data_range: (Required) A list specifying the min and max values for the data in this band.
        :param str type: (Required) The data interpretation of the band. One of ['spectral', 'derived', 'mask', 'class']

        :param int nodata: Pixel value indicating no data available.
        :param list(str) read: A list of groups, or user hashes to give read access to.
                                     Defaults to the same as the parent product if not specified.
        :param str color: The color interpretation of this band.
                          One of ['Alpha', 'Black', 'Blue', 'Cyan', 'Gray', 'Green', 'Hue',
                          'Ligntness', 'Magenta', 'Palette', 'Red', 'Saturation',
                          'Undefined', 'YCbCr_Cb', 'YCbCr_Cr', 'YCbCr_Y', 'Yellow'].
                          Must be 'Alpha' if `type` is 'mask'.
                          Must be 'Palette' if `colormap_name` or `colormap` is set.
        :param str colormap_name: A named colormap to use for this band, one of
                                  ['plasma', 'magma', 'viridis', 'msw', 'inferno']
        :param list(list(str)) colormap: A custom colormap to use for this band. Takes a list of lists, where each
                                         nested list is a 4-tuple of rgba values to map pixels whose value is the index
                                         of the tuple. i.e the colormap [["100", "20", "200", "255"]] would map pixels
                                         whose value is 0 in the original band, to the rgba color vector at colormap[0].
                                         Less than 2^nbits 4-tuples may be provided, and omitted values
                                         will default map to black.

        :param str data_description: Description of band data.
        :param list(float) physical_range: If band represents a physical value e.g reflectance
                                           or radiance what are the possible range of those values.
        :param str data_unit: Units of the physical range e.g w/sr for radiance
        :param str data_unit_description: Description of the data unit.
        :param list(int) default_range: A good default scale for the band to use
                                        when rastering for display purposes.
        :param str description: Description of the band.
        :param str name_common: Standard name for band
        :param str name_vendor: What the vendor calls the band eg. B1
        :param int vendor_order: The index of the band in the vendors band tables.
                                 Useful for referencing the band to other processing
                                 properties like surface reflectance.
        :param str processing_level: Description of how the band was processed if at all.
        :param int res_factor: Scaling of this band relative to the native band resolution.
        :param int resolution: Resolution of this band.
        :param str resolution_unit: Unit of the resolution.
        :param float wavelength_center: Center position of wavelength.
        :param float wavelength_fwhm: Full width at half maximum value of the wavelength spread.
        :param float wavelength_min: Minimum wavelength this band is sensitive to.
        :param float wavelength_max: Maximum wavelength this band is sensitive to.
        :param str wavelength_unit: Units the wavelength is expressed in.

        :return: JSON API representation of the band.
        :rtype: dict
        """

        for k, v in locals().items():
            if k in ['data_range', 'dtype', 'nbits', 'srcband', 'type']:
                if v is None:
                    raise TypeError("required arg `{}` not provided".format(k))
                kwargs[k] = v
        if add_namespace:
            product_id = self.namespace_product(product_id)

        r = self.session.put('/products/{}/bands/{}'.format(product_id, name), json=kwargs)
        return r.json()

    def change_band(self, product_id, name, add_namespace=False, **kwargs):
        """Add a data band to an existing product.

        :param str product_id: (Required) Product to which this band belongs.
        :param str name: Name or id of band to modify.
        :param bool add_namespace: Add your user namespace to the product_id. *Deprecated*
        :param list(int) data_range: A list specifying the min and max values for the data in this band.
        :param str dtype: The data type used to store this band e.g Byte or Uint16 or Float32.
        :param int jpx_layer: If data is processed to JPEG2000, which layer is the band in. Use 0 for other formats.
        :param int nbits: The number of bits of the dtype used to store this band.
        :param int srcband: The 1-based index of the band in the jpx_layer specified.
        :param int srcfile: If the product was processed into multiple files,
                            which one is this in.
        :param str type: The data interpretation of the band. One of ['spectral', 'derived', 'mask', 'class']

        :param list(str) read: A list of groups, or user hashes to give read access to.
        :param str color: The color interpretation of this band.
                          One of ['Alpha', 'Black', 'Blue', 'Cyan', 'Gray', 'Green', 'Hue',
                          'Ligntness', 'Magenta', 'Palette', 'Red', 'Saturation',
                          'Undefined', 'YCbCr_Cb', 'YCbCr_Cr', 'YCbCr_Y', 'Yellow'].
                          Must be 'Alpha' if `type` is 'mask'.
                          Must be 'Palette' if `colormap_name` or `colormap` is set.
        :param str colormap_name: A named colormap to use for this band, one of
                                  ['plasma', 'magma', 'viridis', 'msw', 'inferno']
        :param list(list(str)) colormap: A custom colormap to use for this band. Takes a list of lists, where each
                                         nested list is a 4-tuple of rgba values to map pixels whose value is the index
                                         of the tuple. i.e the colormap [["100", "20", "200", "255"]] would map pixels
                                         whose value is 0 in the original band, to the rgba color vector at colormap[0].
                                         Less than 2^nbits 4-tuples may be provided, and omitted values
                                         will default map to black.

        :param str data_description: Description of band data.
        :param list(float) physical_range: If band represents a physical value e.g reflectance
                                           or radiance what are the possible range of those values.
        :param str data_unit: Units of the physical range e.g w/sr for radiance
        :param str data_unit_description: Description of the data unit.
        :param list(int) default_range: A good default scale for the band to use
                                        when rastering for display purposes.
        :param str description: Description of the band.
        :param str name_common: Standard name for band
        :param str name_vendor: What the vendor calls the band eg. B1
        :param int vendor_order: The index of the band in the vendors band tables.
                                 Useful for referencing the band to other processing
                                 properties like surface reflectance.
        :param int nodata:
        :param str processing_level: Description of how the band was processed if at all.
        :param int res_factor: Scaling of this band relative to the native band resolution.
        :param int resolution: Resolution of this band.
        :param str resolution_unit: Unit of the resolution.
        :param float wavelength_center: Center position of wavelength.
        :param float wavelength_fwhm: Full width at half maximum value of the wavelength spread.
        :param float wavelength_min: Minimum wavelength this band is sensitive to.
        :param float wavelength_max: Maximum wavelength this band is sensitive to.
        :param str wavelength_unit: Units the wavelength is expressed in.

        :return: JSON API representation of the band.
        :rtype: dict
        """
        if add_namespace:
            product_id = self.namespace_product(product_id)
        r = self.session.patch('/products/{}/bands/{}'.format(product_id, name), json=kwargs)
        return r.json()

    def remove_band(self, product_id, name, add_namespace=False):
        if add_namespace:
            product_id = self.namespace_product(product_id)
        self.session.delete('/products/{}/bands/{}'.format(product_id, name))

    def get_image(self, product_id, image_id, add_namespace=False):
        """Get a single image metadata entry.

        :return: JSON API representation of the image.
        :rtype: dict
        """

        if add_namespace:
            product_id = self.namespace_product(product_id)

        r = self.session.get('/products/{}/images/{}'.format(product_id, image_id))
        return r.json()

    def add_image(self, product_id, image_id, add_namespace=False, **kwargs):
        """Add an image metadata entry to a product.

        :param str product_id: (Required) Product to which this image belongs.
        :param str image_id: (Required) New image's id = <product_id>:<image_id>.
        :param bool add_namespace: Add your user namespace to the product_id. *Deprecated*
        :param list(str) read: A list of groups, or user hashes to give read access to.
                                     Defaults to the same as the parent product if not specified.
        :param int absolute_orbit: Orbit number since mission start.
        :param str acquired: Date the imagery was acquired
        :param str archived: Date the imagery was archived.
        :param float area: Surface area the image covers
        :param float azimuth_angle:
        :param float azimuth_angle_1:
        :param list(float) bits_per_pixel: Average bits of data per pixel per band.
        :param float bright_fraction: Fraction of the image that has reflectance greater than .4 in the blue band.
        :param str bucket: Name of Google Cloud Bucket. Must be public bucket or Descartes Labs user bucket.
        :param str catalog_id:
        :param float cirrus_fraction: Fraction of pixel which are distorted by cirrus clouds.
        :param float cloud_fraction: Fraction of pixels which are obscured by clouds.
        :param float cloud_fraction_0: Fraction of pixels which are obscured by clouds.
        :param str cs_code: Spatial coordinate system code eg. `EPSG:4326`
        :param datastrip_id: ID of the data strip this image was taken in.
        :param float degraded_fraction_0:
        :param str descartes_version:
        :param str directory: Subdirectory location.
        :param float duration: duration of the scan in seconds
        :param int duration_days:
        :param float earth_sun_distance: Earth sun distance at time of image capture.
        :param list(str) files: Names of the files this image is stored in.
        :param list(str) file_md5s: File integrity checksums.
        :param list(int) file_sizes: Number of bytes of each file
        :param float fill_fraction: Fraction of this image which has data.
        :param float geolocation_accuracy:
        :param str|dict geometry: GeoJSON representation of the image coverage.
        :param float geotrans: Geographic Translator values.
        :param float gpt_time:
        :param str identifier: Vendor scene identifier.
        :param int ifg_tdelta_days:
        :param float incidence_angle: Sensor incidence angle.
        :param str pass: On which pass was this image taken.
        :param str processed: Date which this image was processed.
        :param str proj4: proj.4 transformation parameters
        :param str projcs: Projection coordinate system.
        :param str published: Date the image was published.
        :param str radiometric_version:
        :param list(int) raster_size: Dimensions of image in pixels in (width, height).
        :param list(float) reflectance_scale: Scale factors converting TOA radiances to TOA reflectances
        :param list(float) reflectance_scale_1:
        :param int relative_orbit: Orbit number in revisit cycle.
        :param float roll_angle:
        :param str safe_id: Standard Archive Format for Europe.
        :param str sat_id: Satellite identifier.
        :param float scan_gap_interpolation:
        :param float solar_azimuth_angle:
        :param float solar_azimuth_angle_0:
        :param float solar_azimuth_angle_1:
        :param float solar_elevation_angle:
        :param float solar_elevation_angle_0:
        :param float solar_elevation_angle_1:
        :param str tile_id:
        :param float view_angle:
        :param float view_angle_1:
        :param dict extra_properties: User defined custom properties for this image.
                Only 10 keys are allowed. The dict can only map strings to primitive types (str -> str|float|int).

        :return: JSON API representation of the image.
        :rtype: dict
        """
        check_deprecated_kwargs(kwargs, {
            "bpp": "bits_per_pixel",
            "key": "image_id",
        })
        if add_namespace:
            product_id = self.namespace_product(product_id)
        kwargs['id'] = image_id
        r = self.session.post('/products/{}/images'.format(product_id), json=kwargs)
        return r.json()

    def replace_image(self, product_id, image_id, add_namespace=False, **kwargs):
        """Replace image metadata with a new version.

        :param str product_id: (Required) Product to which this image belongs.
        :param str image_id: (Required) ID of the image to replace.
        :param bool add_namespace: Add your user namespace to the product_id. *Deprecated*
        :param list(str) read: A list of groups, or user hashes to give read access to.
                                     Defaults to the same as the parent product if not specified.
        :param int absolute_orbit: Orbit number since mission start.
        :param str acquired: Date the imagery was acquired
        :param str archived: Date the imagery was archived.
        :param float area: Surface area the image covers
        :param float azimuth_angle:
        :param float azimuth_angle_1:
        :param list(float) bits_per_pixel: Average bits of data per pixel.
        :param float bright_fraction: Fraction of the image that has reflectance greater than .4 in the blue band.
        :param str bucket: Name of Google Cloud Bucket. Must be public bucket or Descartes Labs user bucket.
        :param str catalog_id:
        :param float cirrus_fraction: Fraction of pixel which are distorted by cirrus clouds.
        :param float cloud_fraction: Fraction of pixels which are obscured by clouds.
        :param float cloud_fraction_0: Fraction of pixels which are obscured by clouds.
        :param str cs_code: Spatial coordinate system code eg. `EPSG:4326`
        :param datastrip_id: ID of the data strip this image was taken in.
        :param float degraded_fraction_0:
        :param str descartes_version:
        :param str directory: Subdirectory location.
        :param float duration:
        :param int duration_days:
        :param float earth_sun_distance: Earth sun distance at time of image capture.
        :param list(str) files: Names of the files this image is stored in.
        :param list(str) file_md5s: File integrity checksums.
        :param list(int) file_sizes: Number of bytes of each file
        :param float fill_fraction: Fraction of this image which has data.
        :param float geolocation_accuracy:
        :param str|dict geometry: GeoJSON representation of the image coverage.
        :param float geotrans: Geographic Translator values.
        :param float gpt_time:
        :param str identifier: Vendor scene identifier.
        :param int ifg_tdelta_days:
        :param float incidence_angle: Sensor incidence angle.
        :param str pass: On which pass was this image taken.
        :param str processed: Date which this image was processed.
        :param str proj4: proj.4 transformation parameters
        :param str projcs: Projection coordinate system.
        :param str published: Date the image was published.
        :param str radiometric_version:
        :param list(int) raster_size: Dimensions of image in pixels in (width, height).
        :param list(float) reflectance_scale: Scale factors converting TOA radiances to TOA reflectances
        :param list(float) reflectance_scale_1:
        :param int relative_orbit: Orbit number in revisit cycle.
        :param float roll_angle:
        :param str safe_id: Standard Archive Format for Europe.
        :param str sat_id: Satellite identifier.
        :param float scan_gap_interpolation:
        :param float solar_azimuth_angle:
        :param float solar_azimuth_angle_0:
        :param float solar_azimuth_angle_1:
        :param float solar_elevation_angle:
        :param float solar_elevation_angle_0:
        :param float solar_elevation_angle_1:
        :param str tile_id:
        :param float view_angle:
        :param float view_angle_1:

        :return: JSON API representation of the image.
        :rtype: dict
        """
        check_deprecated_kwargs(kwargs, {"bpp": "bits_per_pixel"})
        if add_namespace:
            product_id = self.namespace_product(product_id)

        r = self.session.put('/products/{}/images/{}'.format(product_id, image_id), json=kwargs)
        return r.json()

    def _add_core_image(self, product_id, image_id, **kwargs):
        check_deprecated_kwargs(kwargs, {"bpp": "bits_per_pixel"})
        kwargs['id'] = image_id
        r = self.session.post('/core/products/{}/images'.format(product_id), json=kwargs)
        return r.json()

    def change_image(self, product_id, image_id, add_namespace=False, **kwargs):
        """Add an image metadata entry to a product.

        :param str product_id: (Required) Product to which this image belongs.
        :param str image_id: (Required) ID of the image to modify.
        :param bool add_namespace: Add your user namespace to the product_id. *Deprecated*
        :param list(str) read: A list of groups, or user hashes to give read access to.
        :param int absolute_orbit: Orbit number since mission start.
        :param str acquired: Date the imagery was acquired
        :param str archived: Date the imagery was archived.
        :param float area: Surface area the image covers
        :param float azimuth_angle:
        :param float azimuth_angle_1:
        :param list(float) bits_per_pixel: Average bits of data per pixel.
        :param float bright_fraction: Fraction of the image that has reflectance greater than .4 in the blue band.
        :param str bucket: Name of Google Cloud Bucket. Must be public bucket or Descartes Labs user bucket.
        :param str catalog_id:
        :param float cirrus_fraction: Fraction of pixel which are distorted by cirrus clouds.
        :param float cloud_fraction: Fraction of pixels which are obscured by clouds.
        :param float cloud_fraction_0: Fraction of pixels which are obscured by clouds.
        :param str cs_code: Spatial coordinate system code eg. `EPSG:4326`
        :param datastrip_id: ID of the data strip this image was taken in.
        :param float degraded_fraction_0:
        :param str descartes_version:
        :param str directory: Subdirectory location.
        :param float duration:
        :param int duration_days:
        :param float earth_sun_distance: Earth sun distance at time of image capture.
        :param list(str) files: Names of the files this image is stored in.
        :param list(str) file_md5s: File integrity checksums.
        :param list(int) file_sizes: Number of bytes of each file
        :param float fill_fraction: Fraction of this image which has data.
        :param float geolocation_accuracy:
        :param str|dict geometry: GeoJSON representation of the image coverage.
        :param float geotrans: Geographic Translator values.
        :param float gpt_time:
        :param str identifier: Vendor scene identifier.
        :param int ifg_tdelta_days:
        :param float incidence_angle: Sensor incidence angle.
        :param str pass: On which pass was this image taken.
        :param str processed: Date which this image was processed.
        :param str proj4: proj.4 transformation parameters
        :param str projcs: Projection coordinate system.
        :param str published: Date the image was published.
        :param str radiometric_version:
        :param list(int) raster_size: Dimensions of image in pixels (width, height).
        :param list(float) reflectance_scale: Scale factors converting TOA radiances to TOA reflectances
        :param list(float) reflectance_scale_1:
        :param int relative_orbit: Orbit number in revisit cycle.
        :param float roll_angle:
        :param str safe_id: Standard Archive Format for Europe.
        :param str sat_id: Satellite identifier.
        :param float scan_gap_interpolation:
        :param float solar_azimuth_angle:
        :param float solar_azimuth_angle_0:
        :param float solar_azimuth_angle_1:
        :param float solar_elevation_angle:
        :param float solar_elevation_angle_0:
        :param float solar_elevation_angle_1:
        :param str tile_id:
        :param float view_angle:
        :param float view_angle_1:

        :return: JSON API representation of the image.
        :rtype: dict
        """
        check_deprecated_kwargs(kwargs, {"bpp": "bits_per_pixel"})
        if add_namespace:
            product_id = self.namespace_product(product_id)
        r = self.session.patch('/products/{}/images/{}'.format(product_id, image_id), json=kwargs)
        return r.json()

    def remove_image(self, product_id, image_id, add_namespace=False):
        if add_namespace:
            product_id = self.namespace_product(product_id)
        self.session.delete('/products/{}/images/{}'.format(product_id, image_id))

    def upload_image(self, files, product_id, metadata=None, multi=False, image_id=None, **kwargs):
        """Upload an image for a product you own.

        :param str|file|list(str)|list(file) files: (Required) a reference to the file to upload.
        :param str product_id: (Required) The id of the product this image belongs to.
        :param dict metadata: Image metadata to use instead of the computed default values.
        :param \**kwargs: All image metadata can also be passed as kwargs,
            see :meth:`Catalog.add_image` for allowed fields.
        """

        if metadata is None:
            metadata = {}
        metadata.update(kwargs)
        check_deprecated_kwargs(metadata, {"bpp": "bits_per_pixel"})
        if multi is True:
            if not hasattr(files, '__iter__'):
                raise ValueError("Using `multi=True` requires `files` to be iterable")
            elif image_id is None:
                raise ValueError("Using `multi=True` requires `image_id` to be specified")
            else:
                upload = self._do_multi_file_upload(files, product_id, image_id, metadata)
        else:
            upload = self._do_upload(files, product_id, metadata=metadata)
        if upload[0]:
            raise upload[2]

    def upload_ndarray(
            self,
            ndarray,
            product_id,
            image_id,
            proj4=None,
            wkt_srs=None,
            geotrans=None,
            raster_meta=None,
            overviews=None,
            overview_resampler=None,
            **kwargs
    ):
        """Upload an ndarray with georeferencing information.

        :param ndarray ndarray: (Required) A numpy ndarray with image data. If you are providing a multi-band image
            it should have 3 dimensions and the 3rd dimension of the array should index the bands. The dtype of the
            ndarray must also be one of the following:
            ['uint8', 'int8', 'uint16', 'int16', 'uint32', 'int32', 'float32', 'float64']
        :param str product_id: (Required) The id of the product this image belongs to.
        :param str image_id: (Required) Resulting image's id = <product_id>:<image_id>.
        :param str proj4: (One of proj4 or wkt_srs is required) A proj4 formatted string representing the
            spatial reference system used by the image.
        :param str wkt_srs: (One of proj4 or wkt_srs is required) A well known text string representing the
            spatial reference system used by the image.
        :param list(float) geotrans: (Required) The 6 number geographic transform of the image. Maps pixel coordinates
            to coordinates in the specified spatial reference system.
        :param dict raster_meta: Metadata returned from the :meth:`descarteslabs.client.services.raster.Raster.ndarray`
            request which generated the initial data for the :param ndarray: being uploaded. Passing :param geotrans:
            and :param wkt_srs: is unnecessary in this case.
        :param list(int) overviews: a list of overview resolution magnification factors i.e [2, 4] would make two
            overviews at 2x and 4x the native resolution. Maximum number of overviews allowed is 16.
        :param str overview_resampler: Resampler algorithm to use when building overviews. Controls how pixels are
            combined to make lower res pixels in overviews. Allowed resampler algorithms are:
            ['nearest', 'average', 'gauss', 'cubic', 'cubicspline', 'lanczos', 'average_mp',
            'average_magphase', 'mode'].
        :param \**kwargs: Metadata for the new image; see :meth:`Catalog.add_image` for allowed fields.

        .. note:: Only one of `proj4` or `wkt_srs` can be provided.
        """
        if ndarray.dtype.name not in self.UPLOAD_NDARRAY_SUPPORTED_DTYPES:
            raise TypeError("{} is not in supported dtypes {}".format(ndarray.dtype.name,
                                                                      self.UPLOAD_NDARRAY_SUPPORTED_DTYPES))

        metadata = kwargs
        metadata.setdefault('process_controls', {}).update({'upload_type': 'ndarray'})
        if raster_meta is not None:
            geotrans = raster_meta.get('geoTransform')
            wkt_srs = raster_meta.get('coordinateSystem', {}).get('wkt')
        for arg in ['image_id', 'proj4', 'wkt_srs', 'geotrans']:
            if locals()[arg] is not None:
                kwargs[arg] = locals()[arg]
        for arg in ['overviews', 'overview_resampler']:
            if locals()[arg] is not None:
                metadata['process_controls'][arg] = locals()[arg]
        with NamedTemporaryFile() as tmp:
            np.save(tmp, ndarray, allow_pickle=False)
            # From tempfile docs:
            # Whether the name can be used to open the file a second time, while
            # the named temporary file is still open, varies across platforms
            # (it can be so used on Unix; it cannot on Windows NT or later)
            #
            # We close the underlying file object so _do_upload can open the path again
            # in a cross platform compatible way.
            # When leaving the context manager the tempfile wrapper will still cleanup
            # and unlink the file descriptor.
            tmp.file.close()
            upload = self._do_upload(tmp.name, product_id, metadata=metadata)
            if upload[0]:
                raise upload[2]

    def upload_results(
            self,
            product_id,
            limit=100,
            offset=None,
            status=None,
            updated=None,
            created=None,
            continuation_token=None,
    ):
        """Get result information for debugging your uploads.

        :param str product_id: Product ID to get upload results for.
        :param int limit: Number of results to get, useful for paging.
        :param int offset: Start of results to get, useful for paging.
        :param str status: Filter results by status, values are ["SUCCESS", "FAILURE"]
        :param str|int updated: Unix timestamp or ISO8601 formatted date for filtering results updated after this time.
        :param str|int created: Unix timestamp or ISO8601 formatted date for filtering results created after this time.

        :return: A list of upload result objects.
        :rtype: list
        """
        kwargs = {'limit': limit}
        for arg in ['offset', 'status', 'updated', 'created', 'continuation_token']:
            if locals()[arg] is not None:
                kwargs[arg] = locals()[arg]
        results = self.session.post(
            '/products/{}/uploads'.format(product_id),
            json=kwargs
        )
        return results.json()

    def iter_upload_results(
            self,
            product_id,
            status=None,
            updated=None,
            created=None,
    ):
        """Get result information for debugging your uploads.

        :param str product_id: Product ID to get upload results for.
        :param str status: Filter results by status, values are ["SUCCESS", "FAILURE"]
        :param str|int updated: Unix timestamp or ISO8601 formatted date for filtering results updated after this time.
        :param str|int created: Unix timestamp or ISO8601 formatted date for filtering results created after this time.

        :return: iterator to upload results.
        :rtype: generator
        """
        continuation_token = None
        kwargs = {}
        for arg in ['status', 'updated', 'created']:
            if locals()[arg] is not None:
                kwargs[arg] = locals()[arg]
        while True:
            page = self.upload_results(product_id, continuation_token=continuation_token, **kwargs)
            for res in page['data']:
                yield res
            continuation_token = page['meta']['continuation_token']
            if continuation_token is None:
                break

    def upload_result(self, product_id, upload_id):
        """Get one upload result with the processing logs.

        This is useful for debugging failed uploads.
        :param str product_id: Product ID to get upload result for.
        :param str upload_id: ID of specific upload to get a result record of, includes the run logs.

        :return: One upload result with run logs.
        :rtype: dict
        """
        result = self.session.get('/products/{}/uploads/{}'.format(product_id, upload_id))
        return result.json()

    def _do_multi_file_upload(self, files, product_id, image_id, metadata):
        file_keys = [os.path.basename(_f) for _f in files]
        process_controls = metadata.setdefault('process_controls', {'upload_type': 'file'})
        multi_file_args = {
            'multi_file': {
                'image_files': file_keys,
                'image_id': image_id,
            }
        }
        process_controls.update(multi_file_args)
        for _file in files:
            upload = self._do_upload(_file, product_id, metadata=metadata)
            if upload[0]:
                return upload
        else:
            return upload

    def _do_upload(self, file_ish, product_id, metadata=None):
        # kwargs are treated as metadata fields and restricted to primitives
        # for the key val pairs.
        fd = None
        product_id = self.namespace_product(product_id)

        if metadata is None:
            metadata = {}
        metadata.setdefault('process_controls', {'upload_type': 'file'})
        check_deprecated_kwargs(metadata, {"bpp": "bits_per_pixel"})

        if isinstance(file_ish, io.IOBase):
            if 'b' not in file_ish.mode:
                file_ish = io.open(file_ish.name, 'rb')
            fd = file_ish
        elif isinstance(file_ish, six.string_types) and os.path.exists(file_ish):
            fd = io.open(file_ish, 'rb')
        else:
            return 1, file_ish, Exception(
                'Could not handle file: `{}` pass a valid path or open IOBase instance'.format(file_ish)
            )
        try:
            r = self.session.post(
                '/products/{}/images/upload/{}'.format(
                    product_id,
                    metadata.pop('image_id', None) or os.path.basename(fd.name)
                ),
                json=metadata
            )
            upload_url = r.text
            r = self._gcs_upload_service.session.put(upload_url, data=fd)
        except (ServerError, RequestException) as e:
            return 1, fd.name, e
        finally:
            fd.close()

        return 0, fd.name, ''


catalog = Catalog()
