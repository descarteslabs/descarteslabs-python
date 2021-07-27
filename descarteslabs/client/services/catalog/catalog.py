import os
import six
import io

from tempfile import NamedTemporaryFile
from requests.exceptions import RequestException

from descarteslabs.client.addons import numpy as np
from descarteslabs.client.auth import Auth
from descarteslabs.client.deprecation import check_deprecated_kwargs
from descarteslabs.client.exceptions import ServerError, NotFoundError
from descarteslabs.client.services.metadata import Metadata
from descarteslabs.client.services.service import Service, ThirdPartyService


class Catalog(Service):
    """The Descartes Labs (DL) Catalog allows you to add georeferenced raster products
    into the Descartes Labs Platform. Catalog products can be used in other DL services
    like Raster and Tasks and Metadata.

    The entrypoint for using catalog is creating a Product using :meth:`add_product`.
    After creating a product you should add band(s) to it using :meth:`add_band`, and
    upload imagery using :meth:`upload_image`.

    Bands define where and how data is encoded into imagery files, and how it should be displayed.
    Images define metadata about a specific groups of pixels (in the form of images), their georeferencing, geometry,
    coordinate system, and other pertinent information.
    """

    UPLOAD_NDARRAY_SUPPORTED_DTYPES = [
        "uint8",
        "int8",
        "uint16",
        "int16",
        "uint32",
        "int32",
        "float32",
        "float64",
    ]
    TIMEOUT = (9.5, 30)

    def __init__(self, url=None, auth=None, metadata=None, retries=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.

        :param str url: A HTTP URL pointing to a version of the storage service
            (defaults to current version)
        :param Auth auth: A custom user authentication (defaults to the user
            authenticated locally by token information on disk or by environment
            variables)
        :param Metadata metadata: A custom metadata client to use
        :param urllib3.util.retry.Retry retries: A custom retry configuration
            used for all API requests (defaults to a reasonable amount of retries)
        """
        if auth is None:
            auth = Auth()

        if metadata is None:
            self._metadata = Metadata(auth=auth, retries=retries)
        else:
            self._metadata = metadata

        self._gcs_upload_service = ThirdPartyService()

        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_CATALOG_URL",
                "https://platform.descarteslabs.com/metadata/v1/catalog",
            )

        super(Catalog, self).__init__(url, auth=auth)

    def namespace_product(self, product_id):
        """
        Prefix your user namespace to a product id if it isn't already there.

        :param str product_id: (Required) Id to prefix.

        :rtype: str
        :return: Namespace prefixed product id.
        """
        namespace = self.auth.namespace

        if not product_id.startswith(namespace):
            product_id = "{}:{}".format(namespace, product_id)

        return product_id

    def own_products(self):
        """
        Gets products owned by you.

        :rtype: list[DotDict]
        :return: A list of product ``DotDicts``, which may have the following keys:

            .. highlight:: none

            ::

                id:               The ID of the product.
                description:      Information about the product
                                  why it exists, and what it provides.
                title:            Official product title.
                modified:         Time that the product was modified
                                  in ISO-8601 UTC.
                tags:             A list of searchable tags.
                owner_type:       One of ["user", "core"].  "core"
                                  products are owned by Descartes
                                  Labs, while "user" products are
                                  owned by individual users.
                owners:           A list of groups, organizations
                                  or users who own the product.
                                  Access Control List identifiers
                                  can have the following formats:
                                  organizations, e.g. org:orgname.
                                  groups, e.g. group:groupname.
                                  user email, e.g. email:user@company.com.
                read:             A list of groups, organizations
                                  or users having read access.
                                  Access Control List identifiers
                                  can have the following formats:
                                  organizations, e.g. org:orgname.
                                  groups, e.g. group:groupname.
                                  user email, e.g.  email:user@company.com.
                writers:          A list of groups, organizations
                                  or users having write access.
                                  Access Control List identifiers
                                  can have the following formats:
                                  organizations, e.g. org:orgname.
                                  groups, e.g. group:groupname.
                                  user email, e.g.  email:user@company.com.
                end_date:         End date of the product data,
                                  None means open interval.
                native_bands:     A list of the names of the native
                                  bands of this product (most
                                  applicable to satellite sensors).
                notes:            Any notes to relay about the product.
                orbit:            Type of orbit (satellite only).
                processing_level: Way in which raw data has been
                                  processed if any.
                resolution:       Pixel density of the data, provide units.
                revisit:          How often an AOI can expect updated data.
                sensor:           Name of the sensor used.
                spectral_bands:   Number of spectral bands the product has.
                start_date:       Start date of the product data.
                swath:            How large an area the sensor
                                  captures at a given time.

        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        return self._metadata.products(owner=self.auth.payload["sub"])

    def own_bands(self):
        """
        Get bands owned by you.

        :rtype: list[DotDict]
        :return: A list of band ``DotDicts`` which may have the following keys:

            .. highlight:: none

            ::

                color:                 The color interpretation of this
                                       band.  One of ["Alpha", "Black",
                                       "Blue", "Cyan", "Gray", "Green",
                                       "Hue", "Ligntness", "Magenta",
                                       "Palette", "Red", "Saturation",
                                       "Undefined", "YCbCr_Cb",
                                       "YCbCr_Cr", "YCbCr_Y", "Yellow"].

                                       "Alpha" if "type" is "mask".
                                       "Palette" if "colormap_name" or
                                       "colormap" is set.
                colormap:              A custom colormap for this band.
                                       A list of lists, where each nested
                                       list is a 4-tuple of rgba values
                                       to map pixels whose value is the
                                       index of the tuple. e.g. the
                                       colormap [[100, 20, 200, 255]]
                                       would map pixels whose value is 0 in
                                       the original band, to the rgba color
                                       vector at colormap[0].  Less than
                                       2^nbits 4-tuples may be provided,
                                       and omitted values will default
                                       map to black.
                colormap_name:         A named colormap for this band,
                                       one of ["plasma", "magma",
                                       "viridis", "msw", "inferno"].
                data_description:      Description of band data.
                data_range:            A list specifying the min and
                                       max values for the data in this band.
                data_unit:             Units of the physical range e.g
                                       "w/sr" for radiance.
                data_unit_description: Description of the data unit.
                default_range:         A default scale for the
                                       band to use when rastering
                                       for display purposes.
                description:           Description of the band.
                dtype:                 The data type used to store
                                       this band e.g Byte or
                                       Uint16 or Float32.
                id:                    The ID of the band.
                jpx_layer:             If data is processed to JPEG2000,
                                       which layer is the band in. 0
                                       for other formats.
                modified:              Time that the band was modified
                                       in ISO-8601 UTC.
                name:                  Name of the band.
                name_common:           Standard name for band.
                name_vendor:           What the vendor calls the band
                                       e.g. B1.
                nbits:                 The number of bits of the dtype
                                       used to store this band.
                nodata:                Pixel value indicating no data
                                       available.
                owner_type:            One of ["user", "core"].  "core"
                                       bands are owned by Descartes Labs,
                                       while "user" bands are owned by
                                       individual users.
                owners:                A list of groups, organizations
                                       or users who own the product.
                                       Access Control List identifiers
                                       can have the following formats:
                                       organizations, e.g. org:orgname.
                                       groups, e.g. group:groupname.
                                       user email, e.g. email:user@company.com.
                physical_range:        If band represents a physical
                                       value e.g reflectance or
                                       radiance what are the possible
                                       range of those values.
                processing_level:      Description of how the band was
                                       processed if at all.
                product:               ID of the product the band
                                       belongs to.
                read:                  A list of groups, organizations
                                       or users having read access.
                                       Access Control List identifiers
                                       can have the following formats:
                                       organizations, e.g. org:orgname.
                                       groups, e.g. group:groupname.
                                       user email, e.g. email:user@company.com.
                res_factor:            Scaling of this band relative
                                       to the native band resolution.
                resolution:            Resolution of this band.
                resolution_unit:       Unit of the resolution. One
                                       of ["meters", "degrees"].
                src_band:              The 1-based index of the band
                                       in the "jpx_layer" specified.
                src_file:              If the product was processed
                                       into multiple files, which one
                                       is this band in.
                tags:                  A list of searchable tags.
                type:                  The data interpretation of
                                       the band. One of ["spectral",
                                       "derived", "mask", "class"].
                vendor_order:          The index of the band in the
                                       vendor's band tables. Useful
                                       for referencing the band to
                                       other processing properties
                                       like surface reflectance.
                wavelength_center:     Center position of wavelength.
                wavelength_fwhm:       Full width at half maximum value
                                       of the wavelength spread.
                wavelength_max:        Maximum wavelength this band
                                       is sensitive to.
                wavelength_min:        Minimum wavelength this band
                                       is sensitive to.
                wavelength_unit:       Must be "nm" if any wavelength
                                       params defined. Otherwise None.
                writers:               A list of groups, organizations
                                       or users having write access.
                                       Access Control List identifiers
                                       can have the following formats:
                                       organizations, e.g. org:orgname.
                                       groups, e.g. group:groupname.
                                       user email, e.g. email:user@company.com.

        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        return self._metadata.bands(owner=self.auth.payload["sub"])

    def _add_core_product(self, product_id, **kwargs):
        kwargs["id"] = product_id
        check_deprecated_kwargs(
            kwargs, {"start_time": "start_datetime", "end_time": "end_datetime"}
        )
        r = self.session.post("/core/products", json=kwargs)
        return r.json()

    def get_product(self, product_id, add_namespace=False):
        """
        Get a product.

        :param str product_id: ID of the product to get.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        :rtype: dict
        :return: A single product as a JSON API resource object. The keys are:

            .. highlight:: none

            ::

                data: A dict with the following keys:

                    id:   The ID of the product.
                    type: "product".
                    meta: A dict with the following keys:

                        modified:   Time that the product
                                    was modified in ISO-8601 UTC.
                        owner_type: One of ["user", "core"].
                                    "core" products are owned by
                                    Descartes Labs, while "user"
                                    products are owned by
                                    individual users.

                    attributes: A dict which may contain
                                the following keys:

                        description:      Information about the
                                          product why it exists,
                                          and what it provides.
                        end_date:         End date of the product
                                          data, None means open interval.
                        native_bands:     A list of the names of
                                          the native bands of this
                                          product (most applicable to
                                          satellite sensors).
                        notes:            Any notes to relay about
                                          the product.
                        orbit:            Type of orbit (satellite only).
                        owners:           A list of groups, organizations
                                          or users who own the product.
                                          Access Control List identifiers
                                          can have the following formats:
                                          organizations, e.g. org:orgname.
                                          groups, e.g. group:groupname.
                                          user email, e.g.  email:user@company.com.
                        processing_level: Way in which raw data has been
                                          processed if any.
                        read:             A list of groups, organizations
                                          or users having read access.
                                          Access Control List identifiers
                                          can have the following formats:
                                          organizations, e.g. org:orgname.
                                          groups, e.g. group:groupname.
                                          user email, e.g.  email:user@company.com.
                        resolution:       Pixel density of the data, provide units.
                        revisit:          How often an AOI can expect updated data.
                        sensor:           Name of the sensor used.
                        spectral_bands:   Number of spectral bands the product has.
                        start_date:       Start date of the product data.
                        swath:            How large an area the sensor captures
                                          at a given time.
                        tags:             A list of searchable tags.
                        title:            Official product title.
                        writers:          A list of groups, organizations
                                          or users having write access.
                                          Access Control List identifiers
                                          can have the following formats:
                                          organizations, e.g. org:orgname.
                                          groups, e.g. group:groupname.
                                          user email, e.g.  email:user@company.com.

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            product cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)

        r = self.session.get("/products/{}".format(product_id))
        return r.json()

    def add_product(
        self, product_id, title, description, add_namespace=False, **kwargs
    ):
        """Add a product to your catalog.

        :param str product_id: (Required) A unique name for this product. In the created
            product a namespace consisting of your user id (e.g.
            "ae60fc891312ggadc94ade8062213b0063335a3c:") or your organization id (e.g.,
            "yourcompany:") will be prefixed to this, if it doesn't already have one, in
            order to make the id globally unique.
        :param str title: (Required) Official product title.
        :param str description: (Required) Information about the product,
                                why it exists, and what it provides.

        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.
        :param list(str) read: A list of groups, or user hashes to give read access to.
        :param int spectral_bands: Number of spectral bands the product has.
        :param list(str) native_bands: A list of the names of the native bands of this product
                                        (most applicable to satellite sensors).
        :param str start_datetime: ISO-8601 compliant date string, indicating start of product data.
        :param str end_datetime: ISO-8601 compliant date string, indicating end of product data.
        :param str notes: Any notes to relay about the product.
        :param str orbit: Type of orbit (satellite only).
        :param str processing_level: Way in which raw data has been processed if any.
        :param str resolution: Pixel density of the data, provide units.
        :param str revisit: How often an AOI can expect updated data.
        :param str sensor: Name of the sensor used.
        :param str swath: How large an area the sensor captures at a given time.
        :param list(str) writers: A list of groups, or user hashes to give read access to.

        :rtype: dict
        :return: JSON API representation of the product. See :meth:`get_product`
            for information about returned keys.

        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the owners list is missing prefixes.
        :raises ~descarteslabs.client.exceptions.ConflictError: Raised when
            a product with the specified ID already exists.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        for k, v in locals().items():
            if k in ["title", "description"]:
                if v is None:
                    raise TypeError("required arg `{}` not provided".format(k))
                kwargs[k] = v
        check_deprecated_kwargs(
            kwargs, {"start_time": "start_datetime", "end_time": "end_datetime"}
        )

        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            kwargs["id"] = self.namespace_product(product_id)
        else:
            kwargs["id"] = product_id

        r = self.session.post("/products", json=kwargs)
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

        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.
        :param list(str) read: A list of groups, or user hashes to give read access to.
        :param int spectral_bands: Number of spectral bands the product has.
        :param list(str) native_bands: A list of the names of the native bands of this product
                                        (most applicable to satellite sensors).
        :param str start_datetime: ISO-8601 compliant date string, indicating start of product data.
        :param str end_datetime: ISO-8601 compliant date string, indicating end of product data.
        :param str notes: Any notes to relay about the product.
        :param str orbit: Type of orbit (satellite only).
        :param str processing_level: Way in which raw data has been processed if any.
        :param str resolution: Pixel density of the data, provide units.
        :param str revisit: How often an AOI can expect updated data.
        :param str sensor: Name of the sensor used.
        :param str swath: How large an area the sensor captures at a given time.
        :param list(str) writers: A list of groups, or user hashes to give read access to.

        :rtype: dict
        :return: JSON API representation of the product. See :meth:`get_product`
            for information about returned keys.

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            product cannot be found.
        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the owners list is missing prefixes.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        for k, v in locals().items():
            if k in ["title", "description"]:
                if v is None:
                    raise TypeError("required arg `{}` not provided".format(k))
                kwargs[k] = v
        check_deprecated_kwargs(
            kwargs, {"start_time": "start_datetime", "end_time": "end_datetime"}
        )

        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)

        params = None
        if set_global_permissions is True:
            params = {"set_global_permissions": "true"}

        r = self.session.put(
            "/products/{}".format(product_id), json=kwargs, params=params
        )
        return r.json()

    def change_product(
        self, product_id, add_namespace=False, set_global_permissions=False, **kwargs
    ):
        """Update a product in your catalog.

        :param str product_id: (Required) The ID of the product to change.
        :param list(str) read: A list of groups, or user hashes to give read access to.
        :param int spectral_bands: Number of spectral bands the product has.
        :param list(str) native_bands: A list of the names of the native bands of this product
                                        (most applicable to satellite sensors).
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.
        :param bool set_global_permissions: Update permissions of all existing bands and products
                                            that belong to this product with the updated permission set
                                            specified in the `read` param. Default to false.
        :param str start_datetime: ISO-8601 compliant date string, indicating start of product data.
        :param str end_datetime: ISO-8601 compliant date string, indicating end of product data.
        :param str title: Official product title.
        :param str description: Information about the product, why it exists, and what it provides.
        :param str notes: Any notes to relay about the product.
        :param str orbit: Type of orbit (satellite only).
        :param str processing_level: Way in which raw data has been processed if any.
        :param str resolution: Pixel density of the data, provide units.
        :param str revisit: How often an AOI can expect updated data.
        :param str sensor: Name of the sensor used.
        :param str swath: How large an area the sensor captures at a given time.
        :param list(str) writers: A list of groups, or user hashes to give read access to.

        :rtype: dict
        :return: JSON API representation of the product. See :meth:`get_product`
            for information about returned keys.

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            product cannot be found.
        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the owners list is missing prefixes.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        check_deprecated_kwargs(
            kwargs, {"start_time": "start_datetime", "end_time": "end_datetime"}
        )

        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)

        params = None
        if set_global_permissions is True:
            params = {"set_global_permissions": "true"}

        r = self.session.patch(
            "/products/{}".format(product_id), json=kwargs, params=params
        )
        return r.json()

    def remove_product(self, product_id, add_namespace=False, cascade=False):
        """Remove a product from the catalog.

        :param str product_id: ID of the product to remove.
        :param bool cascade: Force deletion of all the associated bands and images. BEWARE this cannot be undone.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        :rtype: dict or None
        :return: If called with ``cascade=False`` returns ``None``, otherwise returns a dict
            with the following keys:

            .. highlight:: none

            ::

                deletion_task: Identifier for the task
                               that is removing bands and images.

        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            ``cascade=False`` and there are dependant bands or images.
        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            product cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)
        params = {"cascade": cascade}
        r = self.session.delete("/products/{}".format(product_id), params=params)
        if r.headers["content-type"] == "application/json":
            return r.json()

    def product_deletion_status(self, deletion_task_id):
        """Get the status of a long running product deletion job.

        :param str deletion_task_id: deletion_task ID returned from a call to :meth:`remove_product`
            with ``cascade=True``.

        :rtype: dict
        :return: Information about product deletion progress as a dict
            with the following keys:

            .. highlight:: none

            ::

                completed: Boolean indicating whether
                           or not the task is completed.
                response:  Information about a completed
                           task. A dict with the following keys:

                    batches:                Number of batches the operation
                                            was broken into.
                    created:                Number of products, bands or
                                            images created, always 0.
                    deleted:                Number of products, bands or
                                            images deleted.
                    failures:               List of products, bands or
                                            images that could not be deleted.
                    noops:                  Number of products, bands or
                                            images ignored for deletion.
                    requests_per_second:    Number of deletion requests
                                            executed per second. -1.0 for
                                            completed tasks.
                    retries:                A dict with the following keys:

                        bulk:   Number of product, band or image deletion
                                requests retried.
                        search: Number of product, band or image search
                                requests retried.

                    throttled_millis:       Number of milliseconds the
                                            request slept to conform to
                                            "requests_per_second".
                    throttled_until_millis: Always 0.
                    timed_out:              Boolean indicating if operations
                                            executed timed out.
                    took:                   The number of milliseconds from
                                            start to end of the whole operation.
                    total:                  The number of products, bands and
                                            images processed.
                    updated:                The number of products, bands and
                                            images updated.
                    version_conflicts:      The number of products, bands and
                                            images that could not be updated
                                            due to multiple concurrent updates.

                task:      Information about a running task.
                           A dict with the following keys:

                    batches:                Number of batches the operation
                                            was broken into.
                    created:                Number of products, bands or
                                            images created, always 0.
                    deleted:                Number of products, bands or
                                            images deleted.
                    failures:               List of products, bands or
                                            images that could not be deleted.
                    noops:                  Number of products, bands or
                                            images ignored for deletion.
                    requests_per_second:    Number of deletion requests
                                            executed per second. -1.0 for
                                            completed tasks.
                    retries:                A dict with the following keys:

                        bulk:   Number of product, band or image deletion
                                requests retried.
                        search: Number of product, band or image search
                                requests retried.

                    throttled_millis:       Number of milliseconds the
                                            request slept to conform to
                                            "requests_per_second".
                    throttled_until_millis: Always 0.
                    timed_out:              Boolean indicating if operations
                                            executed timed out.
                    took:                   The number of milliseconds from
                                            start to end of the whole operation.
                    total:                  The number of products, bands and
                                            images processed.
                    updated:                The number of products, bands and
                                            images updated.
                    version_conflicts:      The number of products, bands and
                                            images that could not be updated
                                            due to multiple concurrent updates.

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            task cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        r = self.session.get("/products/deletion_tasks/{}".format(deletion_task_id))
        return r.json()

    def get_band(self, product_id, name, add_namespace=False):
        """Get a band by name.

        :param str product_id: ID of the product the band belongs to.
        :param str name: Name of the band.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        :rtype: dict
        :return: A single band as a JSON API resource object. The keys are:

            .. highlight:: none

            ::

                data: A dict with the following keys:

                    id: The ID of the band.
                    type: "band".
                    meta: A dict with the following keys:

                        modified:   Time that the band was modified
                                    in ISO-8601 UTC.
                        owner_type: One of ["user", "core"].  "core"
                                    bands are owned by Descartes
                                    Labs, while "user" bands are
                                    owned by individual users.

                    attributes: A dict which may contain the following keys:

                        color:                 The color interpretation of this
                                               band.  One of ["Alpha", "Black",
                                               "Blue", "Cyan", "Gray", "Green",
                                               "Hue", "Ligntness", "Magenta",
                                               "Palette", "Red", "Saturation",
                                               "Undefined", "YCbCr_Cb",
                                               "YCbCr_Cr", "YCbCr_Y", "Yellow"].

                                               "Alpha" if "type" is "mask".
                                               "Palette" if "colormap_name" or
                                               "colormap" is set.
                        colormap:              A custom colormap for this band.
                                               A list of lists, where each nested
                                               list is a 4-tuple of rgba values
                                               to map pixels whose value is the
                                               index of the tuple. e.g. the colormap
                                               [[100, 20, 200, 255]] would
                                               map pixels whose value is 0 in the
                                               original band, to the rgba color
                                               vector at colormap[0].  Less than
                                               2^nbits 4-tuples may be provided,
                                               and omitted values will default
                                               map to black.
                        colormap_name:         A named colormap for this band,
                                               one of ["plasma", "magma",
                                               "viridis", "msw", "inferno"]
                        data_description:      Description of band data.
                        data_range:            A list specifying the min and
                                               max values for the data in
                                               this band.
                        data_unit:             Units of the physical range e.g
                                               "w/sr" for radiance.
                        data_unit_description: Description of the data unit.
                        default_range:         A default scale for the band
                                               to use when rastering for
                                               display purposes.
                        description:           Description of the band.
                        dtype:                 The data type used to store this
                                               band e.g Byte or Uint16 or Float32.
                        jpx_layer:             If data is processed to JPEG2000,
                                               which layer is the band in. 0
                                               for other formats.
                        name:                  Name of the band.
                        name_common:           Standard name for band.
                        name_vendor:           What the vendor calls the band
                                               e.g. B1.
                        nbits:                 The number of bits of the dtype
                                               used to store this band.
                        nodata:                Pixel value indicating no data
                                               available.
                        owners:                A list of groups, organizations
                                               or users who own the product.
                                               Access Control List identifiers
                                               can have the following formats:
                                               organizations, e.g. org:orgname.
                                               groups, e.g. group:groupname.
                                               user email, e.g. email:user@company.com.
                        physical_range:        If band represents a physical
                                               value e.g reflectance or
                                               radiance what are the possible
                                               range of those values.
                        processing_level:      Description of how the band was
                                               processed if at all.
                        product:               ID of the product the band
                                               belongs to.
                        read:                  A list of groups, organizations
                                               or users having read access.
                                               Access Control List identifiers
                                               can have the following formats:
                                               organizations, e.g. org:orgname.
                                               groups, e.g. group:groupname.
                                               user email, e.g. email:user@company.com.
                        res_factor:            Scaling of this band relative to
                                               the native band resolution.
                        resolution:            Resolution of this band.
                        resolution_unit:       Unit of the resolution. One of
                                               ["meters", "degrees"].
                        src_band:              The 1-based index of the band
                                               in the "jpx_layer" specified.
                        src_file:              If the product was processed
                                               into multiple files, which one
                                               is this band in.
                        tags:                  A list of searchable tags.
                        type:                  The data interpretation of the
                                               band. One of ["spectral",
                                               "derived", "mask", "class"]
                        vendor_order:          The index of the band in the
                                               vendor's band tables. Useful
                                               for referencing the band to
                                               other processing properties
                                               like surface reflectance.
                        wavelength_center:     Center position of wavelength.
                        wavelength_fwhm:       Full width at half maximum value
                                               of the wavelength spread.
                        wavelength_max:        Maximum wavelength this band
                                               is sensitive to.
                        wavelength_min:        Minimum wavelength this band
                                               is sensitive to.
                        wavelength_unit:       Must be "nm" if any wavelength
                                               params defined. Otherwise None.
                        writers:               A list of groups, organizations
                                               or users having write access.
                                               Access Control List identifiers
                                               can have the following formats:
                                               organizations, e.g. org:orgname.
                                               groups, e.g. group:groupname.
                                               user email, e.g. email:user@company.com.

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            band cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)

        r = self.session.get("/products/{}/bands/{}".format(product_id, name))
        return r.json()

    def _add_core_band(
        self,
        product_id,
        name,
        srcband=None,
        dtype=None,
        data_range=None,
        type=None,
        **kwargs
    ):
        for k, v in locals().items():
            if k in ["name", "data_range", "dtype", "srcband", "type"]:
                if v is None:
                    raise TypeError("required arg `{}` not provided".format(k))
                kwargs[k] = v
        check_deprecated_kwargs(kwargs, {"nbits": None})
        kwargs["id"] = name
        r = self.session.post("/core/products/{}/bands".format(product_id), json=kwargs)
        return r.json()

    def add_band(
        self,
        product_id,
        name,
        add_namespace=False,
        srcband=None,
        dtype=None,
        data_range=None,
        type=None,
        **kwargs
    ):
        """Add a data band to an existing product.

        :param str product_id: (Required) Product to which this band will belong.
        :param str name: (Required) Name of this band.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.
        :param int jpx_layer: If data is processed to JPEG2000, which layer is the band in. Defaults to 0.
        :param int srcband: (Required) The 1-based index of the band in the jpx_layer specified.
        :param int srcfile: If the product was processed into multiple files,
                            which one is this in. Defaults to 0 (first file).
        :param str dtype: (Required) The data type used to store this band e.g Byte or Uint16 or Float32.
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
                                         of the tuple. e.g. the colormap [[100, 20, 200, 255]] would map pixels
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
        :param int vendor_order: The index of the band in the vendor's band tables.
                                 Useful for referencing the band to other processing
                                 properties like surface reflectance.
        :param str processing_level: Description of how the band was processed if at all.
        :param int res_factor: Scaling of this band relative to the native band resolution.
        :param int resolution: Resolution of this band.
        :param str resolution_unit: Unit of the resolution, must be either "meters" or "degrees".
                                    Required if `resolution` is specified.
        :param float wavelength_center: Center position of wavelength.
        :param float wavelength_fwhm: Full width at half maximum value of the wavelength spread.
        :param float wavelength_min: Minimum wavelength this band is sensitive to.
        :param float wavelength_max: Maximum wavelength this band is sensitive to.
        :param str wavelength_unit: Units the wavelength is expressed in, must be "nm" if provided.
        :param list(str) writers: A list of groups, or user hashes to give read access to.

        :rtype: dict
        :return: JSON API representation of the band. See :meth:`get_band`
            for information about returned keys.

        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the owners list is missing prefixes.
        :raises ~descarteslabs.client.exceptions.ConflictError: Raised when
            a band with the specified name already exists.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """

        for k, v in locals().items():
            if k in ["name", "data_range", "dtype", "srcband", "type"]:
                if v is None:
                    raise TypeError("required arg `{}` not provided".format(k))
                kwargs[k] = v
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)
        check_deprecated_kwargs(kwargs, {"id": "name", "nbits": None})

        r = self.session.post("/products/{}/bands".format(product_id), json=kwargs)
        return r.json()

    def replace_band(
        self,
        product_id,
        name,
        srcband=None,
        dtype=None,
        data_range=None,
        add_namespace=False,
        type=None,
        **kwargs
    ):
        """Replaces an existing data band with a new document.

        :param str product_id: (Required) Product to which this band will belong.
        :param str name: (Required) Name of this band.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        .. note::
            - See :meth:`add_band` for additional kwargs.

        :rtype: dict
        :return: JSON API representation of the band. See :meth:`get_band`
            for information about returned keys.

        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the owners list is missing prefixes.
        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            band cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """

        for k, v in locals().items():
            if k in ["data_range", "dtype", "srcband", "type"]:
                if v is None:
                    raise TypeError("required arg `{}` not provided".format(k))
                kwargs[k] = v
        check_deprecated_kwargs(kwargs, {"nbits": None})
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)

        r = self.session.put(
            "/products/{}/bands/{}".format(product_id, name), json=kwargs
        )
        return r.json()

    def change_band(self, product_id, name, add_namespace=False, **kwargs):
        """Update a data band of a product.

        :param str product_id: (Required) Product to which this band belongs.
        :param str name: Name or id of band to modify.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        .. note::
            - See :meth:`add_band` for additional kwargs.

        :rtype: dict
        :return: JSON API representation of the band. See :meth:`get_band`
            for information about returned keys.

        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the owners list is missing prefixes.
        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            band cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)
        r = self.session.patch(
            "/products/{}/bands/{}".format(product_id, name), json=kwargs
        )
        return r.json()

    def remove_band(self, product_id, name, add_namespace=False):
        """Remove a band from the catalog.

        :param str product_id: ID of the product to remove band from.
        :param str name: Name of the band to remove.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            band cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)
        self.session.delete("/products/{}/bands/{}".format(product_id, name))

    def get_image(self, product_id, image_id, add_namespace=False):
        """Get a single image catalog entry.

        :param str product_id: ID of the product the image belongs to.
        :param str image_id: ID of the image.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        :rtype: dict
        :return: A single image as a JSON API resource object. The keys are:

            .. highlight:: none

            ::

                 data: A dict with the following keys:

                    id: The ID of the image.
                    type: "image".
                    meta: A dict with the following keys:

                        modified:   Time that the image was modified
                                    in ISO-8601 UTC.
                        owner_type: One of ["user", "core"].  "core"
                                    images are owned by Descartes Labs,
                                    while "user" images are owned by
                                    individual users.

                    attributes: A dict which may contain the following keys:

                        absolute_orbit:         Orbit number since mission start.
                        acquired:               Date the imagery was acquired
                        archived:               Date the imagery was archived.
                        area:                   Surface area the image covers.
                        azimuth_angle:          Satellite azimuth angle in degrees.
                        bits_per_pixel:         Average bits of data per pixel
                                                per band.
                        bright_fraction:        Fraction of the image that has
                                                reflectance greater than .4 in
                                                the blue band.
                        bucket:                 Name of Google Cloud Bucket(s).
                                                May be a string or a list of strings
                                                of length equal to that of files.
                        cirrus_fraction:        Fraction of pixel which are
                                                distorted by cirrus clouds.
                        cloud_fraction:         Fraction of pixels which are
                                                obscured by clouds.  Calculated
                                                by Descartes Labs.
                        cloud_fraction_0:       Fraction of pixels which are
                                                obscured by clouds.  Calculated
                                                by image provider.
                        cs_code:                Spatial coordinate system code
                                                eg. "EPSG:4326"
                        datastrip_id:           ID of the data strip this image
                                                was taken in.
                        degraded_fraction_0:    Applicable only to Sentinel-2,
                                                DEGRADED_MSI_DATA_PERCENTAGE.
                        descartes_version:      Processing pipeline version number.
                        directory:              Subdirectory location. Optional,
                                                may be a string or a list of strings
                                                equal in length to that of files.
                        duration:               Duration of the scan in seconds.
                        earth_sun_distance:     Earth sun distance at time of
                                                image capture.
                        file_md5s:              File integrity checksums.
                        file_sizes:             Number of bytes of each file.
                        files:                  Names of the files this image
                                                is stored in.
                        fill_fraction:          Fraction of this image which has
                                                data.
                        geometry:               GeoJSON representation of the
                                                image coverage.
                        geotrans:               Geographic Translator values.
                        pass:                   On which pass was this image taken.
                        identifier:             Vendor scene identifier.
                        incidence_angle:        Sensor incidence angle.
                        owners:                 A list of groups, organizations
                                                or users who own the product.
                                                Access Control List identifiers
                                                can have the following formats:
                                                organizations, e.g. org:orgname.
                                                groups, e.g. group:groupname.
                                                user email, e.g. email:user@company.com.
                        product:                Product to which this image belongs.
                        processed:              Date which this image was processed.
                        proj4:                  proj.4 transformation parameters.
                        projcs:                 Projection coordinate system.
                        published:              Date the image was published.
                        raster_size:            Dimensions of image in pixels in
                                                (width, height).
                        read:                   A list of groups, organizations
                                                or users having read access.
                                                Access Control List identifiers
                                                can have the following formats:
                                                organizations, e.g. org:orgname.
                                                groups, e.g. group:groupname.
                                                user email, e.g. email:user@company.com.
                        reflectance_scale:      Scale factors converting TOA
                                                radiances to TOA reflectances
                        relative_orbit:         Orbit number in revisit cycle.
                        roll_angle:             Applicable only to Landsat 8,
                                                roll angle.
                        safe_id:                Standard Archive Format for Europe.
                        sat_id:                 Satellite identifier.
                        scan_gap_interpolation: Applicable only to Landsat-7,
                                                width of pixels interpolated for
                                                scan gaps.
                        storage_state:          A string indicating whether data
                                                for the image is stored on the
                                                Descartes Labs Platform. Allowed
                                                values are "available" and "remote".
                                                If "remote", entry may not include
                                                the fields bucket, directory,
                                                files, file_md5s, file_sizes.
                                                Default is "available".
                        writers:                A list of groups, organizations
                                                or users having write access.
                                                Access Control List identifiers
                                                can have the following formats:
                                                organizations, e.g. org:orgname.
                                                groups, e.g. group:groupname.
                                                user email, e.g. email:user@company.com.

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            image cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)

        r = self.session.get("/products/{}/images/{}".format(product_id, image_id))
        return r.json()

    def add_image(
        self,
        product_id,
        image_id,
        add_namespace=False,
        storage_state="available",
        **kwargs
    ):
        """Add an image metadata entry to a product.

        :param str product_id: (Required) Product to which this image belongs.
        :param str image_id: (Required) New image's id = <product_id>:<image_id>.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.
        :param list(str) read: A list of groups, or user hashes to give read access to.
                                     Defaults to the same as the parent product if not specified.
        :param int absolute_orbit: Orbit number since mission start.
        :param str acquired: Date the imagery was acquired
        :param str archived: Date the imagery was archived.
        :param float area: Surface area the image covers
        :param float azimuth_angle: Satellite azimuth angle in degrees.
        :param list(float) bits_per_pixel: Average bits of data per pixel per band.
        :param float bright_fraction: Fraction of the image that has reflectance greater than .4 in the blue band.
        :param list(str) bucket: Name of Google Cloud Bucket. Must be public bucket or Descartes Labs user bucket.
            May be a string or a list of strings equal in length to that of files.
        :param float cirrus_fraction: Fraction of pixel which are distorted by cirrus clouds.
        :param float cloud_fraction: Fraction of pixels which are obscured by clouds.
        :param float cloud_fraction_0: Fraction of pixels which are obscured by clouds.
        :param str cs_code: Spatial coordinate system code eg. `EPSG:4326`
        :param str datastrip_id: ID of the data strip this image was taken in.
        :param float degraded_fraction_0: Applicable only to Sentinel-2, DEGRADED_MSI_DATA_PERCENTAGE.
        :param str descartes_version: Processing pipeline version number.
        :param list(str) directory: Subdirectory location. Optional, may be a string or a list
            of strings equal in length to that of files.
        :param float duration: duration of the scan in seconds
        :param float earth_sun_distance: Earth sun distance at time of image capture.
        :param list(str) files: Names of the files this image is stored in.
        :param list(str) file_md5s: File integrity checksums.
        :param list(int) file_sizes: Number of bytes of each file
        :param float fill_fraction: Fraction of this image which has data.
        :param str or dict geometry: GeoJSON representation of the image coverage.
        :param float geotrans: Geographic Translator values.
        :param str identifier: Vendor scene identifier.
        :param float incidence_angle: Sensor incidence angle.
        :param str pass: On which pass was this image taken.
        :param str processed: Date which this image was processed.
        :param str proj4: proj.4 transformation parameters
        :param str projcs: Projection coordinate system.
        :param str published: Date the image was published.
        :param list(int) raster_size: Dimensions of image in pixels in (width, height).
        :param list(float) reflectance_scale: Scale factors converting TOA radiances to TOA reflectances
        :param int relative_orbit: Orbit number in revisit cycle.
        :param float roll_angle: Applicable only to Landsat 8, roll angle.
        :param str safe_id: Standard Archive Format for Europe.
        :param str sat_id: Satellite identifier.
        :param float scan_gap_interpolation: Applicable only to Landsat-7, width of pixels interpolated for scan gaps.
        :param str storage_state: A string indicating whether data for the image is stored on the Descartes
            Labs platform. Allowed values are "available" and "remote". If `"remote"`, entry may not include the
            fields bucket, directory, files, file_md5s, file_sizes. Default is `"available"`.
        :param list(str) writers: A list of groups, or user hashes to give read access to.
        :param dict extra_properties: User defined custom properties for this image.
            Up to 50 keys are allowed. The dict can only map strings to primitive types (str -> str|float|int).

        :rtype: dict
        :return: JSON API representation of the band. See :meth:`get_image`
            for information about returned keys.

        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the owners list is missing prefixes.
        :raises ~descarteslabs.client.exceptions.ConflictError: Raised when
            a image with the specified ID already exists.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        check_deprecated_kwargs(kwargs, {"bpp": "bits_per_pixel", "key": "image_id"})
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)
        kwargs["id"] = image_id
        kwargs["storage_state"] = storage_state
        r = self.session.post("/products/{}/images".format(product_id), json=kwargs)
        return r.json()

    def replace_image(
        self,
        product_id,
        image_id,
        add_namespace=False,
        storage_state="available",
        **kwargs
    ):
        """Replace image metadata with a new version.

        :param str product_id: (Required) Product to which this image belongs.
        :param str image_id: (Required) ID of the image to replace.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.
        :param str storage_state: A string indicating whether data for the image is stored on the Descartes
            Labs platform. Allowed values are "available" and "remote". If `"remote"`, entry may not include the
            fields bucket, directory, files, file_md5s, file_sizes. Default is `"available"`.

        .. note::
            - See :meth:`add_image` for additional kwargs.

        :rtype: dict
        :return: JSON API representation of the band. See :meth:`get_image`
            for information about returned keys.

        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the owners list is missing prefixes.
        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            image cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """

        check_deprecated_kwargs(kwargs, {"bpp": "bits_per_pixel"})
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)

        kwargs["storage_state"] = storage_state

        r = self.session.put(
            "/products/{}/images/{}".format(product_id, image_id), json=kwargs
        )
        return r.json()

    def _add_core_image(self, product_id, image_id, **kwargs):
        check_deprecated_kwargs(kwargs, {"bpp": "bits_per_pixel"})
        kwargs["id"] = image_id
        r = self.session.post(
            "/core/products/{}/images".format(product_id), json=kwargs
        )
        return r.json()

    def change_image(self, product_id, image_id, add_namespace=False, **kwargs):
        """Update an image metadata entry of a product.

        :param str product_id: (Required) Product to which this image belongs.
        :param str image_id: (Required) ID of the image to modify.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        .. note::
            - See :meth:`add_image` for additional kwargs.

        :rtype: dict
        :return: JSON API representation of the band. See :meth:`get_image`
            for information about returned keys.

        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the owners list is missing prefixes.
        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            image cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        check_deprecated_kwargs(kwargs, {"bpp": "bits_per_pixel"})
        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)
        r = self.session.patch(
            "/products/{}/images/{}".format(product_id, image_id), json=kwargs
        )
        return r.json()

    def remove_image(self, product_id, image_id, add_namespace=False):
        """Remove a image from the catalog.

        :param str product_id: ID of the product to remove image from.
        :param str image_id: ID of the image to remove.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            product or image cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """

        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)
        self.session.delete("/products/{}/images/{}".format(product_id, image_id))

    def upload_image(
        self,
        files,
        product_id,
        metadata=None,
        multi=False,
        image_id=None,
        add_namespace=False,
        **kwargs
    ):
        """Upload an image for a product you own.

        This is an asynchronous operation and you can query for the status
        using :meth:`upload_result` with the upload id returned by this
        method.  The upload id is the ``image_id``, which defaults to the
        name of the file to be uploaded. The uploaded image can be accessed
        with :class:`~descarteslabs.client.services.storage.Storage` using
        the ``products`` storage type. (See the `Uploading Data to the Catalog
        <https://docs.descarteslabs.com/guides/catalog.html#uploading-data-to-the-catalog>`_
        for an example.)

        :type files: str or file or list(str) or list(file)
        :param files: (Required) a reference to the file to upload.
        :param str product_id: (Required) The id of the product this image belongs to.
        :param dict metadata: Image metadata to use instead of the computed default values.
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        .. note::
            - See :meth:`add_image` for additional kwargs.

        :rtype: str
        :return: The upload id.

        :raises ValueError: Raised when ``multi=True`` but multiple ``files`` aren't provided,
            or if ``image_id`` isn't specified.
        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            product cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """

        if metadata is None:
            metadata = {}

        metadata.update(kwargs)
        check_deprecated_kwargs(metadata, {"bpp": "bits_per_pixel"})

        if multi is True:
            if not hasattr(files, "__iter__"):
                raise ValueError("Using `multi=True` requires `files` to be iterable")
            elif image_id is None:
                raise ValueError(
                    "Using `multi=True` requires `image_id` to be specified"
                )
            else:
                failed, upload_id, error = self._do_multi_file_upload(
                    files, product_id, image_id, metadata, add_namespace=add_namespace
                )
        else:
            failed, upload_id, error = self._do_upload(
                files,
                product_id,
                image_id,
                metadata=metadata,
                add_namespace=add_namespace,
            )

        if failed:
            raise error

        return upload_id

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
        add_namespace=False,
        **kwargs
    ):
        """Upload an ndarray with georeferencing information.

        This is an asynchronous operation and you can query for the status
        using :meth:`upload_result` with the upload id returned by this
        method.  The upload id is the ``image_id``.

        :param ndarray ndarray: (Required) A numpy ndarray with image data. If you are providing a multi-band image
            it should have 3 dimensions and the 3rd dimension of the array should index the bands. The dtype of the
            ndarray must also be one of the following:
            ["uint8", "int8", "uint16", "int16", "uint32", "int32", "float32", "float64"]
        :param str product_id: (Required) The id of the product this image belongs to.
        :param str image_id: (Required) Resulting image's id = <product_id>:<image_id>.
        :param str proj4: (One of proj4 or wkt_srs is required) A proj4 formatted string representing the
            spatial reference system used by the image.
        :param str wkt_srs: (One of proj4 or wkt_srs is required) A well known text string representing the
            spatial reference system used by the image.
        :param list(float) geotrans: (Required) The 6 number geographic transform of the image. Maps pixel coordinates
            to coordinates in the specified spatial reference system.
        :param dict raster_meta: Metadata returned from the
            :meth:`Raster.ndarray() <descarteslabs.client.services.raster.Raster.ndarray>`
            request which generated the initial data for the ``ndarray`` being uploaded. Passing ``geotrans``
            and ``wkt_srs`` is unnecessary in this case.
        :param list(int) overviews: a list of overview resolution magnification factors e.g. [2, 4] would make two
            overviews at 2x and 4x the native resolution. Maximum number of overviews allowed is 16.
        :param str overview_resampler: Resampler algorithm to use when building overviews. Controls how pixels are
            combined to make lower res pixels in overviews. Allowed resampler algorithms are:
            ["nearest", "average", "gauss", "cubic", "cubicspline", "lanczos", "average_mp",
            "average_magphase", "mode"].
        :param bool add_namespace: (Deprecated) Add your user namespace to the ``product_id``.

        .. note::
            - See :meth:`add_image` for additional kwargs.
            - Only one of ``proj4`` or ``wkt_srs`` can be provided.

        :rtype: str
        :return: The upload id.

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            product cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        if ndarray.dtype.name not in self.UPLOAD_NDARRAY_SUPPORTED_DTYPES:
            raise TypeError(
                "{} is not in supported dtypes {}".format(
                    ndarray.dtype.name, self.UPLOAD_NDARRAY_SUPPORTED_DTYPES
                )
            )

        metadata = kwargs
        metadata.setdefault("process_controls", {}).update({"upload_type": "ndarray"})
        if raster_meta is not None:
            geotrans = raster_meta.get("geoTransform")
            wkt_srs = raster_meta.get("coordinateSystem", {}).get("wkt")
        for arg in ["image_id", "proj4", "wkt_srs", "geotrans"]:
            if locals()[arg] is not None:
                kwargs[arg] = locals()[arg]
        for arg in ["overviews", "overview_resampler"]:
            if locals()[arg] is not None:
                metadata["process_controls"][arg] = locals()[arg]
        with NamedTemporaryFile(delete=False) as tmp:
            try:
                np.save(tmp, ndarray, allow_pickle=False)
                # From tempfile docs:
                # Whether the name can be used to open the file a second time,
                # while the named temporary file is still open, varies across
                # platforms (it can be so used on Unix; it cannot on Windows
                # NT or later)
                #
                # We close the underlying file object so _do_upload can open
                # the path again in a cross platform compatible way.
                # Cleanup is manual in the finally block.
                tmp.close()
                failed, upload_id, error = self._do_upload(
                    tmp.name, product_id, metadata=metadata, add_namespace=add_namespace
                )

                if failed:
                    raise error

                # For ndarrays we add `.tif` to the upload id in the service
                if not upload_id.endswith(".tif"):
                    upload_id += ".tif"

                return upload_id
            finally:
                os.unlink(tmp.name)

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
        :type updated: str or int
        :param updated: Unix timestamp or ISO-8601 formatted date for filtering results updated after this time.
        :type created: str or int
        :param created: Unix timestamp or ISO-8601 formatted date for filtering results created after this time.

        :rtype: dict
        :return: A JSON API representation of the upload results, with the following keys:

            .. highlight:: none

            ::

                data: A list of upload result dicts with the following keys:

                    id:         ID of the upload task.
                    type:       "upload".
                    attributes: A list of dicts which contains the following keys:

                        created:           Time the task was created
                                           in ISO-8601 UTC.
                        exception_name:    None, or if the task failed
                                           the name of the exception raised.
                        failure_type:      None, or type of failure if
                                           the task failed.  One of
                                           ["exception", "oom", "timeout",
                                           "internal", "unknown",
                                           "py_version_mismatch"].
                        peak_memory_usage: Peak memory usage in bytes.
                        runtime:           Time in seconds that the task
                                           took to complete.
                        status:            Status of the task, one of
                                           ["SUCCESS", "FAILURE"].

                meta: A dict with the following keys:

                    continuation_token: Token used for paging responses.

        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the status filter has an invalid value.
        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            product cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        kwargs = {"limit": limit}
        for arg in ["offset", "status", "updated", "created", "continuation_token"]:
            if locals()[arg] is not None:
                kwargs[arg] = locals()[arg]
        results = self.session.post(
            "/products/{}/uploads".format(product_id), json=kwargs
        )
        return results.json()

    def iter_upload_results(self, product_id, status=None, updated=None, created=None):
        """Get result information for debugging your uploads.

        :param str product_id: Product ID to get upload results for.
        :param str status: Filter results by status, values are ["SUCCESS", "FAILURE"]
        :type updated: str or int
        :param updated: Unix timestamp or ISO-8601 formatted date for filtering results updated after this time.
        :type created: str or int
        :param created: Unix timestamp or ISO-8601 formatted date for filtering results created after this time.

        :rtype: generator of dicts
        :return: An iterator of upload results, with the following keys:

            .. highlight:: none

            ::

                id:         ID of the upload task.
                type:       "upload".
                attributes: A dict which contains the following keys:

                    created:           Time the task was created
                                       in ISO-8601 UTC.
                    exception_name:    None, or if the task failed
                                       the name of the exception raised,
                    failure_type:      None, or type of failure if
                                       the task failed.  One of
                                       ["exception", "oom", "timeout",
                                       "internal", "unknown",
                                       "py_version_mismatch"].
                    peak_memory_usage: Peak memory usage in bytes.
                    runtime:           Time in seconds that the task
                                       took to complete.
                    status:            Status of the task, one of
                                       ["SUCCESS", "FAILURE"].

        :raises ~descarteslabs.client.exceptions.BadRequestError: Raised when
            the request is malformed, e.g. the status filter has an invalid value.
        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            product cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        continuation_token = None
        kwargs = {}
        for arg in ["status", "updated", "created"]:
            if locals()[arg] is not None:
                kwargs[arg] = locals()[arg]
        while True:
            page = self.upload_results(
                product_id, continuation_token=continuation_token, **kwargs
            )
            for res in page["data"]:
                yield res
            continuation_token = page["meta"]["continuation_token"]
            if continuation_token is None:
                break

    def upload_result(self, product_id, upload_id):
        """Get one upload result with the processing logs.

        This is useful for debugging failed uploads.

        :param str product_id: Product ID to get upload result for.
        :param str upload_id: ID of specific upload.

        :rtype: dict
        :return: An single upload result, with the following keys:

            .. highlight:: none

            ::

                id:         ID of the upload task.
                type:       "upload".
                attributes: A dict which contains the following keys:

                    created:           Time the task was created
                                       in ISO-8601 UTC.
                    exception_name:    None, or if the task failed
                                       the name of the exception raised,
                    failure_type:      None, or type of failure if
                                       the task failed.  One of
                                       ["exception", "oom", "timeout",
                                       "internal", "unknown",
                                       "py_version_mismatch"]
                    labels:            List of labels added to the task.
                    logs:              Log information output by the task.
                    peak_memory_usage: Peak memory usage in bytes.
                    runtime:           Time in seconds that the task
                                       took to complete.
                    stacktrace:        None, or if the task failed,
                                       the stacktrace of the exception raised.
                    status:            Status of the task, one of
                                       ["SUCCESS", "FAILURE"].

        :raises ~descarteslabs.client.exceptions.NotFoundError: Raised if the
            product or upload cannot be found.
        :raises ~descarteslabs.client.exceptions.RateLimitError: Raised when
            too many requests have been made within a given time period.
        :raises ~descarteslabs.client.exceptions.ServerError: Raised when
            a unknown error occurred on the server.
        """
        result = self.session.get(
            "/products/{}/uploads/{}".format(product_id, upload_id)
        )
        return result.json()

    def _do_multi_file_upload(
        self, files, product_id, image_id, metadata, add_namespace=False
    ):
        file_keys = [os.path.basename(_f) for _f in files]
        process_controls = metadata.setdefault(
            "process_controls", {"upload_type": "file"}
        )
        multi_file_args = {
            "multi_file": {"image_files": file_keys, "image_id": image_id}
        }
        process_controls.update(multi_file_args)

        for _file in files:
            failed, upload_id, error = self._do_upload(
                _file, product_id, metadata=metadata, add_namespace=add_namespace
            )

            if failed:
                break

        return failed, upload_id, error

    def _do_upload(
        self, file_ish, product_id, image_id=None, metadata=None, add_namespace=False
    ):
        # kwargs are treated as metadata fields and restricted to primitives
        # for the key val pairs.
        fd = None
        upload_id = None

        if add_namespace:
            check_deprecated_kwargs(locals(), {"add_namespace": None})
            product_id = self.namespace_product(product_id)

        if metadata is None:
            metadata = {}

        metadata.setdefault("process_controls", {"upload_type": "file"})
        check_deprecated_kwargs(metadata, {"bpp": "bits_per_pixel"})

        if not isinstance(product_id, six.string_types):
            raise TypeError(
                "product_id={} is invalid. "
                "product_id must be a string.".format(product_id)
            )

        if isinstance(file_ish, io.IOBase):
            if "b" not in file_ish.mode:
                file_ish.close()
                file_ish = io.open(file_ish.name, "rb")

            fd = file_ish
        elif isinstance(file_ish, six.string_types) and os.path.exists(file_ish):
            fd = io.open(file_ish, "rb")
        else:
            e = Exception(
                "Could not handle file: `{}` pass a valid path "
                "or open IOBase file".format(file_ish)
            )
            return True, upload_id, e

        try:
            upload_id = (
                image_id or metadata.pop("image_id", None) or os.path.basename(fd.name)
            )

            r = self.session.post(
                "/products/{}/images/upload/{}".format(product_id, upload_id),
                json=metadata,
            )
            upload_url = r.text
            r = self._gcs_upload_service.session.put(upload_url, data=fd)
        except (ServerError, RequestException) as e:
            return True, upload_id, e
        except NotFoundError as e:
            raise NotFoundError(
                "Make sure product_id exists in the catalog before"
                " attempting to upload data. %s" % e.message
            )
        finally:
            fd.close()

        return False, upload_id, None


catalog = Catalog(auth=Auth(_suppress_warning=True))
