import os
import six
import io

from descarteslabs.common.property_filtering import GenericProperties
from descarteslabs.client.services.service import JsonApiService, ThirdPartyService
from descarteslabs.client.auth import Auth
from descarteslabs.common.dotdict import DotDict


class Vector(JsonApiService):
    """
    Client for storing and querying vector data.

    The Descartes Labs Vector service allows you store vector features (points, polygons, etc.)
    with associated key-value properties, and query that data by geometry or by properties.

    It works best at the scale of millions of features. For small amounts of vector data
    that easily fit in memory, working directly with a GeoJSON file or similar may be more efficient.

    Concepts:

    * "Feature": a single geometric entity and its associated metadata (equivalent to a GeoJSON Feature).
    * "Product": a collection of related Features, with a common name, description, and access controls.

    This client currently returns data as dictionaries in `JSON API format <http://jsonapi.org/>`_.
    """
    TIMEOUT = (9.5, 60)
    SEARCH_PAGE_SIZE = 1000
    properties = GenericProperties()

    def __init__(self, url=None, auth=None):
        """
        The parent Service class implements authentication and exponential backoff/retry.
        Override the url parameter to use a different instance of the backing service.
        """
        if auth is None:
            auth = Auth()

        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_VECTOR_URL",
                "https://platform.descarteslabs.com/vector/v2"
            )
        self._gcs_upload_service = ThirdPartyService()

        super(Vector, self).__init__(url, auth=auth)

    def list_products(self, page_size=100, page=1):
        """
        Get all vector products that you have access using JSON API pagination.
        The first page (1) will always succeed but may be empty.
        Subsequent pages may throw `NotFoundError`.

        :param int page_size: Maximum number of vector products to return per page; default is 100.
        :param int page: Which page of results to fetch, if there are more results than ``page_size``.

        :rtype: DotDict
        :return: Available vector products and their properties, as a JSON API collection.

                 The list of products is under the ``data`` key;
                 a product's ID is under ``.data[i].id``
                 and its properties are under ``.data[i].attributes``.
        """

        params = dict(
            limit=page_size,
            page=page
        )

        # override the json api content type which is default.
        r = self.session.get('/products', params=params, headers={'Content-Type': 'application/json'})
        return DotDict(r.json())

    def get_product(self, product_id):
        """
        Get a product's properties.

        :param str product_id: (Required) The ID of the vector product to fetch.

        :rtype: DotDict
        :return: Metadata for the provided product, as a JSON API resource object.

                 The product's ID is under ``.data.id``
                 and its properties are under ``.data.attributes``.
        """
        r = self.session.get('/products/{}'.format(product_id))
        return DotDict(r.json())

    def create_product(self, name, title, description, owners=None, readers=None, writers=None):
        """
        Add a vector product to your catalog.

        :param str name: (Required) A name for this product.
        :param str title: (Required) Official product title.
        :param str description: (Required) Information about the product,
                                why it exists, and what it provides.
        :param: list(str) owners: User, group, or organization IDs that own this product.
                                  Each ID must be prefixed with ``user:``, ``group:``, or ``org:``.
                                  Defaults to [current user, current user's org].
        :param: list(str) readers: User, group, or organization IDs that can read this product.
                                  Each ID must be prefixed with ``user:``, ``group:``, or ``org:``.
        :param: list(str) writers: User, group, or organization IDs that can edit this product.
                                  Each ID must be prefixed with ``user:``, ``group:``, or ``org:``.

        :rtype: DotDict
        :return: Created vector product, as a JSON API resource object.

                 The new product's ID is under ``.data.id``
                 and its properties are under ``.data.attributes``.
        """
        params = dict(
            name=name,
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers
        )

        jsonapi = self.jsonapi_document(type="product", attributes=params)
        r = self.session.post('/products', json=jsonapi)
        return DotDict(r.json())

    def replace_product(
            self,
            product_id,
            name,
            title,
            description,
            owners=None,
            readers=None,
            writers=None
    ):
        """
        Replace a vector product in your catalog.

        :param str product_id: (Required) The vector product to replace.
        :param str name: (Required) A name for this product.
        :param str title: (Required) Official product title.
        :param str description: (Required) Information about the product,
                                why it exists, and what it provides.
        :param: list(str) owners: User, group, or organization IDs that own this product.
                                  Each ID must be prefixed with `user:`, `group:`, or `org:`.
                                  Defaults to [current user, current user's org].
        :param: list(str) readers: User, group, or organization IDs that can read this product.
                                  Each ID must be prefixed with `user:`, `group:`, or `org:`.
        :param: list(str) writers: User, group, or organization IDs that can edit this product.
                                  Each ID must be prefixed with `user:`, `group:`, or `org:`.

        :rtype: DotDict
        :return: Replaced vector product, as a JSON API resource object

                 The new product's ID is under ``.data.id``
                 and its properties are under ``.data.attributes``.
        """
        params = dict(
            name=name,
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers,
        )

        jsonapi = self.jsonapi_document(type="product", attributes=params, id=product_id)
        r = self.session.put('/products/{}'.format(product_id), json=jsonapi)
        return DotDict(r.json())

    def update_product(
            self,
            product_id,
            name=None,
            title=None,
            description=None,
            owners=None,
            readers=None,
            writers=None
    ):
        """Update a vector product in your catalog.

        :param str product_id: (Required) The vector product to replace.
        :param str name: Name for this product.
        :param str title: Official product title.
        :param str description: Information about the product,
                                why it exists, and what it provides.
        :param: list(str) owners: User, group, or organization IDs that own this product.
                                  Each ID must be prefixed with `user:`, `group:`, or `org:`.
        :param: list(str) readers: User, group, or organization IDs that can read this product.
                                  Each ID must be prefixed with `user:`, `group:`, or `org:`.
        :param: list(str) writers: User, group, or organization IDs that can edit this product.
                                  Each ID must be prefixed with `user:`, `group:`, or `org:`.

        :return: Updated vector product, as a JSON API resource object.
        :rtype: DotDict
        """
        params = dict(
            name=name,
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers,
        )
        params = {k: v for k, v in six.iteritems(params) if v is not None}

        jsonapi = self.jsonapi_document(type="product", attributes=params, id=product_id)
        r = self.session.patch('/products/{}'.format(product_id), json=jsonapi)
        return DotDict(r.json())

    def delete_product(self, product_id):
        """
        Remove a vector product from the catalog.

        :param str product_id: ID of the vector product to remove.
        """

        self.session.delete('/products/{}'.format(product_id))

    def create_feature(self, product_id, geometry, properties=None):
        """
        Add a feature to an existing vector product.

        :param str product_id: (Required) Product to which this feature will belong.
        :param dict geometry: (Required) Shape associated with this vector feature.
                              This accepts the following types of GeoJSON geometries:
                              - Points
                              - MultiPoints
                              - Polygons
                              - MultiPolygons
                              - LineStrings
                              - MultiLineStrings
                              - GeometryCollections

        :param dict properties: Dictionary of arbitrary properties.

        :rtype: DotDict
        :return: Created Feature, as a JSON API resource collection.

                 The new Feature's ID is under ``.data[0].id``,
                 and its properties are under ``.data[0].attributes``.
        """
        params = dict(
            geometry=geometry,
            properties=properties
        )

        jsonapi = self.jsonapi_document(type="feature", attributes=params)
        r = self.session.post('/products/{}/features'.format(product_id), json=jsonapi)
        return DotDict(r.json())

    def create_features(self, product_id, features):
        """
        Add multiple features to an existing vector product.

        :param str product_id: (Required) Product to which this feature will belong.
        :param list(dict) features: (Required) Each feature must be a dict with a geometry
                                    and properties field.

        :rtype: DotDict
        :return: Created features, as a JSON API resource collection.

                 The new Features' IDs are under ``.data[i].id``,
                 and their properties are under ``.data[i].attributes``.
        """
        jsonapi = self.jsonapi_collection(type="feature", attributes_list=features)
        r = self.session.post('/products/{}/features'.format(product_id), json=jsonapi)
        return DotDict(r.json())

    def upload_features(self, file_ish, product_id):
        """
        Asynchonously upload a file of `Newline Delimited JSON <https://github.com/ndjson/ndjson-spec>`_ features.

        :param str|:py:class:`io.IOBase` file_ish: an open file object, or a path to the file to upload.
        :param str product_id: Product to which these features will belong.
        """
        if isinstance(file_ish, io.IOBase):
            file_name = file_ish.name
        elif isinstance(file_ish, six.string_types) and os.path.exists(file_ish):
            file_name = file_ish
        else:
            raise Exception('Could not handle file: `{}` pass a valid path or open IOBase instance'.format(file_ish))
        with io.open(file_name, 'rb') as fd:
            r = self.session.post('/products/{}/features/uploads'.format(product_id))
            upload = r.json()
            upload_url = upload['url']
            r = self._gcs_upload_service.session.put(upload_url, data=fd)
            return upload['upload_id']

    def _fetch_upload_result_page(self, product_id, continuation_token=None):
        r = self.session.get(
            '/products/{}/features/uploads'.format(product_id),
            params={'continuation_token': continuation_token},
            headers={'Content-Type': 'application/json'},
        )
        return DotDict(r.json())

    def get_upload_results(self, product_id):
        """
        Get a list of the uploads submitted to a vector product, and status
        information about each.

        :param str product_id: (required)
        :return: An iterator over all upload resources created with :meth:`Vector.upload_features`
        :rtype: Iterator
        """
        continuation_token = None
        while True:
            page = self._fetch_upload_result_page(product_id, continuation_token=continuation_token)
            for feature in page.data:
                yield feature
            continuation_token = page.meta.continuation_token

            if continuation_token is None:
                break

    def get_upload_result(self, product_id, upload_id):
        """
        Get details about a specific upload job. Included information about
        processing error streams, which can help debug failed uploads.

        :param str product_id: (required)
        :param str upload_id: (required) An id pertaining to this requested upload,
            either returned by :meth:`Vector.get_upload_results` or :meth:`Vector.upload_features`.
        """
        r = self.session.get(
            '/products/{}/features/uploads/{}'.format(product_id, upload_id),
            headers={'Content-Type': 'application/json'},
        )
        return DotDict(r.json())

    def _fetch_feature_page(
        self,
        product_id,
        geometry,
        query_expr=None,
        query_limit=None,
        continuation_token=None,
        **kwargs
    ):
        """
        Query vector features within an existing product.

        :param str product_id: (Required) Product within which to search.
        :param dict geometry: (Required) Search for Features intersecting
            this shape, as a GeoJSON geometry
        :param descarteslabs.client.common.filtering.Expression query_expr:
            A rich query expression generator that represents
            an arbitrary tree of boolean combinations of property
            comparisons.  Using the properties filter factory inside
            `descarteslabs.client.services.vector.properties` as
            `p`, you can E.g `query_expr=(p.temperature >= 50) &
            (p.hour_of_day > 18)`, or even more complicated expressions
            like `query_expr=(100 > p.temperature >= 50) | ((p.month
            != 10) & (p.day_of_month > 14))` This expression gets
            serialized and applied to the properties mapping supplied
            with the features in the vector product. If you supply a
            property which doesn't exist as part of the expression that
            comparison will evaluate to False.
        :param int query_limit: Number of features to limit the query result to
            (unlimited by default)
        :param str continuation_token: Token returned from a previous call
            to this method that can be used to fetch the next page
            of search results. Search parameters must stay consistent
            between calls using a continuation token.

        :rtype: DotDict
        :return: Features satisfying the query, as a JSON API resource
            collection.

                 The Features' IDs are under ``.data[i].id``,
                 and their properties are under ``.data[i].attributes``.
        """
        params = {k: v for k, v in dict(
            kwargs,
            geometry=geometry,
            query_expr=(query_expr.serialize() if query_expr is not None else None),
            limit=Vector.SEARCH_PAGE_SIZE,
            query_limit=query_limit,
            continuation_token=continuation_token,
        ).items() if v is not None}

        r = self.session.post('/products/{}/search'.format(product_id), json=params)
        return DotDict(r.json())

    def search_features(
        self,
        product_id,
        geometry=None,
        query_expr=None,
        query_limit=None,
        **kwargs
    ):
        """
        Iterate over vector features within an existing product.

        At least one of `geometry`, `query_expr`, or `properties` is required.

        :param str product_id: (Required) Product within which to search.
        :param dict geometry: Search for Features intersecting this shape.
                              This accepts the following types of GeoJSON
                              geometries:

                              - Points
                              - MultiPoints
                              - Polygons
                              - MultiPolygons
                              - LineStrings
                              - MultiLineStrings
                              - GeometryCollections

        :param descarteslabs.client.common.filtering.Expression query_expr:
            A rich query expression generator that represents
            an arbitrary tree of boolean combinations of property
            comparisons.  Using the properties filter factory inside
            `descarteslabs.client.services.vector.properties` as
            `p`, you can E.g `query_expr=(p.temperature >= 50) &
            (p.hour_of_day > 18)`, or even more complicated expressions
            like `query_expr=(100 > p.temperature >= 50) | ((p.month
            != 10) & (p.day_of_month > 14))` This expression gets
            serialized and applied to the properties mapping supplied
            with the features in the vector product. If you supply a
            property which doesn't exist as part of the expression that
            comparison will evaluate to False.
        :param int query_limit: Maximum number of features to return for this query, defaults to all.
        :rtype: Iterator
        :return: Features satisfying the query, as JSONAPI primary data objects.

                 The Features' IDs are under ``.id``,
                 and their properties are under ``.attributes``.
        """
        continuation_token = None

        while True:
            page = self._fetch_feature_page(
                product_id,
                geometry,
                query_expr=query_expr,
                query_limit=query_limit,
                continuation_token=continuation_token,
                **kwargs
            )

            for feature in page.data:
                yield feature

            continuation_token = page.meta.continuation_token

            if continuation_token is None:
                break
