import os
import six
import io
import logging

from descarteslabs.common.property_filtering import GenericProperties
from descarteslabs.client.deprecation import deprecate
from descarteslabs.client.services.service import JsonApiService, ThirdPartyService
from descarteslabs.client.auth import Auth
from descarteslabs.common.dotdict import DotDict


class _SearchFeaturesIterator(object):
    """Private iterator for search_features() that also returns length"""

    def __init__(
        self,
        client,
        product_id,
        geometry,
        query_expr,
        query_limit,
        **kwargs
    ):
        self._client = client
        self._product_id = product_id
        self._geometry = geometry
        self._query_expr = query_expr
        self._query_limit = query_limit
        self._kwargs = kwargs
        self._continuation_token = None
        self._page_offset = 0
        self._page_len = 0

        # updates _continuation_token, _page_offset, _page_len
        self._next_page()
        self._length = self._page.meta.total_results

    def __iter__(self):
        return self

    def __len__(self):
        return self._length

    def __next__(self):
        if self._page_offset >= self._page_len:
            if self._continuation_token is None:
                raise StopIteration()

            self._next_page()

        try:
            return self._page.data[self._page_offset]
        finally:
            self._page_offset += 1

    def _next_page(self):
        self._page = self._client._fetch_feature_page(
            product_id=self._product_id,
            geometry=self._geometry,
            query_expr=self._query_expr,
            query_limit=self._query_limit,
            continuation_token=self._continuation_token,
            **self._kwargs
        )

        self._continuation_token = self._page.meta.continuation_token
        self._page_offset = 0
        self._page_len = len(self._page.data)

    def next(self):
        """Backwards compatibility for Python 2"""
        return self.__next__()


class Vector(JsonApiService):
    """
    Client for storing and querying vector data.

    The Descartes Labs Vector service allows you store vector features (points, polygons, etc.)
    with associated key-value properties, and query that data by geometry or by properties.

    It works best at the scale of millions of features. For small amounts of vector data
    that easily fit in memory, working directly with a GeoJSON file or similar may be more efficient.

    Concepts:

    * "Feature": a single geometric entity and its associated metadata (equivalent to a GeoJSON Feature).
    * "Product": a collection of related Features, with a common id, description, and access controls.

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

    @deprecate(renames={"name": "product_id"})
    def create_product(self, product_id, title, description, owners=None, readers=None, writers=None):
        """
        Add a vector product to your catalog.

        :param str product_id: (Required) A unique name for this product. In the created
            product a namespace consisting of your user id (e.g.
            "ae60fc891312ggadc94ade8062213b0063335a3c:") or your organization id (e.g.,
            "yourcompany:") will be prefixed to this, if it doesn't already have one, in
            order to make the id globally unique.
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
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers
        )

        jsonapi = self.jsonapi_document(type="product", attributes=params, id=product_id)
        r = self.session.post('/products', json=jsonapi)
        return DotDict(r.json())

    @deprecate(required=["product_id", "title", "description"], renames={"name": None})
    def replace_product(
            self,
            product_id=None,
            name=None,
            title=None,
            description=None,
            owners=None,
            readers=None,
            writers=None
    ):
        """
        Replace a vector product in your catalog.

        :param str product_id: (Required) The vector product to replace.
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
        :param str name: (Deprecated) Will be removed completely in future versions.

        :rtype: DotDict
        :return: Replaced vector product, as a JSON API resource object

                 The new product's ID is under ``.data.id``
                 and its properties are under ``.data.attributes``.
        """
        # TODO: fully deprecate `name` and remove from params completely
        params = dict(
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers,
        )

        jsonapi = self.jsonapi_document(type="product", attributes=params, id=product_id)
        r = self.session.put('/products/{}'.format(product_id), json=jsonapi)
        return DotDict(r.json())

    @deprecate(renames={"name": None})
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
        :param str name: (Deprecated) Will be removed completely in future versions.
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
        # TODO: fully deprecate name and remove from params completely
        params = dict(
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
                                    and properties field. If more than 100 features,
                                    will be batched in groups of 100, but consider
                                    using upload_features() instead.

        :rtype: DotDict
        :return: Created features, as a JSON API resource collection.

                 The new Features' IDs are under ``.data[i].id``,
                 and their properties are under ``.data[i].attributes``.
        :raises ClientError: A variety of http-related exceptions can
                 thrown. If more than 100 features were passed in, some
                 of these may have been successfully inserted, others not.
                 If this is a problem, then stick with <= 100 features.
        """

        if len(features) > 100:
            logging.warning(
                'create_features: feature collection has more than 100 features,'
                + ' will batch by 100 but consider using upload_features')

        # forcibly pass a zero-length list for appropriate validation error
        for i in range(0, max(len(features), 1), 100):
            jsonapi = self.jsonapi_collection(type="feature",
                                              attributes_list=features[i:i + 100])
            r = self.session.post('/products/{}/features'.format(product_id),
                                  json=jsonapi)
            if i == 0:
                result = DotDict(r.json())
            else:
                result.data.extend(DotDict(r.json()).data)

        return result

    def upload_features(self, file_ish, product_id, max_errors=0):
        """
        Asynchonously upload a file or stream of
        `Newline Delimited JSON <https://github.com/ndjson/ndjson-spec>`_
        features.

        It is recommended that the IOBase object is a byte-oriented (not
        text-oriented) object, although Python 3 allows `io.StringIO` to
        be used.

        :param str|:py:class:`io.IOBase` file_ish: an open IOBase object, or a path to the file to upload.
        :param str product_id: Product to which these features will belong.
        :param int max_errors: The maximum number of errors permitted before declaring failure.
        """
        if isinstance(file_ish, io.IOBase):
            return self._upload_features(file_ish, product_id, max_errors)
        elif isinstance(file_ish, six.string_types):
            with io.open(file_ish, 'rb') as stream:
                return self._upload_features(stream, product_id, max_errors)
        else:
            raise Exception('Could not handle file: `{}`; pass a path or open IOBase instance'.format(file_ish))

    def _upload_features(self, iobase, product_id, max_errors):
        jsonapi = self.jsonapi_document(type="features", attributes={'max_errors': max_errors})
        r = self.session.post('/products/{}/features/uploads'.format(product_id), json=jsonapi)
        upload = r.json()
        upload_url = upload['url']
        r = self._gcs_upload_service.session.put(upload_url, data=iobase)
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

        The returned iterator has a length that indicates the size of
        the query.

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
                 len() can be used on the returned iterator to determine
                 the query size.
        """
        return _SearchFeaturesIterator(
            self, product_id, geometry, query_expr, query_limit)

    @deprecate(renames={"name": "new_product_id"})
    def create_product_from_query(
        self,
        new_product_id,
        title,
        description,
        product_id,
        owners=None,
        readers=None,
        writers=None,
        geometry=None,
        query_expr=None,
        query_limit=None
    ):
        """
        Query vector features within an existing product and create a new
        vector product to your catalog from the query result.

        At least one of `geometry`, `query_expr`, or `query_limit` is required.

        :param str new_product_id: (Required) A unique name for this product. In the created
            product a namespace consisting of your user id (e.g.
            "ae60fc891312ggadc94ade8062213b0063335a3c:") or your organization id (e.g.,
            "yourcompany:") will be prefixed to this, if it doesn't already have one, in
            order to make the id globally unique.
        :param str title: (Required) Official product title.
        :param str description: (Required) Information about the product,
                                why it exists, and what it provides.
        :param str product_id: (Required) Product within which to search.
        :param: list(str) owners: User, group, or organization IDs that own this product.
                            Each ID must be prefixed with ``user:``, ``group:``, or ``org:``.
                            Defaults to [current user, current user's org].
        :param: list(str) readers: User, group, or organization IDs that can read this product.
                                  Each ID must be prefixed with ``user:``, ``group:``, or ``org:``.
        :param: list(str) writers: User, group, or organization IDs that can edit this product.
                                  Each ID must be prefixed with ``user:``, ``group:``, or ``org:``.
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

        :rtype: DotDict
        :return: Created vector product, as a JSON API resource object.

                 The new product's ID is under ``.data.id``
                 and its properties are under ``.data.attributes``.
        """
        product_params = dict(
            id=new_product_id,
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers
        )

        query_params = {k: v for k, v in dict(
            geometry=geometry,
            query_expr=(query_expr.serialize() if query_expr is not None else None),
            query_limit=query_limit,
        ).items() if v is not None}

        params = dict(
            product=product_params,
            query=query_params
        )

        r = self.session.post('/products/{}/search/copy'.format(product_id), json=params)
        return DotDict(r.json())

    def get_product_from_query_status(self, product_id):
        """
        Get the status of the job creating a new product from a query.

        :param str product_id: (Required) Id of the product created
                               by a call to `create_product_from_query`.
        """
        r = self.session.get("/products/{}/search/copy/status".format(product_id))
        return DotDict(r.json())

    def delete_features_from_query(self, product_id, geometry=None, query_expr=None, **kwargs):
        """
        Query an existing Vector product and delete features that match
        the query results.

        At least one of `geometry`, `query_expr`, or `properties` is required.

        :param str product_id: (Required) Product within which to search for features to delete.
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

        :rtype: DotDict
        :return: The Vector product features were deleted from, as a JSON API resource object.

                 The new product's ID is under ``.data.id``
                 and its properties are under ``.data.attributes``.
        """
        query_params = {k: v for k, v in dict(
            geometry=geometry,
            query_expr=(query_expr.serialize() if query_expr is not None else None)
        ).items() if v is not None}

        r = self.session.delete("/products/{}/search".format(product_id), json=query_params)
        return DotDict(r.json())

    def get_delete_features_status(self, product_id):
        """
        Get the status of the job deleting features from a query.

        :param str product_id: (Required) Id of the product created
                               by a call to `create_product_from_query`.
        """
        r = self.session.get("/products/{}/search/delete/status".format(product_id))
        return DotDict(r.json())

    def count_features(self, product_id):
        """
        Get the count of the features in a product.

        :param str product_id: (Required) Id of the product created
                               by a call to `create_product_from_query`.
        """
        r = self.session.get("/products/{}/features/count".format(product_id))
        return r.json()
