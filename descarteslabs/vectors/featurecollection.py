import copy
from shapely.geometry import shape

from ..common.dotdict import DotDict
from descarteslabs.exceptions import NotFoundError
from ..client.deprecation import deprecate
from ..common.tasks import UploadTask, ExportTask, TransientResultError
from ..client.services.vector import Vector
from .feature import Feature

# import these exceptions for backwards compatibility
from .exceptions import (  # noqa
    InvalidQueryException,  # noqa
    VectorException,  # noqa
    FailedCopyError,  # noqa
    WaitTimeoutError,  # noqa
)
from .async_job import DeleteJob, CopyJob


class _FeaturesIterator(object):
    """Private iterator for features() that also returns length"""

    def __init__(self, response):
        self._response = response

    def __len__(self):
        return len(self._response)

    def __iter__(self):
        return self

    def __next__(self):
        return Feature._create_from_jsonapi(next(self._response))

    def next(self):
        """Backwards compatibility for Python 2"""
        return self.__next__()


class FeatureCollection(object):
    """
    A proxy object for accesssing millions of features within a collection
    having similar access controls, geometries and properties.  Such a
    grouping is named a ``product`` and identified by ``id``.

    If creating a new ``FeatureCollection`` use :meth:`create`
    instead.

    Features will not be retrieved from the ``FeatureCollection`` until
    :meth:`features` is called.

    Attributes
    ----------
    id : str
        The unique identifier for this ``FeatureCollection``.
    name : str
        (Deprecated) Will be removed in future versions.
    title : str
        A more verbose and expressive name for display purposes.
    description : str
        Information about the ``FeatureCollection``, why it exists,
        and what it provides.
    owners : list(str)
        User, group, or organization IDs that own
        this ``FeatureCollection``.  Defaults to
        [``user:current_user``, ``org:current_org``].
        The owner can edit, delete, and change access to
        this ``FeatureCollection``.
    readers : list(str)
        User, group, or organization IDs that can read
        this ``FeatureCollection``.
    writers : list(str)
        User, group, or organization IDs that can edit
        this ``FeatureCollection`` (includes read permission).

    Note
    ----
    All ``owner``, ``reader``, and ``writer`` IDs must be prefixed with
    ``email:``, ``user:``, ``group:`` or ``org:``.  Using ``org:`` as an
    ``owner`` will assign those privileges only to administrators for
    that organization; using ``org:`` as a ``reader`` or ``writer``
    assigns those privileges to everyone in that organization.
    """

    ATTRIBUTES = ["owners", "writers", "readers", "id", "name", "title", "description"]
    COMPLETE_STATUSES = ["DONE", "SUCCESS", "FAILURE"]
    COMPLETION_POLL_INTERVAL_SECONDS = 5

    def __init__(self, id=None, vector_client=None, refresh=True):

        self.id = id
        self._vector_client = vector_client

        self._query_geometry = None
        self._query_limit = None
        self._query_property_expression = None

        if refresh:
            self.refresh()

    @classmethod
    def _from_jsonapi(cls, response, vector_client=None):
        self = cls(response.id, vector_client=vector_client, refresh=False)
        self.__dict__.update(response.attributes)

        return self

    @classmethod
    @deprecate(renamed={"name": "product_id"})
    def create(
        cls,
        product_id,
        title,
        description,
        owners=None,
        readers=None,
        writers=None,
        vector_client=None,
    ):
        """
        Create a vector product in your catalog.

        Parameters
        ----------
        product_id : str
            A unique name for this product. In the created
            product a namespace consisting of your user id (e.g.
            "ae60fc891312ggadc94ade8062213b0063335a3c:") or your organization id (e.g.,
            "yourcompany:") will be prefixed to this, if it doesn't already have one, in
            order to make the id globally unique.
        title : str
            A more verbose and expressive name for display purposes.
        description : str
            Information about the ``FeatureCollection``, why it exists,
            and what it provides.
        owners : list(str), optional
            User, group, or organization IDs that own
            the newly created FeatureCollection.  Defaults to
            [``current user``, ``current org``].
            The owner can edit and delete this ``FeatureCollection``.
        readers : list(str), optional
            User, group, or organization IDs that can read
            the newly created ``FeatureCollection``.
        writers : list(str), optional
            User, group, or organization IDs that can edit
            the newly created ``FeatureCollection`` (includes read permission).

        Returns
        -------
        :class:`FeatureCollection`
            A new ``FeatureCollection``.

        Raises
        ------
        ~descarteslabs.exceptions.BadRequestError
            Raised when the request is malformed, e.g. the supplied product id is already in use.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> FeatureCollection.create(product_id='my-vector-product-id',
        ...    title='My Vector Collection',
        ...    description='Just a test')  # doctest: +SKIP
        """
        params = dict(
            product_id=product_id,
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers,
        )

        if vector_client is None:
            vector_client = Vector()

        return cls._from_jsonapi(
            vector_client.create_product(**params).data, vector_client
        )

    @classmethod
    def list(cls, vector_client=None):
        """
        List all ``FeatureCollection`` products that you have access to.

        Returns
        -------
        list(:class:`FeatureCollection`)
            A list of all products that you have access to.

        Raises
        ------
        ~descarteslabs.exceptions.NotFoundError
            Raised if subsequent pages cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> FeatureCollection.list()  # doctest: +SKIP
        """

        if vector_client is None:
            vector_client = Vector()

        list = []

        page = 1
        # The first page will always succeed...
        response = vector_client.list_products(page=page)

        while len(response) > 0:
            partial_list = [
                cls._from_jsonapi(fc, vector_client) for fc in response.data
            ]
            list.extend(partial_list)
            page += 1

            # Subsequent pages may throw NotFoundError
            try:
                response = vector_client.list_products(page=page)
            except NotFoundError:
                response = []

        return list

    @property
    def vector_client(self):
        if self._vector_client is None:
            self._vector_client = Vector()
        return self._vector_client

    def count(self):
        """
        Return the number of features in the product, regardless of
        what filters have been applied to the ``FeatureCollection``.

        Returns
        -------
        int
            Total number of features in the product.

        Raises
        ------
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, properties as p
        >>> all_us_cities = FeatureCollection('d1349cc2d8854d998aa6da92dc2bd24')  # doctest: +SKIP
        >>> count = all_us_cities.count()  # doctest: +SKIP
        """
        return self.vector_client.count_features(self.id)

    def filter(self, geometry=None, properties=None):
        """
        Include only the features matching the given ``geometry`` and ``properties``.
        Filters are not evaluated until iterating over the ``FeatureCollection``,
        and can be chained by calling filter multiple times.

        Parameters
        ----------
        geometry: GeoJSON-like dict, object with ``__geo_interface__``; optional
            Include features intersecting this geometry. If this
            ``FeatureCollection`` is already filtered by a geometry,
            the new geometry will override it -- they cannot be chained.
        properties : ~descarteslabs.common.property_filtering.filtering.Expression; optional
            Expression used to filter features by their properties, built from
            :class:`dl.properties <descarteslabs.common.property_filtering.filtering.GenericProperties>`.
            You can construct filter expression using the ``==``, ``!=``,
            ``<``, ``>``, ``<=`` and ``>=`` operators as well as the
            :meth:`~descarteslabs.common.property_filtering.filtering.Property.like`
            and :meth:`~descarteslabs.common.property_filtering.filtering.Property.in_`
            methods. You cannot use the boolean keywords ``and`` and ``or``
            because of Python language limitations; instead you can combine
            filter expressions with ``&`` (boolean "and") and ``|`` (boolean
            "or").
            Example (assuming ``from descarteslabs import properties as p``):
            ``query_expr=(p.temperature >= 50) & (p.hour_of_day > 18)``.
            More complex example:
            ``query_expr=(100 > p.temperature >= 50) | ((p.month
            != 10) & (p.day_of_month > 14))``.
            If you supply a property which doesn't exist as part of the
            expression that comparison will evaluate to ``False``.

        Returns
        -------
        :class:`FeatureCollection`
            A new ``FeatureCollection`` with the given filter.

        Raises
        ------
        ~descarteslabs.vectors.exceptions.InvalidQueryException
            Raised when there is a previously applied geometry filter and a new geometry was provided.
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, properties as p
        >>> aoi_geometry = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-109, 31], [-102, 31], [-102, 37], [-109, 37], [-109, 31]]]}
        >>> all_us_cities = FeatureCollection('d1349cc2d8854d998aa6da92dc2bd24')  # doctest: +SKIP
        >>> filtered_cities = all_us_cities.filter(properties=(p.name.like("S%")))  # doctest: +SKIP
        >>> filtered_cities = filtered_cities.filter(geometry=aoi_geometry)  # doctest: +SKIP
        >>> filtered_cities = filtered_cities.filter(properties=(p.area_land_meters > 1000))  # doctest: +SKIP

        """
        if geometry is not None and self._query_geometry is not None:
            # Oopsies; we're about to overwrite an existing geometry filter
            raise InvalidQueryException(
                "You cannot overwrite an existing geometry filter"
            )

        copied_fc = copy.deepcopy(self)

        if geometry is not None:
            copied_fc._query_geometry = getattr(geometry, "__geo_interface__", geometry)

        if properties is not None:
            if copied_fc._query_property_expression is None:
                copied_fc._query_property_expression = properties
            else:
                copied_fc._query_property_expression = (
                    copied_fc._query_property_expression & properties
                )

        return copied_fc

    def limit(self, limit):
        """
        Limit the number of ``Feature`` yielded in :meth:`features`.

        Parameters
        ----------
        limit : int
            The number of rows to limit the result to.

        Returns
        -------
        :class:`FeatureCollection`
            A new ``FeatureCollection`` with the given limit.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> fc = FeatureCollection('my-vector-product-id')  # doctest: +SKIP
        >>> fc = fc.limit(10)  # doctest: +SKIP

        """
        copied_fc = copy.deepcopy(self)
        copied_fc._query_limit = limit
        return copied_fc

    def features(self):
        """
        Iterate through each ``Feature`` in the ``FeatureCollection``, taking into
        account calls to :meth:`filter` and :meth:`limit`.

        A query or limit of some sort must be set, otherwise a ``BadRequestError``
        will be raised.

        The length of the returned iterator indicates the full query size.

        Returns
        -------
        `Iterator` which returns :class:`Feature <descarteslabs.vectors.feature.Feature>` and has a length.

        Raises
        ------
        ~descarteslabs.exceptions.BadRequestError
            Raised when the request is malformed, e.g. the limit is not a number.
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> fc = FeatureCollection("a35126a241bd022c026e96ab9fe5e0ea23967d08:USBuildingFootprints")  # doctest: +SKIP
        >>> features = fc.limit(10).features()  #doctest: +SKIP
        >>> print(len(features))  #doctest: +SKIP
        >>> for feature in features:  # doctest: +SKIP
        ...    print(feature)  # doctest: +SKIP
        """
        params = dict(
            product_id=self.id,
            geometry=self._query_geometry,
            query_expr=self._query_property_expression,
            query_limit=self._query_limit,
        )

        return _FeaturesIterator(self.vector_client.search_features(**params))

    # TODO: remove name from params
    @deprecate(removed=["name"])
    def update(
        self,
        name=None,
        title=None,
        description=None,
        owners=None,
        readers=None,
        writers=None,
    ):
        """
        Updates the attributes of the ``FeatureCollection``.

        Parameters
        ----------
        name : str, optional
            (Deprecated) Will be removed in future versions.
        title : str, optional
            A more verbose and expressive name for display purposes.
        description : str, optional
            Information about the ``FeatureCollection``, why it exists,
            and what it provides.
        owners : list(str), optional
            User, group, or organization IDs that own
            the FeatureCollection.  Defaults to
            [``current user``, ``current org``].
            The owner can edit and delete this ``FeatureCollection``.
        readers : list(str), optional
            User, group, or organization IDs that can read
            the ``FeatureCollection``.
        writers : list(str), optional
            User, group, or organization IDs that can edit
            the ``FeatureCollection`` (includes read permission).

        Raises
        ------
        ~descarteslabs.exceptions.BadRequestError
            Raised when the request is malformed, e.g. the owners list is missing prefixes.
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> attributes = dict(owners=['email:me@org.com'],
        ...    readers=['group:trusted'])
        >>> FeatureCollection('my-vector-product-id').update(**attributes)  # doctest: +SKIP

        """
        params = dict(
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers,
        )

        params = {k: v for k, v in params.items() if v is not None}

        response = self.vector_client.update_product(self.id, **params)
        self.__dict__.update(response["data"]["attributes"])

    # TODO: remove name from params
    @deprecate(required=["title", "description"], removed=["name"])
    def replace(
        self,
        name=None,
        title=None,
        description=None,
        owners=None,
        readers=None,
        writers=None,
    ):
        """
        Replaces the attributes of the ``FeatureCollection``.

        To change a single attribute, see :meth:`update`.

        Parameters
        ----------
        name : str, optional
            (Deprecated) Will be removed in future version.
        title : str
            (Required) A more verbose name for display purposes.
        description : str
            (Required) Information about the ``FeatureCollection``, why it exists,
            and what it provides.
        owners : list(str), optional
            User, group, or organization IDs that own
            the FeatureCollection.  Defaults to
            [``current user``, ``current org``].
            The owner can edit and delete this ``FeatureCollection``.
        readers : list(str), optional
            User, group, or organization IDs that can read
            the ``FeatureCollection``.
        writers : list(str), optional
            User, group, or organization IDs that can edit
            the ``FeatureCollection`` (includes read permission).

        Raises
        ------
        ~descarteslabs.exceptions.BadRequestError
            Raised when the request is malformed, e.g. the owners list is missing prefixes.
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.


        Example
        -------
        >>> attributes = dict(title='title',
        ...    description='description',
        ...    owners=['email:you@org.com'],
        ...    readers=['group:readers'],
        ...    writers=[])
        >>> FeatureCollection('my-vector-product-id').replace(**attributes)  # doctest: +SKIP
        """
        params = dict(
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers,
        )

        response = self.vector_client.replace_product(self.id, **params)
        self.__dict__.update(response["data"]["attributes"])

    def refresh(self):
        """
        Loads the attributes for the ``FeatureCollection``.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> FeatureCollection('my-vector-product-id').refresh()  # doctest: +SKIP

        """
        response = self.vector_client.get_product(self.id)
        self.__dict__.update(response.data.attributes)

    def delete(self):
        """
        Delete the ``FeatureCollection`` from the catalog.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> FeatureCollection('my-vector-product-id').delete()  # doctest: +SKIP

        """

        self.vector_client.delete_product(self.id)

    def add(self, features, fix_geometry="accept"):
        """
        Add multiple features to an existing ``FeatureCollection``.

        Parameters
        ----------
        features : ``Feature`` or list(``Feature``)
            A single feature or list of features to add. Collections
            of more than 100 features will be batched in groups of 100,
            but consider using upload() instead.
        fix_geometry : str
            String specifying how to handle certain problem geometries, including those
            which do not follow counter-clockwise winding order (which is required by the
            GeoJSON spec but not many popular tools). Allowed values are ``reject`` (reject
            invalid geometries with an error), ``fix`` (correct invalid geometries if
            possible and use this corrected value when creating the feature), and ``accept``
            (the default) which will correct the geometry for internal use but retain the
            original geometry in the results.

        Returns
        -------
        list(:class:`Feature <descarteslabs.vectors.feature.Feature>`)
            A copy of the given list of features that includes the ``id``.

        Raises
        ------
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.BadRequestError
            Raised when the request is malformed.  May also indicate that too many features were included.
            If more than 100 features were provided, some of these features may have been successfuly
            inserted while others may not have been inserted.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, Feature
        >>> polygon = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-95, 42],[-93, 42],[-93, 40],[-95, 41],[-95, 42]]]}
        >>> features = [Feature(geometry=polygon, properties={}) for _ in range(100)]
        >>> FeatureCollection('my-vector-product-id').add(features)  # doctest: +SKIP

        """
        if isinstance(features, Feature):
            features = [features]

        attributes = [
            {k: v for k, v in f.geojson.items() if k in ["properties", "geometry"]}
            for f in features
        ]

        documents = self.vector_client.create_features(
            self.id, attributes, fix_geometry=fix_geometry
        )

        copied_features = copy.deepcopy(features)

        for feature, doc in zip(copied_features, documents.data):
            feature.id = doc.id
            if fix_geometry == "fix":
                feature.geometry = shape(doc.attributes.geometry)

        return copied_features

    def upload(self, file_ref, max_errors=0, fix_geometry="accept"):
        """
        Asynchronously add features from a file of
        `Newline Delimited JSON <https://github.com/ndjson/ndjson-spec>`_
        features.  The file itself will be uploaded synchronously,
        but loading the features is done asynchronously.

        Parameters
        ----------
        file_ref : io.IOBase or str
            An open file object, or a path to the file to upload.
        max_errors : int
            The maximum number of errors permitted before declaring failure.
        fix_geometry : str
            String specifying how to handle certain problem geometries, including those
            which do not follow counter-clockwise winding order (which is required by the
            GeoJSON spec but not many popular tools). Allowed values are ``reject`` (reject
            invalid geometries with an error), ``fix`` (correct invalid geometries if
            possible and use this corrected value when creating the feature), and ``accept``
            (the default) which will correct the geometry for internal use but retain the
            original geometry in the results.

        Returns
        -------
        :class:`UploadTask <descarteslabs.common.tasks.uploadtask.UploadTask>`
            The upload task.  The details may take time to become available
            so asking for them before they're available will block
            until the details are available.

        Raises
        ------
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, Feature
        >>> fc = FeatureCollection('my-vector-product-id')   # doctest: +SKIP
        >>> task = fc.upload("/path/to/features.ndjson")    # doctest: +SKIP

        """
        upload_id = self.vector_client.upload_features(
            file_ref, self.id, max_errors=max_errors, fix_geometry=fix_geometry
        )
        return UploadTask(self.id, upload_id=upload_id, client=self.vector_client)

    def list_uploads(self, pending=True):
        """
        Get all the upload tasks for this product.

        Parameters
        ----------
        pending : bool
            If ``True`` then include pending/currently running upload tasks in the result,
            otherwise only include complete upload tasks. Defaults to ``True``.

        Returns
        -------
        list(:class:`UploadTask <descarteslabs.common.tasks.uploadtask.UploadTask>`)
            The list of tasks for the product.

        Raises
        ------
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, Feature
        >>> fc = FeatureCollection('my-vector-product-id')   # doctest: +SKIP
        >>> task = fc.upload("/path/to/features.ndjson")    # doctest: +SKIP
        >>> uploads = fc.list_uploads()   # doctest: +SKIP

        """
        results = []

        for result in self.vector_client.get_upload_results(self.id, pending=pending):
            # PENDING tasks aren't really tasks yet, and the id is the upload_id, not the task_id
            if result.attributes and result.attributes.get("status") == "PENDING":
                results.append(
                    UploadTask(
                        self.id,
                        upload_id=result.id,
                        result_attrs=result.attributes,
                        client=self.vector_client,
                    )
                )
            else:
                upload = UploadTask(
                    self.id,
                    tuid=result.id,
                    result_attrs=result.attributes,
                    client=self.vector_client,
                )
                # get_upload_results does not (and cannot) include the task result,
                # so force an update
                try:
                    upload.get_result(wait=False)
                except TransientResultError:
                    pass
                results.append(upload)

        return results

    @deprecate(renamed={"name": "product_id"})
    def copy(
        self, product_id, title, description, owners=None, readers=None, writers=None
    ):
        """
        Apply a filter to an existing product and create a new vector product in your catalog
        from the result, taking into account calls to ``filter`` and ``limit``.

        A query of some sort must be set, otherwise a ``BadRequestError`` will be raised.

        Copies occur asynchronously and can take a long time to complete.  Features
        will not be accessible in the new ``FeatureCollection`` until the copy completes.  Use
        :meth:`wait_for_copy` to block until the copy completes.

        Parameters
        ----------
        product_id : str
            A unique name for this product. In the created
            product a namespace consisting of your user id (e.g.
            "ae60fc891312ggadc94ade8062213b0063335a3c:") or your organization id (e.g.,
            "yourcompany:") will be prefixed to this, if it doesn't already have one, in
            order to make the id globally unique.
        title : str
            A more verbose and expressive name for display purposes.
        description : str
            Information about the ``FeatureCollection``, why it exists,
            and what it provides.
        owners : list(str), optional
            User, group, or organization IDs that own
            the newly created FeatureCollection.  Defaults to
            [``current user``, ``current org``].
            The owner can edit and delete this ``FeatureCollection``.
        readers : list(str), optional
            User, group, or organization IDs that can read
            the newly created ``FeatureCollection``.
        writers : list(str), optional
            User, group, or organization IDs that can edit
            the newly created ``FeatureCollection`` (includes read permission).

        Returns
        -------
        :class:`FeatureCollection`
            A new ``FeatureCollection``.

        Raises
        ------
        ~descarteslabs.exceptions.BadRequestError
            Raised when the request is malformed, e.g. no query was specified.
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, properties as p
        >>> aoi_geometry = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-109, 31], [-102, 31], [-102, 37], [-109, 37], [-109, 31]]]}
        >>> all_us_cities = FeatureCollection('d1349cc2d8854d998aa6da92dc2bd24')  # doctest: +SKIP
        >>> filtered_cities = all_us_cities.filter(properties=(p.name.like("S%")))  # doctest: +SKIP
        >>> filtered_cities = filtered_cities.filter(geometry=aoi_geometry)  # doctest: +SKIP
        >>> filtered_cities = filtered_cities.filter(properties=(p.area_land_meters > 1000))  # doctest: +SKIP
        >>> filtered_cities_fc = filtered_cities.copy(product_id='filtered-cities',
        ...    title='My Filtered US Cities Vector Collection',
        ...    description='A collection of cities in the US')  # doctest: +SKIP
        """
        params = dict(
            product_id=self.id,
            geometry=self._query_geometry,
            query_expr=self._query_property_expression,
            query_limit=self._query_limit,
            new_product_id=product_id,
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers,
        )

        return self._from_jsonapi(
            self.vector_client.create_product_from_query(**params).data
        )

    def wait_for_copy(self, timeout=None):
        """
        Wait for a copy operation to complete. Copies occur asynchronously
        and can take a long time to complete.  Features will not be accessible
        in the FeatureCollection until the copy completes.

        If the product was not created using a copy job, a ``BadRequestError`` is raised.
        If the copy job ran, but failed, a FailedJobError is raised.
        If a timeout is specified and the timeout is reached, a ``WaitTimeoutError`` is raised.

        Parameters
        ----------
        timeout : int
            Number of seconds to wait before the wait times out.  If not specified, will
            wait indefinitely.

        Raises
        ------
        ~descarteslabs.vectors.exceptions.FailedJobError
            Raised when the copy job fails to complete successfully.
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product or status cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.
        ~descarteslabs.vectors.exceptions.WaitTimeoutError
            Raised when the copy job doesn't complete before the timeout is reached.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, properties as p
        >>> aoi_geometry = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-109, 31], [-102, 31], [-102, 37], [-109, 37], [-109, 31]]]}
        >>> all_us_cities = FeatureCollection('d1349cc2d8854d998aa6da92dc2bd24')  # doctest: +SKIP
        >>> filtered_cities = all_us_cities.filter(properties=(p.name.like("S%")))  # doctest: +SKIP
        >>> filtered_cities_fc = filtered_cities.copy(product_id='filtered-cities',
        ...    title='My Filtered US Cities Vector Collection',
        ...    description='A collection of cities in the US')  # doctest: +SKIP
        >>> filtered_cities_fc.wait_for_copy(timeout=120)  # doctest: +SKIP
        """
        job = CopyJob(self.id, self.vector_client)
        job.wait_for_completion(timeout)

    def export(self, key):
        """
        Either export the full product, or the result of a filter chain.
        The exported geojson features will be stored as a ``data`` file
        in Descartes Labs Storage.

        The export will occur asynchronously and can take a long time
        to complete.  The data file will not be accessible until the export
        is complete.

        Parameters
        ----------
        key : str
            The name under which the export will be available in the Storage service.
            The ``storage_type`` will be ``data``.
            Note that this will overwrite any existing data if the key already exists.

        Returns
        -------
        :class:`ExportTask <descarteslabs.common.tasks.exporttask.ExportTask>`
            The export task.

        Raises
        ------
        ~descarteslabs.exceptions.BadRequestError
            Raised when the request is malformed, e.g. the query limit is not a number.
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.


        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, properties as p
        >>> from descarteslabs import Storage  # doctest: +SKIP
        >>> aoi_geometry = {
        ...    "type": "Polygon",
        ...    "coordinates": [[ # A small area in Washington DC
        ...        [-77.05501556396483, 38.90946877327506],
        ...        [-77.0419692993164, 38.90946877327506],
        ...        [-77.0419692993164, 38.91855139233948],
        ...        [-77.05501556396483, 38.91855139233948],
        ...        [-77.05501556396483, 38.90946877327506]
        ...     ]]
        ... }
        >>> buildings = FeatureCollection(
        ...     "a35126a241bd022c026e96ab9fe5e0ea23967d08:USBuildingFootprints")  # doctest: +SKIP
        >>> filtered_buildings = buildings.filter(geometry=aoi_geometry)  # doctest: +SKIP
        >>> task = filtered_buildings.export("my_export")  # doctest: +SKIP
        >>> if task.is_success: # This waits for the task to complete
        ...     task.get_file("some_local_file.geojson")  # doctest: +SKIP
        """

        params = dict(
            product_id=self.id,
            key=key,
            geometry=self._query_geometry,
            query_expr=self._query_property_expression,
            query_limit=self._query_limit,
        )

        task_id = self.vector_client.export_product_from_query(**params)
        return ExportTask(self.id, task_id, client=self.vector_client, key=key)

    def list_exports(self):
        """
        Get all the export tasks for this product.

        Returns
        -------
        list(:class:`ExportTask <descarteslabs.common.tasks.exporttask.ExportTask>`)
            The list of tasks for the product.

        Raises
        ------
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, properties as p
        >>> aoi_geometry = {
        ...    "type": "Polygon",
        ...    "coordinates": [[ # A small area in Washington DC
        ...        [-77.05501556396483, 38.90946877327506],
        ...        [-77.0419692993164, 38.90946877327506],
        ...        [-77.0419692993164, 38.91855139233948],
        ...        [-77.05501556396483, 38.91855139233948],
        ...        [-77.05501556396483, 38.90946877327506]
        ...     ]]
        ... }
        >>> buildings = FeatureCollection(
        ...     "a35126a241bd022c026e96ab9fe5e0ea23967d08:USBuildingFootprints")  # doctest: +SKIP
        >>> filtered_buildings = buildings.filter(geometry=aoi_geometry)  # doctest: +SKIP
        >>> task = filtered_buildings.export("my_export")  # doctest: +SKIP
        >>> exports = filtered_buildings.list_exports()   # doctest: +SKIP

        """
        results = []

        for result in self.vector_client.get_export_results(self.id):
            results.append(
                ExportTask(
                    self.id,
                    tuid=result.id,
                    result_attrs=result.attributes,
                    client=self.vector_client,
                )
            )

        return results

    def delete_features(self):
        """
        Apply a filter to a product and delete features that match the filter criteria,
        taking into account calls to :meth:`filter`.  Cannot be used with
        calls to :meth:`limit`

        A query of some sort must be set, otherwise a ``BadRequestError`` will be raised.

        Delete jobs occur asynchronously and can take a long time to complete. You
        can access :meth:`features` while a delete job is running,
        but you cannot issue another :meth:`delete_features` until
        the current job has completed running.  Use
        :meth:`DeleteJob.wait_for_completion() <descarteslabs.vectors.async_job.DeleteJob.wait_for_completion>`
        to block until the job is done.

        Returns
        -------
        :class:`DeleteJob <descarteslabs.vectors.async_job.DeleteJob>`
            A new `DeleteJob`.

        Raises
        ------
        ~descarteslabs.exceptions.BadRequestError
            Raised when the request is malformed, e.g. the query limit is not a number.
        ~descarteslabs.vectors.exceptions.InvalidQueryException
            Raised when a limit was applied to the ``FeatureCollection``.
        ~descarteslabs.exceptions.NotFoundError
            Raised if the product cannot be found.
        ~descarteslabs.exceptions.RateLimitError
            Raised when too many requests have been made within a given time period.
        ~descarteslabs.exceptions.ServerError
            Raised when a unknown error occurred on the server.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> aoi_geometry = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-109, 31], [-102, 31], [-102, 37], [-109, 37], [-109, 31]]]}
        >>> fc = FeatureCollection('my-vector-product-id')  # doctest: +SKIP
        >>> fc.filter(geometry=aoi_geometry)  # doctest: +SKIP
        >>> delete_job = fc.delete_features()  # doctest: +SKIP
        >>> delete_job.wait_for_completion()  # doctest: +SKIP
        """
        if self._query_limit:
            raise InvalidQueryException("limits cannot be used when deleting features")

        params = dict(
            product_id=self.id,
            geometry=self._query_geometry,
            query_expr=self._query_property_expression,
        )

        product = self.vector_client.delete_features_from_query(**params).data
        return DeleteJob(product.id)

    def _repr_json_(self):
        return DotDict(
            (k, v)
            for k, v in self.__dict__.items()
            if k in FeatureCollection.ATTRIBUTES
        )

    def __repr__(self):
        return "FeatureCollection({})".format(repr(self._repr_json_()))

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k in ["_vector_client"]:
                setattr(result, k, v)
            else:
                setattr(result, k, copy.deepcopy(v, memo))
        return result
