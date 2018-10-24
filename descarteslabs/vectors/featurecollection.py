from descarteslabs.common.dotdict import DotDict
from descarteslabs.client.exceptions import NotFoundError
from descarteslabs.common.tasks import UploadTask
from descarteslabs.client.services.vector import Vector
from descarteslabs.vectors.feature import Feature
import six

import copy


class FeatureCollection(object):
    """
    A proxy object for accesssing millions of features within a collection
    having similar access controls, geometries and properties.  Such a
    grouping is named a ``product`` and identified by ``id``.

    If creating a new `FeatureCollection` use `FeatureCollection.create()`
    instead.

    Features will not be retrieved from the `FeatureCollection` until
    `FeatureCollection.features()` is called.

    Attributes
    ----------
    id : str
        The unique identifier for this `FeatureCollection`.
    name : str
        A short name without spaces (like a handle).
    title : str
        A more verbose and expressive name for display purposes.
    description : str
        Information about the `FeatureCollection`, why it exists,
        and what it provides.
    owners : list(str)
        User, group, or organization IDs that own
        this FeatureCollection.  Defaults to
        [``user:current_user``, ``org:current_org``].
        The owner can edit, delete, and change access to
        this `FeatureCollection`.
    readers : list(str)
        User, group, or organization IDs that can read
        this `FeatureCollection`.
    writers : list(str)
        User, group, or organization IDs that can edit
        this `FeatureCollection` (includes read permission).

    Note
    ----
    All ``owner``, ``reader``, and ``writer`` IDs must be prefixed with
    ``email:``, ``user:``, ``group:`` or ``org:``.  Using ``org:`` as an
    ``owner`` will assign those privileges only to administrators for
    that organization; using ``org:`` as a ``reader`` or ``writer``
    assigns those privileges to everyone in that organization.
    """

    ATTRIBUTES = ['owners', 'writers', 'readers', 'id', 'name', 'title', 'description']

    def __init__(self, id=None, vector_client=None):

        self.id = id
        self._vector_client = vector_client

        self._query_geometry = None
        self._query_limit = None
        self._query_property_expression = None

        self.refresh()

    @classmethod
    def _from_jsonapi(cls, response, vector_client=None):
        self = cls(response.id, vector_client=vector_client)
        self.__dict__.update(response.attributes)

        return self

    @classmethod
    def create(cls, name, title, description, owners=None, readers=None, writers=None, vector_client=None):
        """
        Create a vector product in your catalog.

        Parameters
        ----------
        name : str
            A short name without spaces (like a handle).
        title : str
            A more verbose and expressive name for display purposes.
        description : str
            Information about the `FeatureCollection`, why it exists,
            and what it provides.
        owners : list(str), optional
            User, group, or organization IDs that own
            the newly created FeatureCollection.  Defaults to
            [``current user``, ``current org``].
            The owner can edit and delete this `FeatureCollection`.
        readers : list(str), optional
            User, group, or organization IDs that can read
            the newly created `FeatureCollection`.
        writers : list(str), optional
            User, group, or organization IDs that can edit
            the newly created `FeatureCollection` (includes read permission).

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> FeatureCollection.create(name='foo',
        ...    title='My Foo Vector Collection',
        ...    description='Just a test')  # doctest: +SKIP
        """
        params = dict(
            name=name,
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers,
        )

        if vector_client is None:
            vector_client = Vector()

        return cls._from_jsonapi(vector_client.create_product(**params).data, vector_client)

    @classmethod
    def list(cls, vector_client=None):
        """
        List all `FeatureCollection` products that you have access to.

        Returns
        -------
        list(`FeatureCollection`)
            A list of all products that you have access to.

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
            partial_list = [cls._from_jsonapi(fc, vector_client)
                            for fc in response.data]
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

    def filter(self, geometry=None, properties=None):
        """
        Include only the features matching the given geometry and properties.
        Filters are not evaluated until iterating over the FeatureCollection,
        and can be chained by calling filter multiple times.

        Parameters
        ----------
        geometry: GeoJSON-like dict, object with ``__geo_interface__``; optional
            Include features intersecting this geometry. If this
            FeatureCollection is already filtered by a geometry,
            the new geometry will override it -- they cannot be chained.
        properties : descarteslabs.common.property_filtering.Expression
            Include features having properties where the expression
            evaluates as ``True``
            E.g ``properties=(p.temperature >= 50) & (p.hour_of_day > 18)``,
            or even more complicated expressions like
            ``properties=(100 > p.temperature >= 50) | ((p.month != 10) & (p.day_of_month > 14))``
            If you supply a property which doesn't exist as part of the
            expression that comparison will evaluate to False.

        Returns
        -------
        vectors.FeatureCollection
            A new `FeatureCollection` with the given filter.

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
        copied_fc = copy.deepcopy(self)

        if geometry is not None:
            copied_fc._query_geometry = getattr(geometry, '__geo_interface__', geometry)

        if properties is not None:
            if copied_fc._query_property_expression is None:
                copied_fc._query_property_expression = properties
            else:
                copied_fc._query_property_expression = copied_fc._query_property_expression & properties

        return copied_fc

    def limit(self, limit):
        """
        Limit the number of `Feature` yielded in `FeatureCollection.features()`.

        Parameters
        ----------
        limit : int
            The number of rows to limit the result to.

        Returns
        -------
        vectors.FeatureCollection
            A new `FeatureCollection` with the given limit.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> fc = FeatureCollection('d1349cc2d8854d998aa6da92dc2bd24')  # doctest: +SKIP
        >>> fc = fc.limit(10)  # doctest: +SKIP

        """
        copied_fc = copy.deepcopy(self)
        copied_fc._query_limit = limit
        return copied_fc

    def features(self):
        """
        Iterate through each `Feature` in the `FeatureCollection`, taking into
        account calls to `FeatureCollection.filter()`
        and `FeatureCollection.limit()`.

        A query of some sort must be set, otherwise a BadRequestError will be raised.

        Yields
        ------
        `Feature`

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> fc = FeatureCollection('d1349cc2d8854d998aa6da92dc2bd24')  # doctest: +SKIP
        >>> for feature in fc.features():  # doctest: +SKIP
        ...    print(feature)  # doctest: +SKIP
        """
        params = dict(
            product_id=self.id,
            geometry=self._query_geometry,
            query_expr=self._query_property_expression,
            query_limit=self._query_limit,
        )

        for response in self.vector_client.search_features(**params):
            yield Feature._create_from_jsonapi(response)

    def update(self,
               name=None,
               title=None,
               description=None,
               owners=None,
               readers=None,
               writers=None):
        """
        Updates the attributes of the `FeatureCollection`.

        Parameters
        ----------
        name : str, optional
            A short name without spaces (like a handle).
        title : str, optional
            A more verbose and expressive name for display purposes.
        description : str, optional
            Information about the `FeatureCollection`, why it exists,
            and what it provides.
        owners : list(str), optional
            User, group, or organization IDs that own
            the FeatureCollection.  Defaults to
            [``current user``, ``current org``].
            The owner can edit and delete this `FeatureCollection`.
        readers : list(str), optional
            User, group, or organization IDs that can read
            the `FeatureCollection`.
        writers : list(str), optional
            User, group, or organization IDs that can edit
            the `FeatureCollection` (includes read permission).

        Example
        -------
        >>> attributes = dict(name='name',
        ...    owners=['email:me@org.com'],
        ...    readers=['group:trusted'])
        >>> FeatureCollection('d1349cc2d8854d998aa6da92dc2bd24').update(**attributes)  # doctest: +SKIP

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

        response = self.vector_client.update_product(self.id, **params)
        self.__dict__.update(response['data']['attributes'])

    def replace(
            self,
            name,
            title,
            description,
            owners=None,
            readers=None,
            writers=None,
    ):
        """
        Replaces the attributes of the `FeatureCollection`.

        To change a single attribute, see `FeatureCollection.update()`.

        Parameters
        ----------
        name : str
            A short name without spaces (like a handle).
        title : str
            A more verbose name for display purposes.
        description : str
            Information about the `FeatureCollection`, why it exists,
            and what it provides.
        owners : list(str), optional
            User, group, or organization IDs that own
            the FeatureCollection.  Defaults to
            [``current user``, ``current org``].
            The owner can edit and delete this `FeatureCollection`.
        readers : list(str), optional
            User, group, or organization IDs that can read
            the `FeatureCollection`.
        writers : list(str), optional
            User, group, or organization IDs that can edit
            the `FeatureCollection` (includes read permission).

        Example
        -------
        >>> attributes = dict(name='name',
        ...    title='title',
        ...    description='description',
        ...    owners=['email:you@org.com'],
        ...    readers=['group:readers'],
        ...    writers=[])
        >>> FeatureCollection('foo').replace(**attributes)  # doctest: +SKIP
        """

        params = dict(
            name=name,
            title=title,
            description=description,
            owners=owners,
            readers=readers,
            writers=writers,
        )

        response = self.vector_client.replace(self.id, **params)
        self.__dict__.update(response['data']['attributes'])

    def refresh(self):
        """
        Loads the attributes for the `FeatureCollection`.

        """
        response = self.vector_client.get_product(self.id)
        self.__dict__.update(response.data.attributes)

    def delete(self):
        """
        Delete the `FeatureCollection` from the catalog.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> FeatureCollection('foo').delete()  # doctest: +SKIP

        """

        self.vector_client.delete_product(self.id)

    def add(self, features):
        """
        Add multiple features to an existing `FeatureCollection`.

        Parameters
        ----------
        features : `Feature` or list(`Feature`)
            A single feature or list of features to add.

        Returns
        -------
        list(`Feature`)
            A copy of the given list of features that includes the ``id``.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, Feature
        >>> polygon = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-95, 42],[-93, 42],[-93, 40],[-95, 41],[-95, 42]]]}
        >>> features = [Feature(geometry=polygon, properties={}) for _ in range(100)]
        >>> FeatureCollection('foo').add(features)  # doctest: +SKIP

        """
        if isinstance(features, Feature):
            features = [features]

        attributes = [{k: v for k, v in f.geojson.items() if k in ['properties', 'geometry']} for f in features]

        documents = self.vector_client.create_features(self.id, attributes)

        copied_features = copy.deepcopy(features)

        for feature, doc in zip(copied_features, documents.data):
            feature.id = doc.id

        return copied_features

    def upload(self, file_ref):
        """
        Asynchonously add features from a file of
        `Newline Delimited JSON <https://github.com/ndjson/ndjson-spec>`_
        features.  The file itself will be uploaded synchronously,
        but loading the features is done asynchronously.

        Parameters
        ----------
        file_ref : io.IOBase or str
            An open file object, or a path to the file to upload.

        Returns
        -------
        `UploadTask`
            The upload task.  The details may take time to become available
            so asking for them before they're available will block
            until the details are available.
        """
        upload_id = self.vector_client.upload_features(file_ref, self.id)
        return UploadTask(self.id, upload_id=upload_id,
                          client=self.vector_client)

    def list_uploads(self):
        """
        Get all the upload tasks for this product.

        Returns
        -------
        list(`UploadTask`)
            The list of tasks for the product.
        """
        results = []

        for result in self.vector_client.get_upload_results(self.id):
            results.append(UploadTask(self.id, tuid=result.id,
                                      result_attrs=result.attributes,
                                      client=self.vector_client))

        return results

    def _repr_json_(self):
        return DotDict((k, v) for k, v in self.__dict__.items() if k in FeatureCollection.ATTRIBUTES)

    def __repr__(self):
        return "FeatureCollection({})".format(repr(self._repr_json_()))

    def __deepcopy__(self, memo):
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k in ['_vector_client']:
                setattr(result, k, v)
            else:
                setattr(result, k, copy.deepcopy(v, memo))
        return result
