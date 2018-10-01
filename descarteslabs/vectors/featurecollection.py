from descarteslabs.common.dotdict import DotDict
from descarteslabs.client.services.vector import Vector
from descarteslabs.vectors.feature import Feature
import six

import copy


MAX_RESULT_WINDOW = 10000


class FeatureCollection(object):
    """
    A proxy object for accesssing millions of features within a collection
    having similar access controls, geometries and properties.

    If creating a new `FeatureCollection` use `FeatureCollection.create` instead.

    Features will not be retreived from the `FeatureCollection` until
    `FeatureCollection.features()` is called.

    Attributes
    ----------
    name : str, A name for this FeatureCollection
    title : str, Official title
    description : str, Information about the FeatureCollection, why it exists, and what it provides
    owners : list(str), User, group, or organization IDs that own this FeatureCollection.
    readers : list(str), User, group, or organization IDs that can read this FeatureCollection.
    writers : list(str), User, group, or organization IDs that can edit this FeatureCollection.
    """

    ATTRIBUTES = ['owners', 'writers', 'readers', 'id', 'name', 'title', 'description']

    def __init__(self, id=None, name=None, vector_client=None):
        if id is not None and name is not None:
            raise ValueError("You cannot provide both a 'id' and an 'name' to instantiate this class")

        self.id = id
        self._name = name
        self._vector_client = vector_client

        self._query_geometry = None
        self._query_limit = MAX_RESULT_WINDOW
        self._query_property_expression = None

        self.refresh()

    @classmethod
    def _from_jsonapi(cls, response, vector_client=None):
        self = cls(response.id, vector_client=vector_client)
        self.__dict__.update(response.attributes)

        return self

    @classmethod
    def by_id(cls, product_id, vector_client=None):
        """Retrieve a vector product by id"""
        return cls(id=product_id, vector_client=vector_client)

    @classmethod
    def by_name(cls, product_name, vector_client=None):
        """Retrieve a vector product by name"""
        return cls(name=product_name, vector_client=vector_client)

    @classmethod
    def create(cls, name, title, description, owners=None, readers=None, writers=None, vector_client=None):
        """
        Create a vector product in your catalog.

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> FeatureCollection.create(name='foo',
        ...    title='My Foo Vectory Collection',
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
            Include features intersecting this geometry. If this FeatureCollection is already filtered by a geometry,
            the new geometry will override it -- they cannot be chained.
        properties : descarteslabs.common.property_filtering.Expression
            Include features having properties where the expression evaluates as ``True``
            E.g ``properties=(p.temperature >= 50) & (p.hour_of_day > 18)``, or even more complicated expressions like
            ``properties=(100 > p.temperature >= 50) | ((p.month != 10) & (p.day_of_month > 14))``
            If you supply a property which doesn't exist as part of the expression
            that comparison will evaluate to False.

        Returns
        -------
        FeatureCollection

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection, properties as p
        >>> aoi_geometry = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-95, 42],[-93, 42],[-93, 40],[-95, 41],[-95, 42]]]}
        >>> all_feautures = FeatureCollection('foo')
        >>> cold_features = all_feautures.filter(properties=(p.temperature <= 50))
        >>> cold_features_in_aoi = cold_features.filter(geometry=aoi_geometry)
        >>> cold_features_in_aoi_red = cold_features.filter(properties=(p.color == 'red'))

        """
        self = copy.deepcopy(self)

        if geometry is not None:
            self._query_geometry = getattr(geometry, '__geo_interface__', geometry)

        if properties is not None:
            if self._query_property_expression is None:
                self._query_property_expression = properties
            else:
                self._query_property_expression = self._query_property_expression & properties

        return self

    def limit(self, limit):
        """
        Limit the number of `Feature` yielded in `FeatureCollection.features`.

        Returns
        -------
        FeatureCollection

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> fc = FeatureCollection('foo')
        >>> fc = fc.limit(10)

        """
        self = copy.deepcopy(self)
        self._query_limit = limit
        return self

    def features(self):
        """
        Iterate through each `Feature` in the `FeatureCollection`, taking into
        account calls to `FeatureCollection.filter` and `FeatureCollection.limit`.

        A query of some sort must be set, otherwise a BadRequestError will be raised.

        Yields
        ------
        `Feature`

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> fc = FeatureCollection('foo')
        >>> feature_ids = []
        >>> for feature in fc.features():  # doctest: +SKIP
        ...    print(feature)  # doctest: +SKIP
        """
        params = dict(
            product_id=self.id,
            geometry=self._query_geometry,
            query_expr=self._query_property_expression,
            limit=self._query_limit
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

        Example
        -------
        >>> attributes = dict(name='name',
        ...    owners=['owners'],
        ...    readers=['readers'])
        >>> FeatureCollection('foo').update(**attributes)  # doctest: +SKIP

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
        self.__dict__.update(response['attributes'])

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

        To change a single attribute, see `FeatureCollection.update`.

        Example
        -------
        >>> attributes = dict(name='name',
        ...    title='title',
        ...    description='description',
        ...    owners=['owners'],
        ...    readers=['readers'],
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
        self.__dict__.update(response['attributes'])

    @classmethod
    def list(cls, limit=100, page=1, vector_client=None):
        """
        Returns all `FeatureCollection` that you have access to.

        Returns
        -------
        list(`FeatureCollection`)

        Example
        -------
        >>> from descarteslabs.vectors import FeatureCollection
        >>> FeatureCollection.list()  # doctest: +SKIP
        """

        params = dict(
            limit=limit,
            page=page
        )

        if vector_client is None:
            vector_client = Vector()

        response = vector_client.list_products(**params)

        return [cls._from_jsonapi(fc, vector_client) for fc in response.data]

    def refresh(self):
        """
        Loads the attributes for the `FeatureCollection`.

        It always uses the 'id' attribute to load the FeatureCollection
        except during instantiation, at which time it may use the name.
        """
        if self.id is not None:
            response = self.vector_client.get_product(self.id)
        elif self._name is not None:
            response = self.vector_client.get_product_by_name(self._name)
        else:
            raise ValueError("You must provide either a 'name' or an 'id' to instantiate this class")

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

        Returns
        -------
        list(`Feature`)

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
