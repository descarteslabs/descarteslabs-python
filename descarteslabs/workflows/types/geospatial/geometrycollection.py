from ...cereal import serializable
from ..core import typecheck_promote
from ..primitives import Str, Int, Float
from ..containers import List, Struct, CollectionMixin
from .geometry import Geometry
from .mixins import GeometryMixin

GeometryCollectionStruct = Struct[{"type": Str, "geometries": List[Geometry]}]


@serializable(is_named_concrete_type=True)
class GeometryCollection(GeometryCollectionStruct, GeometryMixin, CollectionMixin):
    """Proxy GeoJSON GeometryCollection constructed from a sequence of Geometries.

    Examples
    --------
    >>> from descarteslabs.workflows import Geometry, GeometryCollection
    >>> geom = Geometry(type="Point", coordinates=[1, 2])
    >>> gc = GeometryCollection(geometries=[geom, geom, geom])
    >>> gc
    <descarteslabs.workflows.types.geospatial.geometrycollection.GeometryCollection object at 0x...>
    >>> gc.compute() # doctest: +SKIP
    GeometryCollectionResult(type=GeometryCollection,
            geometries=(
                GeometryResult(type=Point, coordinates=[1, 2]),
                GeometryResult(type=Point, coordinates=[1, 2]),
                GeometryResult(type=Point, coordinates=[1, 2])))

    >>> # constructing similar GeometryCollection to previous example, but using from_geojson
    >>> from descarteslabs.workflows import GeometryCollection
    >>> geojson = {"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [1, 2]}]}
    >>> gc = GeometryCollection.from_geojson(geojson)
    >>> gc.compute().__geo_interface__ # doctest: +SKIP
    {'type': 'GeometryCollection', 'geometries': [{'type': 'Point', 'coordinates': [1, 2]}]}
    """

    _constructor = "wf.GeometryCollection.create"
    _element_type = Geometry

    def __init__(self, geometries, type="GeometryCollection"):
        return super(GeometryCollection, self).__init__(
            type=type, geometries=geometries
        )

    @classmethod
    def from_geojson(cls, geojson):
        """
        Construct a Workflows GeometryCollection from a GeoJSON mapping.

        Note that the GeoJSON must be relatively small (under 10MiB of serialized JSON).

        Parameters
        ----------
        geojson: Dict

        Returns
        -------
        ~descarteslabs.workflows.GeometryCollection

        Example
        -------
        >>> from descarteslabs.workflows import GeometryCollection
        >>> geojson = {"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [1, 2]}]}
        >>> gc = GeometryCollection.from_geojson(geojson)
        >>> gc.compute().__geo_interface__ # doctest: +SKIP
        {'type': 'GeometryCollection', 'geometries': [{'type': 'Point', 'coordinates': [1, 2]}]}
        """
        try:
            return cls._from_apply(
                cls._constructor, type=geojson["type"], geometries=geojson["geometries"]
            )
        except KeyError:
            raise ValueError(
                "Expected a GeoJSON mapping containing the fields 'type' and 'geometries', "
                "but got {}".format(geojson)
            )

    @classmethod
    def _promote(cls, obj):
        if hasattr(obj, "__geo_interface__"):
            return cls.from_geo_interface(obj)
        if isinstance(obj, dict):
            return cls.from_geojson(obj)
        return super()._promote(obj)

    @typecheck_promote((Int, Float))
    def buffer(self, distance):
        """
        Take the envelope of all the geometries, and buffer that by a given distance.

        Parameters
        ----------
        distance: Int or Float
            The distance (in decimal degrees) to buffer the area around the Geometry.

        Returns
        -------
        ~descarteslabs.workflows.Geometry

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> geom = wf.Geometry(type="Point", coordinates=[1, 2])
        >>> gc = wf.GeometryCollection(geometries=[geom, geom, geom])
        >>> gc.buffer(2)
        <descarteslabs.workflows.types.geospatial.geometry.Geometry object at 0x...>
        """
        return Geometry._from_apply("wf.buffer", self, distance)

    def length(self):
        """Length is equivalent to the Python ``len`` operator.

        Returns
        -------
        Int
            An Int Proxytype

        Example
        -------
        >>> from descarteslabs.workflows import List, Int
        >>> my_list = List[Int]([1, 2, 3])
        >>> my_list.length().compute() # doctest: +SKIP
        3
        """
        return Int._from_apply("wf.length", self)

    def __reversed__(self):
        return self._from_apply("wf.reversed", self)
