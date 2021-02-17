from ...cereal import serializable
from ..primitives import Any, Str
from ..containers import List, Struct
from .mixins import GeometryMixin

GeometryStruct = Struct[{"type": Str, "coordinates": List[Any]}]


@serializable(is_named_concrete_type=True)
class Geometry(GeometryStruct, GeometryMixin):
    """Proxy GeoJSON Geometry representing a geometry's type and coordinates.

    Examples
    --------
    >>> from descarteslabs.workflows import Geometry
    >>> geom = Geometry(type="Point", coordinates=[1, 2])
    >>> geom
    <descarteslabs.workflows.types.geospatial.geometry.Geometry object at 0x...>
    >>> geom.compute() # doctest: +SKIP
    GeometryResult(type=Point, coordinates=[1, 2])

    >>> # constructing same Geometry as previous example, but using from_geojson
    >>> from descarteslabs.workflows import Geometry
    >>> geojson = {"type": "Point", "coordinates": [1, 2]}
    >>> geom = Geometry.from_geojson(geojson)
    >>> geom.compute().__geo_interface__ # doctest: +SKIP
    {'type': 'Point', 'coordinates': [1, 2]}
    """

    _constructor = "wf.Geometry.create"

    def __init__(self, type, coordinates):
        return super(Geometry, self).__init__(type=type, coordinates=coordinates)

    @classmethod
    def from_geo_interface(cls, obj):
        """
        Construct a Workflows Geometry from a __geo_interface__.

        Parameters
        ----------
        obj: object with a __geo_interface__ attribute
            See https://gist.github.com/sgillies/2217756 for information about
            the ``__geo_interface__`` attribute

        Returns
        -------
        ~descarteslabs.workflows.Geometry

        Example
        -------
        >>> from descarteslabs import vectors
        >>> from descarteslabs.workflows import Geometry
        >>> polygon = { 'type': 'Polygon', 'coordinates': [[[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]]}
        >>> feat = vectors.Feature(geometry=polygon, properties={})
        >>> feat.__geo_interface__ # doctest: +SKIP
        {'type': 'Polygon',
         'coordinates': (((-95.0, 42.0),
           (-93.0, 42.0),
           (-93.0, 40.0),
           (-95.0, 41.0),
           (-95.0, 42.0)),)}
        >>> Geometry.from_geo_interface(feat).compute() # doctest: +SKIP
        GeometryResult(type=Polygon, coordinates=[[[-95.0, 42.0], [-93.0, 42.0],
        ... [-93.0, 40.0], [-95.0, 41.0], [-95.0, 42.0]]])
        """
        try:
            geo_interface = obj.__geo_interface__
        except AttributeError:
            raise TypeError(
                "Expected an object with a `__geo_interface__` attribute, not {}".format(
                    obj
                )
            )

        return cls.from_geojson(geo_interface)

    @classmethod
    def from_geojson(cls, geojson):
        """
        Construct a Workflows Geometry from a GeoJSON mapping.

        Note that the GeoJSON must be relatively small (under 10MiB of serialized JSON).

        Parameters
        ----------
        geojson: Dict

        Returns
        -------
        ~descarteslabs.workflows.Geometry

        Example
        -------
        >>> from descarteslabs.workflows import Geometry
        >>> geojson = {"type": "Point", "coordinates": [1, 2]}
        >>> geom = Geometry.from_geojson(geojson)
        >>> geom.compute().__geo_interface__ # doctest: +SKIP
        {'type': 'Point', 'coordinates': [1, 2]}
        """
        try:
            return cls._from_apply(
                cls._constructor,
                type=geojson["type"],
                coordinates=geojson["coordinates"],
            )
        except KeyError:
            raise ValueError(
                "Expected a GeoJSON mapping containing the fields 'type' and 'coordinates', "
                "but got {}".format(geojson)
            )

    @classmethod
    def _promote(cls, obj):
        if hasattr(obj, "__geo_interface__"):
            return cls.from_geo_interface(obj)
        if isinstance(obj, dict):
            return cls.from_geojson(obj)
        return super(Geometry, cls)._promote(obj)
