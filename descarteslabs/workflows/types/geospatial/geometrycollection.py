from ...cereal import serializable
from ..core import typecheck_promote
from ..primitives import Str, Int, Float
from ..containers import List, Struct
from .geometry import Geometry

GeometryCollectionStruct = Struct[{"type": Str, "geometries": List[Geometry]}]


@serializable(is_named_concrete_type=True)
class GeometryCollection(GeometryCollectionStruct, Geometry):
    "Proxy GeometryCollection constructed from a sequence of Geometries."

    _constructor = "GeometryCollection.create"

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
        """
        return Geometry._from_apply("buffer", self, distance)
