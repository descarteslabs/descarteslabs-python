from ... import env

from ...cereal import serializable
from ..core import typecheck_promote
from ..primitives import Any, Str, Int, Float
from ..containers import List, Struct
from .mixins import GeometryMixin

GeometryStruct = Struct[{"type": Str, "coordinates": List[Any]}]


@serializable(is_named_concrete_type=True)
class Geometry(GeometryStruct, GeometryMixin):
    _constructor = "Geometry.create"

    @classmethod
    def from_geo_interface(cls, obj):
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

    @typecheck_promote((Int, Float))
    def rasterize(self, value=1):
        """
        Rasterize this Geometry into an `~.geospatial.Image`

        Parameters
        ----------
        value: Int, Float, default=1
            Fill pixels within the Geometry with this value.
            Pixels outside the Geometry will be masked, and set to 0.

        Note
        ----
        Rasterization happens according to the `~.workflows.types.geospatial.GeoContext`
        of the `.Job`, so the geometry is projected into and rasterized at
        that CRS and resolution.

        Returns
        -------
        rasterized: ~.geospatial.Image
            An Image with 1 band named ``"features"``, and empty properties and bandinfo.
        """
        from .image import Image

        return Image._from_apply("rasterize", self, value, env.geoctx)


GeometryCollectionStruct = Struct[{"type": Str, "geometries": List[Geometry]}]


@serializable(is_named_concrete_type=True)
class GeometryCollection(GeometryCollectionStruct, Geometry):
    _constructor = "GeometryCollection.create"

    @classmethod
    def from_geojson(cls, geojson):
        try:
            return cls._from_apply(
                cls._constructor, type=geojson["type"], geometries=geojson["geometries"]
            )
        except KeyError:
            raise ValueError(
                "Expected a GeoJSON mapping containing the fields 'type' and 'geometries', "
                "but got {}".format(geojson)
            )
