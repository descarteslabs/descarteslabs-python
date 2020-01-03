from ... import env

from ...cereal import serializable
from ..core import typecheck_promote
from ..primitives import Any, Str, Int, Float
from ..containers import Dict, Struct
from .geometry import Geometry
from .mixins import GeometryMixin

FeatureStruct = Struct[{"properties": Dict[Str, Any], "geometry": Geometry}]


@serializable(is_named_concrete_type=True)
class Feature(FeatureStruct, GeometryMixin):
    "Proxy Feature representing a `Geometry` and a `Dict` of properties."

    _constructor = "Feature.create"

    @classmethod
    def from_geojson(cls, geojson):
        """
        Construct a Workflows Feature from a GeoJSON mapping.

        Note that the GeoJSON must be relatively small (under 10MiB of serialized JSON).

        Parameters
        ----------
        geojson: Dict

        Returns
        -------
        ~descarteslabs.workflows.Feature
        """
        try:
            if geojson["type"].lower() != "feature":
                raise ValueError(
                    "Expected a GeoJSON Feature type, not {!r}".format(geojson["type"])
                )
            # Embed the JSON directly in the graft
            # Note this bypasses any validation in Geometry and assumes properties contains no Proxytypes
            return cls._from_apply(
                cls._constructor,
                geometry=geojson["geometry"],
                properties=geojson["properties"],
            )
        except KeyError:
            raise ValueError(
                "Expected a GeoJSON mapping containing the fields 'type', 'geometry' and 'properties', "
                "but got {}".format(geojson)
            )

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, dict):
            return cls.from_geojson(obj)
        return super(Feature, cls)._promote(obj)

    @typecheck_promote(value=(Int, Float, Str), default_value=(Int, Float))
    def rasterize(self, value=1, default_value=1):
        """
        Rasterize this `Feature` into an `~.geospatial.Image`

        Parameters
        ----------
        value: Int, Float, Str, default=1
            Fill pixels within the `Feature` with this value.
            Pixels outside the `Feature` will be masked, and set to 0.
            If a string, it will look up that key in ``self.properties``;
            the value there must be a number.
        default_value: Int, Float, default=1
            Value to use if ``value`` is a string and the key does
            not exist in ``self.properties``

        Notes
        -----
        Rasterization happens according to the `~.workflows.types.geospatial.GeoContext`
        of the `.Job`, so the geometry is projected into and rasterized at
        that CRS and resolution.

        Returns
        -------
        rasterized: ~.geospatial.Image
            An Image with 1 band named ``"features"``, the same properties
            as this `Feature`, and empty bandinfo.
        """
        from .image import Image

        return Image._from_apply(
            "rasterize", self, value, env.geoctx, default_value=default_value
        )
