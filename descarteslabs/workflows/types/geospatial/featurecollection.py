from ... import env
from ...cereal import serializable
from ..core import typecheck_promote
from ..primitives import Str, Int, Float
from ..containers import List, Struct, CollectionMixin
from .feature import Feature

FeatureCollectionStruct = Struct[{"features": List[Feature]}]


@serializable(is_named_concrete_type=True)
class FeatureCollection(FeatureCollectionStruct, CollectionMixin):
    _constructor = "FeatureCollection.create"
    _element_type = Feature

    @typecheck_promote(List[Feature])
    def __init__(self, features):
        "Construct a FeatureCollection from a sequence of Features"
        super(FeatureCollection, self).__init__(features=features)

    @classmethod
    @typecheck_promote(Str)
    def from_vector_id(cls, id):
        return cls._from_apply(
            "FeatureCollection.from_vector",
            id,
            geocontext=env.geoctx,
            __token__=env._token,
        )

    @classmethod
    def from_geojson(cls, geojson):
        try:
            if geojson["type"].lower() != "featurecollection":
                raise ValueError(
                    "Expected a GeoJSON FeatureCollection type, "
                    "not {!r}".format(geojson["type"])
                )

            # Embed the JSON directly in the graft
            # Note this bypasses any validation in Feature, Geometry, etc.
            return cls._from_apply(cls._constructor, geojson["features"])
        except KeyError:
            raise ValueError(
                "Expected a GeoJSON mapping containing the field 'type' and 'features', "
                "but got {}".format(geojson)
            )

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, dict):
            return cls.from_geojson(obj)
        return super(FeatureCollection, cls)._promote(obj)

    @typecheck_promote(
        value=(Int, Float, Str), default_value=(Int, Float), merge_algorithm=Str
    )
    def rasterize(self, value=1, default_value=1, merge_algorithm="add"):
        """
        Rasterize all Features into one `.Image`

        Parameters
        ----------
        value: Int, Float, Str, default=1
            Fill enclosed pixels with this value.
            Pixels outside the `FeatureCollection` will be masked, and set to 0.

            If a string, it will look up that key in the properties of each `Feature`;
            the value there must be a number.
        default_value: Int, Float, default=1
            Value to use if ``value`` is a string and the key does
            not exist in the properties of a `Feature`.
        merge_algorithm: Str, default="add"
            How to combine values where Features overlap. Options are
            ``"add"``, to sum the values, or ``"replace"``, to use the value
            from the `Feature` that comes last in the `FeatureCollection`.

        Notes
        -----
        Rasterization happens according to the `~.workflows.types.geospatial.GeoContext`
        of the `.Job`, so the geometry is projected into and rasterized at
        that CRS and resolution.

        Returns
        -------
        rasterized: Image
            An Image with 1 band named ``"features"``, and empty properties and bandinfo.
        """
        from .image import Image

        return Image._from_apply(
            "rasterize",
            self,
            value,
            env.geoctx,
            default_value=default_value,
            merge_algorithm=merge_algorithm,
        )
