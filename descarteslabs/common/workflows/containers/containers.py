import collections

import numpy as np
from descarteslabs.common.workflows import unmarshal

unmarshal.register("Number", unmarshal.identity)
unmarshal.register("Int", unmarshal.identity)
unmarshal.register("Float", unmarshal.identity)
unmarshal.register("NoneType", unmarshal.identity)
unmarshal.register("Bool", unmarshal.astype(bool))
unmarshal.register("String", unmarshal.astype(str))
unmarshal.register("Str", unmarshal.astype(str))
# ^ TODO(gabe): on py2 these should possibly be unicode
# ^ TODO(gabe): remove "String" once old client is deprecated
unmarshal.register("List", unmarshal.astype(list))
unmarshal.register("Tuple", unmarshal.astype(tuple))
unmarshal.register("Dict", unmarshal.astype(dict))


class EqualityMixin(object):
    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        try:
            np.testing.assert_equal(self.__dict__, other.__dict__)
        except AssertionError:
            return False
        else:
            return True


class Image(EqualityMixin):
    def __init__(self, bands, properties, bandinfo):
        self.bands = bands
        self.properties = properties
        self.bandinfo = bandinfo

        if not isinstance(bandinfo, collections.OrderedDict):
            raise TypeError(
                "bandinfo must be {}, not {}".format(
                    collections.OrderedDict, type(bandinfo)
                )
            )

        if len(self.bands) != len(self.bandinfo):
            raise ValueError(
                "bands mismatch between bands and bandinfo. "
                "Bandinfo indicates {} keys, while bands indicates {} keys".format(
                    len(self.bandinfo), len(self.bands)
                )
            )

    def __len__(self):
        return len(self.bandinfo)


unmarshal.register("Image", unmarshal.unpack_into(Image))


class ImageCollection(EqualityMixin):
    def __init__(self, images, properties, bandinfo):
        self.images = images
        self.properties = properties
        self.bandinfo = bandinfo

    def __len__(self):
        return len(self.properties)


unmarshal.register("ImageCollection", unmarshal.unpack_into(ImageCollection))


class Geometry(EqualityMixin):
    TYPES = {
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
        "LinearRing",
    }

    def __init__(self, type, coordinates, crs=None):
        # TODO(gabe): handle GeometryCollection
        if type not in self.TYPES:
            raise ValueError(
                "Invalid type {!r} for Geometry; must be one of {}".format(
                    type, self.TYPES
                )
            )
        self.type = type
        self.coordinates = coordinates
        self.crs = crs
        self._shape = None

    @property
    def __geo_interface__(self):
        return {"type": self.type, "coordinates": self.coordinates}

    @property
    def shapely(self):
        import shapely  # Import within function so shapely is an optional dependency

        if self._shape is None:
            self._shape = shapely.geometry.shape(self)
        return self._shape

    def __eq__(self, other):
        return (
            isinstance(other, type(self))
            and self.type == other.type
            and self.coordinates == other.coordinates
            and self.crs == other.crs
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class GeometryCollection(Geometry):
    TYPES = {"GeometryCollection"}

    def __init__(self, type, geometries, crs=None):
        if type not in self.TYPES:
            raise ValueError("Invalid type {!r} for GeometryCollection")
        self.type = type
        self.geometries = geometries
        self.crs = crs
        self._shape = None

    @property
    def __geo_interface__(self):
        return {
            "type": self.type,
            "geometries": [geom.__geo_interface__ for geom in self.geometries],
        }

    def __eq__(self, other):
        return (
            isinstance(other, type(self))
            and self.type == other.type
            and self.geometries == other.geometries
            and self.crs == other.crs
        )

    def __ne__(self, other):
        return not self.__eq__(other)


class Feature(EqualityMixin):
    GEOMETRY = Geometry
    GEOMETRY_COLLECTION = GeometryCollection

    def __init__(self, geometry, properties):
        self.geometry = (
            geometry
            if isinstance(geometry, (self.GEOMETRY, self.GEOMETRY_COLLECTION))
            else self.GEOMETRY_COLLECTION(**geometry)
            if geometry["type"] == "GeometryCollection"
            else self.GEOMETRY(**geometry)
        )
        self.properties = properties
        self._shape = None

    @property
    def __geo_interface__(self):
        return {
            "type": "Feature",
            "geometry": self.geometry.__geo_interface__,
            "properties": self.properties,
        }

    def __eq__(self, other):
        return (
            isinstance(other, type(self))
            and self.geometry == other.geometry
            and self.properties == other.properties
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def shapely(self):
        if self._shape is None:
            self._shape = self.geometry.shapely
        return self._shape


class FeatureCollection(EqualityMixin):
    FEATURE = Feature

    def __init__(self, features):
        self.features = tuple(
            self.FEATURE(feature["geometry"], feature["properties"])
            if not isinstance(feature, Feature)
            else feature
            for feature in features
        )

    @property
    def __geo_interface__(self):
        # NOTE: library support may be limited with this
        # interface for FeatureCollections
        return {"type": "FeatureCollection", "features": self.features}

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.features == other.features

    def __ne__(self, other):
        return not self.__eq__(other)

    def __iter__(self):
        return iter(self.features)

    def __len__(self):
        return len(self.features)


unmarshal.register("Geometry", unmarshal.unpack_into(Geometry))
unmarshal.register("GeometryCollection", unmarshal.unpack_into(GeometryCollection))
unmarshal.register(
    "Feature", lambda geojson: Feature(geojson["geometry"], geojson["properties"])
)
unmarshal.register(
    "FeatureCollection", lambda geojson: FeatureCollection(geojson["features"])
)
