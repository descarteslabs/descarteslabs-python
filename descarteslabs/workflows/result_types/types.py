import datetime
import collections
import itertools
import warnings
import sys
import functools

import six

try:
    from backports.datetime_fromisoformat import MonkeyPatch

    MonkeyPatch.patch_fromisoformat()
except ImportError:
    # not installed in python3.7 to limit surface
    # area for installation issues
    pass
import numpy as np

from . import unmarshal


unmarshal.register("Number", unmarshal.identity)
unmarshal.register("Int", unmarshal.identity)
unmarshal.register("Float", unmarshal.identity)
unmarshal.register("NoneType", unmarshal.identity)
unmarshal.register("Bool", unmarshal.astype(bool))
unmarshal.register("Str", unmarshal.astype(str))
unmarshal.register("List", unmarshal.astype(list))
unmarshal.register("Tuple", unmarshal.astype(tuple))
unmarshal.register("Dict", unmarshal.astype(dict))
unmarshal.register("KnownDict", unmarshal.astype(dict))
unmarshal.register("Slice", unmarshal.astype(slice))
unmarshal.register("AOI", unmarshal.astype(dict))
unmarshal.register("DLTile", unmarshal.astype(dict))
unmarshal.register("XYZTile", unmarshal.astype(dict))
unmarshal.register("GeoContext", unmarshal.astype(dict))
unmarshal.register("Array", unmarshal.identity)
unmarshal.register("MaskedArray", unmarshal.identity)
unmarshal.register("Scalar", unmarshal.identity)


def dtype_from_string(s):
    return np.dtype(s)


unmarshal.register("DType", dtype_from_string)


def datetime_from_string(s):
    return datetime.datetime.fromisoformat(s)


def timedelta_from_seconds(s):
    return datetime.timedelta(seconds=s)


unmarshal.register("Datetime", datetime_from_string)
unmarshal.register("Timedelta", timedelta_from_seconds)


def warn_on_old_python_wrapper(unmarshal_func):
    @functools.wraps(unmarshal_func)
    def unmarshaler(*args, **kwargs):
        if sys.version_info[:2] < (3, 6):
            warnings.warn(
                "Using Python version < 3.6 will result in a nondeterministic ordering "
                "of bandinfos for `Image` and `ImageCollection`. "
                "Update to Python 3.6 or greater to fix this.",
                RuntimeWarning,
            )
        return unmarshal_func(*args, **kwargs)

    return unmarshaler


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


def _join_dict_keys(dct, up_to=4):
    keys = ", ".join(repr(k) for k in itertools.islice(six.iterkeys(dct), up_to))
    if len(dct) > up_to:
        keys += ", ..."
    return keys


class ImageResult(EqualityMixin):
    """
    Result of calling `~.models.compute` on an `~.geospatial.Image`.

    Examples
    --------
    >>> from descarteslabs.workflows import Image
    >>> my_img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
    >>> my_img.compute(my_geoctx) # my_geoctx is an arbitrary geocontext for 'my_img' # doctest: +SKIP
    ImageResult:
      * ndarray: MaskedArray<shape=(27, 512, 512), dtype=float64>
      * properties: 'absolute_orbit', 'acquired', 'archived', 'area', ...
      * bandinfo: 'coastal-aerosol', 'blue', 'green', 'red', ...
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...

    Attributes
    ----------
    ndarray: numpy.ndarray
        3-dimensional array of image data, in order ``(band, y, x)``
    properties: dict[str, any]
        dict of metadata about the `~.geospatial.Image`.
    bandinfo: OrderedDict[str, dict[str, any]]
        OrderedDict of metadata about each band.
        The order corresponds to the bands in the `ndarray`.
    geocontext: dict
        GeoContext over which computation was done.
    """

    def __init__(self, ndarray, properties, bandinfo, geocontext):
        self.ndarray = ndarray
        self.properties = properties
        self.bandinfo = collections.OrderedDict(**bandinfo)
        self.geocontext = geocontext

    def __len__(self):
        return len(self.bandinfo)

    def __repr__(self):
        name_header = type(self).__name__ + ":"

        try:
            ndarray = "{}<shape={}, dtype={}>".format(
                type(self.ndarray).__name__, self.ndarray.shape, self.ndarray.dtype
            )
        except AttributeError:
            ndarray = "None (Empty Image)"

        ndarray_line = "  * ndarray: {}".format(ndarray)

        properties_line = "  * properties: {}".format(_join_dict_keys(self.properties))
        bandinfo_line = "  * bandinfo: {}".format(_join_dict_keys(self.bandinfo))

        geocontext_line = "  * geocontext: {}".format(_join_dict_keys(self.geocontext))

        return "\n".join(
            (name_header, ndarray_line, properties_line, bandinfo_line, geocontext_line)
        )


unmarshal.register(
    "Image", warn_on_old_python_wrapper(unmarshal.unpack_into(ImageResult))
)


class ImageCollectionResult(EqualityMixin):
    """
    Result of calling `~.models.compute` on an `~.geospatial.ImageCollection`.

    Examples
    --------
    >>> from descarteslabs.workflows import ImageCollection
    >>> my_col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
    ...        start_datetime="2017-01-01",
    ...        end_datetime="2017-05-30")
    >>> my_col.compute(my_geoctx) # my_geoctx is an arbitrary geocontext for 'my_col' # doctest: +SKIP
    ImageCollectionResult of length 2:
      * ndarray: MaskedArray<shape=(2, 27, 512, 512), dtype=float64>
      * properties: 2 items
      * bandinfo: 'coastal-aerosol', 'blue', 'green', 'red', ...
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...

    Attributes
    ----------
    ndarray: numpy.ndarray
        4-dimensional array of image data, in order ``(image, band, y, x)``
    properties: list[dict[str, any]]
        List of metadata dicts about each `~.geospatial.Image` in the `~.geospatial.ImageCollection`.
        The order corresponds to axis 0 of the `ndarray`.
    bandinfo: OrderedDict[str, dict[str, any]]
        OrderedDict of metadata about each band.
        The order corresponds to the bands (axis 1) in the `ndarray`.
    geocontext: dict
        GeoContext over which computation was done.
    """

    def __init__(self, ndarray, properties, bandinfo, geocontext):
        self.ndarray = ndarray
        self.properties = properties
        self.bandinfo = collections.OrderedDict(**bandinfo)
        self.geocontext = geocontext

    def __len__(self):
        return len(self.properties)

    def __repr__(self):
        name_header = "{} of length {}:".format(
            type(self).__name__, len(self.properties)
        )

        try:
            ndarray = "{}<shape={}, dtype={}>".format(
                type(self.ndarray).__name__, self.ndarray.shape, self.ndarray.dtype
            )
        except AttributeError:
            ndarray = "None (Empty ImageCollection)"

        ndarray_line = "  * ndarray: {}".format(ndarray)

        properties_line = "  * properties: {} items".format(len(self.properties))
        bandinfo_line = "  * bandinfo: {}".format(_join_dict_keys(self.bandinfo))

        geocontext_line = "  * geocontext: {}".format(_join_dict_keys(self.geocontext))

        return "\n".join(
            (name_header, ndarray_line, properties_line, bandinfo_line, geocontext_line)
        )


unmarshal.register(
    "ImageCollection",
    warn_on_old_python_wrapper(unmarshal.unpack_into(ImageCollectionResult)),
)


class GeometryResult(EqualityMixin):
    """
    Result of calling `~.models.compute` on a `~.geospatial.Geometry`.

    Examples
    --------
    >>> from descarteslabs.workflows import Geometry
    >>> my_geom = Geometry(type="Point", coordinates=[1, 2])
    >>> my_geom.compute() # doctest: +SKIP
    GeometryResult(type=Point, coordinates=[1, 2])

    Attributes
    ----------
    type: str
        The type of geometry. One of "Point", "MultiPoint", "LineString",
        "MultiLineString", "Polygon", "MultiPolygon", or "LinearRing".
    coordinates: list
        Coordinates for the geometry, in WGS84 lat-lon (EPSG:4326).
        May be a list of floats, a list of lists of floats,
        or a list of lists of lists of floats depending on the `type`.
    __geo_interface__: dict
        GeoJSON representation of the `Geometry`, following the Python
        ``__geo_interface__`` `convention <https://gist.github.com/sgillies/2217756>`_.
    shapely: shapely.geometry.BaseGeometry
        The `GeometryResult` as a shapely shape. Raises ``ImportError`` if the shapely
        package is not installed.
    """

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

    def __repr__(self):
        return "{}(type={}, coordinates={})".format(
            type(self).__name__, self.type, self.coordinates
        )


class GeometryCollectionResult(GeometryResult):
    """
    Result of calling `~.models.compute` on a `~.geospatial.GeometryCollection`.

    Examples
    --------
    >>> from descarteslabs.workflows import Geometry, GeometryCollection
    >>> my_geom = Geometry(type="Point", coordinates=[1, 2])
    >>> my_gc = GeometryCollection(type="GeometryCollection", geometries=[my_geom, my_geom, my_geom])
    >>> my_gc.compute() # doctest: +SKIP
    GeometryCollectionResult(type=GeometryCollection,
            geometries=(
                GeometryResult(type=Point, coordinates=[1, 2]),
                GeometryResult(type=Point, coordinates=[1, 2]),
                GeometryResult(type=Point, coordinates=[1, 2])))

    Attributes
    ----------
    type: str
        The type of geometry. Always "GeometryCollection".
    geometries: list[GeometryResult]
        List of `GeometryResult` objects in the collection.
    __geo_interface__: dict
        GeoJSON representation of the `GeometryCollection`, following the Python
        ``__geo_interface__`` `convention <https://gist.github.com/sgillies/2217756>`_.
    shapely: shapely.geometry.GeometryCollection
        The `GeometryCollection` as a shapely shape. Raises ``ImportError`` if the shapely
        package is not installed.
    """

    TYPES = {"GeometryCollection"}

    def __init__(self, type, geometries, crs=None):
        if type not in self.TYPES:
            raise ValueError("Invalid type {!r} for GeometryCollection")
        self.type = type
        self.geometries = tuple(
            geometry
            if isinstance(geometry, GeometryResult)
            else GeometryResult(geometry["type"], geometry["coordinates"])
            if isinstance(geometry, dict)
            else GeometryResult(geometry.type, geometry.coordinates)
            for geometry in geometries
        )
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

    def __repr__(self):
        return "{}(type={}, geometries={})".format(
            type(self).__name__, self.type, self.geometries
        )


class FeatureResult(EqualityMixin):
    """
    Result of calling `~.models.compute` on a `~.geospatial.Feature`.

    Examples
    --------
    >>> from descarteslabs.workflows import Geometry, Feature
    >>> my_geom = Geometry(type="Point", coordinates=[1, 2])
    >>> my_feat = Feature(geometry=my_geom, properties={"foo": "bar"})
    >>> my_feat.compute() # doctest: +SKIP
    FeatureResult(geometry=GeometryResult(type=Point, coordinates=[1, 2]), properties={'foo': 'bar'})

    Attributes
    ----------
    geometry: GeometryResult
        The `GeometryResult` of the feature.
    properties: dict
        Properties associated with the `FeatureResult`.
    __geo_interface__: dict
        GeoJSON representation of the `FeatureResult`, following the Python
        ``_geo_interface__`` `convention <https://gist.github.com/sgillies/2217756>`_.
    shapely: shapely.geometry.BaseGeometry
        The `geometry` of the `FeatureResult` as a shapely shape.
        Raises ``ImportError`` if the shapely package is not installed.
    """

    def __init__(self, geometry, properties):
        try:
            self.geometry = (
                geometry
                if isinstance(geometry, (GeometryResult, GeometryCollectionResult))
                else GeometryCollectionResult(**geometry)
                if geometry["type"] == "GeometryCollection"
                else GeometryResult(**geometry)
            )
        except TypeError:
            if geometry.type == "GeometryCollection":
                self.geometry = GeometryCollectionResult(
                    geometry.type, geometry.geometries, geometry.crs
                )
            else:
                self.geometry = GeometryResult(
                    geometry.type, geometry.coordinates, geometry.crs
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

    def __repr__(self):
        return "{}(geometry={}, properties={})".format(
            type(self).__name__, self.geometry, self.properties
        )


class FeatureCollectionResult(EqualityMixin):
    """
    Result of calling `~.models.compute` on a `~.geospatial.FeatureCollection`.

    Examples
    --------
    >>> from descarteslabs.workflows import Geometry, Feature, FeatureCollection
    >>> my_geom = Geometry(type="Point", coordinates=[1, 2])
    >>> my_feat = Feature(geometry=my_geom, properties={"foo": "bar"})
    >>> my_fc = FeatureCollection(features=[my_feat, my_feat, my_feat])
    >>> my_fc.compute() # doctest: +SKIP
    FeatureCollectionResult(features=(
        FeatureResult(geometry=GeometryResult(type=Point, coordinates=[1, 2]), properties={'foo': 'bar'}),
        FeatureResult(geometry=GeometryResult(type=Point, coordinates=[1, 2]), properties={'foo': 'bar'}),
        FeatureResult(geometry=GeometryResult(type=Point, coordinates=[1, 2]), properties={'foo': 'bar'})))

    Attributes
    ----------
    features: list[FeatureResult]
        List of `FeatureResult` result objects in the collection.
    __geo_interface__: dict
        GeoJSON representation of the `FeatureCollectionResult`, following the Python
        ``__geo_interface__`` `convention <https://gist.github.com/sgillies/2217756>`_.
    """

    def __init__(self, features):
        self.features = tuple(
            feature
            if isinstance(feature, FeatureResult)
            else FeatureResult(feature["geometry"], feature["properties"])
            if isinstance(feature, dict)
            else FeatureResult(feature.geometry, feature.properties)
            for feature in features
        )

    @property
    def __geo_interface__(self):
        # NOTE: library support may be limited with this
        # interface for FeatureCollections
        return {
            "type": "FeatureCollection",
            "features": [feat.__geo_interface__ for feat in self.features],
        }

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.features == other.features

    def __ne__(self, other):
        return not self.__eq__(other)

    def __iter__(self):
        return iter(self.features)

    def __len__(self):
        return len(self.features)

    def __repr__(self):
        return "{}(features={})".format(type(self).__name__, self.features)


unmarshal.register("Geometry", unmarshal.unpack_into(GeometryResult))
unmarshal.register(
    "GeometryCollection", unmarshal.unpack_into(GeometryCollectionResult)
)
unmarshal.register(
    "Feature", lambda geojson: FeatureResult(geojson["geometry"], geojson["properties"])
)
unmarshal.register(
    "FeatureCollection", lambda geojson: FeatureCollectionResult(geojson["features"])
)
