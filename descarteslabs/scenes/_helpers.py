import collections

import six
import shapely.geometry
import geojson


def polygon_from_bounds(bounds):
    "Return a GeoJSON Polygon dict from a (minx, miny, maxx, maxy) tuple"
    return {
        "type": "Polygon",
        "coordinates": ((
            (bounds[2], bounds[1]),
            (bounds[2], bounds[3]),
            (bounds[0], bounds[3]),
            (bounds[0], bounds[1]),
            (bounds[2], bounds[1]),
        ),)
    }


def test_valid_bounds(bounds):
    """
    Test given bounds are correct type and in correct order.

    Raises TypeError or ValueError if bounds are invalid, otherwise returns None
    """
    try:
        if not isinstance(bounds, (list, tuple)):
            raise TypeError("Bounds must be a list or tuple, instead got type {}".format(type(bounds)))

        if len(bounds) != 4:
            raise ValueError(
                "Bounds must a sequence of (minx, miny, maxx, maxy), "
                "got sequence of length {}".format(len(bounds))
            )
    except TypeError:
        six.raise_from(
            TypeError("Bounds must a sequence of (minx, miny, maxx, maxy), got {}".format(type(bounds))),
            None
        )

    if bounds[0] >= bounds[2]:
        raise ValueError("minx >= maxx in given bounds, should be (minx, miny, maxx, maxy)")
    if bounds[1] >= bounds[3]:
        raise ValueError("miny >= maxy in given bounds, should be (minx, miny, maxx, maxy)")


def valid_latlon_bounds(bounds):
    "Return whether bounds fall within [-180, 180] for x and [-90, 90] for y"
    return (-180 <= bounds[0] <= 180 and
            -90 <= bounds[1] <= 90 and
            -180 <= bounds[2] <= 180 and
            -90 <= bounds[3] <= 90)


def is_geographic_crs(crs):
    if not isinstance(crs, six.string_types):
        return False
    lower_crs = crs.lower()
    return (
        lower_crs == "epsg:4326"  # WGS84 geodetic CRS. Other geodetic EPSG codes (e.g. NAD27) are incorrectly rejected.
        or lower_crs.startswith("+proj=longlat")  # PROJ.4
        # OGC WKT
        or lower_crs.startswith("geogcs[")  # deprecated
        or lower_crs.startswith("geodcrs[")
        or lower_crs.startswith("geodeticcrs[")
    )


def is_wgs84_crs(crs):
    if not isinstance(crs, six.string_types):
        return False
    lower_crs = crs.lower()
    return (
        lower_crs == "epsg:4326"  # WGS84 geodetic CRS
        or lower_crs.startswith("+proj=longlat +datum=wgs84")  # PROJ.4
        # OGC WKT
        # NOTE: this is a totally heuristic, non-robust guess at whether a WKT string is WGS84.
        # The correct way would be to parse out the spheroid, prime meridian, and unit parameters
        # and check their values. However, parsing WKT is outside the scope of this client,
        # and this method is used only to provide more helpful error messages, so accuracy isn't essential.
        # Here, we'll hope that anyone using a WGS 84 WKT generated it with a tool that sensibly
        # named the CRS as "WGS 84", or with a tool that sensibly *didn't* name a non-WGS84 CRS as "WGS 84".
        or lower_crs.startswith('geogcs["wgs 84"')  # deprecated
        or lower_crs.startswith('geodcrs["wgs 84"')
        or lower_crs.startswith('geodeticcrs["wgs 84"')
    )


def geometry_like_to_shapely(geometry):
    """
    Convert a GeoJSON dict, or __geo_interface__ object, to a Shapely geometry.

    Handles Features and FeatureCollections (FeatureCollections become GeometryCollections).
    """
    if isinstance(geometry, shapely.geometry.base.BaseGeometry):
        return geometry

    if not isinstance(geometry, collections.Mapping):
        try:
            geometry = geometry.__geo_interface__
        except AttributeError:
            six.raise_from(
                TypeError("geometry object is not a GeoJSON dict, nor has a `__geo_interface__`: {}".format(geometry)),
                None
            )

    geoj = as_geojson_geometry(geometry)
    try:
        shape = shapely.geometry.shape(geoj)
    except Exception:
        raise ValueError("Could not interpret this geometry as a Shapely shape: {}".format(geometry))

    # test that geometry is in WGS84
    test_valid_bounds(shape.bounds)
    return shape


def as_geojson_geometry(geojson_dict):
    """
    Return a mapping as a GeoJSON instance, converting Feature types to Geometry types.
    """
    try:
        geoj = geojson.GeoJSON.to_instance(geojson_dict, strict=True)
    except (TypeError, KeyError, UnicodeEncodeError) as ex:
        raise ValueError("geometry not recognized as valid GeoJSON ({}): {}".format(str(ex), geojson_dict))
    # Shapely cannot handle GeoJSON Features or FeatureCollections
    if isinstance(geoj, geojson.Feature):
        geoj = geoj.geometry
    elif isinstance(geoj, geojson.FeatureCollection):
        features = []
        for feature in geoj.features:
            try:
                features.append(geojson.GeoJSON.to_instance(feature, strict=True).geometry)
            except (TypeError, KeyError, UnicodeEncodeError) as ex:
                raise ValueError(
                    "feature in FeatureCollection not recognized as valid ({}): {}".format(str(ex), feature))
        geoj = geojson.GeometryCollection(features)
    return geoj
