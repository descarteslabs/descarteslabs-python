""" Descartes Labs utilities for our Universal Transverse Mercator (UTM)-based
projection system. """

# The Descartes Labs projection system is slightly different from the
# canonical UTM standard. Only North UTM zones are used, including for the
# southern hemisphere; so there are no false northings. Also, the latitude
# range is extended to the full +/-90 (instead of -80 to +84).

from collections.abc import Sequence
import json
import numpy as np
import shapely.geometry as geo

from .conversions import points_from_polygon
from .exceptions import InvalidLatLonError

# WGS84 constants:

# usually written 'a'
EARTH_MAJOR_AXIS = 6378137  # in meters
# usually written 'f'
FLATTENING = 1.0 / 298.257223563

# UTM constants
# usually written 'k0'
POINT_SCALE_FACTOR = 0.9996
# usually written 'E0'
FALSE_EASTING = 500000  # in meters
# Note that we do not use a false northing.

# usually written 'n'
THIRD_FLATTENING = FLATTENING / (2 - FLATTENING)
# Usually written 'A'
RECTIFYING_RADIUS = (
    EARTH_MAJOR_AXIS
    / (1 + THIRD_FLATTENING)
    * (
        1.0
        + 1.0 / 4.0 * THIRD_FLATTENING ** 2
        + 1.0 / 64.0 * THIRD_FLATTENING ** 4
    )
)

# Numbers outside these ranges are surely outside their UTM zone
# (but not strictly invalid)
UTM_MIN_EAST = FALSE_EASTING - 334000
UTM_MAX_EAST = FALSE_EASTING + 334000

# Distances from equator to south/north poles, according to our transformation
# of points of latitude -90.0 and 90.0 respectively
UTM_MIN_NORTH = -9997964.943
UTM_MAX_NORTH = 9997964.943

# Numbers outside these ranges are not supported by our UTM system
UTM_MIN_LON = -180.0
UTM_MAX_LON = 180.0
UTM_MIN_LAT = -90.0
UTM_MAX_LAT = 90.0

# The width of a zone, in degrees longitude
ZONE_WIDTH_LON = 6


def zone_to_lon(zone: int):
    """Returns the middle longitude of a zone"""
    if zone < 1 or zone > 60:
        raise ValueError("Zones must be between 1 and 60 (inclusive)")
    return zone * ZONE_WIDTH_LON - 183.0


def lon_to_zone(lon: float):
    if lon < UTM_MIN_LON or lon > UTM_MAX_LON:
        raise InvalidLatLonError(
            "Longitude must be between -180.0 and 180.0 " "(inclusive)"
        )
    return max(1, 1 + np.floor((lon + 180.0) / 6.0).astype(int))


def coordinate_transform(function):
    """Decorate a function which accepts numpy arrays of shape (?, 2), and
    optionally other arguments, and returns numpy arrays of the same shape;
    then the function will work for shapes and non-numpy sequences as well as
    numpy arrays, and will attempt to return arguments of the same type as its
    points parameter.
    """

    def _transform(points, *args, axis=-1, **kwargs):
        if isinstance(points, np.ndarray):
            pass

        elif isinstance(points, str):
            points = json.loads(points)
            points = geo.shape(points)
            transformed_points = _transform(points, *args, **kwargs)
            return json.dumps(geo.mapping(transformed_points))

        elif isinstance(points, dict):
            points = geo.shape(points)
            transformed_points = _transform(points, *args, **kwargs)
            return geo.mapping(transformed_points)

        elif isinstance(points, geo.MultiPolygon):
            return geo.MultiPolygon(
                [_transform(polygon, *args, **kwargs) for polygon in points]
            )

        elif isinstance(points, Sequence):
            try:
                if np.isfinite(points).all():
                    points = np.array(points, dtype=np.double)
                else:
                    raise TypeError  # Catch
            except TypeError:
                # The elements of this sequence could not become a numpy array,
                # try instead to see if this is a list of polygons.
                return [
                    _transform(polygon, *args, **kwargs) for polygon in points
                ]

        elif isinstance(points, geo.Polygon):
            exterior_points, *interiors_points = points_from_polygon(points)
            return geo.Polygon(
                _transform(exterior_points, *args, **kwargs),
                holes=[
                    _transform(interior_points, *args, **kwargs)
                    for interior_points in interiors_points
                ]
                or None,
            )

        else:
            raise TypeError(
                "Could not interpret points of type %s, "
                "try passing an ndarray or shape" % type(points)
            )

        points = points.swapaxes(axis, -1).reshape((-1, 2)).astype(np.double)
        shape = list(points.shape)
        shape[axis] = points.shape[-1]
        shape[-1] = points.shape[axis]
        transformed_points = function(points, *args, **kwargs)
        return transformed_points.reshape(shape).swapaxes(axis, -1)

    return _transform


@coordinate_transform
def lonlat_to_utm(points, zone=None, ref_lon=None):
    """ Convert lon,lat points in a numpy array or shapely shape to UTM
    coordinates in the given zone.

    Parameters
    ----------

    points: numpy array, shapely polygon/multipolygon, geojson, or array-like
        Points of WGS84 lon,lat coordinates
    zone: int, optional
        UTM zone from 1 to 60 inclusive, must be specified if ref_lon is not
    ref_lon: float, optional
        Reference longitude to determine zone from
    axis: int, default=-1
        The given axis should have size 2, with lon,lat pairs.

    Returns
    -------

    utm_points: tries to be the same type as points, or numpy array

    Raises
    ------

    ValueError
        When UTM zone is outside of 1 to 60 inclusive, or the numpy array
        axis does not have size==2.
    """

    if zone is None:
        if ref_lon is None:
            raise TypeError("Either `zone` or `ref_lon` must be specified")
        zone = lon_to_zone(ref_lon)

    # These series expansion coefficients are sufficient to approximate the UTM
    # projection system to a precision of millimeters.
    n = THIRD_FLATTENING
    N = 2 * np.sqrt(n) / (1.0 + n)

    a1 = 1.0 / 2.0 * n - 2.0 / 3.0 * n ** 2 + 5.0 / 16.0 * n ** 3
    a2 = 13.0 / 48.0 * n ** 2 - 3.0 / 5.0 * n ** 3
    a3 = 61.0 / 240.0 * n ** 3

    lon = points[:, 0]
    lat = points[:, 1]
    radlon = np.deg2rad(lon - 6.0 * zone + 183.0)
    radlat = np.deg2rad(lat)

    sinlat = np.sin(radlat)
    t = np.sinh(np.arctanh(sinlat) - N * np.arctanh(N * sinlat))
    etap = np.arctanh(np.sin(radlon) / np.sqrt(1 + t ** 2))
    xip = np.arctan(t / np.cos(radlon))

    easting = FALSE_EASTING + POINT_SCALE_FACTOR * RECTIFYING_RADIUS * (
        etap
        + a1 * np.cos(2 * xip) * np.sinh(2 * etap)
        + a2 * np.cos(4 * xip) * np.sinh(4 * etap)
        + a3 * np.cos(6 * xip) * np.sinh(6 * etap)
    )

    northing = (
        POINT_SCALE_FACTOR
        * RECTIFYING_RADIUS
        * (
            xip
            + a1 * np.sin(2 * xip) * np.cosh(2 * etap)
            + a2 * np.sin(4 * xip) * np.cosh(4 * etap)
            + a3 * np.sin(6 * xip) * np.cosh(6 * etap)
        )
    )

    return np.stack((easting, northing), axis=-1)


@coordinate_transform
def utm_to_lonlat(points, zone):
    """ Convert UTM points in a numpy array or shapely shape to lon,lat
    coordinates in the given zone.

    Parameters
    ----------

    points: numpy array, shapely polygon/multipolygon, geojson, or array-like
        Points of x,y coordinates in the given UTM north zone
    zone: int
        UTM north zone from 1 to 60 inclusive
    axis: int, default=-1
        The given axis should have size 2, with UTM x,y pairs.

    Returns
    -------

    lonlat_points: tries to be the same type as points, or numpy array

    Raises
    ------

    ValueError
        When UTM zone is outside of 1 to 60 inclusive, or the numpy array
        axis does not have size==2.
    """
    # These series expansion coefficients are sufficient to approximate the UTM
    # projection system to a precision of millimeters.
    n = THIRD_FLATTENING

    b1 = 1.0 / 2.0 * n - 2.0 / 3.0 * n ** 2 + 37.0 / 96.0 * n ** 3
    b2 = 1.0 / 48.0 * n ** 2 + 1.0 / 15.0 * n ** 3
    b3 = 17.0 / 480.0 * n ** 3

    d1 = 2.0 * n - 2.0 / 3.0 * n ** 2 - 2.0 * n ** 3
    d2 = 7.0 / 3.0 * n ** 2 - 8.0 / 5.0 * n ** 3
    d3 = 56.0 / 15.0 * n ** 3

    easting = points[:, 0]
    northing = points[:, 1]

    xi = northing / (POINT_SCALE_FACTOR * RECTIFYING_RADIUS)
    eta = (easting - FALSE_EASTING) / (POINT_SCALE_FACTOR * RECTIFYING_RADIUS)

    xip = xi - (
        b1 * np.sin(2 * xi) * np.cosh(2 * eta)
        + b2 * np.sin(4 * xi) * np.cosh(4 * eta)
        + b3 * np.sin(6 * xi) * np.cosh(6 * eta)
    )

    etap = eta - (
        b1 * np.cos(2 * xi) * np.sinh(2 * eta)
        + b2 * np.cos(4 * xi) * np.sinh(4 * eta)
        + b3 * np.cos(6 * xi) * np.sinh(6 * eta)
    )

    chi = np.arcsin(np.sin(xip) / np.cosh(etap))

    lat = np.rad2deg(
        chi
        + d1 * np.sin(2 * chi)
        + d2 * np.sin(4 * chi)
        + d3 * np.sin(6 * chi)
    )
    lon = (
        6.0 * zone - 183.0 + np.rad2deg(np.arctan(np.sinh(etap) / np.cos(xip)))
    )

    # Return all longitude outputs within the range -180.0, +180.0
    lon = (lon + 180.0) % 360.0 - 180.0

    return np.stack((lon, lat), axis=-1)


@coordinate_transform
def utm_to_rowcol(utm_points, tile):
    """ Convert UTM points in an array of shape (?, 2) to row,col array indices
    given a tile. """
    if not utm_points.shape[1] == 2:
        raise ValueError(
            "Expected array of utm points of shape (?, 2), got %s"
            % str(utm_points.shape)
        )

    min_col = tile.tilesize * tile.path - tile.pad
    max_row = tile.tilesize * (tile.row + 1) + tile.pad

    east = utm_points[:, 0] - FALSE_EASTING
    north = utm_points[:, 1]

    row = max_row - north / tile.resolution
    col = east / tile.resolution - min_col

    return np.stack((row, col), axis=-1)


@coordinate_transform
def rowcol_to_utm(indices, tile):
    """ Convert row,col array indices in an array of shape (?, 2) to UTM
    coordinates given a tile.  """
    if not indices.shape[1] == 2:
        raise ValueError(
            "Expected array of utm points of shape (?, 2), got %s"
            % str(indices.shape)
        )

    min_col = tile.tilesize * tile.path - tile.pad
    max_row = tile.tilesize * (tile.row + 1) + tile.pad

    row = indices[:, 0]
    col = indices[:, 1]

    east = (col + min_col) * tile.resolution + FALSE_EASTING
    north = (max_row - row) * tile.resolution

    return np.stack((east, north), axis=-1)
