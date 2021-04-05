import numpy as np
import shapely.geometry as geo

from .conversions import points_from_polygon
from . import utm as utm


def utm_box_to_lonlat(polygon: geo.Polygon, zone: int) -> geo.base.BaseGeometry:
    """ Given a box (polygon with four corners) in UTM coordinates, return
    a box in lonlat coordinates which behaves appropriately at the prime
    antimeridian and polar regions. This is used in tile.py to construct
    appropriate tile boundaries. """
    min_x, min_y, max_x, max_y = polygon.bounds

    polygon_lonlat = utm.utm_to_lonlat(polygon, zone)
    min_lon, min_lat, max_lon, max_lat = polygon_lonlat.bounds

    points_lonlat = np.array(points_from_polygon(polygon_lonlat)[0])

    # First we check if the polygon touches or contains a pole when
    # transforming it. If it does, we need to include points at the pole to
    # cover the correct area in lonlat coordinates.
    north_pole = geo.Point((utm.FALSE_EASTING, utm.UTM_MAX_NORTH))
    south_pole = geo.Point((utm.FALSE_EASTING, utm.UTM_MIN_NORTH))

    wrap_north = polygon.touches(north_pole) or polygon.contains(north_pole)
    wrap_south = polygon.touches(south_pole) or polygon.contains(south_pole)

    if wrap_north or wrap_south:
        n_points, _ = points_lonlat.shape

        points_by_lon = np.sort(points_lonlat, axis=0)
        wrapforward = points_by_lon[0, :][np.newaxis] + np.array(
            [(360.0, 0.0)]
        )
        wrapback = points_by_lon[-1, :][np.newaxis] - np.array([(360.0, 0.0)])

        # Create a polygon that goes beyond 90deg latitude, then cut it down
        # to the real range of lonlat.
        if wrap_north:
            excessive_polygon = geo.Polygon(
                np.concatenate(
                    (
                        wrapback,
                        points_by_lon,
                        wrapforward,
                        np.array(
                            [(wrapforward[0, 0], 91.0), (wrapback[0, 0], 91.0)]
                        ),
                    ),
                    axis=0,
                )
            )
        else:  # wrap_south
            excessive_polygon = geo.Polygon(
                np.concatenate(
                    (
                        wrapforward,
                        points_by_lon[::-1],
                        wrapback,
                        np.array(
                            [
                                (wrapback[0, 0], -91.0),
                                (wrapforward[0, 0], -91.0),
                            ]
                        ),
                    ),
                    axis=0,
                )
            )
        return excessive_polygon.intersection(
            geo.box(-180.0, -90.0, 180.0, 90.0)
        )

    # A square in UTM coordinates which does not contain a pole can never
    # map to a polygon with longitude range greater than 180deg.
    # So because this polygon crosses the prime antimeridian, we know it
    # doesn't cross the prime meridian, and we can split it accordingly.

    wraps_prime_antimeridian = (
        np.max(points_lonlat[:, 0]) - np.min(points_lonlat[:, 0]) > 180.0
    )
    if not wraps_prime_antimeridian:
        return polygon_lonlat

    lons = points_lonlat[:, 0]
    eastern_lons_mask = lons >= 0.0
    western_lons_mask = lons < 0.0

    western_points = points_lonlat.copy()
    western_points[eastern_lons_mask, 0] -= 360.0
    western_hemisphere = geo.box(-180.0, -90.0, 0.0, 90.0)
    western_polygon = geo.Polygon(western_points).intersection(
        western_hemisphere
    )

    eastern_points = points_lonlat.copy()
    eastern_points[western_lons_mask, 0] += 360.0
    eastern_hemisphere = geo.box(0.0, -90.0, 180.0, 90.0)
    eastern_polygon = geo.Polygon(eastern_points).intersection(
        eastern_hemisphere
    )

    return geo.MultiPolygon([western_polygon, eastern_polygon])
