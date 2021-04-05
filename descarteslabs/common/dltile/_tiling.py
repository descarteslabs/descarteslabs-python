"""Implementation details for tile.Grid.tiles_from_shape"""

import numpy as np
import shapely.geometry as geo

from .exceptions import InvalidShapeError
from .utm import (
    UTM_MIN_LAT,
    UTM_MAX_LAT,
    UTM_MIN_EAST,
    UTM_MAX_EAST,
    FALSE_EASTING,
    lonlat_to_utm,
)
from .utils import utm_box_to_lonlat


def _get_next_tiling(polygon, grid_width):
    """ This function yields (zone, path, row) tuples corresponding to
    all tiles over a shape, given grid_width (resolution*tilesize) in meters. """

    min_lon, min_lat, max_lon, max_lat = polygon.bounds

    if min_lon < -180.0:
        raise InvalidShapeError("Polygon goes beyond -180deg longitude")

    if max_lon > 180.0:
        raise InvalidShapeError("Polygon goes beyond +180deg longitude")

    if min_lat < UTM_MIN_LAT:
        raise InvalidShapeError("Polygon goes beyond -90deg latitude")

    if max_lat > UTM_MAX_LAT:
        raise InvalidShapeError("Polygon goes beyond +90deg latitude")

    yield from _tiling_method_appropriate_zones(polygon, grid_width)


def _tiling_method_appropriate_zones(polygon, grid_width):
    """Chooses the most appropriate zones to tile a shape."""
    min_lon, _, max_lon, _ = polygon.bounds
    min_zone = max(1, 1 + np.floor((min_lon + 180.0) / 6.0).astype(int))
    max_zone = 1 + min(60, 1 + np.floor((max_lon + 180.0) / 6.0).astype(int))  # exclusive range

    for zone in range(min_zone, max_zone):
        zone_min_lon = 6 * zone - 186.0
        zone_max_lon = 6 * zone - 180.0
        zone_box = geo.box(zone_min_lon, UTM_MIN_LAT, zone_max_lon, UTM_MAX_LAT)

        polygon_in_zone = polygon.intersection(zone_box)

        if isinstance(polygon_in_zone, (geo.Polygon, geo.MultiPolygon)) and not polygon_in_zone.is_empty:
            yield from _tile_zone(polygon_in_zone, grid_width, zone, zone_min_lon, zone_max_lon)

        if isinstance(polygon_in_zone, geo.GeometryCollection):
            for shape in polygon_in_zone:
                if isinstance(shape, (geo.Polygon, geo.MultiPolygon)) and not shape.is_empty:
                    yield from _tile_zone(shape, grid_width, zone, zone_min_lon, zone_max_lon)


def _tile_zone(polygon, grid_width, zone, zone_min_lon, zone_max_lon):
    polygon_utm = lonlat_to_utm(polygon, zone=zone).buffer(0)
    min_east, min_north, max_east, max_north = polygon_utm.bounds

    min_east = max(min_east, UTM_MIN_EAST)
    max_east = min(max_east, UTM_MAX_EAST)

    min_path = int(np.floor((min_east - FALSE_EASTING) / grid_width))
    min_row = int(np.floor(min_north / grid_width))
    max_path = int(np.ceil((max_east - FALSE_EASTING) / grid_width))  # exclusive
    max_row = int(np.ceil(max_north / grid_width))  # exclusive

    quads = [(min_path, min_row, max_path, max_row, False)]

    # Traverse a quadtree division of tiles to efficiently find which ones
    # intersect with the given shape
    while len(quads) > 0:
        (
            quad_min_path,
            quad_min_row,
            quad_max_path,
            quad_max_row,
            certainly_within_zone,
        ) = quads.pop()
        quadbox = geo.box(
            FALSE_EASTING + quad_min_path * grid_width,
            quad_min_row * grid_width,
            FALSE_EASTING + quad_max_path * grid_width,
            quad_max_row * grid_width,
        )

        quad_min_lon, _, quad_max_lon, _ = utm_box_to_lonlat(quadbox, zone).bounds
        if quad_min_lon > zone_max_lon or quad_max_lon < zone_min_lon:
            continue

        if quad_min_lon > zone_min_lon and quad_max_lon < zone_max_lon:
            certainly_within_zone = True

        quad_h = quad_max_row - quad_min_row
        quad_w = quad_max_path - quad_min_path

        if quad_h <= 3 and quad_w <= 3:
            # Just do an exhaustive check when both dimensions are
            # less than 4 tiles across
            for row in range(quad_min_row, quad_max_row):
                for path in range(quad_min_path, quad_max_path):
                    quadbox = geo.box(
                        FALSE_EASTING + path * grid_width,
                        row * grid_width,
                        FALSE_EASTING + (path + 1) * grid_width,
                        (row + 1) * grid_width,
                    )
                    if quadbox.intersects(polygon_utm):
                        if certainly_within_zone:
                            yield zone, path, row
                        else:
                            (
                                quad_min_lon,
                                _,
                                quad_max_lon,
                                _,
                            ) = utm_box_to_lonlat(quadbox, zone).bounds
                            if quad_min_lon <= zone_max_lon and quad_max_lon >= zone_min_lon:
                                yield zone, path, row

        elif quadbox.within(polygon_utm):
            # If the quadbox is entirely within our polygon,
            # return all tiles in it
            for row in range(quad_min_row, quad_max_row):
                for path in range(quad_min_path, quad_max_path):
                    if certainly_within_zone:
                        yield zone, path, row
                    else:
                        quadbox = geo.box(
                            FALSE_EASTING + path * grid_width,
                            row * grid_width,
                            FALSE_EASTING + (path + 1) * grid_width,
                            (row + 1) * grid_width,
                        )
                        quad_min_lon, _, quad_max_lon, _ = utm_box_to_lonlat(
                            quadbox, zone
                        ).bounds
                        if quad_min_lon <= zone_max_lon and quad_max_lon >= zone_min_lon:
                            yield zone, path, row

        elif quadbox.intersects(polygon_utm):
            quad_mid_path = int(np.floor((quad_max_path + quad_min_path) / 2))
            quad_mid_row = int(np.floor((quad_max_row + quad_min_row) / 2))

            if quad_h <= 1:
                # Split the quad into two quads to check independently
                quads.append(
                    (
                        quad_min_path,
                        quad_min_row,
                        quad_mid_path,
                        quad_max_row,
                        certainly_within_zone,
                    )
                )
                quads.append(
                    (
                        quad_mid_path,
                        quad_min_row,
                        quad_max_path,
                        quad_max_row,
                        certainly_within_zone,
                    )
                )

            elif quad_w <= 1:
                # Split the quad into two quads to check independently
                quads.append(
                    (
                        quad_min_path,
                        quad_min_row,
                        quad_max_path,
                        quad_mid_row,
                        certainly_within_zone,
                    )
                )
                quads.append(
                    (
                        quad_min_path,
                        quad_mid_row,
                        quad_max_path,
                        quad_max_row,
                        certainly_within_zone,
                    )
                )

            else:
                # Split the quad into four quads to check independently
                quads.append(
                    (
                        quad_min_path,
                        quad_mid_row,
                        quad_mid_path,
                        quad_max_row,
                        certainly_within_zone,
                    )
                )
                quads.append(
                    (
                        quad_mid_path,
                        quad_mid_row,
                        quad_max_path,
                        quad_max_row,
                        certainly_within_zone,
                    )
                )
                quads.append(
                    (
                        quad_min_path,
                        quad_min_row,
                        quad_mid_path,
                        quad_mid_row,
                        certainly_within_zone,
                    )
                )
                quads.append(
                    (
                        quad_mid_path,
                        quad_min_row,
                        quad_max_path,
                        quad_mid_row,
                        certainly_within_zone,
                    )
                )

        else:
            # This quad doesn't intersect our polygon, continue
            pass
