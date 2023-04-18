# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import numbers
import numpy as np
import shapely.geometry as geo
from typing import List, Union

from .exceptions import InvalidShapeError

AnyShapes = Union[List[geo.base.BaseGeometry], geo.base.BaseGeometry, dict, str]

AnyPoints = Union[List[geo.Point], geo.Point, dict, str, np.ndarray]


def normalize_polygons(shape_or_shapes: AnyShapes) -> List[geo.base.BaseGeometry]:
    """Given a collection of shapes in some format, try to make it into a
    list of shapely polygons."""
    if isinstance(shape_or_shapes, str):
        shape_or_shapes = json.loads(shape_or_shapes)

    if isinstance(shape_or_shapes, list):
        out = list()
        for item in shape_or_shapes:
            out.extend(normalize_polygons(item))
        return out

    if isinstance(shape_or_shapes, dict):
        if "geometry" in shape_or_shapes:
            shape = geo.shape(shape_or_shapes["geometry"])
        elif "features" in shape_or_shapes:
            return [
                geo.shape(feature["geometry"])
                for feature in shape_or_shapes["features"]
            ]
        else:
            shape = geo.shape(shape_or_shapes)
        return normalize_polygons(shape)

    elif isinstance(shape_or_shapes, geo.MultiPolygon):
        return [shape_or_shapes]

    elif isinstance(shape_or_shapes, geo.Polygon):
        return [shape_or_shapes]

    elif isinstance(shape_or_shapes, geo.base.BaseGeometry):
        raise InvalidShapeError(
            "Geometries must be polygon or multipolygon, got %s" % type(shape_or_shapes)
        )

    elif hasattr(shape_or_shapes, "__geo_interface__"):
        return normalize_polygons(shape_or_shapes.__geo_interface__)

    raise InvalidShapeError(
        "Could not normalize shape or shapes of type %s" % type(shape_or_shapes)
    )


def normalize_points(point_or_points: AnyPoints) -> np.ndarray:
    """Given a collection of points in some format, try to make it into a
    numpy array."""
    if isinstance(point_or_points, list):
        if isinstance(point_or_points, numbers.Number):
            return np.array([point_or_points])
        out = list()
        for item in point_or_points:
            out.extend(normalize_points(item))
        return np.array(out)

    if isinstance(point_or_points, str):
        point_or_points = json.loads(point_or_points)

    if isinstance(point_or_points, dict):
        if "geometry" in point_or_points:
            return np.array(point_or_points["geometry"]).reshape((-1, 2))
        elif "features" in point_or_points:
            return np.array(
                [
                    np.array(feature["geometry"]).reshape((-1, 2))
                    for feature in point_or_points["features"]
                ]
            )
        else:
            raise InvalidShapeError(
                "Could not normalize point or points of type dict without "
                "geometry or features"
            )

    if isinstance(point_or_points, geo.Point):
        x, y = geo.Point
        return np.array([[x, y]])

    elif isinstance(point_or_points, np.ndarray):
        if len(point_or_points.shape) != 2:
            raise InvalidShapeError(
                "Incorrect number of dimensions for point_or_points array, "
                "expected 2, got %i" % len(point_or_points.shape)
            )
        if point_or_points.shape[-1] != 2:
            raise InvalidShapeError(
                "Incorrect size of last dimension for point_or_points array, "
                "expected 2, got %i" % point_or_points.shape[-1]
            )
        return point_or_points

    raise InvalidShapeError(
        "Could not normalize point or points of type %s" % type(point_or_points)
    )


def points_from_polygon(polygon: geo.Polygon) -> List[np.array]:
    """Get the exterior and interior points of a polygon from shapely"""
    if not isinstance(polygon, geo.Polygon):
        raise InvalidShapeError(
            "Expected a shapely Polygon object, got %s" % type(polygon)
        )
    if not polygon.exterior.coords:
        return np.array([[], []])

    points_list = [np.array(polygon.exterior.coords.xy).T[:-1, :]]
    for interior in polygon.interiors:
        points_list.append(np.array(interior.coords.xy).T[:-1, :])
    return points_list
