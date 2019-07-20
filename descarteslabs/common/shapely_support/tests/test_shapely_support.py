import unittest
import mock

import shapely.geometry
import geojson

from descarteslabs.common.shapely_support import (
    as_geojson_geometry,
    check_valid_bounds,
    geometry_like_to_shapely,
)


class ShapelySupportTest(unittest.TestCase):
    def setUp(self):
        self.feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-93.52300099792355, 41.241436141055345],
                        [-93.7138666, 40.703737],
                        [-94.37053769704536, 40.83098709945576],
                        [-94.2036617, 41.3717716],
                        [-93.52300099792355, 41.241436141055345],
                    ]
                ],
            },
            "properties": {"foo": "bar"},
        }

    def test_check_valid_bounds(self):
        bounds_wgs84 = (-94.37053769704536, 40.703737, -93.52300099792355, 41.3717716)
        bounds_wrong_order = (
            bounds_wgs84[2],
            bounds_wgs84[1],
            bounds_wgs84[0],
            bounds_wgs84[3],
        )
        bounds_wrong_number = bounds_wgs84[:2]
        bounds_wrong_type = dict(left=1, right=2, top=3, bottom=4)
        bounds_point = (-90.0, 35.0, -90.0, 35.0)

        check_valid_bounds(bounds_wgs84)

        with self.assertRaises(ValueError):
            check_valid_bounds(bounds_wrong_order)
        with self.assertRaises(ValueError):
            check_valid_bounds(bounds_wrong_number)
        with self.assertRaises(TypeError):
            check_valid_bounds(bounds_wrong_type)
        with self.assertRaises(ValueError):
            check_valid_bounds(bounds_point)

    def test_as_geojson_geometry(self):
        geoj = as_geojson_geometry(self.feature["geometry"])
        self.assertIsInstance(geoj, geojson.Polygon)
        self.assertEqual(geoj, self.feature["geometry"])

    def test_as_geojson_geometry_feature(self):
        geoj = as_geojson_geometry(self.feature)
        self.assertIsInstance(geoj, geojson.Polygon)
        self.assertEqual(geoj, self.feature["geometry"])

    def test_as_geojson_geometry_featurecollection(self):
        fc = {
            "type": "FeatureCollection",
            "features": [self.feature, self.feature, self.feature],
        }
        gc = {
            "type": "GeometryCollection",
            "geometries": [
                self.feature["geometry"],
                self.feature["geometry"],
                self.feature["geometry"],
            ],
        }
        geoj = as_geojson_geometry(fc)
        self.assertIsInstance(geoj, geojson.GeometryCollection)
        self.assertEqual(geoj, gc)

    def test_as_geojson_geometry_invalid(self):
        self.assertRaises(ValueError, as_geojson_geometry, 1.2)
        self.assertRaises(ValueError, as_geojson_geometry, {})
        self.assertRaises(
            ValueError, as_geojson_geometry, dict(self.feature["geometry"], type="Foo")
        )
        self.assertRaises(
            ValueError,
            as_geojson_geometry,
            dict(self.feature["geometry"], coordinates=1),
        )
        self.assertRaises(
            ValueError,
            as_geojson_geometry,
            {"type": "FeatureCollection", "features": [self.feature, "hey"]},
        )

    def test_geometry_like_to_shapely(self):
        shape = shapely.geometry.box(10, 20, 15, 30)
        as_shapely = geometry_like_to_shapely(shape)
        self.assertIsInstance(as_shapely, shapely.geometry.Polygon)
        self.assertEqual(shape, as_shapely)

    def test_geometry_like_to_shapely_dict(self):
        shape = shapely.geometry.box(10, 20, 15, 30)
        mapping = shape.__geo_interface__
        as_shapely = geometry_like_to_shapely(mapping)
        self.assertIsInstance(as_shapely, shapely.geometry.Polygon)
        self.assertEqual(shape, as_shapely)

    def test_geometry_like_to_shapely_geo_interface(self):
        shape = shapely.geometry.Point(-5, 10).buffer(5)
        obj = mock.Mock()
        obj.__geo_interface__ = shape.__geo_interface__
        as_shapely = geometry_like_to_shapely(obj)
        self.assertIsInstance(as_shapely, shapely.geometry.Polygon)
        self.assertEqual(shape, as_shapely)

    def test_geometry_like_to_shapely_not_mapping_or_geo_interface(self):
        unhelpful = (1, 2, 3, 4)
        with self.assertRaises(TypeError):
            geometry_like_to_shapely(unhelpful)

    def test_geometry_like_to_shapely_featurecollection(self):
        shapes = (
            shapely.geometry.Point(-5, 10),
            shapely.geometry.Point(-5, 10).buffer(5),
        )
        fc = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": shape.__geo_interface__,
                    "properties": {"foo": "bar"},
                }
                for shape in shapes
            ],
        }
        as_shapely = geometry_like_to_shapely(fc)
        self.assertIsInstance(as_shapely, shapely.geometry.GeometryCollection)
        self.assertEqual(as_shapely, shapely.geometry.GeometryCollection(shapes))
