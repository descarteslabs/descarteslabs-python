import unittest
import mock

import shapely.geometry
import geojson

from descarteslabs.scenes import _helpers


class TestSimpleHelpers(unittest.TestCase):
    def test_polygon_from_bounds(self):
        bounds = (-95.8364984, 39.2784859, -92.0686956, 42.7999878)
        geom = {
            'coordinates': ((
                (-92.0686956, 39.2784859),
                (-92.0686956, 42.7999878),
                (-95.8364984, 42.7999878),
                (-95.8364984, 39.2784859),
                (-92.0686956, 39.2784859)
            ),),
            'type': 'Polygon'
        }
        self.assertEqual(geom, shapely.geometry.box(*bounds).__geo_interface__)
        self.assertEqual(_helpers.polygon_from_bounds(bounds), shapely.geometry.box(*bounds).__geo_interface__)

    def test_invalid_bounds(self):
        bounds_utm = (361760.0, 4531200.0, 515360.0, 4684800.0)
        bounds_wgs84 = (-94.37053769704536, 40.703737, -93.52300099792355, 41.3717716)
        bounds_wrong_order = (bounds_wgs84[2], bounds_wgs84[1], bounds_wgs84[0], bounds_wgs84[3])
        bounds_wrong_number = bounds_wgs84[:2]
        bounds_wrong_type = dict(left=1, right=2, top=3, bottom=4)
        bounds_point = (-90.0, 35.0, -90.0, 35.0)

        _helpers.test_valid_bounds(bounds_wgs84)

        with self.assertRaises(ValueError):
            _helpers.test_valid_bounds(bounds_utm)
        with self.assertRaises(ValueError):
            _helpers.test_valid_bounds(bounds_wrong_order)
        with self.assertRaises(ValueError):
            _helpers.test_valid_bounds(bounds_wrong_number)
        with self.assertRaises(TypeError):
            _helpers.test_valid_bounds(bounds_wrong_type)
        with self.assertRaises(ValueError):
            _helpers.test_valid_bounds(bounds_point)


class TestAsGeojsonInstance(unittest.TestCase):
    def setUp(self):
        self.feature = {
            'type': 'Feature',
            'geometry': {
                'type': 'Polygon',
                'coordinates': [[
                    [-93.52300099792355, 41.241436141055345],
                    [-93.7138666, 40.703737],
                    [-94.37053769704536, 40.83098709945576],
                    [-94.2036617, 41.3717716],
                    [-93.52300099792355, 41.241436141055345]
                ]],
            },
            'properties': {
                'foo': 'bar',
            }
        }

    def test_geometry(self):
        geoj = _helpers.as_geojson_geometry(self.feature["geometry"])
        self.assertIsInstance(geoj, geojson.Polygon)
        self.assertEqual(geoj, self.feature["geometry"])

    def test_feature(self):
        geoj = _helpers.as_geojson_geometry(self.feature)
        self.assertIsInstance(geoj, geojson.Polygon)
        self.assertEqual(geoj, self.feature["geometry"])

    def test_featurecollection(self):
        fc = {
            'type': 'FeatureCollection',
            'features': [self.feature, self.feature, self.feature],
        }
        gc = {
            'type': 'GeometryCollection',
            'geometries': [self.feature['geometry'], self.feature['geometry'], self.feature['geometry']],
        }
        geoj = _helpers.as_geojson_geometry(fc)
        self.assertIsInstance(geoj, geojson.GeometryCollection)
        self.assertEqual(geoj, gc)

    def test_invalid(self):
        self.assertRaises(ValueError, _helpers.as_geojson_geometry, 1.2)
        self.assertRaises(ValueError, _helpers.as_geojson_geometry, {})
        self.assertRaises(ValueError, _helpers.as_geojson_geometry, dict(self.feature['geometry'], type='Foo'))
        self.assertRaises(ValueError, _helpers.as_geojson_geometry, dict(self.feature['geometry'], coordinates=1))
        self.assertRaises(ValueError, _helpers.as_geojson_geometry, {
            "type": "FeatureCollection",
            "features": [self.feature, "hey"],
        })


class TestGeometryLikeToShapely(unittest.TestCase):
    def test_shapely_obj(self):
        shape = shapely.geometry.box(10, 20, 15, 30)
        as_shapely = _helpers.geometry_like_to_shapely(shape)
        self.assertIsInstance(as_shapely, shapely.geometry.Polygon)
        self.assertEqual(shape, as_shapely)

    def test_dict(self):
        shape = shapely.geometry.box(10, 20, 15, 30)
        mapping = shape.__geo_interface__
        as_shapely = _helpers.geometry_like_to_shapely(mapping)
        self.assertIsInstance(as_shapely, shapely.geometry.Polygon)
        self.assertEqual(shape, as_shapely)

    def test_geo_interface(self):
        shape = shapely.geometry.Point(-5, 10).buffer(5)
        obj = mock.Mock()
        obj.__geo_interface__ = shape.__geo_interface__
        as_shapely = _helpers.geometry_like_to_shapely(obj)
        self.assertIsInstance(as_shapely, shapely.geometry.Polygon)
        self.assertEqual(shape, as_shapely)

    def test_not_mapping_or_geo_interface(self):
        unhelpful = (1, 2, 3, 4)
        with self.assertRaises(TypeError):
            _helpers.geometry_like_to_shapely(unhelpful)

    def test_featurecollection(self):
        shapes = (shapely.geometry.Point(-5, 10), shapely.geometry.Point(-5, 10).buffer(5))
        fc = {
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'geometry': shape.__geo_interface__,
                    'properties': {'foo': 'bar'}
                } for shape in shapes
            ],
        }
        as_shapely = _helpers.geometry_like_to_shapely(fc)
        self.assertIsInstance(as_shapely, shapely.geometry.GeometryCollection)
        self.assertEqual(as_shapely, shapely.geometry.GeometryCollection(shapes))
