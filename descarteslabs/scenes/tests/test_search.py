import unittest
import datetime

from descarteslabs.scenes import geocontext, search


class TestScenesSearch(unittest.TestCase):
    geom = {
        'coordinates': ((
            (-95.8364984, 39.2784859),
            (-92.0686956, 39.2784859),
            (-92.0686956, 42.7999878),
            (-95.8364984, 42.7999878),
            (-95.8364984, 39.2784859)
        ),),
        'type': 'Polygon'
    }

    def test_search_geom(self):
        sc, ctx = search(self.geom, products="landsat:LC08:PRE:TOAR", limit=4)
        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available

        self.assertIsInstance(ctx, geocontext.AOI)
        self.assertEqual(ctx.__geo_interface__, self.geom)
        self.assertEqual(ctx.resolution, 15)
        self.assertEqual(ctx.crs, "EPSG:32615")

        for scene in sc:
            # allow for changes in publicly available data
            self.assertAlmostEqual(len(scene.properties.bands), 24, delta=4)
            self.assertIn("derived:ndvi", scene.properties.bands)

    def test_search_AOI(self):
        aoi = geocontext.AOI(self.geom, resolution=5)
        sc, ctx = search(aoi, products="landsat:LC08:PRE:TOAR", limit=4)
        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available

        self.assertEqual(ctx.resolution, 5)
        self.assertEqual(ctx.crs, "EPSG:32615")

    def test_search_AOI_with_shape(self):
        aoi = geocontext.AOI(self.geom, shape=(100, 100))
        sc, ctx = search(aoi, products="landsat:LC08:PRE:TOAR", limit=4)
        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available

        self.assertEqual(ctx.resolution, None)
        self.assertEqual(ctx.shape, aoi.shape)
        self.assertEqual(ctx.crs, "EPSG:32615")

    def test_search_dltile(self):
        tile = geocontext.DLTile.from_key('64:0:1000.0:15:-2:70')
        sc, ctx = search(tile, products="landsat:LC08:PRE:TOAR", limit=4)
        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available
        self.assertEqual(ctx, tile)

    def test_search_no_products(self):
        sc, ctx = search(self.geom, limit=4)
        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available

    def test_search_datetime(self):
        start_datetime = datetime.datetime(2016, 7, 6)
        end_datetime = datetime.datetime(2016, 7, 15)
        sc, ctx = search(
            self.geom,
            products="landsat:LC08:PRE:TOAR",
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            limit=4
        )

        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available

        for scene in sc:
            self.assertGreaterEqual(scene.properties['date'], start_datetime)
            self.assertLessEqual(scene.properties['date'], end_datetime)
