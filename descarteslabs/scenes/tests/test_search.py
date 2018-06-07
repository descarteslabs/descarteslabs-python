import unittest
import mock

from descarteslabs.client.addons import ThirdParty
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
        self.assertEqual(len(sc), 2)  # test client only has 2 scenes available

        self.assertIsInstance(ctx, geocontext.AOI)
        self.assertEqual(ctx.__geo_interface__, self.geom)
        self.assertEqual(ctx.resolution, 15)
        self.assertEqual(ctx.crs, "EPSG:32615")

        for scene in sc:
            self.assertEqual(len(scene.properties.bands), 16)

    @mock.patch("descarteslabs.scenes.geocontext.shapely", ThirdParty("shapely"))
    @mock.patch("descarteslabs.scenes.geocontext.have_shapely", False)
    def test_search_geom_no_shapely(self):
        with self.assertRaisesRegexp(NotImplementedError, "pip install"):
            sc, ctx = search(self.geom, products="landsat:LC08:PRE:TOAR", limit=4)

    def test_search_AOI(self):
        aoi = geocontext.AOI(self.geom, resolution=5)
        sc, ctx = search(aoi, products="landsat:LC08:PRE:TOAR", limit=4)
        self.assertEqual(len(sc), 2)  # test client only has 2 scenes available

        self.assertEqual(ctx.resolution, 5)
        self.assertEqual(ctx.crs, "EPSG:32615")

    def test_search_dltile(self):
        tile = geocontext.DLTile.from_key('64:0:1000.0:15:-2:70')
        sc, ctx = search(tile, products="landsat:LC08:PRE:TOAR", limit=4)
        self.assertEqual(len(sc), 2)  # test client only has 2 scenes available
        self.assertEqual(ctx, tile)

    def test_search_no_products(self):
        sc, ctx = search(self.geom, limit=4)
        self.assertEqual(len(sc), 2)  # test client only has 2 scenes available

        self.assertTrue(all(s.properties.product == "landsat:LC08:PRE:TOAR" for s in sc))
        for scene in sc:
            self.assertEqual(len(scene.properties.bands), 16)
