import unittest
import datetime

from descarteslabs.scenes import geocontext, search
from shapely.geometry import shape

import mock
from .mock_data import _metadata_search, _metadata_get_bands_by_product


class TestScenesSearch(unittest.TestCase):
    geom = {
        "coordinates": (
            (
                (-95.836498, 39.278486),
                (-92.068696, 39.278486),
                (-92.068696, 42.799988),
                (-95.836498, 42.799988),
                (-95.836498, 39.278486),
            ),
        ),
        "type": "Polygon",
    }

    @mock.patch("descarteslabs.scenes._search.Metadata.search", _metadata_search)
    @mock.patch(
        "descarteslabs.scenes._search.Metadata.get_bands_by_product",
        _metadata_get_bands_by_product,
    )
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

    @mock.patch("descarteslabs.scenes._search.Metadata.search", _metadata_search)
    @mock.patch(
        "descarteslabs.scenes._search.Metadata.get_bands_by_product",
        _metadata_get_bands_by_product,
    )
    def test_search_shapely(self):
        sc, ctx = search(shape(self.geom), products="landsat:LC08:PRE:TOAR", limit=4)
        self.assertEqual(len(sc), 2)

        self.assertIsInstance(ctx, geocontext.AOI)
        self.assertEqual(ctx.__geo_interface__, self.geom)
        self.assertEqual(ctx.resolution, 15)
        self.assertEqual(ctx.crs, "EPSG:32615")

        for scene in sc:
            # allow for changes in publicly available data
            self.assertAlmostEqual(len(scene.properties.bands), 24, delta=4)
            self.assertIn("derived:ndvi", scene.properties.bands)

    @mock.patch("descarteslabs.scenes._search.Metadata.search", _metadata_search)
    @mock.patch(
        "descarteslabs.scenes._search.Metadata.get_bands_by_product",
        _metadata_get_bands_by_product,
    )
    def test_search_AOI(self):
        aoi = geocontext.AOI(self.geom, resolution=5)
        sc, ctx = search(aoi, products="landsat:LC08:PRE:TOAR", limit=4)
        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available

        self.assertEqual(ctx.resolution, 5)
        self.assertEqual(ctx.crs, "EPSG:32615")

    @mock.patch("descarteslabs.scenes._search.Metadata.search", _metadata_search)
    @mock.patch(
        "descarteslabs.scenes._search.Metadata.get_bands_by_product",
        _metadata_get_bands_by_product,
    )
    def test_search_AOI_with_shape(self):
        aoi = geocontext.AOI(self.geom, shape=(100, 100))
        sc, ctx = search(aoi, products="landsat:LC08:PRE:TOAR", limit=4)
        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available

        self.assertEqual(ctx.resolution, None)
        self.assertEqual(ctx.shape, aoi.shape)
        self.assertEqual(ctx.crs, "EPSG:32615")

    @mock.patch("descarteslabs.scenes._search.Metadata.search", _metadata_search)
    @mock.patch(
        "descarteslabs.scenes._search.Metadata.get_bands_by_product",
        _metadata_get_bands_by_product,
    )
    def test_search_dltile(self):
        tile = geocontext.DLTile(
            {
                "geometry": {
                    "coordinates": [
                        [
                            [-94.50970627780103, 40.460817879515986],
                            [-93.75494640538922, 40.468212507270195],
                            [-93.76149667591069, 41.04471363474632],
                            [-94.5228005945451, 41.03716803374444],
                            [-94.50970627780103, 40.460817879515986],
                        ]
                    ],
                    "type": "Polygon",
                },
                "properties": {
                    "cs_code": "EPSG:32615",
                    "key": "64:0:1000.0:15:-2:70",
                    "outputBounds": [372000.0, 4480000.0, 436000.0, 4544000.0],
                    "pad": 0,
                    "resolution": 1000.0,
                    "ti": -2,
                    "tilesize": 64,
                    "tj": 70,
                    "zone": 15,
                },
            }
        )
        sc, ctx = search(tile, products="landsat:LC08:PRE:TOAR", limit=4)
        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available
        self.assertEqual(ctx, tile)

    @mock.patch("descarteslabs.scenes._search.Metadata.search", _metadata_search)
    @mock.patch(
        "descarteslabs.scenes._search.Metadata.get_bands_by_product",
        _metadata_get_bands_by_product,
    )
    def test_search_no_products(self):
        sc, ctx = search(self.geom, limit=4)
        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available

    @mock.patch("descarteslabs.scenes._search.Metadata.search", _metadata_search)
    @mock.patch(
        "descarteslabs.scenes._search.Metadata.get_bands_by_product",
        _metadata_get_bands_by_product,
    )
    def test_search_datetime(self):
        start_datetime = datetime.datetime(2016, 7, 6)
        end_datetime = datetime.datetime(2016, 7, 15)
        sc, ctx = search(
            self.geom,
            products="landsat:LC08:PRE:TOAR",
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            limit=4,
        )

        self.assertGreater(len(sc), 0)
        self.assertLessEqual(len(sc), 4)  # test client only has 2 scenes available

        for scene in sc:
            self.assertGreaterEqual(scene.properties["date"], start_datetime)
            self.assertLessEqual(scene.properties["date"], end_datetime)
