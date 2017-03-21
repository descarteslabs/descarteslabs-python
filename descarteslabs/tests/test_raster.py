import unittest

from descarteslabs.services import Raster
from descarteslabs.services import Waldo


class TestRaster(unittest.TestCase):
    raster = None
    waldo = None

    @classmethod
    def setUpClass(cls):
        cls.raster = Raster()
        cls.waldo = Waldo()

    def test_raster(self):
        r = self.raster.raster(
            keys=['meta_LC80270312016188_v1'],
            bands=['red', 'green', 'blue', 'alpha'],
            resolution=960,
        )
        self.assertTrue("metadata" in r)
        self.assertTrue("files" in r)
        self.assertTrue("meta_LC80270312016188_v1_red-green-blue-alpha.tif" in r['files'])
        self.assertIsNotNone(r['files']['meta_LC80270312016188_v1_red-green-blue-alpha.tif'])

    def test_thumbnail(self):
        r = self.raster.raster(
            keys=['meta_LC80270312016188_v1'],
            bands=['red', 'green', 'blue', 'alpha'],
            outsize=[256, 256],
            scales=[[0, 4000]] * 4,
            of='PNG',
            ot='Byte',
        )
        self.assertTrue("metadata" in r)
        self.assertTrue("files" in r)
        self.assertIsNotNone(r['files']['meta_LC80270312016188_v1_red-green-blue-alpha.png'])

    def test_get_bands_by_key(self):
        r = self.raster.get_bands_by_key('meta_LC80270312016188_v1')
        for band in ['red', 'green', 'blue', 'alpha', 'swir1', 'swir2', 'ndvi',
                     'ndwi', 'evi', 'cirrus']:
            self.assertTrue(band in r)

    def test_landsat8_bands(self):
        r = self.raster.get_bands_by_constellation('L8')
        for band in ['red', 'green', 'blue', 'alpha', 'swir1', 'swir2', 'ndvi',
                     'ndwi', 'evi', 'cirrus']:
            self.assertTrue(band in r)

    def test_dlkeys_from_shape(self):
        iowa = self.waldo.shape('north-america_united-states_iowa', geom='low')
        iowa_geom = iowa['geometry']
        dlkeys_geojson = self.raster.dlkeys_from_shape(30.0, 2048, 16, iowa_geom)
        self.assertEqual(len(dlkeys_geojson['features']), 58)

    def test_dlkeys_from_latlon(self):
        dlkey_geojson = self.raster.dlkey_from_latlon(45.0, -90.0, 30.0, 2048, 16)
        self.assertEqual(dlkey_geojson['properties']['key'], "2048:16:30.0:16:-4:81")

    def test_dlkey(self):
        dlkey_geojson = self.raster.dlkey("2048:16:30.0:16:-4:81")
        self.assertEqual(dlkey_geojson['properties']['key'], "2048:16:30.0:16:-4:81")
