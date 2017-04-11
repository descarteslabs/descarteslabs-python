# Copyright 2017 Descartes Labs.
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

import unittest
import json

import descarteslabs as dl
from descarteslabs.addons import numpy as np


class TestRaster(unittest.TestCase):
    raster = None
    places = None

    @classmethod
    def setUpClass(cls):
        cls.raster = dl.raster
        cls.places = dl.places

    def test_raster(self):
        r = self.raster.raster(
            inputs=['meta_LC80270312016188_v1'],
            bands=['red', 'green', 'blue', 'alpha'],
            resolution=960,
        )
        self.assertTrue("metadata" in r)
        self.assertTrue("files" in r)
        self.assertTrue("meta_LC80270312016188_v1_red-green-blue-alpha.tif" in r['files'])
        self.assertIsNotNone(r['files']['meta_LC80270312016188_v1_red-green-blue-alpha.tif'])

    def test_ndarray(self):
        try:
            data, metadata = self.raster.ndarray(
                    inputs=['meta_LC80270312016188_v1'],
                    bands=['red', 'green', 'blue', 'alpha'],
                    resolution=960,
                    )
            self.assertEqual(data.shape, (249, 245, 4))
            self.assertEqual(data.dtype, np.uint16)
            self.assertEqual(len(metadata['bands']), 4)
        except ImportError:
            pass

    def test_ndarray_single_band(self):
        try:
            data, metadata = self.raster.ndarray(
                    inputs=['meta_LC80270312016188_v1'],
                    bands=['red'],
                    resolution=960,
                    )
            self.assertEqual(data.shape, (249, 245))
            self.assertEqual(data.dtype, np.uint16)
            self.assertEqual(len(metadata['bands']), 1)
        except ImportError:
            pass

    def test_cutline_dict(self):
        shape = {"geometry":
                 {"type": "Polygon",
                  "coordinates": [[[-95.2989209, 42.7999878], [-93.1167728, 42.3858464],
                                   [-93.7138666, 40.703737], [-95.8364984, 41.1150618],
                                   [-95.2989209, 42.7999878]]]
                  }
                 }
        try:
            data, metadata = self.raster.ndarray(
                    inputs=['meta_LC80270312016188_v1'],
                    bands=['red'],
                    resolution=960,
                    cutline=shape,
                    )
            self.assertEqual(data.shape, (245, 238))
            self.assertEqual(data.dtype, np.uint16)
            self.assertEqual(len(metadata['bands']), 1)
        except ImportError:
            pass

    def test_cutline_str(self):
        shape = {"geometry":
                 {"type": "Polygon",
                  "coordinates": [[[-95.2989209, 42.7999878], [-93.1167728, 42.3858464],
                                   [-93.7138666, 40.703737], [-95.8364984, 41.1150618],
                                   [-95.2989209, 42.7999878]]]}}
        try:
            data, metadata = self.raster.ndarray(
                    inputs=['meta_LC80270312016188_v1'],
                    bands=['red'],
                    resolution=960,
                    cutline=json.dumps(shape),
                    )
            self.assertEqual(data.shape, (245, 238))
            self.assertEqual(data.dtype, np.uint16)
            self.assertEqual(len(metadata['bands']), 1)
        except ImportError:
            pass

    def test_thumbnail(self):
        r = self.raster.raster(
            inputs=['meta_LC80270312016188_v1'],
            bands=['red', 'green', 'blue', 'alpha'],
            dimensions=[256, 256],
            scales=[[0, 4000]] * 4,
            output_format='PNG',
            data_type='Byte',
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

    def test_dlkeys_from_place(self):
        iowa = self.places.shape('north-america_united-states_iowa', geom='low')
        iowa_geom = iowa['geometry']
        dlkeys_geojson = self.raster.dlkeys_from_shape(30.0, 2048, 16, iowa_geom)
        self.assertEqual(len(dlkeys_geojson['features']), 58)

    def test_dlkeys_from_latlon(self):
        dlkey_geojson = self.raster.dlkey_from_latlon(45.0, -90.0, 30.0, 2048, 16)
        self.assertEqual(dlkey_geojson['properties']['key'], "2048:16:30.0:16:-4:81")

    def test_dlkey(self):
        dlkey_geojson = self.raster.dlkey("2048:16:30.0:16:-4:81")
        self.assertEqual(dlkey_geojson['properties']['key'], "2048:16:30.0:16:-4:81")
