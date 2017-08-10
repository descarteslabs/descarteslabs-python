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

import os
import tempfile
import shutil
import unittest
import json
from warnings import catch_warnings

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
        # test with product key
        r = self.raster.raster(
            inputs=['meta_LC80270312016188_v1'],
            bands=['red', 'green', 'blue', 'alpha'],
            resolution=960,
        )
        self.assertTrue("metadata" in r)
        self.assertTrue("files" in r)
        self.assertTrue("meta_LC80270312016188_v1_red-green-blue-alpha.tif" in r['files'])

        # test with product id
        r = self.raster.raster(
            inputs=['landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1'],
            bands=['red', 'green', 'blue', 'alpha'],
            resolution=960,
        )
        self.assertTrue("metadata" in r)
        self.assertTrue("files" in r)
        self.assertTrue("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1_red-green-blue-alpha.tif" in r['files'])

    def test_raster_save(self):
        tmpdir = tempfile.mkdtemp()
        try:
            response = self.raster.raster(
                inputs=['meta_LC80270312016188_v1'],
                bands=['red', 'green', 'blue', 'alpha'],
                resolution=960, save=True,
                outfile_basename="{}/my-raster".format(tmpdir)
            )
            with open("{}/my-raster.tif".format(tmpdir), "rb") as f:
                f.seek(0, os.SEEK_END)
                length = f.tell()
            self.assertEqual(
                length,
                len(response['files']["{}/my-raster.tif".format(tmpdir)])
            )
        finally:
            shutil.rmtree(tmpdir)

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
        with catch_warnings(record=True) as w:
            r = self.raster.get_bands_by_key('meta_LC80270312016188_v1')
            self.assertEqual(len(w), 1)
            for band in ['red', 'green', 'blue', 'alpha', 'swir1', 'swir2', 'ndvi',
                         'ndwi', 'evi', 'cirrus']:
                self.assertTrue(band in r)

    def test_landsat8_bands(self):
        with catch_warnings(record=True) as w:
            r = self.raster.get_bands_by_constellation('L8')
            self.assertEqual(len(w), 1)
            for band in ['red', 'green', 'blue', 'alpha', 'swir1', 'swir2', 'ndvi',
                         'ndwi', 'evi', 'cirrus']:
                self.assertTrue(band in r)

    def test_dltiles_from_place(self):
        iowa = self.places.shape('north-america_united-states_iowa', geom='low')
        iowa_geom = iowa['geometry']
        dltiles_feature_collection = self.raster.dltiles_from_shape(30.0, 2048, 16, iowa_geom)
        self.assertEqual(len(dltiles_feature_collection['features']), 58)

    def test_dltiles_from_latlon(self):
        dltile_feature = self.raster.dltile_from_latlon(45.0, -90.0, 30.0, 2048, 16)
        self.assertEqual(dltile_feature['properties']['key'], "2048:16:30.0:16:-4:81")

    def test_dltile(self):
        dltile_feature = self.raster.dltile("2048:16:30.0:16:-4:81")
        self.assertEqual(dltile_feature['properties']['key'], "2048:16:30.0:16:-4:81")

    def test_raster_dltile(self):
        dltile_feature = self.raster.dltile_from_latlon(41.0, -94.0, 30.0, 256, 16)
        arr, meta = self.raster.ndarray(
            inputs=['meta_LC80270312016188_v1'],
            bands=['red', 'green', 'blue', 'alpha'],
            dltile=dltile_feature['properties']['key'],
        )
        self.assertEqual(arr.shape[0], 256 + 2 * 16)
        self.assertEqual(arr.shape[1], 256 + 2 * 16)
        self.assertEqual(arr.shape[2], 4)

    def test_raster_dltile_dict(self):
        dltile_feature = self.raster.dltile_from_latlon(41.0, -94.0, 30.0, 256, 16)
        arr, meta = self.raster.ndarray(
            inputs=['meta_LC80270312016188_v1'],
            bands=['red', 'green', 'blue', 'alpha'],
            dltile=dltile_feature,
        )
        self.assertEqual(arr.shape[0], 256 + 2 * 16)
        self.assertEqual(arr.shape[1], 256 + 2 * 16)
        self.assertEqual(arr.shape[2], 4)

    def test_dlkeys_from_place(self):
        iowa = self.places.shape('north-america_united-states_iowa', geom='low')
        iowa_geom = iowa['geometry']
        with catch_warnings(record=True) as w:
            dlkeys_geojson = self.raster.dlkeys_from_shape(30.0, 2048, 16, iowa_geom)
            self.assertEqual(len(dlkeys_geojson['features']), 58)
            self.assertEqual(DeprecationWarning, w[0].category)

    def test_dlkeys_from_latlon(self):
        with catch_warnings(record=True) as w:
            dlkey_geojson = self.raster.dlkey_from_latlon(45.0, -90.0, 30.0, 2048, 16)
            self.assertEqual(dlkey_geojson['properties']['key'], "2048:16:30.0:16:-4:81")
            self.assertEqual(DeprecationWarning, w[0].category)

    def test_dlkey(self):
        with catch_warnings(record=True) as w:
            dlkey_geojson = self.raster.dlkey("2048:16:30.0:16:-4:81")
            self.assertEqual(dlkey_geojson['properties']['key'], "2048:16:30.0:16:-4:81")
            self.assertEqual(DeprecationWarning, w[0].category)
