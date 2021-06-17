# Copyright 2018-2020 Descartes Labs.
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

from descarteslabs.client.addons import numpy as np
from descarteslabs.client.services.raster import Raster


class TestRaster(unittest.TestCase):
    raster = None
    places = None

    @classmethod
    def setUpClass(cls):
        cls.raster = Raster()

    def test_raster(self):
        filename, metadata = self.raster.raster(
            inputs=["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"],
            bands=["red", "green", "blue", "alpha"],
            resolution=960,
        )
        assert os.path.exists(filename)
        try:
            assert metadata is not None
        finally:
            os.unlink(filename)

    def test_raster_basename(self):
        tmpdir = tempfile.mkdtemp()
        try:
            filename, metadata = self.raster.raster(
                inputs=["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"],
                bands=["red", "green", "blue", "alpha"],
                resolution=960,
                outfile_basename="{}/my-raster".format(tmpdir),
            )
            assert filename == "{}/my-raster.tif".format(tmpdir)
            assert os.path.exists(filename)
        finally:
            shutil.rmtree(tmpdir)

    def test_ndarray(self):
        data, metadata = self.raster.ndarray(
            inputs=["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"],
            bands=["red", "green", "blue", "alpha"],
            resolution=960,
        )
        assert data.shape == (249, 245, 4)
        assert data.dtype == np.uint16
        assert len(metadata["bands"]) == 4

    def test_ndarray_single_band(self):
        data, metadata = self.raster.ndarray(
            inputs=["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"],
            bands=["red"],
            resolution=960,
        )
        assert data.shape == (249, 245, 1)
        assert data.dtype == np.uint16
        assert len(metadata["bands"]) == 1

    def test_stack_dltile(self):
        dltile = "128:16:960.0:15:-2:37"
        keys = [
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        ]

        stack, metadata = self.raster.stack(
            keys, dltile=dltile, bands=["red", "green", "blue", "alpha"]
        )
        assert stack.shape == (2, 160, 160, 4)
        assert stack.dtype == np.uint16
        assert len(metadata) == 2
        assert metadata[0] != metadata[1]

    def test_stack_dltile_gdal_order(self):
        dltile = "128:16:960.0:15:-2:37"
        keys = [
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        ]

        stack, metadata = self.raster.stack(
            keys, dltile=dltile, bands=["red", "green", "blue", "alpha"], order="gdal"
        )
        assert stack.shape == (2, 4, 160, 160)
        assert stack.dtype == np.uint16
        assert len(metadata) == 2
        assert metadata[0] != metadata[1]

    def test_stack_one_image(self):
        dltile = "128:16:960.0:15:-2:37"
        keys = ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"]

        stack, metadata = self.raster.stack(
            keys, dltile=dltile, bands=["red", "green", "blue", "alpha"]
        )
        assert stack.shape == (1, 160, 160, 4)
        assert stack.dtype == np.uint16
        assert len(metadata) == 1

    def test_stack_one_band(self):
        dltile = "128:16:960.0:15:-2:37"
        keys = ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"]

        stack, metadata = self.raster.stack(keys, dltile=dltile, bands=["red"])
        assert stack.shape == (1, 160, 160, 1)
        assert stack.dtype == np.uint16
        assert len(metadata) == 1

    def test_stack_one_band_gdal_order(self):
        dltile = "128:16:960.0:15:-2:37"
        keys = ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"]

        stack, metadata = self.raster.stack(
            keys, dltile=dltile, bands=["red"], order="gdal"
        )
        assert stack.shape == (1, 1, 160, 160)
        assert stack.dtype == np.uint16

    def test_stack_res_cutline_utm(self):
        geom = {
            "coordinates": (
                (
                    (-95.66055514862535, 41.24469400862013),
                    (-94.74931826062456, 41.26199387228942),
                    (-94.76311013534223, 41.95357639323731),
                    (-95.69397431605952, 41.93542085595837),
                    (-95.66055514862535, 41.24469400862013),
                ),
            ),
            "type": "Polygon",
        }
        keys = [
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        ]
        resolution = 960
        stack, metadata = self.raster.stack(
            keys,
            resolution=resolution,
            cutline=geom,
            srs="EPSG:32615",
            bounds=(277280.0, 4569600.0, 354080.0, 4646400.0),
            bands=["red", "green", "blue", "alpha"],
        )
        assert stack.shape == (2, 80, 80, 4)
        assert stack.dtype == np.uint16

    def test_stack_res_cutline_wgs84(self):
        geom = {
            "coordinates": (
                (
                    (-95.66055514862535, 41.24469400862013),
                    (-94.74931826062456, 41.26199387228942),
                    (-94.76311013534223, 41.95357639323731),
                    (-95.69397431605952, 41.93542085595837),
                    (-95.66055514862535, 41.24469400862013),
                ),
            ),
            "type": "Polygon",
        }
        keys = [
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        ]
        resolution = 960
        stack, metadata = self.raster.stack(
            keys,
            resolution=resolution,
            cutline=geom,
            srs="EPSG:32615",
            bounds=(
                -95.69397431605952,
                41.24469400862013,
                -94.74931826062456,
                41.95357639323731,
            ),
            bounds_srs="EPSG:4326",
            bands=["red", "green", "blue", "alpha"],
        )
        assert stack.shape == (2, 84, 84, 4)
        assert stack.dtype == np.uint16

    def test_cutline_dict(self):
        shape = {
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-95.2989209, 42.7999878],
                        [-93.1167728, 42.3858464],
                        [-93.7138666, 40.703737],
                        [-95.8364984, 41.1150618],
                        [-95.2989209, 42.7999878],
                    ]
                ],
            }
        }
        try:
            data, metadata = self.raster.ndarray(
                inputs=["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"],
                bands=["red"],
                resolution=960,
                cutline=shape,
            )
            assert data.shape == (245, 238, 1)
            assert data.dtype == np.uint16
            assert len(metadata["bands"]) == 1
        except ImportError:
            pass

    def test_cutline_str(self):
        shape = {
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-95.2989209, 42.7999878],
                        [-93.1167728, 42.3858464],
                        [-93.7138666, 40.703737],
                        [-95.8364984, 41.1150618],
                        [-95.2989209, 42.7999878],
                    ]
                ],
            }
        }
        try:
            data, metadata = self.raster.ndarray(
                inputs=["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"],
                bands=["red"],
                resolution=960,
                cutline=json.dumps(shape),
            )
            assert data.shape == (245, 238, 1)
            assert data.dtype == np.uint16
            assert len(metadata["bands"]) == 1
        except ImportError:
            pass

    def test_thumbnail(self):
        filename, metadata = self.raster.raster(
            inputs=["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"],
            bands=["red", "green", "blue", "alpha"],
            dimensions=[256, 256],
            scales=[[0, 4000]] * 4,
            output_format="PNG",
            data_type="Byte",
        )
        assert os.path.exists(filename)
        try:
            assert metadata is not None
        finally:
            os.unlink(filename)


if __name__ == "__main__":
    unittest.main()
