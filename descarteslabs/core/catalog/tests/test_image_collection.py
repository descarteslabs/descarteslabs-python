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

import pytest
import unittest
from unittest.mock import patch
import os.path
import shapely.geometry
import numpy as np

from ...common.geo import AOI

from .. import image_collection as icmod
from .. import image as imod
from ..image_collection import ImageCollection
from ..image import Image
from ..image_types import ResampleAlgorithm, DownloadFileFormat
from .mock_data import _image_get, _cached_bands_by_product, _raster_ndarray


class TestImageCollection(unittest.TestCase):
    @patch.object(Image, "get", _image_get)
    @patch.object(
        imod,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(
        icmod,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(imod.Raster, "ndarray", _raster_ndarray)
    def test_stack(self):
        image_ids = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        images = [Image.get(image_id) for image_id in image_ids]

        overlap = images[0].geometry.intersection(images[1].geometry)
        geocontext = images[0].geocontext.assign(
            geometry=overlap, bounds="update", resolution=600
        )

        ic = ImageCollection(images, geocontext=geocontext)
        stack, metas = ic.stack("nir", raster_info=True)
        assert stack.shape == (2, 1, 122, 120)
        assert (stack.mask[:, 0, 2, 2]).all()
        assert len(metas) == 2
        assert all(len(m["geoTransform"]) == 6 for m in metas)

        img_stack = ic.stack("nir red", bands_axis=-1)
        assert img_stack.shape == (2, 122, 120, 2)

        # no_alpha = scenes.stack("nir", mask_alpha=False)
        # # assert raster not called with alpha once mocks exist

        no_mask = ic.stack("nir", mask_alpha=False, mask_nodata=False)
        assert not hasattr(no_mask, "mask")
        assert no_mask.shape == (2, 1, 122, 120)

        with pytest.raises(NotImplementedError):
            ic.stack("nir red", geocontext=geocontext, bands_axis=0)

        stack_axis_1 = ic.stack("nir red", bands_axis=1)
        assert stack_axis_1.shape == (2, 2, 122, 120)

    @patch.object(Image, "get", _image_get)
    @patch.object(
        imod,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(
        icmod,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(imod.Raster, "ndarray", _raster_ndarray)
    def test_stack_scaling(self):
        image_ids = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        images = [Image.get(image_id) for image_id in image_ids]

        overlap = images[0].geometry.intersection(images[1].geometry)
        geocontext = images[0].geocontext.assign(
            geometry=overlap, bounds="update", resolution=600
        )
        ic = ImageCollection(images, geocontext=geocontext)

        stack = ic.stack("nir alpha", scaling="raw")
        assert stack.shape == (2, 2, 122, 120)
        assert stack.dtype == np.uint16

        stack = ic.stack("nir", scaling="raw")
        assert stack.shape == (2, 1, 122, 120)
        assert stack.dtype == np.uint16

        stack = ic.stack("nir", scaling=[None])
        assert stack.shape == (2, 1, 122, 120)
        assert stack.dtype == np.uint16

    @patch.object(Image, "get", _image_get)
    @patch.object(
        imod,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(
        icmod,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(imod.Raster, "ndarray", _raster_ndarray)
    @patch.object(icmod.Raster, "ndarray", _raster_ndarray)
    def test_stack_flatten(self):
        image_ids = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",  # note: just duplicated
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        images = [Image.get(image_id) for image_id in image_ids]

        overlap = images[0].geometry.intersection(images[2].geometry)
        geocontext = images[0].geocontext.assign(
            geometry=overlap, bounds="update", resolution=600
        )

        ic = ImageCollection(images, geocontext=geocontext)

        flattened, metas = ic.stack("nir", flatten="id", raster_info=True)

        assert len(flattened) == 2
        assert len(metas) == 2

        mosaic = ic.mosaic("nir")
        allflat = ic.stack("nir", flatten="product_id")
        assert (mosaic == allflat).all()

    @patch.object(Image, "get", _image_get)
    @patch.object(
        icmod,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(icmod.Raster, "ndarray", _raster_ndarray)
    def test_mosaic(self):
        image_ids = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        images = [Image.get(image_id) for image_id in image_ids]

        overlap = images[0].geometry.intersection(images[1].geometry)
        geocontext = images[0].geocontext.assign(
            geometry=overlap, bounds="update", resolution=600
        )

        ic = ImageCollection(images, geocontext=geocontext)
        mosaic, meta = ic.mosaic("nir", raster_info=True)
        assert mosaic.shape == (1, 122, 120)
        assert (mosaic.mask[:, 2, 2]).all()
        assert len(meta["geoTransform"]) == 6

        img_mosaic = ic.mosaic("nir red", bands_axis=-1)
        assert img_mosaic.shape == (122, 120, 2)

        mosaic_with_alpha = ic.mosaic(["red", "alpha"])
        assert mosaic_with_alpha.shape == (2, 122, 120)

        mosaic_only_alpha = ic.mosaic("alpha")
        assert mosaic_only_alpha.shape == (1, 122, 120)
        assert ((mosaic_only_alpha.data == 0) == mosaic_only_alpha.mask).all()

        # no_alpha = scenes.mosaic("nir", mask_alpha=False)
        # # assert raster not called with alpha once mocks exist

        no_mask = ic.mosaic("nir", mask_alpha=False, mask_nodata=False)
        assert not hasattr(no_mask, "mask")
        assert no_mask.shape == (1, 122, 120)

        with pytest.raises(ValueError):
            ic.mosaic("alpha red")

        with pytest.raises(TypeError):
            ic.mosaic("red", invalid_argument=True)

        mask_non_alpha = mosaic_with_alpha = ic.mosaic(["nir", "red"], mask_alpha="red")
        assert hasattr(mask_non_alpha, "mask")
        assert mask_non_alpha.shape == (2, 122, 120)

    @patch.object(Image, "get", _image_get)
    @patch.object(
        icmod,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(icmod.Raster, "ndarray", _raster_ndarray)
    def test_mosaic_scaling(self):
        image_ids = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        images = [Image.get(image_id) for image_id in image_ids]

        overlap = images[0].geometry.intersection(images[1].geometry)
        geocontext = images[0].geocontext.assign(
            geometry=overlap, bounds="update", resolution=600
        )
        ic = ImageCollection(images, geocontext=geocontext)

        mosaic = ic.mosaic("nir alpha", scaling="raw")
        assert mosaic.shape == (2, 122, 120)
        assert mosaic.dtype == np.uint16

        mosaic = ic.mosaic("nir", scaling="raw")
        assert mosaic.shape == (1, 122, 120)
        assert mosaic.dtype == np.uint16

        mosaic = ic.mosaic("nir", scaling=[None])
        assert mosaic.shape == (1, 122, 120)
        assert mosaic.dtype == np.uint16

    @patch.object(Image, "get", _image_get)
    @patch.object(
        icmod,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(icmod.Raster, "ndarray", _raster_ndarray)
    def test_mosaic_no_alpha(self):
        image_ids = (
            "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1",
            "modis:mod11a2:006:meta_MOD11A2.A2000049.h08v05.006.2015058135046_v1",
        )
        images = [Image.get(image_id) for image_id in image_ids]
        overlap = images[0].geometry.intersection(images[1].geometry)
        geocontext = images[0].geocontext.assign(
            geometry=overlap, bounds="update", resolution=600
        )

        ic = ImageCollection(images, geocontext=geocontext)
        no_mask = ic.mosaic(["Clear_sky_days", "Clear_sky_nights"], mask_nodata=False)
        assert not hasattr(no_mask, "mask")

        masked_alt_alpha_band = ic.mosaic(
            ["Clear_sky_days", "Clear_sky_nights"], mask_alpha="Clear_sky_nights"
        )
        assert hasattr(masked_alt_alpha_band, "mask")

        # errors when alternate alpha band is provided but not available in the scene
        with pytest.raises(ValueError):
            ic.mosaic(["Clear_sky_days", "Clear_sky_nights"], mask_alpha="alt-alpha")

    @patch.object(Image, "get", _image_get)
    def test_filter_coverage(self):
        polygon = shapely.geometry.Point(0.0, 0.0).buffer(3)
        geocontext = AOI(geometry=polygon)

        scenes = ImageCollection(
            [
                Image(id="foo:bar", geometry=polygon),
                Image(id="foo:baz", geometry=polygon.buffer(-0.1)),
            ]
        )

        assert len(scenes.filter_coverage(geocontext)) == 1

    @patch.object(Image, "get", _image_get)
    @patch.object(
        icmod,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    def test_scaling_parameters(self):
        image_ids = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        images = [Image.get(image_id) for image_id in image_ids]

        ic = ImageCollection(images)
        scales, data_type = ic.scaling_parameters("red green blue alpha")
        assert scales is None
        assert data_type == "UInt16"


@patch.object(Image, "get", _image_get)
@patch.object(
    imod,
    "cached_bands_by_product",
    _cached_bands_by_product,
)
@patch.object(
    icmod,
    "cached_bands_by_product",
    _cached_bands_by_product,
)
class TestSceneCollectionDownload(unittest.TestCase):
    def setUp(self):
        with patch.object(Image, "get", _image_get):
            images = [
                Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"),
                Image.get("landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"),
            ]
        overlap = images[0].geometry.intersection(images[1].geometry)
        geocontext = images[0].geocontext.assign(
            geometry=overlap, bounds="update", resolution=600
        )
        self.images = ImageCollection(images, geocontext=geocontext)

    @patch.object(imod, "download")
    def test_directory(self, mock_download):
        dest = "rasters"
        paths = self.images.download(
            "nir green", dest=dest, format=DownloadFileFormat.PNG
        )

        assert paths == [
            os.path.join(dest, f"{self.images[0].id}-nir-green.png"),
            os.path.join(dest, f"{self.images[1].id}-nir-green.png"),
        ]

        assert mock_download.call_count == len(self.images)
        for image, path in zip(self.images, paths):
            mock_download.assert_any_call(
                inputs=[image.id],
                bands_list=["nir", "green"],
                geocontext=self.images.geocontext,
                dest=path,
                format=DownloadFileFormat.TIF,
                resampler=ResampleAlgorithm.NEAR,
                processing_level=None,
                nodata=None,
                scales=None,
                data_type="UInt16",
                progress=None,
            )

    @patch.object(imod, "download")
    def test_custom_paths(self, mock_download):
        filenames = [
            os.path.join("foo", "img1.tif"),
            os.path.join("bar", "img2.jpg"),
        ]
        result = self.images.download("nir green", dest=filenames)
        assert result == filenames

        assert mock_download.call_count == len(self.images)
        for image, path in zip(self.images, filenames):
            mock_download.assert_any_call(
                inputs=[image.id],
                bands_list=["nir", "green"],
                geocontext=self.images.geocontext,
                dest=path,
                format=DownloadFileFormat.TIF,
                resampler=ResampleAlgorithm.NEAR,
                processing_level=None,
                nodata=None,
                scales=None,
                data_type="UInt16",
                progress=None,
            )

    @patch.object(imod, "download")
    def test_non_unique_paths(self, mock_download):
        nonunique_paths = ["img.tif", "img.tif"]
        with pytest.raises(RuntimeError):
            self.images.download("nir green", dest=nonunique_paths)

    @patch.object(imod, "download")
    def test_wrong_number_of_dest(self, mock_download):
        with pytest.raises(ValueError):
            self.images.download("nir", dest=["a", "b", "c"])

    @patch.object(imod, "download")
    def test_wrong_type_of_dest(self, mock_download):
        with pytest.raises(TypeError):
            self.images.download("nir", dest=4)

    @patch.object(imod, "download")
    def test_download_failure(self, mock_download):
        mock_download.side_effect = RuntimeError("blarf")
        dest = "rasters"
        with pytest.raises(RuntimeError):
            self.images.download("nir", dest=dest)

    @patch.object(icmod, "download")
    def test_download_mosaic(self, mock_download):
        self.images.download_mosaic("nir green")

        mock_download.assert_called_once()
        called_ids = mock_download.call_args[1]["inputs"]
        assert called_ids == self.images.each.id.collect()
