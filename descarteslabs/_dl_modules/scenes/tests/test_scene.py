import pytest
import unittest
from unittest.mock import patch
import shapely.geometry

from ...common.geo import AOI
from ..scene import Scene
from .. import scene as scene_module
from ..helpers import REQUEST_PARAMS

from ...catalog import Image
from ...catalog import image as image_module

from .mock_data import (
    _image_get,
    _cached_bands_by_product,
    _raster_ndarray,
    catalog_mock_data,
)


class TestScene(unittest.TestCase):
    @patch.object(Image, "get", _image_get)
    @patch.object(scene_module, "cached_bands_by_product", _cached_bands_by_product)
    def test_properties(self):
        scene_id = "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        image = Image.get(scene_id, request_params=REQUEST_PARAMS)
        scene = Scene(image)

        assert scene.properties.id == scene_id
        assert scene.properties.product == "landsat:LC08:PRE:TOAR"
        assert len(scene.properties.bands) == 24
        assert isinstance(scene.properties.bands, dict)
        assert scene.properties.crs == "EPSG:32615"
        assert isinstance(scene.geometry, shapely.geometry.Polygon)
        assert isinstance(scene.__geo_interface__, dict)

    @patch.object(Image, "get", _image_get)
    @patch.object(scene_module, "cached_bands_by_product", _cached_bands_by_product)
    def test_default_ctx(self):
        scene_id = "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        image = Image.get(scene_id, request_params=REQUEST_PARAMS)
        scene = Scene(image)

        assert scene.default_ctx() == image.geocontext

    @patch.object(Image, "get", _image_get)
    @patch.object(scene_module, "cached_bands_by_product", _cached_bands_by_product)
    def test_from_id(self):
        scene_id = "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        scene, ctx = Scene.from_id(scene_id)

        assert scene.properties.id == scene_id
        assert isinstance(scene.geometry, shapely.geometry.Polygon)
        assert isinstance(ctx, AOI)

    @patch.object(Image, "get", _image_get)
    @patch.object(image_module, "cached_bands_by_product", _cached_bands_by_product)
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_ndarray(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr, info = scene.ndarray(
            "red green blue", ctx.assign(resolution=1000), raster_info=True
        )

        assert arr.shape == (3, 239, 235)
        assert arr.mask[0, 2, 2]
        assert not arr.mask[0, 115, 116]
        assert len(info["geoTransform"]) == 6

        with pytest.raises(TypeError):
            scene.ndarray("blue", ctx, invalid_argument=True)

    @patch.object(Image, "get", _image_get)
    def test_coverage(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        geom = ctx.geometry.buffer(1)

        assert scene.coverage(geom) == scene._image.coverage(geom)

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        catalog_mock_data._cached_bands_by_product,
    )
    def test_scaling_parameters_display(self):
        scene, _ = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", scaling="display"
        )
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        assert data_type == "Byte"
