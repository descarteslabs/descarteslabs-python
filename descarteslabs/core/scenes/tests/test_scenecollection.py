import pytest
import unittest
from unittest.mock import patch

from .. import Scene, SceneCollection

from ...catalog import Image, ImageCollection
from ...catalog import image as image_module
from ...catalog import image_collection as image_collection_module
from .. import scene as scene_module
from ..helpers import REQUEST_PARAMS

from .mock_data import (
    _image_get,
    _cached_bands_by_product,
    _raster_ndarray,
)


class TestSceneCollection(unittest.TestCase):
    @patch.object(scene_module.Image, "get", _image_get)
    @patch.object(Image, "get", _image_get)
    def test_init(self):
        scene_ids = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        scenes = SceneCollection(Scene.from_id(scene_id)[0] for scene_id in scene_ids)

        assert len(scenes) == len(scene_ids)
        assert scenes.each.properties.id.collect(tuple) == scene_ids

        images = ImageCollection(
            Image.get(scene_id, request_params=REQUEST_PARAMS) for scene_id in scene_ids
        )
        scenes = SceneCollection(images)

        assert len(scenes) == len(scene_ids)
        assert scenes.each.properties.id.collect(tuple) == scene_ids

    @patch.object(scene_module.Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(
        image_collection_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_stack(self):
        scene_ids = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scene_ids])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds="update", resolution=600)

        scenes = SceneCollection(scenes)
        stack, metas = scenes.stack("nir", ctx, raster_info=True)
        assert stack.shape == (2, 1, 122, 120)
        assert (stack.mask[:, 0, 2, 2]).all()
        assert len(metas) == 2
        assert all(len(m["geoTransform"]) == 6 for m in metas)

        img_stack = scenes.stack("nir red", ctx, bands_axis=-1)
        assert img_stack.shape == (2, 122, 120, 2)

        # no_alpha = scenes.stack("nir", mask_alpha=False)
        # # assert raster not called with alpha once mocks exist

        no_mask = scenes.stack("nir", ctx, mask_alpha=False, mask_nodata=False)
        assert not hasattr(no_mask, "mask")
        assert no_mask.shape == (2, 1, 122, 120)

        with pytest.raises(NotImplementedError):
            scenes.stack("nir red", ctx, bands_axis=0)

        stack_axis_1 = scenes.stack("nir red", ctx, bands_axis=1)
        assert stack_axis_1.shape == (2, 2, 122, 120)

    @patch.object(scene_module.Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(
        image_collection_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_stack_flatten(self):
        scenes = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",  # note: just duplicated
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[2].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds="update", resolution=600)

        scenes = SceneCollection(scenes)

        flattened, metas = scenes.stack(
            "nir", ctx, flatten="properties.id", raster_info=True
        )

        assert len(flattened) == 2
        assert len(metas) == 2

        mosaic = scenes.mosaic("nir", ctx)
        allflat = scenes.stack("nir", ctx, flatten="properties.product")
        assert (mosaic == allflat).all()

    @patch.object(scene_module.Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(
        image_collection_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_mosaic(self):
        scenes = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds="update", resolution=600)

        scenes = SceneCollection(scenes)
        mosaic, meta = scenes.mosaic("nir", ctx, raster_info=True)
        assert mosaic.shape == (1, 122, 120)
        assert (mosaic.mask[:, 2, 2]).all()
        assert len(meta["geoTransform"]) == 6

        img_mosaic = scenes.mosaic("nir red", ctx, bands_axis=-1)
        assert img_mosaic.shape == (122, 120, 2)

        mosaic_with_alpha = scenes.mosaic(["red", "alpha"], ctx)
        assert mosaic_with_alpha.shape == (2, 122, 120)

        mosaic_only_alpha = scenes.mosaic("alpha", ctx)
        assert mosaic_only_alpha.shape == (1, 122, 120)
        assert ((mosaic_only_alpha.data == 0) == mosaic_only_alpha.mask).all()

        # no_alpha = scenes.mosaic("nir", mask_alpha=False)
        # # assert raster not called with alpha once mocks exist

        no_mask = scenes.mosaic("nir", ctx, mask_alpha=False, mask_nodata=False)
        assert not hasattr(no_mask, "mask")
        assert no_mask.shape == (1, 122, 120)

        with pytest.raises(ValueError):
            scenes.mosaic("alpha red", ctx)

        with pytest.raises(TypeError):
            scenes.mosaic("red", ctx, invalid_argument=True)

        mask_non_alpha = mosaic_with_alpha = scenes.mosaic(
            ["nir", "red"], ctx, mask_alpha="red"
        )
        assert hasattr(mask_non_alpha, "mask")
        assert mask_non_alpha.shape == (2, 122, 120)

    @patch.object(scene_module.Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(
        image_collection_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_mosaic_no_alpha(self):
        scenes = (
            "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1",
            "modis:mod11a2:006:meta_MOD11A2.A2000049.h08v05.006.2015058135046_v1",
        )
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])
        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds="update", resolution=600)

        sc = SceneCollection(scenes)
        no_mask = sc.mosaic(
            ["Clear_sky_days", "Clear_sky_nights"], ctx, mask_nodata=False
        )
        assert not hasattr(no_mask, "mask")

        masked_alt_alpha_band = sc.mosaic(
            ["Clear_sky_days", "Clear_sky_nights"], ctx, mask_alpha="Clear_sky_nights"
        )
        assert hasattr(masked_alt_alpha_band, "mask")

        # errors when alternate alpha band is provided but not available in the scene
        with pytest.raises(ValueError):
            sc.mosaic(
                ["Clear_sky_days", "Clear_sky_nights"], ctx, mask_alpha="alt-alpha"
            )
