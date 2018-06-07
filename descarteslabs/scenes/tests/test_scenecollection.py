import unittest
import mock

from descarteslabs.client.addons import ThirdParty
from descarteslabs.scenes import Scene, SceneCollection


class TestSceneCollection(unittest.TestCase):
    def test_stack(self):
        scenes = ("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1")
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds=overlap.bounds, resolution=600)

        scenes = SceneCollection(scenes)
        stack, metas = scenes.stack("nir", ctx, raster_info=True)
        self.assertEqual(stack.shape, (2, 1, 123, 121))
        self.assertTrue((stack.mask[:, 0, 2, 2]).all())
        self.assertEqual(len(metas), 2)
        self.assertTrue(all(len(m["geoTransform"]) == 6 for m in metas))

        img_stack = scenes.stack("nir red", ctx, bands_axis=-1)
        self.assertEqual(img_stack.shape, (2, 123, 121, 2))

        # no_alpha = scenes.stack("nir", mask_alpha=False)
        # # assert raster not called with alpha once mocks exist

        no_mask = scenes.stack("nir", ctx, mask_alpha=False, mask_nodata=False)
        self.assertFalse(hasattr(no_mask, "mask"))
        self.assertEqual(no_mask.shape, (2, 1, 123, 121))

        with self.assertRaises(NotImplementedError):
            scenes.stack("nir red", ctx, bands_axis=0)

        stack_axis_1 = scenes.stack("nir red", ctx, bands_axis=1)
        self.assertEqual(stack_axis_1.shape, (2, 2, 123, 121))

    @mock.patch("descarteslabs.scenes.scenecollection.concurrent", ThirdParty("concurrent"))
    def test_stack_serial(self):
        scenes = ("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1")
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds=overlap.bounds, resolution=600)

        scenes = SceneCollection(scenes)
        stack, metas = scenes.stack("nir", ctx, raster_info=True)
        self.assertEqual(stack.shape, (2, 1, 123, 121))

    def test_mosaic(self):
        scenes = ("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1")
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds=overlap.bounds, resolution=600)

        scenes = SceneCollection(scenes)
        mosaic, meta = scenes.mosaic("nir", ctx, raster_info=True)
        self.assertEqual(mosaic.shape, (1, 123, 121))
        self.assertTrue((mosaic.mask[:, 2, 2]).all())
        self.assertEqual(len(meta["geoTransform"]), 6)

        img_mosaic = scenes.mosaic("nir red", ctx, bands_axis=-1)
        self.assertEqual(img_mosaic.shape, (123, 121, 2))

        mosaic_with_alpha = scenes.mosaic(["red", "alpha"], ctx)
        self.assertEqual(mosaic_with_alpha.shape, (2, 123, 121))

        mosaic_only_alpha = scenes.mosaic("alpha", ctx)
        self.assertEqual(mosaic_only_alpha.shape, (1, 123, 121))
        self.assertTrue(((mosaic_only_alpha.data == 0) == mosaic_only_alpha.mask).all())

        # no_alpha = scenes.mosaic("nir", mask_alpha=False)
        # # assert raster not called with alpha once mocks exist

        no_mask = scenes.mosaic("nir", ctx, mask_alpha=False, mask_nodata=False)
        self.assertFalse(hasattr(no_mask, "mask"))
        self.assertEqual(no_mask.shape, (1, 123, 121))

        with self.assertRaises(ValueError):
            scenes.mosaic("alpha red", ctx)

        with self.assertRaises(TypeError):
            scenes.mosaic("red", ctx, invalid_argument=True)

    def test_fails_with_different_dtypes(self):
        scenes = ("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1", "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1")
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds=overlap.bounds, resolution=600)

        scenes = SceneCollection(scenes)
        scenes[0].properties.bands.nir.dtype = "Byte"
        with self.assertRaises(ValueError):
            mosaic, meta = scenes.mosaic("nir", ctx)
        with self.assertRaises(ValueError):
            stack, meta = scenes.stack("nir", ctx)
