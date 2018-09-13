import unittest
import mock
import os.path
import shapely.geometry

from descarteslabs.client.addons import ThirdParty
from descarteslabs.scenes import Scene, SceneCollection, geocontext

from .test_scene import MockScene


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

    def test_stack_flatten(self):
        scenes = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",  # note: just duplicated
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1"
        )
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[2].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds=overlap.bounds, resolution=600)

        scenes = SceneCollection(scenes)

        unflattened = scenes.stack("nir", ctx)

        flattened, metas = scenes.stack(
            "nir",
            ctx,
            flatten="properties.id",
            raster_info=True,
        )

        self.assertEqual(len(flattened), 2)
        self.assertEqual(len(metas), 2)

        mosaic = scenes.mosaic("nir", ctx)
        allflat = scenes.stack("nir", ctx, flatten="properties.product")
        self.assertTrue((mosaic == allflat).all())

        for i, scene in enumerate(scenes):
            scene.properties.foo = i

        noflat = scenes.stack("nir", ctx, flatten="properties.foo")
        self.assertEqual(len(noflat), len(scenes))
        self.assertTrue((noflat == unflattened).all())

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

    def test_filter_coverage(self):
        polygon = shapely.geometry.Point(0.0, 0.0).buffer(1)
        ctx = geocontext.AOI(shapely.geometry.mapping(polygon))

        scenes = SceneCollection([
            Scene(dict(id='foo', geometry=shapely.geometry.mapping(polygon), properties={}), {}),
            Scene(dict(id='bar', geometry=shapely.geometry.mapping(polygon.buffer(-0.1)), properties={}), {}),
        ])

        self.assertEqual(len(scenes.filter_coverage(ctx)), 1)


@mock.patch.object(MockScene, "download")
class TestSceneCollectionDownload(unittest.TestCase):
    def setUp(self):
        properties = [{
            "id": "foo:bar" + str(i),
            "bands": {
                "nir": {"dtype": "UInt16"},
                "yellow": {"dtype": "UInt16"},
            }
        } for i in range(3)]

        self.scenes = SceneCollection([MockScene({}, p) for p in properties])
        self.ctx = geocontext.AOI(bounds=[30, 40, 50, 60], resolution=2, crs="EPSG:4326")

    def test_directory(self, mock_download):
        dest = "rasters"
        paths = self.scenes.download("nir yellow", self.ctx, dest, format="png")

        self.assertEqual(paths, [
            os.path.join(dest, "foo:bar0-nir-yellow.png"),
            os.path.join(dest, "foo:bar1-nir-yellow.png"),
            os.path.join(dest, "foo:bar2-nir-yellow.png"),
        ])

        self.assertEqual(mock_download.call_count, len(self.scenes))
        for scene, path in zip(self.scenes, paths):
            mock_download.assert_any_call(
                ["nir", "yellow"],
                self.ctx,
                dest=path,
                raster_client=self.scenes._raster_client
            )

    def test_custom_paths(self, mock_download):
        filenames = [
            os.path.join("foo", "img1.tif"),
            os.path.join("bar", "img2.jpg"),
            os.path.join("foo", "img3.tif"),
        ]
        result = self.scenes.download("nir yellow", self.ctx, filenames)
        self.assertEqual(result, filenames)

        self.assertEqual(mock_download.call_count, len(self.scenes))
        for scene, path in zip(self.scenes, filenames):
            mock_download.assert_any_call(
                ["nir", "yellow"],
                self.ctx,
                dest=path,
                raster_client=self.scenes._raster_client
            )

    def test_non_unique_paths(self, mock_download):
        nonunique_paths = [
            "img.tif",
            "img2.tif",
            "img.tif"
        ]
        with self.assertRaises(RuntimeError):
            self.scenes.download("nir yellow", self.ctx, nonunique_paths)

    def test_wrong_number_of_dest(self, mock_download):
        with self.assertRaises(ValueError):
            self.scenes.download("nir", self.ctx, ["a", "b"])

    def test_wrong_type_of_dest(self, mock_download):
        with self.assertRaises(TypeError):
            self.scenes.download("nir", self.ctx, 4)

    @mock.patch("descarteslabs.scenes.scenecollection._download._download")
    def test_download_mosaic(self, mock_base_download, mock_download):
        self.scenes.download_mosaic("nir yellow", self.ctx)

        mock_base_download.assert_called_once()
        called_ids = mock_base_download.call_args[1]["inputs"]
        self.assertEqual(called_ids, self.scenes.each.properties["id"].combine())
