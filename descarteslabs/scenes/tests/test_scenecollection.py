import unittest
import mock
import os.path
import shapely.geometry
import numpy as np

from descarteslabs.client.addons import ThirdParty
from descarteslabs.scenes import Scene, SceneCollection, geocontext

from .test_scene import MockScene
from .mock_data import _metadata_get, _metadata_get_bands, _raster_ndarray


class TestSceneCollection(unittest.TestCase):

    MOCK_RGBA_PROPERTIES = {
        "product": "mock_product",
        "id": "mock_id",
        "bands": {
            "red": {
                "type": "spectral",
                "dtype": "UInt16",
                "data_range": [0, 10000],
                "default_range": [0, 4000],
                "physical_range": [0.0, 1.0],
            },
            "green": {
                "type": "spectral",
                "dtype": "UInt16",
                "data_range": [0, 10000],
                "default_range": [0, 4000],
                "physical_range": [0.0, 1.0],
            },
            "blue": {
                "type": "spectral",
                "dtype": "UInt16",
                "data_range": [0, 10000],
                "default_range": [0, 4000],
                "physical_range": [0.0, 1.0],
            },
            "alpha": {"type": "mask", "dtype": "UInt16", "data_range": [0, 1]},
        },
    }

    MOCK_RGBA_PROPERTIES2 = {
        "product": "mock_product2",
        "id": "mock_id2",
        "bands": {
            "red": {
                "type": "spectral",
                "dtype": "UInt16",
                "data_range": [0, 10000],
                "default_range": [0, 4000],
                "physical_range": [0.0, 1.0],
            },
            "green": {
                "type": "spectral",
                "dtype": "UInt16",
                "data_range": [0, 10000],
                "default_range": [0, 4000],
                "physical_range": [0.0, 1.0],
            },
            "blue": {
                "type": "spectral",
                "dtype": "UInt16",
                "data_range": [0, 10000],
                "default_range": [0, 4000],
                "physical_range": [0.0, 1.0],
            },
            "alpha": {"type": "mask", "dtype": "UInt16", "data_range": [0, 1]},
        },
    }

    MOCK_RGBA_PROPERTIES3 = {
        "product": "mock_product3",
        "id": "mock_id2",
        "bands": {
            "red": {
                "type": "spectral",
                "dtype": "Int16",
                "data_range": [0, 10000],
                "default_range": [0, 4000],
                "physical_range": [0.0, 1.0],
            },
            "green": {
                "type": "spectral",
                "dtype": "UInt16",
                "data_range": [0, 10000],
                "default_range": [0, 4000],
                "physical_range": [-1.0, 1.0],
            },
            "alpha": {"type": "mask", "dtype": "UInt16", "data_range": [0, 1]},
        },
    }

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.Metadata.get_bands_by_id", _metadata_get_bands
    )
    @mock.patch("descarteslabs.scenes.scenecollection.Raster.ndarray", _raster_ndarray)
    def test_stack(self):
        scenes = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds="update", resolution=600)

        scenes = SceneCollection(scenes)
        stack, metas = scenes.stack("nir", ctx, raster_info=True)
        self.assertEqual(stack.shape, (2, 1, 122, 120))
        self.assertTrue((stack.mask[:, 0, 2, 2]).all())
        self.assertEqual(len(metas), 2)
        self.assertTrue(all(len(m["geoTransform"]) == 6 for m in metas))

        img_stack = scenes.stack("nir red", ctx, bands_axis=-1)
        self.assertEqual(img_stack.shape, (2, 122, 120, 2))

        # no_alpha = scenes.stack("nir", mask_alpha=False)
        # # assert raster not called with alpha once mocks exist

        no_mask = scenes.stack("nir", ctx, mask_alpha=False, mask_nodata=False)
        self.assertFalse(hasattr(no_mask, "mask"))
        self.assertEqual(no_mask.shape, (2, 1, 122, 120))

        with self.assertRaises(NotImplementedError):
            scenes.stack("nir red", ctx, bands_axis=0)

        stack_axis_1 = scenes.stack("nir red", ctx, bands_axis=1)
        self.assertEqual(stack_axis_1.shape, (2, 2, 122, 120))

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.Metadata.get_bands_by_id", _metadata_get_bands
    )
    @mock.patch("descarteslabs.scenes.scenecollection.Raster.ndarray", _raster_ndarray)
    def test_stack_scaling(self):
        scenes = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds="update", resolution=600)
        scenes = SceneCollection(scenes)

        stack = scenes.stack("nir alpha", ctx, scaling="raw")
        self.assertEqual(stack.shape, (2, 2, 122, 120))
        self.assertEqual(stack.dtype, np.uint16)

        stack = scenes.stack("nir", ctx, scaling="raw")
        self.assertEqual(stack.shape, (2, 1, 122, 120))
        self.assertEqual(stack.dtype, np.uint16)

        stack = scenes.stack("nir", ctx, scaling=[None])
        self.assertEqual(stack.shape, (2, 1, 122, 120))
        self.assertEqual(stack.dtype, np.uint16)

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.Metadata.get_bands_by_id", _metadata_get_bands
    )
    @mock.patch("descarteslabs.scenes.scenecollection.Raster.ndarray", _raster_ndarray)
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

        unflattened = scenes.stack("nir", ctx)

        flattened, metas = scenes.stack(
            "nir", ctx, flatten="properties.id", raster_info=True
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

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.Metadata.get_bands_by_id", _metadata_get_bands
    )
    @mock.patch("descarteslabs.scenes.scenecollection.Raster.ndarray", _raster_ndarray)
    @mock.patch(
        "descarteslabs.scenes.scenecollection.concurrent", ThirdParty("concurrent")
    )
    def test_stack_serial(self):
        scenes = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds="update", resolution=600)

        scenes = SceneCollection(scenes)
        stack, metas = scenes.stack("nir", ctx, raster_info=True)
        self.assertEqual(stack.shape, (2, 1, 122, 120))

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.Metadata.get_bands_by_id", _metadata_get_bands
    )
    @mock.patch("descarteslabs.scenes.scenecollection.Raster.ndarray", _raster_ndarray)
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
        self.assertEqual(mosaic.shape, (1, 122, 120))
        self.assertTrue((mosaic.mask[:, 2, 2]).all())
        self.assertEqual(len(meta["geoTransform"]), 6)

        img_mosaic = scenes.mosaic("nir red", ctx, bands_axis=-1)
        self.assertEqual(img_mosaic.shape, (122, 120, 2))

        mosaic_with_alpha = scenes.mosaic(["red", "alpha"], ctx)
        self.assertEqual(mosaic_with_alpha.shape, (2, 122, 120))

        mosaic_only_alpha = scenes.mosaic("alpha", ctx)
        self.assertEqual(mosaic_only_alpha.shape, (1, 122, 120))
        self.assertTrue(((mosaic_only_alpha.data == 0) == mosaic_only_alpha.mask).all())

        # no_alpha = scenes.mosaic("nir", mask_alpha=False)
        # # assert raster not called with alpha once mocks exist

        no_mask = scenes.mosaic("nir", ctx, mask_alpha=False, mask_nodata=False)
        self.assertFalse(hasattr(no_mask, "mask"))
        self.assertEqual(no_mask.shape, (1, 122, 120))

        with self.assertRaises(ValueError):
            scenes.mosaic("alpha red", ctx)

        with self.assertRaises(TypeError):
            scenes.mosaic("red", ctx, invalid_argument=True)

        mask_non_alpha = mosaic_with_alpha = scenes.mosaic(
            ["nir", "red"], ctx, mask_alpha="red"
        )
        self.assertTrue(hasattr(mask_non_alpha, "mask"))
        self.assertEqual(mask_non_alpha.shape, (2, 122, 120))

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.Metadata.get_bands_by_id", _metadata_get_bands
    )
    @mock.patch("descarteslabs.scenes.scenecollection.Raster.ndarray", _raster_ndarray)
    def test_mosaic_scaling(self):
        scenes = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds="update", resolution=600)
        scenes = SceneCollection(scenes)

        mosaic = scenes.mosaic("nir alpha", ctx, scaling="raw")
        self.assertEqual(mosaic.shape, (2, 122, 120))
        self.assertEqual(mosaic.dtype, np.uint16)

        mosaic = scenes.mosaic("nir", ctx, scaling="raw")
        self.assertEqual(mosaic.shape, (1, 122, 120))
        self.assertEqual(mosaic.dtype, np.uint16)

        mosaic = scenes.mosaic("nir", ctx, scaling=[None])
        self.assertEqual(mosaic.shape, (1, 122, 120))
        self.assertEqual(mosaic.dtype, np.uint16)

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.Metadata.get_bands_by_id", _metadata_get_bands
    )
    @mock.patch("descarteslabs.scenes.scenecollection.Raster.ndarray", _raster_ndarray)
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
        self.assertFalse(hasattr(no_mask, "mask"))

        masked_alt_alpha_band = sc.mosaic(
            ["Clear_sky_days", "Clear_sky_nights"], ctx, mask_alpha="Clear_sky_nights"
        )
        self.assertTrue(hasattr(masked_alt_alpha_band, "mask"))

        # errors when alternate alpha band is provided but not available in the scene
        with self.assertRaises(ValueError):
            sc.mosaic(
                ["Clear_sky_days", "Clear_sky_nights"], ctx, mask_alpha="alt-alpha"
            )

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.Metadata.get_bands_by_id", _metadata_get_bands
    )
    @mock.patch("descarteslabs.scenes.scenecollection.Raster.ndarray", _raster_ndarray)
    def test_incompatible_dtypes(self):
        scenes = (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )
        scenes, ctxs = zip(*[Scene.from_id(scene) for scene in scenes])

        overlap = scenes[0].geometry.intersection(scenes[1].geometry)
        ctx = ctxs[0].assign(geometry=overlap, bounds="update", resolution=600)

        scenes = SceneCollection(scenes)
        scenes[0].properties.bands.nir.dtype = "Int16"
        mosaic = scenes.mosaic("nir", ctx)
        self.assertEqual(mosaic.dtype.type, np.int32)
        stack, meta = scenes.stack("nir", ctx)
        self.assertEqual(stack.dtype.type, np.int32)

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    def test_filter_coverage(self):
        polygon = shapely.geometry.Point(0.0, 0.0).buffer(1)
        ctx = geocontext.AOI(geometry=polygon)

        scenes = SceneCollection(
            [
                Scene(dict(id="foo", geometry=polygon, properties={}), {}),
                Scene(dict(id="bar", geometry=polygon.buffer(-0.1), properties={}), {}),
            ]
        )

        self.assertEqual(len(scenes.filter_coverage(ctx)), 1)

    def test_scaling_parameters_single(self):
        sc = SceneCollection([MockScene({}, self.MOCK_RGBA_PROPERTIES)])
        scales, data_type = sc.scaling_parameters("red green blue alpha")
        self.assertIsNone(scales)
        self.assertEqual(data_type, "UInt16")

    def test_scaling_parameters_none(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES2),
            ]
        )
        scales, data_type = sc.scaling_parameters("red green blue alpha")
        self.assertIsNone(scales)
        self.assertEqual(data_type, "UInt16")

    def test_scaling_parameters_display(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES2),
            ]
        )
        scales, data_type = sc.scaling_parameters("red green blue alpha", "display")
        self.assertEqual(
            scales, [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        )
        self.assertEqual(data_type, "Byte")

    def test_scaling_parameters_missing_band(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES3),
            ]
        )
        with self.assertRaisesRegexp(ValueError, "not available"):
            scales, data_type = sc.scaling_parameters("red green blue alpha")

    def test_scaling_parameters_none_data_type(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES3),
            ]
        )
        scales, data_type = sc.scaling_parameters("red alpha")
        self.assertIsNone(scales)
        self.assertEqual(data_type, "Int32")

    def test_scaling_parameters_display_range(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES3),
            ]
        )
        scales, data_type = sc.scaling_parameters("red alpha", "display")
        self.assertEquals(scales, [(0, 4000, 0, 255), None])
        self.assertEqual(data_type, "Byte")

    def test_scaling_parameters_raw_range(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES3),
            ]
        )
        scales, data_type = sc.scaling_parameters("red alpha", "raw")
        self.assertEquals(scales, [None, None])
        self.assertEqual(data_type, "Int32")

    def test_scaling_parameters_physical_incompatible(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES3),
            ]
        )
        with self.assertRaisesRegexp(ValueError, "incompatible"):
            scales, data_type = sc.scaling_parameters("green alpha", "physical")


@mock.patch.object(MockScene, "download")
class TestSceneCollectionDownload(unittest.TestCase):
    def setUp(self):
        properties = [
            {
                "id": "foo:bar" + str(i),
                "bands": {"nir": {"dtype": "UInt16"}, "yellow": {"dtype": "UInt16"}},
                "product": "foo",
            }
            for i in range(3)
        ]

        self.scenes = SceneCollection([MockScene({}, p) for p in properties])
        self.ctx = geocontext.AOI(
            bounds=[30, 40, 50, 60], resolution=2, crs="EPSG:4326"
        )

    def test_directory(self, mock_download):
        dest = "rasters"
        paths = self.scenes.download("nir yellow", self.ctx, dest, format="png")

        self.assertEqual(
            paths,
            [
                os.path.join(dest, "foo:bar0-nir-yellow.png"),
                os.path.join(dest, "foo:bar1-nir-yellow.png"),
                os.path.join(dest, "foo:bar2-nir-yellow.png"),
            ],
        )

        self.assertEqual(mock_download.call_count, len(self.scenes))
        for scene, path in zip(self.scenes, paths):
            mock_download.assert_any_call(
                ["nir", "yellow"],
                self.ctx,
                dest=path,
                resampler="near",
                processing_level=None,
                scales=None,
                dtype="UInt16",
                raster_client=self.scenes._raster_client,
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
                resampler="near",
                processing_level=None,
                scales=None,
                dtype="UInt16",
                raster_client=self.scenes._raster_client,
            )

    def test_non_unique_paths(self, mock_download):
        nonunique_paths = ["img.tif", "img2.tif", "img.tif"]
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
