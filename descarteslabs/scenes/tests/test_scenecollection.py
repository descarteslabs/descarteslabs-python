import pytest
import unittest
import mock
import os.path
import shapely.geometry
import numpy as np

from descarteslabs.client.addons import ThirdParty
from descarteslabs.scenes import Scene, SceneCollection, geocontext

from .test_scene import MockScene
from .mock_data import _metadata_get, _cached_bands_by_product, _raster_ndarray


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
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
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

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product", _cached_bands_by_product
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
        assert stack.shape == (2, 2, 122, 120)
        assert stack.dtype == np.uint16

        stack = scenes.stack("nir", ctx, scaling="raw")
        assert stack.shape == (2, 1, 122, 120)
        assert stack.dtype == np.uint16

        stack = scenes.stack("nir", ctx, scaling=[None])
        assert stack.shape == (2, 1, 122, 120)
        assert stack.dtype == np.uint16

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product", _cached_bands_by_product
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

        assert len(flattened) == 2
        assert len(metas) == 2

        mosaic = scenes.mosaic("nir", ctx)
        allflat = scenes.stack("nir", ctx, flatten="properties.product")
        assert (mosaic == allflat).all()

        for i, scene in enumerate(scenes):
            scene.properties.foo = i

        noflat = scenes.stack("nir", ctx, flatten="properties.foo")
        assert len(noflat) == len(scenes)
        assert (noflat == unflattened).all()

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product", _cached_bands_by_product
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
        assert stack.shape == (2, 1, 122, 120)

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product", _cached_bands_by_product
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

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product", _cached_bands_by_product
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
        assert mosaic.shape == (2, 122, 120)
        assert mosaic.dtype == np.uint16

        mosaic = scenes.mosaic("nir", ctx, scaling="raw")
        assert mosaic.shape == (1, 122, 120)
        assert mosaic.dtype == np.uint16

        mosaic = scenes.mosaic("nir", ctx, scaling=[None])
        assert mosaic.shape == (1, 122, 120)
        assert mosaic.dtype == np.uint16

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product", _cached_bands_by_product
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

    @mock.patch("descarteslabs.scenes.scene.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product", _cached_bands_by_product
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
        assert mosaic.dtype.type == np.int32
        stack, meta = scenes.stack("nir", ctx)
        assert stack.dtype.type == np.int32

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

        assert len(scenes.filter_coverage(ctx)) == 1

    def test_scaling_parameters_single(self):
        sc = SceneCollection([MockScene({}, self.MOCK_RGBA_PROPERTIES)])
        scales, data_type = sc.scaling_parameters("red green blue alpha")
        assert scales is None
        assert data_type == "UInt16"

    def test_scaling_parameters_none(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES2),
            ]
        )
        scales, data_type = sc.scaling_parameters("red green blue alpha")
        assert scales is None
        assert data_type == "UInt16"

    def test_scaling_parameters_display(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES2),
            ]
        )
        scales, data_type = sc.scaling_parameters("red green blue alpha", "display")
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        assert data_type == "Byte"

    def test_scaling_parameters_missing_band(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES3),
            ]
        )
        with pytest.raises(ValueError, match="not available"):
            scales, data_type = sc.scaling_parameters("red green blue alpha")

    def test_scaling_parameters_none_data_type(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES3),
            ]
        )
        scales, data_type = sc.scaling_parameters("red alpha")
        assert scales is None
        assert data_type == "Int32"

    def test_scaling_parameters_display_range(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES3),
            ]
        )
        scales, data_type = sc.scaling_parameters("red alpha", "display")
        assert scales == [(0, 4000, 0, 255), None]
        assert data_type == "Byte"

    def test_scaling_parameters_raw_range(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES3),
            ]
        )
        scales, data_type = sc.scaling_parameters("red alpha", "raw")
        assert scales == [None, None]
        assert data_type == "Int32"

    def test_scaling_parameters_physical_incompatible(self):
        sc = SceneCollection(
            [
                MockScene({}, self.MOCK_RGBA_PROPERTIES),
                MockScene({}, self.MOCK_RGBA_PROPERTIES3),
            ]
        )
        with pytest.raises(ValueError, match="incompatible"):
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

        assert paths == [
            os.path.join(dest, "foo:bar0-nir-yellow.png"),
            os.path.join(dest, "foo:bar1-nir-yellow.png"),
            os.path.join(dest, "foo:bar2-nir-yellow.png"),
        ]

        assert mock_download.call_count == len(self.scenes)
        for scene, path in zip(self.scenes, paths):
            mock_download.assert_any_call(
                ["nir", "yellow"],
                self.ctx,
                dest=path,
                resampler="near",
                processing_level=None,
                scaling=None,
                data_type="UInt16",
                raster_client=self.scenes._raster_client,
            )

    def test_custom_paths(self, mock_download):
        filenames = [
            os.path.join("foo", "img1.tif"),
            os.path.join("bar", "img2.jpg"),
            os.path.join("foo", "img3.tif"),
        ]
        result = self.scenes.download("nir yellow", self.ctx, filenames)
        assert result == filenames

        assert mock_download.call_count == len(self.scenes)
        for scene, path in zip(self.scenes, filenames):
            mock_download.assert_any_call(
                ["nir", "yellow"],
                self.ctx,
                dest=path,
                resampler="near",
                processing_level=None,
                scaling=None,
                data_type="UInt16",
                raster_client=self.scenes._raster_client,
            )

    def test_non_unique_paths(self, mock_download):
        nonunique_paths = ["img.tif", "img2.tif", "img.tif"]
        with pytest.raises(RuntimeError):
            self.scenes.download("nir yellow", self.ctx, nonunique_paths)

    def test_wrong_number_of_dest(self, mock_download):
        with pytest.raises(ValueError):
            self.scenes.download("nir", self.ctx, ["a", "b"])

    def test_wrong_type_of_dest(self, mock_download):
        with pytest.raises(TypeError):
            self.scenes.download("nir", self.ctx, 4)

    def test_download_failure(self, mock_download):
        mock_download.side_effect = RuntimeError("blarf")
        dest = "rasters"
        with pytest.raises(RuntimeError):
            self.scenes.download("nir", self.ctx, dest)

    @mock.patch("descarteslabs.scenes.scenecollection._download._download")
    def test_download_mosaic(self, mock_base_download, mock_download):
        self.scenes.download_mosaic("nir yellow", self.ctx)

        mock_base_download.assert_called_once()
        called_ids = mock_base_download.call_args[1]["inputs"]
        assert called_ids == self.scenes.each.properties["id"].combine()
