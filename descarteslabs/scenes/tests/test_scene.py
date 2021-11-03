import pytest
import unittest
import mock
import datetime
import collections
import textwrap
import warnings
import shapely.geometry
import numpy as np

from descarteslabs.common.dotdict import DotDict
from descarteslabs.scenes import Scene, geocontext
from descarteslabs.scenes.scene import _strptime_helper

from descarteslabs.client.services.metadata import Metadata

from .mock_data import _metadata_get, _cached_bands_by_product, _raster_ndarray

metadata_client = Metadata()


class MockScene(Scene):
    "Circumvent __init__ method to create a Scene with arbitrary geometry and properties objects"

    def __init__(self, geometry, properties):
        self.geometry = DotDict(geometry)
        self.properties = DotDict(properties)


class TestScene(unittest.TestCase):

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

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    def test_init(self):
        scene_id = "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        metadata = metadata_client.get(scene_id)
        bands = _cached_bands_by_product(metadata["product"], metadata_client)
        # Scene constructor expects Feature (as returned by metadata.search)
        metadata = {
            "type": "Feature",
            "geometry": metadata.pop("geometry"),
            "id": metadata.pop("id"),
            "key": metadata.pop("key"),
            "properties": metadata,
        }

        scene = Scene(metadata, bands)

        assert scene.properties.id == scene_id
        assert scene.properties.product == "landsat:LC08:PRE:TOAR"
        assert abs(len(scene.properties.bands) - 24) < 4
        assert isinstance(scene.properties.bands, dict)
        assert scene.properties.crs == "EPSG:32615"
        assert isinstance(scene.geometry, shapely.geometry.Polygon)
        assert isinstance(scene.__geo_interface__, dict)

    def test_default_ctx(self):
        # test doesn't fail with nothing
        ctx = MockScene({}, {}).default_ctx()
        assert ctx == geocontext.AOI(bounds_crs=None, align_pixels=False)

        # no geotrans
        ctx = MockScene({}, {"crs": "EPSG:4326"}).default_ctx()
        assert ctx == geocontext.AOI(
            crs="EPSG:4326", bounds_crs=None, align_pixels=False
        )

        # north-up geotrans - resolution
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter(
                "always"
            )  # otherwise, the duplicate warning is suppressed the second time
            ctx = MockScene(
                {},
                {
                    "crs": "EPSG:4326",
                    # origin: (0, 0), pixel size: 2, rotation: 0 degrees
                    "geotrans": [0, 2, 0, 0, 0, -2],
                },
            ).default_ctx()
            assert len(w) == 0
        assert ctx.resolution == 2

        # non-north-up geotrans - resolution
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = MockScene(
                {},
                {
                    "crs": "EPSG:4326",
                    # origin: (0, 0), pixel size: 2, rotation: 30 degrees
                    "geotrans": (
                        0.0,
                        1.7320508075688774,
                        -1,
                        0.0,
                        1,
                        1.7320508075688774,
                    ),
                },
            ).default_ctx()
            warning = w[0]
            assert "The GeoContext will *not* return this Scene's original data" in str(
                warning.message
            )
        assert ctx.resolution == 2

        # north-up geotrans - bounds
        ctx = MockScene(
            {},
            {
                "crs": "EPSG:4326",
                # origin: (10, 20), pixel size: 2, rotation: 0 degrees
                "geotrans": [10, 2, 0, 20, 0, -2],
                "raster_size": [1, 2],
            },
        ).default_ctx()
        assert ctx.bounds == (10, 16, 12, 20)

        # non-north-up geotrans - bounds
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = MockScene(
                {},
                {
                    "crs": "EPSG:4326",
                    # origin: (0, 0), pixel size: 2, rotation: 45 degrees
                    "geotrans": (
                        0.0,
                        np.sqrt(2),
                        np.sqrt(2),
                        0.0,
                        np.sqrt(2),
                        -np.sqrt(2),
                    ),
                    "raster_size": [1, 1],
                },
            ).default_ctx()
            warning = w[0]
            assert "The GeoContext will *not* return this Scene's original data" in str(
                warning.message
            )
        diagonal = np.sqrt(2 ** 2 + 2 ** 2)
        assert ctx.bounds == (0, -diagonal / 2, diagonal, diagonal / 2)

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    def test_from_id(self):
        scene_id = "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        scene, ctx = Scene.from_id(scene_id)

        assert scene.properties.id == scene_id
        assert isinstance(scene.geometry, shapely.geometry.Polygon)
        assert isinstance(ctx, geocontext.AOI)

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def test_load_one_band(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr, info = scene.ndarray("red", ctx.assign(resolution=1000), raster_info=True)

        assert arr.shape == (1, 239, 235)
        assert arr.mask[0, 2, 2]
        assert not arr.mask[0, 115, 116]
        assert len(info["geoTransform"]) == 6

        with pytest.raises(TypeError):
            scene.ndarray("blue", ctx, invalid_argument=True)

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    def test_nonexistent_band_fails(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        with pytest.raises(ValueError):
            scene.ndarray("blue yellow", ctx)

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def test_different_band_dtypes(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        scene.properties.bands["green"]["dtype"] = "Int16"
        arr, info = scene.ndarray(
            "red green", ctx.assign(resolution=600), mask_alpha=False
        )
        assert arr.dtype.type == np.int32

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def test_load_multiband(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = scene.ndarray("red green blue", ctx.assign(resolution=1000))

        assert arr.shape == (3, 239, 235)
        assert (arr.mask[:, 2, 2]).all()
        assert not (arr.mask[:, 115, 116]).all()

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def test_load_multiband_axis_last(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = scene.ndarray(
            "red green blue", ctx.assign(resolution=1000), bands_axis=-1
        )

        assert arr.shape == (239, 235, 3)
        assert (arr.mask[2, 2, :]).all()
        assert not (arr.mask[115, 116, :]).all()

        with pytest.raises(ValueError):
            arr = scene.ndarray(
                "red green blue", ctx.assign(resolution=1000), bands_axis=3
            )
        with pytest.raises(ValueError):
            arr = scene.ndarray(
                "red green blue", ctx.assign(resolution=1000), bands_axis=-3
            )

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def test_load_nomask(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = scene.ndarray(
            ["red", "nir"],
            ctx.assign(resolution=1000),
            mask_nodata=False,
            mask_alpha=False,
        )

        assert not hasattr(arr, "mask")
        assert arr.shape == (2, 239, 235)

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def test_auto_mask_alpha_false(self):
        scene, ctx = Scene.from_id(
            "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1"
        )
        arr = scene.ndarray(
            ["Clear_sky_days", "Clear_sky_nights"],
            ctx.assign(resolution=1000),
            mask_nodata=False,
        )

        assert not hasattr(arr, "mask")
        assert arr.shape == (2, 688, 473)

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def test_mask_alpha_string(self):
        scene, ctx = Scene.from_id(
            "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1"
        )
        arr = scene.ndarray(
            ["Clear_sky_days", "Clear_sky_nights"],
            ctx.assign(resolution=1000),
            mask_alpha="Clear_sky_nights",
            mask_nodata=False,
        )

        assert hasattr(arr, "mask")
        assert arr.shape == (2, 688, 473)

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def test_mask_missing_alpha(self):
        scene, ctx = Scene.from_id(
            "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1"
        )
        with pytest.raises(ValueError):
            scene.ndarray(
                ["Clear_sky_days", "Clear_sky_nights"],
                ctx.assign(resolution=1000),
                mask_alpha=True,
                mask_nodata=False,
            )

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def test_mask_missing_band(self):
        scene, ctx = Scene.from_id(
            "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1"
        )
        with pytest.raises(ValueError):
            scene.ndarray(
                ["Clear_sky_days", "Clear_sky_nights"],
                ctx.assign(resolution=1000),
                mask_alpha="missing_band",
                mask_nodata=False,
            )

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def test_auto_mask_alpha_true(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = scene.ndarray(
            ["red", "green", "blue"], ctx.assign(resolution=1000), mask_nodata=False
        )

        assert hasattr(arr, "mask")
        assert arr.shape == (3, 239, 235)

    @mock.patch("descarteslabs.client.services.metadata.Metadata.get", _metadata_get)
    @mock.patch(
        "descarteslabs.scenes.scene.cached_bands_by_product",
        _cached_bands_by_product,
    )
    @mock.patch("descarteslabs.scenes.scene.Raster.ndarray", _raster_ndarray)
    def with_alpha(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")

        arr = scene.ndarray(["red", "alpha"], ctx.assign(resolution=1000))
        assert arr.shape == (2, 239, 235)
        assert (arr.mask == (arr.data[1] == 0)).all()

        arr = scene.ndarray(["alpha"], ctx.assign(resolution=1000), mask_nodata=False)
        assert arr.shape == (1, 239, 235)
        assert (arr.mask == (arr.data == 0)).all()

        with pytest.raises(ValueError):
            arr = scene.ndarray("alpha red", ctx.assign(resolution=1000))

    def test_bands_to_list(self):
        assert Scene._bands_to_list("one") == ["one"]
        assert Scene._bands_to_list(["one"]) == ["one"]
        assert Scene._bands_to_list("one two three") == ["one", "two", "three"]
        assert Scene._bands_to_list(["one", "two", "three"]) == ["one", "two", "three"]
        with pytest.raises(TypeError):
            Scene._bands_to_list(1)
        with pytest.raises(ValueError):
            Scene._bands_to_list([])

    def test_scenes_bands_dict(self):
        meta_bands = {
            "someproduct:red": {"name": "red", "id": "someproduct:red"},
            "someproduct:green": {"name": "green", "id": "someproduct:green"},
            "someproduct:ndvi": {"name": "ndvi", "id": "someproduct:ndvi"},
            "derived:ndvi": {"name": "ndvi", "id": "derived:ndvi"},
        }
        scenes_bands = Scene._scenes_bands_dict(meta_bands)
        assert set(scenes_bands.keys()) == {"red", "green", "ndvi", "derived:ndvi"}
        assert scenes_bands.ndvi == meta_bands["someproduct:ndvi"]
        assert scenes_bands["derived:ndvi"] == meta_bands["derived:ndvi"]

    def test_raw_data_type(self):
        mock_properties = {
            "product": "mock_product",
            "bands": {
                "one": dict(dtype="UInt16"),
                "two": dict(dtype="UInt16"),
                "derived:three": dict(dtype="UInt16"),
                "derived:one": dict(dtype="UInt16"),
                "its_a_byte": dict(dtype="Byte"),
                "signed": dict(dtype="Int16"),
                "future_unknown_type": dict(dtype="FutureInt16"),
                "alpha": dict(dtype="Byte"),
            },
        }
        s = MockScene({}, mock_properties)
        assert s.scaling_parameters(["its_a_byte"], scaling=None)[1] == "Byte"
        assert s.scaling_parameters(["one", "two"], scaling=None)[1] == "UInt16"
        assert s.scaling_parameters(["its_a_byte", "alpha"], scaling=None)[1] == "Byte"
        # alpha ignored from common datatype
        assert s.scaling_parameters(["one", "alpha"], scaling=None)[1] == "UInt16"
        assert s.scaling_parameters(["alpha"], scaling=None)[1] == "Byte"
        assert (
            s.scaling_parameters(
                ["one", "two", "derived:three", "derived:one"], scaling=None
            )[1]
            == "UInt16"
        )
        assert s.scaling_parameters(["one", "its_a_byte"], scaling=None)[1] == "UInt16"
        assert (
            s.scaling_parameters(["signed", "its_a_byte"], scaling=None)[1] == "Int16"
        )
        assert s.scaling_parameters(["one", "signed"], scaling=None)[1] == "Int32"

        with pytest.raises(ValueError, match="is not available"):
            s.scaling_parameters(["one", "woohoo"], scaling=None)
        with pytest.raises(ValueError, match="did you mean"):
            s.scaling_parameters(
                ["one", "three"], scaling=None
            )  # should hint that derived:three exists
        with pytest.raises(ValueError, match="Invalid data type"):
            s.scaling_parameters(["its_a_byte", "future_unknown_type"], scaling=None)

    def test__naive_dateparse(self):
        assert _strptime_helper("2017-08-31T00:00:00+00:00") is not None
        assert _strptime_helper("2017-08-31T00:00:00.00+00:00") is not None
        assert _strptime_helper("2017-08-31T00:00:00Z") is not None
        assert _strptime_helper("2017-08-31T00:00:00") is not None

    def test_coverage(self):
        scene_geometry = shapely.geometry.Point(0.0, 0.0).buffer(1)

        scene = Scene(dict(id="foo", geometry=scene_geometry, properties={}), {})

        # same geometry (as a GeoJSON)
        assert scene.coverage(scene_geometry.__geo_interface__) == pytest.approx(
            1.0, abs=1e-6
        )

        # geom is larger
        geom_larger = shapely.geometry.Point(0.0, 0.0).buffer(2)
        assert scene.coverage(geom_larger) == pytest.approx(0.25, abs=1e-6)

        # geom is smaller
        geom_smaller = shapely.geometry.Point(0.0, 0.0).buffer(0.5)
        assert scene.coverage(geom_smaller) == pytest.approx(1.0, abs=1e-6)

    @mock.patch("descarteslabs.scenes.scene._download._download")
    def test_download(self, mock_geotiff):
        scene = MockScene(
            {},
            {
                "id": "foo:bar",
                "bands": {"nir": {"dtype": "UInt16"}, "yellow": {"dtype": "UInt16"}},
            },
        )
        ctx = geocontext.AOI(bounds=[30, 40, 50, 60], resolution=2, crs="EPSG:4326")
        scene.download("nir yellow", ctx)
        mock_geotiff.assert_called_once()

    def test_scaling_parameters_none(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters("red green blue alpha")
        assert scales is None
        assert data_type == "UInt16"

    def test_scaling_parameters_dtype(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", None, "UInt32"
        )
        assert scales is None
        assert data_type == "UInt32"

    def test_scaling_parameters_raw(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters("red green blue alpha", "raw")
        assert scales == [None, None, None, None]
        assert data_type == "UInt16"

    def test_scaling_parameters_display(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters("red green blue alpha", "display")
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        assert data_type == "Byte"

    def test_scaling_parameters_display_uint16(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", "display", "UInt16"
        )
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        assert data_type == "UInt16"

    def test_scaling_parameters_auto(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters("red green blue alpha", "auto")
        assert scales == [(), (), (), None]
        assert data_type == "Byte"

    def test_scaling_parameters_physical(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters("red green blue alpha", "physical")
        assert scales == [
            (0, 10000, 0.0, 1.0),
            (0, 10000, 0.0, 1.0),
            (0, 10000, 0.0, 1.0),
            None,
        ]
        assert data_type == "Float64"

    def test_scaling_parameters_physical_int32(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", "physical", "Int32"
        )
        assert scales == [
            (0, 10000, 0.0, 1.0),
            (0, 10000, 0.0, 1.0),
            (0, 10000, 0.0, 1.0),
            None,
        ]
        assert data_type == "Int32"

    def test_scaling_parameters_bad_mode(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        with pytest.raises(ValueError):
            scales, data_type = scene.scaling_parameters("red green blue alpha", "mode")

    def test_scaling_parameters_list(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", [(0, 10000), "display", (), None]
        )
        assert scales == [(0, 10000, 0, 255), (0, 4000, 0, 255), (), None]
        assert data_type == "Byte"

    def test_scaling_parameters_list_alpha(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", [(0, 4000), (0, 4000), (0, 4000), "raw"]
        )
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        assert data_type == "Byte"

    def test_scaling_parameters_list_bad_length(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        with pytest.raises(ValueError):
            scales, data_type = scene.scaling_parameters(
                "red green blue alpha", [(0, 10000), "display", ()]
            )

    def test_scaling_parameters_list_bad_mode(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        with pytest.raises(ValueError):
            scales, data_type = scene.scaling_parameters(
                "red green blue alpha", [(0, 10000), "mode", (), None]
            )

    def test_scaling_parameters_dict(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha",
            {"red": "display", "green": (0, 10000), "default_": "auto"},
        )
        assert scales == [(0, 4000, 0, 255), (0, 10000, 0, 255), (), None]
        assert data_type == "Byte"

    def test_scaling_parameters_dict_default(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", {"red": (0, 4000, 0, 255), "default_": "raw"}
        )
        assert scales == [(0, 4000, 0, 255), None, None, None]
        assert data_type == "UInt16"

    def test_scaling_parameters_dict_default_none(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", {"red": "display", "green": "display"}
        )
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), None, None]
        assert data_type == "Byte"

    def test_scaling_parameters_tuple_range(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", [(0, 10000, 0, 255), (0, 4000), (), None]
        )
        assert scales == [(0, 10000, 0, 255), (0, 4000, 0, 255), (), None]
        assert data_type == "Byte"

    def test_scaling_parameters_tuple_range_uint16(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", [(0, 10000, 0, 10000), (0, 4000), (), None]
        )
        assert scales == [(0, 10000, 0, 10000), (0, 4000, 0, 65535), (), None]
        assert data_type == "UInt16"

    def test_scaling_parameters_tuple_range_float(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha", [(0, 10000, 0, 1.0), (0, 4000), (0, 4000), None]
        )
        assert scales == [(0, 10000, 0, 1), (0, 4000, 0, 1), (0, 4000, 0, 1), None]
        assert data_type == "Float64"

    def test_scaling_parameters_tuple_pct(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha",
            [("0%", "100%", "0%", "100%"), ("2%", "98%", "2%", "98%"), "display", None],
        )
        assert scales == [
            (0, 4000, 0, 255),
            (80, 3920, 5, 250),
            (0, 4000, 0, 255),
            None,
        ]
        assert data_type == "Byte"

    def test_scaling_parameters_tuple_pct_float(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        scales, data_type = scene.scaling_parameters(
            "red green blue alpha",
            [
                ("0%", "100%", "0%", "100%"),
                ("2%", "98%", "2%", "98%"),
                "physical",
                None,
            ],
        )
        assert scales == [
            (0, 10000, 0, 1),
            (200, 9800, 0.02, 0.98),
            (0, 10000, 0, 1),
            None,
        ]
        assert data_type == "Float64"

    def test_scaling_parameters_bad_data_type(self):
        scene = MockScene({}, self.MOCK_RGBA_PROPERTIES)
        with pytest.raises(ValueError):
            scales, data_type = scene.scaling_parameters(
                "red green blue alpha", None, "data_type"
            )


class TestSceneRepr(unittest.TestCase):
    def setUp(self):
        # date format is locale-dependent, so a hardcoded date string could fail for users from different locales
        date = datetime.datetime(2015, 6, 1, 14, 25, 10)
        self.date_str = date.strftime("%c")

        properties = {
            "id": "prod:foo",
            "product": "prod",
            "crs": "EPSG:32615",
            "date": date,
            "bands": collections.OrderedDict(
                [  # necessary to ensure deterministic order in tests
                    (
                        "blue",
                        {
                            "resolution": 5,
                            "resolution_unit": "smoot",
                            "dtype": "UInt16",
                            "data_range": [0, 10000],
                            "physical_range": [0, 1],
                            "data_unit": "TOAR",
                        },
                    ),
                    (
                        "alpha",
                        {
                            "resolution": 5,
                            "resolution_unit": "smoot",
                            "dtype": "UInt8",
                            "data_range": [0, 1],
                            "physical_range": [0, 1],
                        },
                    ),
                ]
            ),
        }
        properties = DotDict(properties)
        self.scene = MockScene({}, properties)

    def test_basic(self):
        repr_str = repr(self.scene)
        match_str = """\
        Scene "prod:foo"
          * Product: "prod"
          * CRS: "EPSG:32615"
          * Date: {}
          * Bands:
            * blue: 5 smoot, UInt16, [0, 10000] -> [0, 1] in units "TOAR"
            * alpha: 5 smoot, UInt8, [0, 1] -> [0, 1]""".format(
            self.date_str
        )

        assert repr_str == textwrap.dedent(match_str)

    def test_missing_band_part(self):
        del self.scene.properties.bands["blue"]["physical_range"]
        del self.scene.properties.bands["blue"]["dtype"]
        repr_str = repr(self.scene)
        match_str = """\
        Scene "prod:foo"
          * Product: "prod"
          * CRS: "EPSG:32615"
          * Date: {}
          * Bands:
            * blue: 5 smoot, [0, 10000] in units "TOAR"
            * alpha: 5 smoot, UInt8, [0, 1] -> [0, 1]""".format(
            self.date_str
        )

        assert repr_str == textwrap.dedent(match_str)

    def test_missing_all_band_parts(self):
        self.scene.properties.bands["alpha"] = {}
        repr_str = repr(self.scene)
        match_str = """\
        Scene "prod:foo"
          * Product: "prod"
          * CRS: "EPSG:32615"
          * Date: {}
          * Bands:
            * blue: 5 smoot, UInt16, [0, 10000] -> [0, 1] in units "TOAR"
            * alpha""".format(
            self.date_str
        )

        assert repr_str == textwrap.dedent(match_str)

    def test_no_bands(self):
        self.scene.properties.bands = {}
        repr_str = repr(self.scene)
        match_str = """\
        Scene "prod:foo"
          * Product: "prod"
          * CRS: "EPSG:32615"
          * Date: {}""".format(
            self.date_str
        )

        assert repr_str == textwrap.dedent(match_str)

    def test_truncate_hella_bands(self):
        self.scene.properties.bands.update({str(i): {} for i in range(100)})
        repr_str = repr(self.scene)
        match_str = """\
        Scene "prod:foo"
          * Product: "prod"
          * CRS: "EPSG:32615"
          * Date: {}
          * Bands: 102""".format(
            self.date_str
        )

        assert repr_str == textwrap.dedent(match_str)
