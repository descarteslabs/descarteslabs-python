import unittest
import mock

from descarteslabs.client.addons import ThirdParty, shapely
from descarteslabs.scenes import Scene, geocontext

from descarteslabs.client.services.metadata import Metadata

metadata_client = Metadata()


class TestScene(unittest.TestCase):
    def test_init(self):
        scene_id = "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        metadata = metadata_client.get(scene_id)
        bands = metadata_client.get_bands_by_id(scene_id)
        # Scene constructor expects Feature (as returned by metadata.search)
        metadata = {
            "type": "Feature",
            "geometry": metadata.pop("geometry"),
            "id": metadata.pop("id"),
            "key": metadata.pop("key"),
            "properties": metadata
        }

        scene = Scene(metadata, bands)

        self.assertEqual(scene.properties.id, scene_id)
        self.assertEqual(scene.properties.product, "landsat:LC08:PRE:TOAR")
        self.assertAlmostEqual(len(scene.properties.bands), 24, delta=4)
        self.assertIsInstance(scene.properties.bands, dict)
        self.assertEqual(scene.properties.crs, "EPSG:32615")
        self.assertIsInstance(scene.geometry, shapely.geometry.Polygon)
        self.assertIsInstance(scene.__geo_interface__, dict)

    @mock.patch("descarteslabs.scenes.scene.shapely", ThirdParty("shapely"))
    def test_no_shapely(self):
        scene_id = "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        metadata = metadata_client.get(scene_id)
        bands = metadata_client.get_bands_by_id(scene_id)
        metadata = {
            "type": "Feature",
            "geometry": metadata.pop("geometry"),
            "id": metadata.pop("id"),
            "key": metadata.pop("key"),
            "properties": metadata
        }
        scene = Scene(metadata, bands)

        self.assertIsInstance(scene.geometry, dict)
        self.assertIs(scene.__geo_interface__, metadata["geometry"])

    def test_from_id(self):
        scene_id = "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        scene, ctx = Scene.from_id(scene_id)

        self.assertEqual(scene.properties.id, scene_id)
        self.assertIsInstance(ctx, geocontext.AOI)
        self.assertEqual(ctx.resolution, 15)
        self.assertEqual(ctx.crs, "EPSG:32615")
        self.assertEqual(ctx.bounds, scene.geometry.bounds)
        self.assertEqual(ctx.geometry, None)

    @mock.patch("descarteslabs.scenes.scene.shapely", ThirdParty("shapely"))
    def test_from_id_no_shapely(self):
        scene_id = "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        scene, ctx = Scene.from_id(scene_id)
        self.assertEqual(ctx.bounds, (-95.8364984, 40.703737, -93.1167728, 42.7999878))

    def test_load_one_band(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr, info = scene.ndarray("red", ctx.assign(resolution=1000), raster_info=True)

        self.assertEqual(arr.shape, (1, 230, 231))
        self.assertTrue(arr.mask[0, 2, 2])
        self.assertFalse(arr.mask[0, 115, 116])
        self.assertEqual(len(info["geoTransform"]), 6)

        with self.assertRaises(TypeError):
            scene.ndarray("blue", ctx, invalid_argument=True)

    def test_nonexistent_band_fails(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        with self.assertRaises(ValueError):
            scene.ndarray("blue yellow", ctx)

    def test_different_band_dtypes_fails(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        scene.properties.bands = {
            "red": {
                "dtype": "UInt16"
            },
            "green": {
                "dtype": "Int16"
            }
        }
        with self.assertRaises(ValueError):
            scene.ndarray("red green", ctx)

    def test_load_multiband(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = scene.ndarray("red green blue", ctx.assign(resolution=1000))

        self.assertEqual(arr.shape, (3, 230, 231))
        self.assertTrue((arr.mask[:, 2, 2]).all())
        self.assertFalse((arr.mask[:, 115, 116]).all())

    def test_load_multiband_axis_last(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = scene.ndarray("red green blue", ctx.assign(resolution=1000), bands_axis=-1)

        self.assertEqual(arr.shape, (230, 231, 3))
        self.assertTrue((arr.mask[2, 2, :]).all())
        self.assertFalse((arr.mask[115, 116, :]).all())

        with self.assertRaises(ValueError):
            arr = scene.ndarray("red green blue", ctx.assign(resolution=1000), bands_axis=3)
        with self.assertRaises(ValueError):
            arr = scene.ndarray("red green blue", ctx.assign(resolution=1000), bands_axis=-3)

    def test_load_nomask(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = scene.ndarray(["red", "nir"], ctx.assign(resolution=1000), mask_nodata=False, mask_alpha=False)

        self.assertFalse(hasattr(arr, "mask"))
        self.assertEqual(arr.shape, (2, 230, 231))

    def with_alpha(self):
        scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")

        arr = scene.ndarray(["red", "alpha"], ctx.assign(resolution=1000))
        self.assertEqual(arr.shape, (2, 230, 231))
        self.assertTrue((arr.mask == (arr.data[1] == 0)).all())

        arr = scene.ndarray(["alpha"], ctx.assign(resolution=1000), mask_nodata=False)
        self.assertEqual(arr.shape, (1, 230, 231))
        self.assertTrue((arr.mask == (arr.data == 0)).all())

        with self.assertRaises(ValueError):
            arr = scene.ndarray("alpha red", ctx.assign(resolution=1000))

    def test_bands_to_list(self):
        self.assertEqual(Scene._bands_to_list("one"), ["one"])
        self.assertEqual(Scene._bands_to_list(["one"]), ["one"])
        self.assertEqual(Scene._bands_to_list("one two three"), ["one", "two", "three"])
        self.assertEqual(Scene._bands_to_list(["one", "two", "three"]), ["one", "two", "three"])
        with self.assertRaises(TypeError):
            Scene._bands_to_list(1)
        with self.assertRaises(ValueError):
            Scene._bands_to_list([])
