import unittest
import mock
import datetime
import collections
import textwrap
import shapely.geometry

from descarteslabs.common.dotdict import DotDict
from descarteslabs.scenes import Scene, geocontext
from descarteslabs.scenes.scene import _strptime_helper

from descarteslabs.client.services.metadata import Metadata

metadata_client = Metadata()


class MockScene(Scene):
    "Circumvent __init__ method to create a Scene with arbitrary geometry and properties objects"
    def __init__(self, geometry, properties):
        self.geometry = DotDict(geometry)
        self.properties = DotDict(properties)


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

    def test_from_id(self):
        scene_id = "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        scene, ctx = Scene.from_id(scene_id)

        self.assertEqual(scene.properties.id, scene_id)
        self.assertIsInstance(ctx, geocontext.AOI)
        self.assertEqual(ctx.resolution, 15)
        self.assertEqual(ctx.crs, "EPSG:32615")
        self.assertEqual(ctx.bounds, scene.geometry.bounds)
        self.assertEqual(ctx.geometry, None)

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

    def test_scenes_bands_dict(self):
        meta_bands = {
            "someproduct:red": {
                "name": "red",
                "id": "someproduct:red"
            },
            "someproduct:green": {
                "name": "green",
                "id": "someproduct:green"
            },
            "someproduct:ndvi": {
                "name": "ndvi",
                "id": "someproduct:ndvi"
            },
            "derived:ndvi": {
                "name": "ndvi",
                "id": "derived:ndvi"
            },
        }
        scenes_bands = Scene._scenes_bands_dict(meta_bands)
        self.assertEqual(
            set(scenes_bands.keys()),
            {"red", "green", "ndvi", "derived:ndvi"}
        )
        self.assertEqual(scenes_bands.ndvi, meta_bands["someproduct:ndvi"])
        self.assertEqual(scenes_bands["derived:ndvi"], meta_bands["derived:ndvi"])

    def test_common_data_type_of_bands(self):
        mock_properties = {
            "product": "mock_product",
            "bands": {
                "one": dict(dtype="UInt16"),
                "two": dict(dtype="UInt16"),
                "derived:three": dict(dtype="UInt16"),
                "derived:one": dict(dtype="UInt16"),
                "its_a_byte": dict(dtype="Byte"),
                "alpha": dict(dtype="Byte"),
                "no_dtype": {},
            }
        }
        s = MockScene({}, mock_properties)
        self.assertEqual(s._common_data_type_of_bands(["its_a_byte"]), "Byte")
        self.assertEqual(s._common_data_type_of_bands(["one", "two"]), "UInt16")
        self.assertEqual(s._common_data_type_of_bands(["its_a_byte", "alpha"]), "Byte")
        self.assertEqual(s._common_data_type_of_bands(["one", "alpha"]), "UInt16")  # alpha ignored from common datatype
        self.assertEqual(s._common_data_type_of_bands(["alpha"]), "Byte")
        self.assertEqual(s._common_data_type_of_bands(["one", "two", "derived:three", "derived:one"]), "UInt16")
        with self.assertRaisesRegexp(ValueError, "is not available"):
            s._common_data_type_of_bands(["one", "woohoo"])
        with self.assertRaisesRegexp(ValueError, "Did you mean"):
            s._common_data_type_of_bands(["one", "three"])  # should hint that derived:three exists
        with self.assertRaisesRegexp(ValueError, "has no 'dtype' field"):
            s._common_data_type_of_bands(["one", "no_dtype"])
        with self.assertRaisesRegexp(ValueError, "Bands must all have the same dtype"):
            s._common_data_type_of_bands(["one", "its_a_byte"])

    def test__naive_dateparse(self):
        self.assertIsNotNone(_strptime_helper("2017-08-31T00:00:00+00:00"))
        self.assertIsNotNone(_strptime_helper("2017-08-31T00:00:00.00+00:00"))
        self.assertIsNotNone(_strptime_helper("2017-08-31T00:00:00Z"))
        self.assertIsNotNone(_strptime_helper("2017-08-31T00:00:00"))

    def test_coverage(self):
        scene_geometry = shapely.geometry.mapping(shapely.geometry.Point(0.0, 0.0).buffer(1))

        scene = Scene(dict(id='foo', geometry=scene_geometry, properties={}), {})

        # same geometry
        ctx = geocontext.AOI(scene_geometry)
        self.assertEqual(scene.coverage(ctx), 1.0)

        # ctx is larger
        ctx = geocontext.AOI(shapely.geometry.mapping(shapely.geometry.Point(0.0, 0.0).buffer(2)))
        self.assertEqual(scene.coverage(ctx), 0.25)

        # ctx is smaller
        ctx = geocontext.AOI(shapely.geometry.mapping(shapely.geometry.Point(0.0, 0.0).buffer(0.5)))
        self.assertEqual(scene.coverage(ctx), 1.0)

    @mock.patch("descarteslabs.scenes.scene._download._download")
    def test_download(self, mock_geotiff):
        scene = MockScene({}, {
            "id": "foo:bar",
            "bands": {
                "nir": {"dtype": "UInt16"},
                "yellow": {"dtype": "UInt16"},
            }
        })
        ctx = geocontext.AOI(bounds=[30, 40, 50, 60], resolution=2, crs="EPSG:4326")
        scene.download("nir yellow", ctx)
        mock_geotiff.assert_called_once()


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
            "bands": collections.OrderedDict([  # necessary to ensure deterministic order in tests
                ("blue", {
                    "resolution": 5,
                    "resolution_unit": "smoot",
                    "dtype": "UInt16",
                    "data_range": [0, 10000],
                    "physical_range": [0, 1],
                    "data_unit": "TOAR",
                }),
                ("alpha", {
                    "resolution": 5,
                    "resolution_unit": "smoot",
                    "dtype": "UInt8",
                    "data_range": [0, 1],
                    "physical_range": [0, 1],
                })
            ])
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
            * alpha: 5 smoot, UInt8, [0, 1] -> [0, 1]""".format(self.date_str)

        self.assertEqual(repr_str, textwrap.dedent(match_str))

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
            * alpha: 5 smoot, UInt8, [0, 1] -> [0, 1]""".format(self.date_str)

        self.assertEqual(repr_str, textwrap.dedent(match_str))

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
            * alpha""".format(self.date_str)

        self.assertEqual(repr_str, textwrap.dedent(match_str))

    def test_no_bands(self):
        self.scene.properties.bands = {}
        repr_str = repr(self.scene)
        match_str = """\
        Scene "prod:foo"
          * Product: "prod"
          * CRS: "EPSG:32615"
          * Date: {}""".format(self.date_str)

        self.assertEqual(repr_str, textwrap.dedent(match_str))

    def test_truncate_hella_bands(self):
        self.scene.properties.bands.update({str(i): {} for i in range(100)})
        repr_str = repr(self.scene)
        match_str = """\
        Scene "prod:foo"
          * Product: "prod"
          * CRS: "EPSG:32615"
          * Date: {}
          * Bands: 102""".format(self.date_str)

        self.assertEqual(repr_str, textwrap.dedent(match_str))
