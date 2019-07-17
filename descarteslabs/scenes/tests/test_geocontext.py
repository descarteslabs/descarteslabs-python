import unittest
import mock
import multiprocessing
import concurrent.futures
import copy
import warnings

from descarteslabs.scenes import geocontext
import shapely.geometry


class SimpleContext(geocontext.GeoContext):
    __slots__ = ("foo", "_bar")

    def __init__(self, foo=None, bar=None):
        super(SimpleContext, self).__init__()
        self.foo = foo
        self._bar = bar


class TestGeoContext(unittest.TestCase):
    def test_repr(self):
        simple = SimpleContext(1, False)
        r = repr(simple)
        expected = """SimpleContext(foo=1,
              bar=False)"""
        self.assertEqual(r, expected)

    def test_eq(self):
        simple = SimpleContext(1, False)
        simple2 = SimpleContext(1, False)
        simple_diff = SimpleContext(1, True)
        not_simple = geocontext.GeoContext()
        self.assertEqual(simple, simple)
        self.assertEqual(simple, simple2)
        self.assertNotEqual(simple, simple_diff)
        self.assertNotEqual(simple, not_simple)

    def test_deepcopy(self):
        simple = SimpleContext(1, False)
        simple_copy = copy.deepcopy(simple)
        self.assertIsNot(simple._geometry_lock_, simple_copy._geometry_lock_)
        self.assertEqual(simple, simple_copy)


class TestAOI(unittest.TestCase):
    def test_init(self):
        feature = {
            'type': 'Feature',
            'geometry': {
                'coordinates': ((
                    (-93.52300099792355, 41.241436141055345),
                    (-93.7138666, 40.703737),
                    (-94.37053769704536, 40.83098709945576),
                    (-94.2036617, 41.3717716),
                    (-93.52300099792355, 41.241436141055345)),
                ),
                'type': 'Polygon'
            }
        }
        collection = {
            'type': 'FeatureCollection',
            'features': [feature, feature, feature],
        }
        bounds_wgs84 = (-94.37053769704536, 40.703737, -93.52300099792355, 41.3717716)
        resolution = 40
        ctx = geocontext.AOI(collection, resolution=resolution)
        self.assertEqual(ctx.resolution, resolution)
        self.assertEqual(ctx.bounds, bounds_wgs84)
        self.assertEqual(ctx.bounds_crs, 'EPSG:4326')
        self.assertIsInstance(ctx.geometry, shapely.geometry.GeometryCollection)
        self.assertEqual(ctx.__geo_interface__['type'], 'GeometryCollection')
        self.assertEqual(ctx.__geo_interface__['geometries'][0], feature['geometry'])

    def test_raster_params(self):
        geom = {
            'coordinates': ((
                (-93.52300099792355, 41.241436141055345),
                (-93.7138666, 40.703737),
                (-94.37053769704536, 40.83098709945576),
                (-94.2036617, 41.3717716),
                (-93.52300099792355, 41.241436141055345)),
            ),
            'type': 'Polygon'
        }
        bounds_wgs84 = (-94.37053769704536, 40.703737, -93.52300099792355, 41.3717716)
        resolution = 40
        crs = "EPSG:32615"
        align_pixels = False

        ctx = geocontext.AOI(geom, resolution, crs, align_pixels)
        raster_params = ctx.raster_params
        expected = {
            "cutline": geom,
            "resolution": resolution,
            "srs": crs,
            "bounds_srs": "EPSG:4326",
            "align_pixels": align_pixels,
            "bounds": bounds_wgs84,
            "dimensions": None,
        }
        self.assertEqual(raster_params, expected)

    def test_assign(self):
        geom = {
            'coordinates': [[
                [-93.52300099792355, 41.241436141055345],
                [-93.7138666, 40.703737],
                [-94.37053769704536, 40.83098709945576],
                [-94.2036617, 41.3717716],
                [-93.52300099792355, 41.241436141055345]],
            ],
            'type': 'Polygon'
        }
        ctx = geocontext.AOI(resolution=40)
        ctx2 = ctx.assign(geometry=geom)
        self.assertEqual(ctx2.geometry, shapely.geometry.shape(geom))
        self.assertEqual(ctx2.resolution, 40)
        self.assertEqual(ctx2.align_pixels, True)
        self.assertEqual(ctx2.shape, None)

        ctx3 = ctx2.assign(geometry=None)
        self.assertEqual(ctx3.geometry, None)

    def test_assign_update_bounds(self):
        geom = shapely.geometry.Point(-90, 30).buffer(1).envelope
        ctx = geocontext.AOI(geometry=geom, resolution=40)

        geom_overlaps = shapely.affinity.translate(geom, xoff=1)
        self.assertTrue(geom.intersects(geom_overlaps))
        ctx_overlap = ctx.assign(geometry=geom_overlaps)
        self.assertEqual(ctx_overlap.bounds, ctx.bounds)

        ctx_updated = ctx.assign(geometry=geom_overlaps, bounds="update")
        self.assertEqual(ctx_updated.bounds, geom_overlaps.bounds)

        geom_doesnt_overlap = shapely.affinity.translate(geom, xoff=3)
        with self.assertRaisesRegexp(ValueError, "Geometry and bounds do not intersect"):
            ctx.assign(geometry=geom_doesnt_overlap)
        ctx_doesnt_overlap_updated = ctx.assign(geometry=geom_doesnt_overlap, bounds="update")
        self.assertEqual(ctx_doesnt_overlap_updated.bounds, geom_doesnt_overlap.bounds)

        with self.assertRaisesRegexp(ValueError, "A geometry must be given with which to update the bounds"):
            ctx.assign(bounds="update")

    def test_assign_update_bounds_crs(self):
        ctx = geocontext.AOI(bounds_crs="EPSG:32615")
        self.assertEqual(ctx.bounds_crs, "EPSG:32615")
        geom = shapely.geometry.Point(-20, 30).buffer(1).envelope

        ctx_no_update_bounds = ctx.assign(geometry=geom)
        self.assertEqual(ctx_no_update_bounds.bounds_crs, "EPSG:32615")

        ctx_update_bounds = ctx.assign(geometry=geom, bounds="update")
        self.assertEqual(ctx_update_bounds.bounds_crs, "EPSG:4326")

        with self.assertRaisesRegexp(ValueError, "Can't compute bounds from a geometry while also explicitly setting"):
            ctx = geocontext.AOI(geometry=geom, resolution=40, bounds_crs="EPSG:32615")

    def test_validate_bounds_values_for_bounds_crs__latlon(self):
        # invalid latlon bounds
        with self.assertRaisesRegexp(ValueError, "Bounds must be in lat-lon coordinates"):
            geocontext.AOI(bounds_crs="EPSG:4326", bounds=[500000, 2000000, 501000, 2001000])
        # valid latlon bounds, no error should raise
        geocontext.AOI(bounds_crs="EPSG:4326", bounds=[12, -41, 14, -40])

    def test_validate_bounds_values_for_bounds_crs__non_latlon(self):
        # valid latlon bounds, should warn
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = geocontext.AOI(bounds_crs="EPSG:32615", bounds=(12, -41, 14, -40))
            self.assertEqual(ctx.bounds_crs, "EPSG:32615")
            self.assertEqual(ctx.bounds, (12, -41, 14, -40))
            warning = w[0]
            self.assertIn("You might have the wrong `bounds_crs` set.", str(warning.message))
        # not latlon bounds, no error should raise
        geocontext.AOI(bounds_crs="EPSG:32615", bounds=[500000, 2000000, 501000, 2001000])

    def test_validate_shape(self):
        with self.assertRaises(TypeError):
            geocontext.AOI(shape=120)
        with self.assertRaises(TypeError):
            geocontext.AOI(shape=(120, 0, 0))

    def test_validate_resolution(self):
        with self.assertRaises(TypeError):
            geocontext.AOI(resolution='foo')
        with self.assertRaises(ValueError):
            geocontext.AOI(resolution=-1)

    def test_validate_resolution_shape(self):
        with self.assertRaises(ValueError):
            geocontext.AOI(resolution=40, shape=(120, 280))

    def test_validate_bound_geom_intersection(self):
        # bounds don't intersect
        with self.assertRaisesRegexp(ValueError, "Geometry and bounds do not intersect"):
            geocontext.AOI(
                geometry=shapely.geometry.box(0, 0, 1, 1),
                bounds=[5, 5, 6, 6],
                bounds_crs="EPSG:4326",
            )

        # bounds do intersect; no error should raise
        geocontext.AOI(
            geometry=shapely.geometry.box(0, 0, 1, 1),
            bounds=[0.5, 0.5, 3, 4],
            bounds_crs="EPSG:4326",
        )

        # bounds_crs is not WGS84, so we can't check if bounds and geometry intersect or not---no error should raise
        geocontext.AOI(
            geometry=shapely.geometry.box(0, 0, 1, 1),
            bounds_crs="EPSG:32615",
            bounds=[500000, 2000000, 501000, 2001000],
        )

    def test_validate_reasonable_resolution(self):
        # different CRSs --- no error
        ctx = geocontext.AOI(
            crs="EPSG:32615",
            bounds_crs="EPSG:4326",
            bounds=[0, 0, 1.5, 1.5],
            resolution=15,
        )
        self.assertEqual(ctx.crs, "EPSG:32615")
        self.assertEqual(ctx.bounds_crs, "EPSG:4326")
        self.assertEqual(ctx.bounds, (0, 0, 1.5, 1.5))
        self.assertEqual(ctx.resolution, 15)

        # same CRSs, bounds < resolution --- no error
        geocontext.AOI(
            crs="EPSG:32615",
            bounds_crs="EPSG:32615",
            bounds=[200000, 5000000, 200100, 5000300],
            resolution=15,
        )

        # same CRSs, width < resolution --- error
        with self.assertRaisesRegexp(ValueError, "less than one pixel wide"):
            geocontext.AOI(
                crs="EPSG:32615",
                bounds_crs="EPSG:32615",
                bounds=[200000, 5000000, 200001, 5000300],
                resolution=15,
            )

        # same CRSs, height < resolution --- error
        with self.assertRaisesRegexp(ValueError, "less than one pixel tall"):
            geocontext.AOI(
                crs="EPSG:32615",
                bounds_crs="EPSG:32615",
                bounds=[200000, 5000000, 200100, 5000001],
                resolution=15,
            )

        # same CRSs, width < resolution, CRS is lat-lon --- error including "decimal degrees"
        with self.assertRaisesRegexp(ValueError, "resolution must be given in decimal degrees"):
            geocontext.AOI(
                crs="EPSG:4326",
                bounds_crs="EPSG:4326",
                bounds=[10, 10, 11, 11],
                resolution=15,
            )


class TestDLTIle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = '128:16:960.0:15:-1:37'
        cls.dltile_dict = {
            'geometry': {
                'coordinates': [[
                    [-94.64171754779824, 40.9202359006794],
                    [-92.81755164322226, 40.93177944075989],
                    [-92.81360932958779, 42.31528732533928],
                    [-94.6771717075502, 42.303172487087394],
                    [-94.64171754779824, 40.9202359006794]
                ]],
                'type': 'Polygon'
            },
            'properties': {
                'cs_code': 'EPSG:32615',
                'key': '128:16:960.0:15:-1:37',
                'outputBounds': [361760.0, 4531200.0, 515360.0, 4684800.0],
                'pad': 16,
                'resolution': 960.0,
                'ti': -1,
                'tilesize': 128,
                'tj': 37,
                'zone': 15,
                'geotrans': [361760.0, 960.0, 0, 4684800.0, 0, -960.0],
                'proj4': '+proj=utm +zone=15 +datum=WGS84 +units=m +no_defs ',
                'wkt': 'PROJCS["WGS 84 / UTM zone 15N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-93],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32615"]]' # noqa
            },
            'type': 'Feature'
        }
        cls.key2 = '128:8:960.0:15:-1:37'
        cls.dltile2_dict = {
            'geometry': {
                'coordinates': [[
                    [-94.55216325894683, 40.99065655298372],
                    [-92.90868033200002, 41.00107128418895],
                    [-92.90690635754177, 42.246233215798036],
                    [-94.58230042864014, 42.235355721757024],
                    [-94.55216325894683, 40.99065655298372]
                ]],
                'type': 'Polygon'
            },
            'properties': {
                'cs_code': 'EPSG:32615',
                'geotrans': [369440.0, 960.0, 0, 4677120.0, 0, -960.0],
                'key': '128:8:960.0:15:-1:37',
                'outputBounds': [369440.0, 4538880.0, 507680.0, 4677120.0],
                'pad': 8,
                'proj4': '+proj=utm +zone=15 +datum=WGS84 +units=m +no_defs ',
                'resolution': 960.0,
                'ti': -1,
                'tilesize': 128,
                'tj': 37,
                'wkt': 'PROJCS["WGS 84 / UTM zone 15N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-93],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32615"]]',  # noqa
                'zone': 15
            },
            'type': 'Feature'
        }

    @mock.patch("descarteslabs.scenes.geocontext.Raster")
    def test_from_key(self, mock_raster):
        mock_raster_instance = mock_raster.return_value
        mock_raster_instance.dltile.return_value = self.dltile_dict

        tile = geocontext.DLTile.from_key(self.key)
        mock_raster_instance.dltile.assert_called_with(self.key)

        self.assertEqual(tile.key, self.key)
        self.assertEqual(tile.resolution, 960)
        self.assertEqual(tile.pad, 16)
        self.assertEqual(tile.tilesize, 128)
        self.assertEqual(tile.crs, "EPSG:32615")
        self.assertEqual(tile.bounds, (361760.0, 4531200.0, 515360.0, 4684800.0))
        self.assertEqual(tile.bounds_crs, "EPSG:32615")
        self.assertEqual(tile.raster_params, {
            "dltile": self.key,
            "align_pixels": False,
        })
        self.assertEqual(tile.geotrans, (361760.0, 960, 0, 4684800.0, 0, -960))
        self.assertEqual(tile.proj4, "+proj=utm +zone=15 +datum=WGS84 +units=m +no_defs ")
        self.assertEqual(tile.wkt, 'PROJCS["WGS 84 / UTM zone 15N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-93],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32615"]]') # noqa

    @mock.patch("descarteslabs.scenes.geocontext.Raster")
    def test_assign(self, mock_raster):
        mock_raster_instance = mock_raster.return_value
        mock_raster_instance.dltile.return_value = self.dltile_dict

        tile = geocontext.DLTile.from_key(self.key)
        mock_raster_instance.dltile.assert_called_with(self.key)

        mock_raster_instance.dltile.return_value = self.dltile2_dict

        tile = tile.assign(8)
        mock_raster_instance.dltile.assert_called_with(self.key2)

        self.assertEqual(tile.key, self.key2)
        self.assertEqual(tile.resolution, 960)
        self.assertEqual(tile.pad, 8)
        self.assertEqual(tile.tilesize, 128)
        self.assertEqual(tile.crs, "EPSG:32615")
        self.assertEqual(tile.bounds, (369440.0, 4538880.0, 507680.0, 4677120.0))
        self.assertEqual(tile.bounds_crs, "EPSG:32615")
        self.assertEqual(tile.raster_params, {
            "dltile": self.key2,
            "align_pixels": False,
        })
        self.assertEqual(tile.geotrans, (369440.0, 960.0, 0, 4677120.0, 0, -960.0))
        self.assertEqual(tile.proj4, "+proj=utm +zone=15 +datum=WGS84 +units=m +no_defs ")
        self.assertEqual(tile.wkt, 'PROJCS["WGS 84 / UTM zone 15N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-93],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32615"]]') # noqa


class TestXYZTile(unittest.TestCase):
    def test_bounds(self):
        tile = geocontext.XYZTile(1, 1, 2)
        self.assertEqual(tile.bounds, (-10018754.171394622, -7.081154551613622e-10, 0.0, 10018754.171394626))

    def test_geometry(self):
        tile = geocontext.XYZTile(1, 1, 2)
        self.assertEqual(tile.geometry.bounds, (-90.0, 0.0, 0.0, 66.51326044311186))

    def test_raster_params(self):
        tile = geocontext.XYZTile(1, 1, 2)
        self.assertEqual(tile.raster_params, {
            'bounds': (-10018754.171394622, -7.081154551613622e-10, 0.0, 10018754.171394626),
            'srs': 'EPSG:3857',
            'bounds_srs': 'EPSG:3857',
            'align_pixels': False,
            'dimensions': (256, 256),
        })

    def test_children_parent(self):
        tile = geocontext.XYZTile(1, 1, 2)
        self.assertEqual(tile, tile.children()[0].parent())


# can't use the word `test` in the function name otherwise nose tries to run it...
def run_threadsafe_experiment(geoctx_factory, property, n=80000):
    "In a subprocess, test whether parallel access to a property on a GeoContext fails (due to Shapely thread-unsafety)"
    conn_ours, conn_theirs = multiprocessing.Pipe(duplex=False)

    # Run actual test in a separate process, because unsafe use of Shapely objects
    # across threads can occasionally cause segfaults, so we want to check the exit
    # code of the process doing the testing.
    def threadsafe_test(geoctx_factory, property, conn, n):
        ctx = geoctx_factory()
        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            futures = [executor.submit(lambda: getattr(ctx, property)) for i in range(n)]

        errors = []
        for future in concurrent.futures.as_completed(futures):
            if future.exception() is not None:
                errors.append("exception: {}".format(future.exception()))
        conn.send(errors)

    p = multiprocessing.Process(target=threadsafe_test, args=(geoctx_factory, property, conn_theirs, n))
    p.start()
    p.join()
    if p.exitcode < 0:
        errors = ["failed with exit code {}".format(p.exitcode)]
    else:
        errors = conn_ours.recv()
    return errors


@unittest.skip("Slow test. Un-skip this and run manually if touching any code related to `_geometry_lock_`!")
class TestShapelyThreadSafe(unittest.TestCase):
    @staticmethod
    def aoi_factory():
        return geocontext.AOI(
            {
                'coordinates': [[
                    [-93.52300099792355, 41.241436141055345],
                    [-93.7138666, 40.703737],
                    [-94.37053769704536, 40.83098709945576],
                    [-94.2036617, 41.3717716],
                    [-93.52300099792355, 41.241436141055345],
                ]],
                'type': 'Polygon'
            },
            crs="EPSG:3857",
            resolution=10
        )

    @staticmethod
    def dltile_factory():
        return geocontext.DLTile({
            'geometry': {
                'coordinates': [[
                    [-94.64171754779824, 40.9202359006794],
                    [-92.81755164322226, 40.93177944075989],
                    [-92.81360932958779, 42.31528732533928],
                    [-94.6771717075502, 42.303172487087394],
                    [-94.64171754779824, 40.9202359006794]
                ]],
                'type': 'Polygon'
            },
            'properties': {
                'cs_code': 'EPSG:32615',
                'key': '128:16:960.0:15:-1:37',
                'outputBounds': [361760.0, 4531200.0, 515360.0, 4684800.0],
                'pad': 16,
                'resolution': 960.0,
                'ti': -1,
                'tilesize': 128,
                'tj': 37,
                'zone': 15,
                'geotrans': [361760.0, 960.0, 0, 4684800.0, 0, -960.0],
                'proj4': '+proj=utm +zone=15 +datum=WGS84 +units=m +no_defs ',
                'wkt': 'PROJCS["WGS 84 / UTM zone 15N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-93],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32615"]]' # noqa
            },
            'type': 'Feature'
        })

    def test_aoi_raster_params_threadsafe(self):
        errors = run_threadsafe_experiment(self.aoi_factory, "raster_params")
        self.assertEqual(errors, [])

    def test_aoi_geo_interface_threadsafe(self):
        errors = run_threadsafe_experiment(self.aoi_factory, "__geo_interface__")
        self.assertEqual(errors, [])

    def test_dltile_geo_interface_threadsafe(self):
        errors = run_threadsafe_experiment(self.dltile_factory, "__geo_interface__")
        self.assertEqual(errors, [])
