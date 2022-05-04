import pytest
import unittest
import multiprocessing
import platform
import concurrent.futures
import copy
import warnings

from .. import geocontext
from ..geocontext import EARTH_CIRCUMFERENCE_WGS84
import shapely.geometry


if platform.system() == "Darwin":
    multiprocessing.set_start_method("fork")


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
              bar=False,
              all_touched=False)"""
        assert r == expected

    def test_eq(self):
        simple = SimpleContext(1, False)
        simple2 = SimpleContext(1, False)
        simple_diff = SimpleContext(1, True)
        not_simple = geocontext.GeoContext()
        assert simple == simple
        assert simple == simple2
        assert simple != simple_diff
        assert simple != not_simple

    def test_deepcopy(self):
        simple = SimpleContext(1, False)
        simple_copy = copy.deepcopy(simple)
        assert simple._geometry_lock_ is not simple_copy._geometry_lock_
        assert simple == simple_copy


class TestAOI(unittest.TestCase):
    def test_init(self):
        feature = {
            "type": "Feature",
            "geometry": {
                "coordinates": (
                    (
                        (-93.52300099792355, 41.241436141055345),
                        (-93.7138666, 40.703737),
                        (-94.37053769704536, 40.83098709945576),
                        (-94.2036617, 41.3717716),
                        (-93.52300099792355, 41.241436141055345),
                    ),
                ),
                "type": "Polygon",
            },
        }
        collection = {
            "type": "FeatureCollection",
            "features": [feature, feature, feature],
        }
        bounds_wgs84 = (-94.37053769704536, 40.703737, -93.52300099792355, 41.3717716)
        resolution = 40
        ctx = geocontext.AOI(collection, resolution=resolution)
        assert ctx.resolution == resolution
        assert tuple(round(e, 5) for e in ctx.bounds) == tuple(
            round(e, 5) for e in bounds_wgs84
        )
        assert ctx.bounds_crs == "EPSG:4326"
        assert isinstance(ctx.geometry, shapely.geometry.GeometryCollection)
        assert ctx.__geo_interface__["type"] == "GeometryCollection"
        assert ctx.__geo_interface__["geometries"][0] == feature["geometry"]

    def test_raster_params(self):
        geom = {
            "coordinates": (
                (
                    (-93.52300099792355, 41.241436141055345),
                    (-93.7138666, 40.703737),
                    (-94.37053769704536, 40.83098709945576),
                    (-94.2036617, 41.3717716),
                    (-93.52300099792355, 41.241436141055345),
                ),
            ),
            "type": "Polygon",
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
        assert raster_params == expected

    def test_assign(self):
        geom = {
            "coordinates": [
                [
                    [-93.52300099792355, 41.241436141055345],
                    [-93.7138666, 40.703737],
                    [-94.37053769704536, 40.83098709945576],
                    [-94.2036617, 41.3717716],
                    [-93.52300099792355, 41.241436141055345],
                ]
            ],
            "type": "Polygon",
        }
        ctx = geocontext.AOI(resolution=40)
        ctx2 = ctx.assign(geometry=geom)
        assert (
            ctx2.geometry.__geo_interface__
            == shapely.geometry.shape(geom).__geo_interface__
        )
        assert ctx2.resolution == 40
        assert ctx2.align_pixels
        assert ctx2.shape is None

        ctx3 = ctx2.assign(geometry=None)
        assert ctx3.geometry is None

    def test_assign_update_bounds(self):
        geom = shapely.geometry.Point(-90, 30).buffer(1).envelope
        ctx = geocontext.AOI(geometry=geom, resolution=40)

        geom_overlaps = shapely.affinity.translate(geom, xoff=1)
        assert geom.intersects(geom_overlaps)
        ctx_overlap = ctx.assign(geometry=geom_overlaps)
        assert ctx_overlap.bounds == ctx.bounds

        ctx_updated = ctx.assign(geometry=geom_overlaps, bounds="update")
        assert ctx_updated.bounds == geom_overlaps.bounds

        geom_doesnt_overlap = shapely.affinity.translate(geom, xoff=3)
        with pytest.raises(ValueError, match="Geometry and bounds do not intersect"):
            ctx.assign(geometry=geom_doesnt_overlap)
        ctx_doesnt_overlap_updated = ctx.assign(
            geometry=geom_doesnt_overlap, bounds="update"
        )
        assert ctx_doesnt_overlap_updated.bounds == geom_doesnt_overlap.bounds

        with pytest.raises(
            ValueError, match="A geometry must be given with which to update the bounds"
        ):
            ctx.assign(bounds="update")

    def test_assign_update_bounds_crs(self):
        ctx = geocontext.AOI(bounds_crs="EPSG:32615")
        assert ctx.bounds_crs == "EPSG:32615"
        geom = shapely.geometry.Point(-20, 30).buffer(1).envelope

        ctx_no_update_bounds = ctx.assign(geometry=geom)
        assert ctx_no_update_bounds.bounds_crs == "EPSG:32615"

        ctx_update_bounds = ctx.assign(geometry=geom, bounds="update")
        assert ctx_update_bounds.bounds_crs == "EPSG:4326"

        with pytest.raises(
            ValueError,
            match="Can't compute bounds from a geometry while also explicitly setting",
        ):
            ctx = geocontext.AOI(geometry=geom, resolution=40, bounds_crs="EPSG:32615")

    def test_validate_bounds_values_for_bounds_crs__latlon(self):
        # invalid latlon bounds
        with pytest.raises(ValueError, match="Bounds must be in lat-lon coordinates"):
            geocontext.AOI(
                bounds_crs="EPSG:4326", bounds=[500000, 2000000, 501000, 2001000]
            )
        # valid latlon bounds, no error should raise
        geocontext.AOI(bounds_crs="EPSG:4326", bounds=[12, -41, 14, -40])

    def test_validate_bounds_values_for_bounds_crs__non_latlon(self):
        # valid latlon bounds, should warn
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = geocontext.AOI(bounds_crs="EPSG:32615", bounds=(12, -41, 14, -40))
            assert ctx.bounds_crs == "EPSG:32615"
            assert ctx.bounds == (12, -41, 14, -40)
            warning = w[0]
            assert "You might have the wrong `bounds_crs` set." in str(warning.message)
        # not latlon bounds, no error should raise
        geocontext.AOI(
            bounds_crs="EPSG:32615", bounds=[500000, 2000000, 501000, 2001000]
        )

    def test_validate_shape(self):
        with pytest.raises(TypeError):
            geocontext.AOI(shape=120)
        with pytest.raises(TypeError):
            geocontext.AOI(shape=(120, 0, 0))

    def test_validate_resolution(self):
        with pytest.raises(TypeError):
            geocontext.AOI(resolution="foo")
        with pytest.raises(ValueError):
            geocontext.AOI(resolution=-1)

    def test_validate_resolution_shape(self):
        with pytest.raises(ValueError):
            geocontext.AOI(resolution=40, shape=(120, 280))

    def test_validate_bound_geom_intersection(self):
        # bounds don't intersect
        with pytest.raises(ValueError, match="Geometry and bounds do not intersect"):
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
        assert ctx.crs == "EPSG:32615"
        assert ctx.bounds_crs == "EPSG:4326"
        assert ctx.bounds == (0, 0, 1.5, 1.5)
        assert ctx.resolution == 15

        # same CRSs, bounds < resolution --- no error
        geocontext.AOI(
            crs="EPSG:32615",
            bounds_crs="EPSG:32615",
            bounds=[200000, 5000000, 200100, 5000300],
            resolution=15,
        )

        # same CRSs, width < resolution --- error
        with pytest.raises(ValueError, match="less than one pixel wide"):
            geocontext.AOI(
                crs="EPSG:32615",
                bounds_crs="EPSG:32615",
                bounds=[200000, 5000000, 200001, 5000300],
                resolution=15,
            )

        # same CRSs, height < resolution --- error
        with pytest.raises(ValueError, match="less than one pixel tall"):
            geocontext.AOI(
                crs="EPSG:32615",
                bounds_crs="EPSG:32615",
                bounds=[200000, 5000000, 200100, 5000001],
                resolution=15,
            )

        # same CRSs, width < resolution, CRS is lat-lon --- error including "decimal degrees"
        with pytest.raises(
            ValueError, match="resolution must be given in decimal degrees"
        ):
            geocontext.AOI(
                crs="EPSG:4326",
                bounds_crs="EPSG:4326",
                bounds=[10, 10, 11, 11],
                resolution=15,
            )


class TestDLTIle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = "128:16:960.0:15:-1:37"
        cls.key2 = "128:8:960.0:15:-1:37"

    def test_from_key(self):
        tile = geocontext.DLTile.from_key(self.key)

        assert tile.key == self.key
        assert tile.resolution == 960
        assert tile.pad == 16
        assert tile.tilesize == 128
        assert tile.crs == "EPSG:32615"
        assert tile.bounds == (361760.0, 4531200.0, 515360.0, 4684800.0)
        assert tile.bounds_crs == "EPSG:32615"
        assert tile.raster_params == {"dltile": self.key, "align_pixels": False}
        assert tile.geotrans == (361760.0, 960, 0, 4684800.0, 0, -960)
        assert tile.proj4 == "+proj=utm +zone=15 +datum=WGS84 +units=m +no_defs "
        assert (
            tile.wkt
            == 'PROJCS["WGS 84 / UTM zone 15N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-93],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32615"]]'  # noqa
        )

    def test_assign(self):
        tile = geocontext.DLTile.from_key(self.key)
        tile = tile.assign(8)

        assert tile.key == self.key.replace(":16:", ":8:")
        assert tile.resolution == 960
        assert tile.pad == 8
        assert tile.tilesize == 128
        assert tile.crs == "EPSG:32615"
        assert tile.bounds == (369440.0, 4538880.0, 507680.0, 4677120.0)
        assert tile.bounds_crs == "EPSG:32615"
        assert tile.raster_params == {"dltile": self.key2, "align_pixels": False}
        assert tile.geotrans == (369440.0, 960.0, 0, 4677120.0, 0, -960.0)
        assert tile.proj4 == "+proj=utm +zone=15 +datum=WGS84 +units=m +no_defs "
        assert (
            tile.wkt
            == 'PROJCS["WGS 84 / UTM zone 15N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-93],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32615"]]'  # noqa
        )

    def latlon_conversions(self):
        tile = geocontext.DLTile.from_key(self.key)
        latlons = tile.rowcol_to_latlon(row=5, col=23)
        rowcols = tile.latlon_to_rowcol(lat=latlons[0], lon=latlons[1])
        assert rowcols[0] == 5
        assert rowcols[1] == 23

    def test_iter_from_shape(self):
        params = {"resolution": 1.5, "tilesize": 512, "pad": 0, "keys_only": True}
        shape = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-122.51140471760839, 37.77130087547876],
                        [-122.45475646845254, 37.77475476721895],
                        [-122.45303985468301, 37.76657207194229],
                        [-122.51057242081689, 37.763446782666094],
                        [-122.51140471760839, 37.77130087547876],
                    ]
                ],
            },
            "properties": None,
        }
        dltiles1 = [tile for tile in geocontext.DLTile.iter_from_shape(shape, **params)]
        dltiles2 = geocontext.DLTile.from_shape(shape, **params)
        assert type(dltiles1[0]) == str
        assert type(dltiles1[1]) == str
        assert len(dltiles1) == len(dltiles2)

    def test_subtile(self):
        tile = geocontext.DLTile.from_key("2048:0:30.0:15:3:80")
        tiles = [t for t in tile.subtile(8, keys_only=True)]
        assert len(tiles) == 64
        assert type(tiles[0]) == str


class TestXYZTile(unittest.TestCase):
    def test_bounds(self):
        tile = geocontext.XYZTile(1, 1, 2)
        assert tile.bounds == (-10018754.171394622, 0.0, 0.0, 10018754.171394622)

    def test_geometry(self):
        tile = geocontext.XYZTile(1, 1, 2)
        assert tile.geometry.bounds == (-90.0, 0.0, 0.0, 66.51326044311186)

    def test_resolution(self):
        tile = geocontext.XYZTile(1, 1, 0)
        assert tile.resolution == EARTH_CIRCUMFERENCE_WGS84 / tile.tilesize
        # resolution at zoom 0 is just the Earth's circumfrence divided by tilesize

        assert geocontext.XYZTile(1, 1, 2).resolution == (
            geocontext.XYZTile(1, 1, 3).resolution * 2
        )
        # resolution halves with each zoom level

        assert (
            geocontext.XYZTile(1, 1, 12).resolution
            == geocontext.XYZTile(2048, 1024, 12).resolution
        )
        # resolution is invariant to location; only depends on zoom

    def test_raster_params(self):
        tile = geocontext.XYZTile(1, 1, 2)
        assert tile.raster_params == {
            "bounds": (-10018754.171394622, 0.0, 0.0, 10018754.171394622),
            "srs": "EPSG:3857",
            "bounds_srs": "EPSG:3857",
            "align_pixels": False,
            "resolution": 39135.75848201024,
        }

    def test_children_parent(self):
        tile = geocontext.XYZTile(1, 1, 2)
        assert tile == tile.children()[0].parent()


# can't use the word `test` in the function name otherwise nose tries to run it...
def run_threadsafe_experiment(geoctx_factory, property, n=80000):
    "In a subprocess, test whether parallel access to a property on a GeoContext fails (due to Shapely thread-unsafety)"
    conn_ours, conn_theirs = multiprocessing.Pipe(duplex=False)

    # Run actual test in a separate process, because unsafe use of Shapely objects
    # across threads can occasionally cause segfaults, so we want to check the exit
    # code of the process doing the testing.
    def threadsafe_test(geoctx_factory, property, conn, n):
        ctx = geoctx_factory()
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=multiprocessing.cpu_count()
        ) as executor:
            futures = [
                executor.submit(lambda: getattr(ctx, property)) for i in range(n)
            ]

        errors = []
        for future in concurrent.futures.as_completed(futures):
            if future.exception() is not None:
                errors.append("exception: {}".format(future.exception()))
        conn.send(errors)

    p = multiprocessing.Process(
        target=threadsafe_test, args=(geoctx_factory, property, conn_theirs, n)
    )
    p.start()
    p.join()
    if p.exitcode < 0:
        errors = ["failed with exit code {}".format(p.exitcode)]
    else:
        errors = conn_ours.recv()
    return errors


@unittest.skip(
    "Slow test. Un-skip this and run manually if touching any code related to `_geometry_lock_`!"
)
class TestShapelyThreadSafe(unittest.TestCase):
    @staticmethod
    def aoi_factory():
        return geocontext.AOI(
            {
                "coordinates": [
                    [
                        [-93.52300099792355, 41.241436141055345],
                        [-93.7138666, 40.703737],
                        [-94.37053769704536, 40.83098709945576],
                        [-94.2036617, 41.3717716],
                        [-93.52300099792355, 41.241436141055345],
                    ]
                ],
                "type": "Polygon",
            },
            crs="EPSG:3857",
            resolution=10,
        )

    @staticmethod
    def dltile_factory():
        return geocontext.DLTile(
            {
                "geometry": {
                    "coordinates": [
                        [
                            [-94.64171754779824, 40.9202359006794],
                            [-92.81755164322226, 40.93177944075989],
                            [-92.81360932958779, 42.31528732533928],
                            [-94.6771717075502, 42.303172487087394],
                            [-94.64171754779824, 40.9202359006794],
                        ]
                    ],
                    "type": "Polygon",
                },
                "properties": {
                    "cs_code": "EPSG:32615",
                    "key": "128:16:960.0:15:-1:37",
                    "outputBounds": [361760.0, 4531200.0, 515360.0, 4684800.0],
                    "pad": 16,
                    "resolution": 960.0,
                    "ti": -1,
                    "tilesize": 128,
                    "tj": 37,
                    "zone": 15,
                    "geotrans": [361760.0, 960.0, 0, 4684800.0, 0, -960.0],
                    "proj4": "+proj=utm +zone=15 +datum=WGS84 +units=m +no_defs ",
                    "wkt": 'PROJCS["WGS 84 / UTM zone 15N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",-93],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32615"]]',  # noqa
                },
                "type": "Feature",
            }
        )

    def test_aoi_raster_params_threadsafe(self):
        errors = run_threadsafe_experiment(self.aoi_factory, "raster_params")
        assert errors == []

    def test_aoi_geo_interface_threadsafe(self):
        errors = run_threadsafe_experiment(self.aoi_factory, "__geo_interface__")
        assert errors == []

    def test_dltile_geo_interface_threadsafe(self):
        errors = run_threadsafe_experiment(self.dltile_factory, "__geo_interface__")
        assert errors == []
