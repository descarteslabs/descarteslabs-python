import unittest
import textwrap

import shapely.geometry

from .. import _helpers


class TestSimpleHelpers(unittest.TestCase):
    def test_polygon_from_bounds(self):
        bounds = (-95.8364984, 39.2784859, -92.0686956, 42.7999878)
        geom = {
            "coordinates": (
                (
                    (-92.0686956, 39.2784859),
                    (-92.0686956, 42.7999878),
                    (-95.8364984, 42.7999878),
                    (-95.8364984, 39.2784859),
                    (-92.0686956, 39.2784859),
                ),
            ),
            "type": "Polygon",
        }
        assert geom == shapely.geometry.box(*bounds).__geo_interface__
        assert (
            _helpers.polygon_from_bounds(bounds)
            == shapely.geometry.box(*bounds).__geo_interface__
        )

    def test_valid_latlon_bounds(self):
        assert _helpers.valid_latlon_bounds([-10, 5, 60, 80])
        assert _helpers.valid_latlon_bounds([-180, -90, 180, 90])
        assert not _helpers.valid_latlon_bounds(
            [361760.0, 4531200.0, 515360.0, 4684800.0]
        )

    def test_is_geographic_crs(self):
        assert _helpers.is_geographic_crs("EPSG:4326")
        assert _helpers.is_geographic_crs("+proj=longlat +datum=NAD27 +no_defs")
        assert _helpers.is_geographic_crs(
            textwrap.dedent(
                """\
        GEOGCS["NAD27",
            DATUM["North_American_Datum_1927",
                SPHEROID["Clarke 1866",6378206.4,294.9786982139006,
                    AUTHORITY["EPSG","7008"]],
                AUTHORITY["EPSG","6267"]],
            PRIMEM["Greenwich",0,
                AUTHORITY["EPSG","8901"]],
            UNIT["degree",0.0174532925199433,
                AUTHORITY["EPSG","9122"]],
            AUTHORITY["EPSG","4267"]]
        """
            )
        )

        assert not _helpers.is_geographic_crs("EPSG:32615")
        assert not _helpers.is_geographic_crs(
            "+proj=utm +zone=15 +datum=WGS84 +units=m +no_defs"
        )
        assert not _helpers.is_geographic_crs(
            textwrap.dedent(
                """\
        PROJCS["WGS 84 / UTM zone 15N",
            GEOGCS["WGS 84",
                DATUM["WGS_1984",
                    SPHEROID["WGS 84",6378137,298.257223563,
                        AUTHORITY["EPSG","7030"]],
                    AUTHORITY["EPSG","6326"]],
                PRIMEM["Greenwich",0,
                    AUTHORITY["EPSG","8901"]],
                UNIT["degree",0.0174532925199433,
                    AUTHORITY["EPSG","9122"]],
                AUTHORITY["EPSG","4326"]],
            PROJECTION["Transverse_Mercator"],
            PARAMETER["latitude_of_origin",0],
            PARAMETER["central_meridian",-93],
            PARAMETER["scale_factor",0.9996],
            PARAMETER["false_easting",500000],
            PARAMETER["false_northing",0],
            UNIT["metre",1,
                AUTHORITY["EPSG","9001"]],
            AXIS["Easting",EAST],
            AXIS["Northing",NORTH],
            AUTHORITY["EPSG","32615"]]
        """
            )
        )

    def test_is_wgs84_crs(self):
        assert _helpers.is_wgs84_crs("EPSG:4326")
        assert _helpers.is_wgs84_crs("+proj=longlat +datum=WGS84 +no_defs")
        assert _helpers.is_wgs84_crs(
            textwrap.dedent(
                """\
        GEOGCS["WGS 84",
            DATUM["WGS_1984",
                SPHEROID["WGS 84",6378137,298.257223563,
                    AUTHORITY["EPSG","7030"]],
                AUTHORITY["EPSG","6326"]],
            PRIMEM["Greenwich",0,
                AUTHORITY["EPSG","8901"]],
            UNIT["degree",0.0174532925199433,
                AUTHORITY["EPSG","9122"]],
            AUTHORITY["EPSG","4326"]]
        """
            )
        )

        assert not _helpers.is_wgs84_crs("+proj=longlat +datum=NAD27 +no_defs")
        assert not _helpers.is_wgs84_crs(
            textwrap.dedent(
                """\
        GEOGCS["NAD27",
            DATUM["North_American_Datum_1927",
                SPHEROID["Clarke 1866",6378206.4,294.9786982139006,
                    AUTHORITY["EPSG","7008"]],
                AUTHORITY["EPSG","6267"]],
            PRIMEM["Greenwich",0,
                AUTHORITY["EPSG","8901"]],
            UNIT["degree",0.0174532925199433,
                AUTHORITY["EPSG","9122"]],
            AUTHORITY["EPSG","4267"]]
        """
            )
        )

        assert not _helpers.is_wgs84_crs("EPSG:32615")
        assert not _helpers.is_wgs84_crs(
            "+proj=utm +zone=15 +datum=WGS84 +units=m +no_defs"
        )
        assert not _helpers.is_wgs84_crs(
            textwrap.dedent(
                """\
        PROJCS["WGS 84 / UTM zone 15N",
            GEOGCS["WGS 84",
                DATUM["WGS_1984",
                    SPHEROID["WGS 84",6378137,298.257223563,
                        AUTHORITY["EPSG","7030"]],
                    AUTHORITY["EPSG","6326"]],
                PRIMEM["Greenwich",0,
                    AUTHORITY["EPSG","8901"]],
                UNIT["degree",0.0174532925199433,
                    AUTHORITY["EPSG","9122"]],
                AUTHORITY["EPSG","4326"]],
            PROJECTION["Transverse_Mercator"],
            PARAMETER["latitude_of_origin",0],
            PARAMETER["central_meridian",-93],
            PARAMETER["scale_factor",0.9996],
            PARAMETER["false_easting",500000],
            PARAMETER["false_northing",0],
            UNIT["metre",1,
                AUTHORITY["EPSG","9001"]],
            AXIS["Easting",EAST],
            AXIS["Northing",NORTH],
            AUTHORITY["EPSG","32615"]]
        """
            )
        )
