"""In this file we define two classes: Grid, which describes a way to
divide UTM zones in a grid, and Tile, which specifies a particular
element in a grid."""
import collections.abc
import numpy as np
import re
import shapely.geometry as geo
from typing import Generator, Sequence, Tuple, Union

from . import _tiling as _tiling
from .conversions import normalize_polygons, AnyShapes
from .exceptions import (
    InvalidTileError,
    InvalidLatLonError,
    InvalidRowColError,
)
from .utm import (
    rowcol_to_utm,
    utm_to_rowcol,
    utm_to_lonlat,
    lonlat_to_utm,
    lon_to_zone,
    FALSE_EASTING,
    UTM_MIN_LON,
    UTM_MAX_LON,
    UTM_MIN_LAT,
    UTM_MAX_LAT,
)


class Grid:
    """A grid specifies for any given UTM zone, an origin-centered regular
    division into square tiles, as well as the resolution and padding used
    when those tiles represent the extent of raster data."""

    gridkey_pattern = (
        r"(?P<tilesize>\d+):" r"(?P<pad>-?\d+):" r"(?P<resolution>\d+.?\d+)"
    )
    gridkey_regex = re.compile(gridkey_pattern)

    def __init__(self, resolution: float, tilesize: int, pad: int):
        self._tilesize = int(tilesize)
        self._pad = int(pad)
        self._resolution = float(resolution)

        if self._tilesize <= 0:
            raise InvalidTileError("Tile size must be greater than zero")
        if self._pad < 0:
            raise InvalidTileError("Pad value must be non-negative")
        if self._resolution <= 0:
            raise InvalidTileError("Resolution must be greater than zero")

    @classmethod
    def from_key(cls, key) -> "Grid":
        """
        Create a grid object from its string representation
        """
        match = cls.gridkey_regex.match(key)
        if match is None:
            raise InvalidTileError("Invalid tile parameters")

        kwargs = match.groupdict()
        return cls(
            tilesize=int(kwargs["tilesize"]),
            resolution=float(kwargs["resolution"]),
            pad=int(kwargs["pad"]),
        )

    def __repr__(self) -> str:
        res_string = ("%f" % self.resolution).rstrip("0")
        if res_string[-1] == ".":
            res_string += "0"
        return "%i:%i:%s" % (self.tilesize, self.pad, res_string)

    @property
    def tilesize(self) -> int:
        """Get the tilesize parameter in meters"""
        return self._tilesize

    @property
    def pad(self) -> int:
        """Get the pad value of the tile in meters"""
        return self._pad

    @property
    def resolution(self) -> float:
        """Get the resolution in meters"""
        return self._resolution

    @property
    def tile_extent(self) -> int:
        """Get the pixel size of a tile raster, including padding."""
        return self.tilesize + 2 * self.pad

    @property
    def utm_tile_extent(self) -> float:
        """Get the size of a tile in meters, including padding."""
        return self.tile_extent * self.resolution

    @property
    def utm_tilesize(self) -> float:
        """Get the size of a tile in meters, not including padding."""
        return self.tilesize * self.resolution

    def tile_from_lonlat(self, lon: float, lat: float) -> "Tile":
        if lon < UTM_MIN_LON or lon > UTM_MAX_LON:
            raise InvalidLatLonError("Longitude must be between -180.0 and 180.0")
        if lat < UTM_MIN_LAT or lat > UTM_MAX_LAT:
            raise InvalidLatLonError("Latitude must be between -90 and 90")

        zone = lon_to_zone(lon)
        x, y = lonlat_to_utm(np.stack((lon, lat), axis=-1), zone=zone)[0]
        x -= FALSE_EASTING
        path = int(x // self.utm_tilesize)
        row = int(y // self.utm_tilesize)
        return Tile(self, zone=zone, path=path, row=row)

    def _estimate_ntiles_from_shape(self, shape: AnyShapes) -> int:
        shape = normalize_polygons(shape)
        ntiles = 0
        for s in shape:
            lat = s.centroid.xy[1][-1]
            m2 = 12321000000 * np.cos(lat * np.pi / 180)
            ntiles += (s.area * m2) // ((self.resolution * self.tilesize) ** 2)
        return int(ntiles)

    def tiles_from_shape(
        self, shape: AnyShapes, keys_only=False
    ) -> Generator[Union["Tile", str], None, None]:
        """Yields tiles which cover the given shape. If zone is given, all
        tiles will come from one UTM zone. This puts everything on the same
        UTM grid, but comes with the trade-off of more area distortion the
        further from the given zone your shape is."""
        shape = normalize_polygons(shape)

        key_set = set()  # remove duplicate tiles
        for polygon in shape:
            for zone, path, row in _tiling._get_next_tiling(
                polygon, self.tilesize * self.resolution
            ):
                tile = Tile(self, zone=zone, path=path, row=row)
                tile_key = str(tile)
                if tile_key not in key_set:
                    key_set.add(tile_key)
                    if keys_only:
                        yield tile_key
                    else:
                        yield tile


class Tile:
    """A tile specifies a particular element of a grid system. Each tile is
    uniquely specified by its parameters."""

    tilekey_pattern = "%s:%s" % (
        Grid.gridkey_pattern,
        r"(?P<zone>\d+):(?P<path>-?\d+):(?P<row>-?\d+)",
    )
    tilekey_regex = re.compile(tilekey_pattern)

    def __init__(self, grid: Grid, zone: int, path: int, row: int):
        self._grid = grid
        self._path = int(path)
        self._row = int(row)
        self._zone = int(zone)
        if self._zone <= 0 or self._zone > 60:
            raise InvalidTileError("Invalid zone")

    @classmethod
    def from_key(cls, key) -> "Tile":
        match = cls.tilekey_regex.match(key)
        if match is None:
            raise InvalidTileError("Invalid DLTile key")

        kwargs = match.groupdict()
        resolution = float(kwargs["resolution"])
        tilesize = int(kwargs["tilesize"])
        pad = int(kwargs["pad"])
        zone = int(kwargs["zone"])
        path = int(kwargs["path"])
        row = int(kwargs["row"])

        if tilesize <= 0:
            raise InvalidTileError("Invalid tile size")
        if pad < 0:
            raise InvalidTileError("Invalid padding")
        if resolution <= 0:
            raise InvalidTileError("Invalid resolution")
        if zone <= 0 or zone > 60:
            raise InvalidTileError("Invalid zone")

        grid = Grid(
            resolution=resolution,
            tilesize=tilesize,
            pad=pad,
        )
        return Tile(
            grid,
            zone=zone,
            path=path,
            row=row,
        )

    def __repr__(self) -> str:
        zone = str(self.zone)
        if len(zone) == 1:
            zone = "0" + zone

        return "%s:%s:%i:%i" % (
            repr(self.grid),
            zone,
            self.path,
            self.row,
        )

    def assign(
        self, resolution: float = None, tilesize: int = None, pad: int = None
    ) -> "Tile":
        """Returns a new Tile with new resolution, tilesize, and / or pad (thus changing grid)
        while keeping the tile region (ignoring pad) the same. If both resolution
        and tilesize are specified, they must multiply to the same value as before."""
        new_grid = Grid(
            tilesize=self.tilesize if tilesize is None else tilesize,
            resolution=self.resolution if resolution is None else resolution,
            pad=self.pad if pad is None else pad,
        )

        if resolution is None:
            if tilesize is None and pad is None:
                return self  # change nothing
        else:
            if tilesize is not None:  # change resolution and tilesize, must check
                if resolution * tilesize != self.resolution * self.tilesize:
                    raise InvalidTileError(
                        "New resolution and tilesize are not compatible"
                        "With the old resolution and tilesize"
                    )

        return Tile(new_grid, self.zone, self.path, self.row)

    @property
    def tilesize(self) -> int:
        return self.grid.tilesize

    @property
    def tile_extent(self) -> int:
        return self.grid.tile_extent

    @property
    def utm_tilesize(self) -> float:
        return self.grid.utm_tilesize

    @property
    def utm_tile_extent(self) -> float:
        return self.grid.utm_tile_extent

    @property
    def pad(self) -> int:
        return self.grid.pad

    @property
    def resolution(self) -> float:
        return self.grid.resolution

    @property
    def row(self) -> int:
        return self._row

    @property
    def path(self) -> int:
        return self._path

    @property
    def zone(self) -> int:
        return self._zone

    @property
    def grid(self) -> Grid:
        return self._grid

    @property
    def key(self) -> str:
        return str(self)

    @property
    def utm_bounds_unpadded(self) -> Tuple[float, float, float, float]:
        # Get the size in meters of the tile (not including pad)
        utm_size = self.resolution * self.tilesize

        # Get the location in meters of the tile.
        x_min = FALSE_EASTING + self.path * utm_size
        y_min = self.row * utm_size
        x_max = x_min + utm_size
        y_max = y_min + utm_size

        return x_min, y_min, x_max, y_max

    @property
    def utm_bounds(self) -> Tuple[float, float, float, float]:
        x_min, y_min, x_max, y_max = self.utm_bounds_unpadded
        utm_pad = self.pad * self.resolution
        return (
            x_min - utm_pad,
            y_min - utm_pad,
            x_max + utm_pad,
            y_max + utm_pad,
        )

    @property
    def utm_polygon_unpadded(self) -> geo.Polygon:
        x_min, y_min, x_max, y_max = self.utm_bounds_unpadded
        utm_points = np.array(
            [
                (x_min, y_min),
                (x_max, y_min),
                (x_max, y_max),
                (x_min, y_max),
                (x_min, y_min),
            ]
        )
        return geo.Polygon(utm_points)

    @property
    def utm_polygon(self) -> geo.Polygon:
        x_min, y_min, x_max, y_max = self.utm_bounds
        utm_points = np.array(
            [
                (x_min, y_min),
                (x_max, y_min),
                (x_max, y_max),
                (x_min, y_max),
                (x_min, y_min),
            ]
        )
        return geo.Polygon(utm_points)

    @property
    def polygon(self) -> geo.Polygon:
        """ Shapely polygon """
        x_min, y_min, x_max, y_max = self.utm_bounds
        utm_points = np.array(
            [
                (x_min, y_min),
                (x_max, y_min),
                (x_max, y_max),
                (x_min, y_max),
                (x_min, y_min),
            ]
        )
        lonlat_points = utm_to_lonlat(utm_points, zone=self.zone)
        return geo.Polygon(lonlat_points)

    @property
    def center(self) -> geo.Point:
        """ Shapely centroid """
        return self.polygon.centroid

    @property
    def epsg(self) -> int:
        """Returns the coordinate system's European Petroleum Survey Group
        geodetic parameter database's standard code, as an integer."""
        return 32600 + self.zone

    @property
    def proj4(self) -> str:
        return "+proj=utm +zone={} +datum=WGS84 +units=m +no_defs ".format(self.zone)

    @property
    def srs(self) -> str:
        """spatial reference system (srs) in well-known text (wkt)"""
        return (
            """PROJCS["WGS 84 / UTM zone {zone}N","""
            """GEOGCS["WGS 84","""
            """DATUM["WGS_1984","""
            """SPHEROID["WGS 84",6378137,298.257223563,"""
            """AUTHORITY["EPSG","7030"]],"""
            """AUTHORITY["EPSG","6326"]],"""
            """PRIMEM["Greenwich",0,"""
            """AUTHORITY["EPSG","8901"]],"""
            """UNIT["degree",0.0174532925199433,"""
            """AUTHORITY["EPSG","9122"]],"""
            """AUTHORITY["EPSG","4326"]],"""
            """PROJECTION["Transverse_Mercator"],"""
            """PARAMETER["latitude_of_origin",0],"""
            """PARAMETER["central_meridian",{central_meridian}],"""
            """PARAMETER["scale_factor",0.9996],"""
            """PARAMETER["false_easting",500000],"""
            """PARAMETER["false_northing",0],"""
            """UNIT["metre",1,"""
            """AUTHORITY["EPSG","9001"]],"""
            """AXIS["Easting",EAST],"""
            """AXIS["Northing",NORTH],"""
            """AUTHORITY["EPSG","{epsg}"]]"""
        ).format(zone=self.zone, central_meridian=(6 * self.zone - 183), epsg=self.epsg)

    @property
    def geotransform(self) -> Tuple[float, float, float, float, float, float]:
        """Returns the affine geotransform parameters in GDAL order"""
        x_min, y_min, x_max, y_max = self.utm_bounds
        left, top = x_min, y_max
        res = self.resolution
        return (left, res, 0.0, top, 0.0, -res)

    @property
    def polygon_unpadded(self) -> geo.Polygon:
        """Returns a shapely polygon object, lonlat coordinates,
        of the unpadded tile extent"""
        x_min, y_min, x_max, y_max = self.utm_bounds_unpadded
        utm_points = [
            (x_min, y_min),
            (x_max, y_min),
            (x_max, y_max),
            (x_min, y_max),
            (x_min, y_min),
        ]
        lonlat_points = utm_to_lonlat(utm_points, zone=self.zone)
        return geo.Polygon(lonlat_points)

    @property
    def geometry(self) -> dict:
        """Returns a geojson polygon geometry, lonlat coordinates"""
        return geo.mapping(self.polygon)

    @property
    def geocontext(self):
        """For compatibility with the current descarteslabs module"""
        properties = dict(
            coordinateSystem={"wkt": self.srs},
            geometry=self.geometry,
            key=str(self),
            resolution=self.resolution,
            tilesize=self.tilesize,
            pad=self.pad,
            cs_code="EPSG:%i" % self.epsg,
            outputBounds=self.utm_bounds,
            size=[self.tile_extent, self.tile_extent],
            zone=self.zone,
            ti=self.path,
            tj=self.row,
            geotrans=self.geotransform,
            geoTransform=self.geotransform,
            wkt=self.srs,
            proj4=self.proj4,
        )
        feature = self.feature
        feature["properties"] = properties
        return feature

    @property
    def feature(self) -> dict:
        """ GeoJSON Feature """
        return dict(
            type="Feature",
            geometry=self.polygon,
            properties={"tilekey": str(self)},
        )

    def lonlat_to_rowcol(
        self,
        lon: Union[float, Sequence[float]],
        lat: Union[float, Sequence[float]],
    ):
        """Convert lonlat coordinates to pixel coordinates"""
        if type(lon) != type(lat):
            raise InvalidLatLonError("lat and lon should have compatible types")

        if isinstance(lon, (collections.abc.Sequence, np.ndarray)):
            if len(lon) != len(lat):
                raise InvalidLatLonError("lat and lon must be the same length")

            utm_coordinates = lonlat_to_utm(
                np.stack((lon, lat), axis=-1), zone=self.zone
            )
            return np.round(utm_to_rowcol(utm_coordinates, tile=self)).astype(int)
        else:
            utm_coordinates = lonlat_to_utm(np.array([(lon, lat)]), zone=self.zone)
            return np.round(utm_to_rowcol(utm_coordinates, tile=self)[0]).astype(int)

    def rowcol_to_lonlat(
        self,
        row: Union[int, Sequence[int]],
        col: Union[int, Sequence[int]],
    ):
        """Convert pixel coordinates to lonlat coordinates"""
        if type(row) != type(col):
            raise InvalidRowColError("row and col should have compatible types")

        if isinstance(row, (collections.abc.Sequence, np.ndarray)):
            if len(row) != len(col):
                raise InvalidRowColError("row and col must be the same length")

            utm_coordinates = rowcol_to_utm(np.stack((row, col), axis=-1), tile=self)
            return utm_to_lonlat(utm_coordinates, zone=self.zone)
        else:
            utm_coordinates = rowcol_to_utm(np.array([(row, col)]), tile=self)
            return utm_to_lonlat(utm_coordinates, zone=self.zone)[0]

    def subtile(
        self,
        subdivide: int,
        row: int = None,
        col: int = None,
        new_resolution: float = None,
        new_pad: float = None,
    ) -> Union[Generator["Tile", None, None], "Tile"]:
        """Divide this tile into subdivide^2 total tiles.
        If row,col is given, returns just the tile in that position
        counting from the upper-left corner."""

        if row is not None and col is None:
            raise InvalidRowColError("col is None but row is not")
        elif row is None and col is not None:
            raise InvalidRowColError("row is None but col is not")

        if row is not None and not (0 <= row < subdivide):
            raise IndexError(
                "row is %i but should be between 0 and %i" % (row, subdivide)
            )
        if col is not None and not (0 <= col < subdivide):
            raise IndexError(
                "col is %i but should be between 0 and %i" % (col, subdivide)
            )

        if new_resolution is None:
            new_resolution = self.resolution
        if new_pad is None:
            new_pad = self.pad

        if not np.allclose(subdivide % 1, 0.0):
            raise InvalidTileError("subdivide ratio must be an integer")
        subdivide = int(subdivide)

        if not np.allclose(self.tilesize % subdivide, 0):
            raise InvalidTileError(
                "The subdivide ratio must evenly divide the original tilesize"
            )

        new_tilesize = (self.resolution * self.tilesize) / (subdivide * new_resolution)

        if not np.allclose(new_tilesize % 1, 0.0):
            raise InvalidTileError(
                "The tile can only be subdivided if the subdivide * new tilesize * new resolution is "
                "equal to the original tilesize * original resolution"
            )
        new_tilesize = int(new_tilesize)

        grid = Grid(resolution=new_resolution, tilesize=new_tilesize, pad=new_pad)

        # Get the path, row of the lower-left corner subtile
        ll_path = self.path * subdivide
        ll_row = self.row * subdivide

        # Get the path, row of the upper-left corner subtile
        ul_path = ll_path
        ul_row = ll_row + subdivide - 1

        if row is not None:
            return Tile(grid=grid, zone=self.zone, path=ul_path + col, row=ul_row - row)

        for j in range(subdivide):
            for i in range(subdivide):
                yield Tile(grid=grid, zone=self.zone, path=ul_path + i, row=ul_row - j)
