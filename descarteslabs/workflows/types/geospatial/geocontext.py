from descarteslabs import scenes

from ...cereal import serializable
from ..core import typecheck_promote
from ..primitives import Str, Int, Float, Bool
from ..containers import Tuple, Struct

from .geometry import Geometry

GeoContextBase = Struct[
    {
        "geometry": Geometry,
        "resolution": Float,
        "crs": Str,
        "align_pixels": Bool,
        "bounds": Tuple[Float, Float, Float, Float],
        "bounds_crs": Str,
        "shape": Tuple[Int, Int],
        "arr_shape": Tuple[Int, Int],
        "gdal_geotrans": Tuple[
            Float, Float, Float, Float, Float, Float
        ],  # 'a', 'b', 'c', 'd', 'e', 'f'
        "projected_bounds": Tuple[Float, Float, Float, Float],
    }
]


@serializable(is_named_concrete_type=True)
class GeoContext(GeoContextBase):
    """
    Proxy `.scenes.geocontext.GeoContext` containing the spatial parameters (AOI, resolution, etc.)
    to use when loading geospatial data. Equivalent to a `.scenes.geocontext.AOI`,
    with the additional read-only properties ``arr_shape``, ``gdal_geotrans``, and ``projected_bounds``.

    You don't often need to construct a Workflows GeoContext yourself. When you call compute(),
    you can pass in any `.scenes.geocontext.GeoContext`, or use
    `wf.map.geocontext() <.interactive.Map.geocontext>` for the current map viewport.

    Note: The ``raster_params`` of a GeoContext can be passed to `.raster.ndarray` to get an
    equivalent array.

    Examples
    --------
    >>> from descarteslabs.workflows import GeoContext
    >>> from descarteslabs import scenes
    >>> scene = scenes.DLTile.from_latlon(10, 30, resolution=10, tilesize=512, pad=0)
    >>> # the above scene could be passed to compute without being changed to a Workflows GeoContext
    >>> geoctx = GeoContext.from_scenes(scene)
    >>> geoctx
    <descarteslabs.workflows.types.geospatial.geocontext.GeoContext object at 0x...>
    >>> geoctx.compute() # doctest: +SKIP
    {'geometry': {'type': 'Polygon',
      'coordinates': (((29.964809031113013, 9.990748782946097),
        (30.011460043000678, 9.991170922969406),
        (30.011036444452188, 10.03741571582387),
        (29.964378844777645, 10.036991583007385),
        (29.964809031113013, 9.990748782946097)),)},
     'key': '512:0:10.0:36:-65:216',
     'resolution': 10.0,
     'tilesize': 512,
     'pad': 0,
     'crs': 'EPSG:32636',
     'bounds': (167200.0, 1105920.0, 172320.0, 1111040.0),
     'bounds_crs': 'EPSG:32636',
     'zone': 36,
     'ti': -65,
     'tj': 216,
     'proj4': '+proj=utm +zone=36 +datum=WGS84 +units=m +no_defs ',
     'wkt': 'PROJCS["WGS 84 / UTM zone 36N",GEOGCS["WGS 84",
             DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,
             AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],
             PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],
             UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],
             AUTHORITY["EPSG","4326"]],PROJECTION["Transverse_Mercator"],
             PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",33],
             PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],
             PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],
             AXIS["Easting",EAST],AXIS["Northing",NORTH],AUTHORITY["EPSG","32636"]]',
     'projected_bounds': (167200.0, 1105920.0, 172320.0, 1111040.0),
     'arr_shape': (512, 512),
     'align_pixels': False,
     'geotrans': (10.0, 0.0, 167200.0, 0.0, -10.0, 1111040.0, 0.0, 0.0, 1.0),
     'gdal_geotrans': (167200.0, 10.0, 0.0, 1111040.0, 0.0, -10.0),
     'raster_params': {'dltile': '512:0:10.0:36:-65:216', 'align_pixels': False}}

    >>> from descarteslabs.workflows import GeoContext
    >>> geoctx = GeoContext.from_dltile_key('512:0:10.0:36:-65:216')
    >>> geoctx
    <descarteslabs.workflows.types.geospatial.geocontext.GeoContext object at 0x...>
    >>> geoctx.compute() # doctest: +SKIP
    {'geometry': {'type': 'Polygon',
                  'coordinates': (((29.964809031113013, 9.990748782946097),
                                   (30.011460043000678, 9.991170922969406),
                                   (30.011036444452188, 10.03741571582387),
                                   (29.964378844777645, 10.036991583007385),
                                   (29.964809031113013, 9.990748782946097)),)},
     'key': '512:0:10.0:36:-65:216',
     'resolution': 10.0,
     'tilesize': 512,
    ...

    >>> from descarteslabs.workflows import GeoContext
    >>> geoctx = GeoContext.from_xyz_tile(1, 2, 3)
    >>> geoctx
    <descarteslabs.workflows.types.geospatial.geocontext.GeoContext object at 0x...>
    >>> geoctx.compute() # doctest: +SKIP
    {'x': 1,
     'y': 2,
     'z': 3,
     'geometry': {'type': 'Polygon',
     'coordinates': (((-90.0, 40.97989806962013),
                      (-90.0, 66.51326044311186),
                      (-135.0, 66.51326044311186),
                      (-135.0, 40.97989806962013),
                      (-90.0, 40.97989806962013)),)},
     'tilesize': 256,
     'crs': 'EPSG:3857',
    ...
    """

    _constructor = "wf.GeoContext.create"
    _optional = {
        "geometry",
        "resolution",
        "crs",
        "align_pixels",
        "bounds",
        "bounds_crs",
        "shape",
    }
    _read_only = {"arr_shape", "gdal_geotrans", "projected_bounds"}

    _doc = {
        "geometry": """\
            Clip data to this `Geometry` (like a cutline).

            Coordinates must be WGS84 (lat-lon).
            If ``None``, data will just be clipped to `bounds`.
            """,
        "resolution": """\
            Distance, in units of the `crs`, that the edge of each pixel represents on the ground.
            """,
        "crs": """\
            Coordinate reference system into which data will be projected,
            expressed as an EPSG code (like ``EPSG:4326``) or a PROJ.4 definition.
            """,
        "align_pixels": """\
            Snap the `bounds` to whole-number intervals of ``resolution``, ensuring non-fractional pixels.

            Imagine the bounds overlayed on on a grid of ``resolution`` (say, 30m) intervals.
            ``align_pixels`` expands the bounds outward to the next grid lines.
            """,
        "bounds": """\
            Clip data to these ``(min_x, min_y, max_x, max_y)`` bounds,
            expressed in the coordinate reference system in `bounds_crs`.

            If `bounds_crs` and `crs` differ, the actual bounds will be the envelope
            of the rectangle defined by `bounds`, when reprojected into `crs`.
            """,
        "bounds_crs": """\
            The coordinate reference system of the `bounds`,
            expressed as an EPSG code (like ``EPSG:4326``) or a PROJ.4 definition.
            """,
        "shape": """\
            The dimensions (rows, columns), in pixels, to fit the output array within.
            """,
        "arr_shape": """\
            ``(height, width)`` (i.e. ``(rows, cols)``) of the array this `GeoContext` will produce.

            This derived property (computed from `projected_bounds`, `resolution`, and `align_pixels`)
            cannot be set in ``__init__``, but you can call `compute` on it
            (useful for uploading to `.Catalog`).
            """,
        "gdal_geotrans": """\
            The 6-element GDAL geotrans this `GeoContext` will use.

            This tuple is in the form ``(a, b, c, d, e, f)``, where:

            * ``a``: top left pixel's x-coordinate
            * ``b``: west-east pixel resolution
            * ``c``: row rotation; always 0 for `GeoContext`
            * ``d``: top left pixel's y-coordinate
            * ``e``: column rotation; always 0 for `GeoContext`
            * ``f``: north-south pixel resolution, always a negative value

            This derived property (computed from `projected_bounds`, `resolution`, and `align_pixels`)
            cannot be set in ``__init__``, but you can call `compute` on it
            (useful for uploading to `.Catalog`).
            """,
        "projected_bounds": """\
            The actual bounds (in units of `crs`), if the `bounds_crs` convenience is used.

            This is the *envelope* of the four corners defined by `bounds`,
            when those corners are reprojected from `bounds_crs` into `crs`.

            This derived property cannot be set in ``__init__``, but you can call `compute` on it
            (useful for uploading to `.Catalog`).
            """,
    }

    def __init__(
        self,
        geometry=None,
        resolution=None,
        crs=None,
        align_pixels=True,
        bounds=None,
        bounds_crs="EPSG:4326",
        shape=None,
    ):
        return super(GeoContext, self).__init__(
            geometry=geometry,
            resolution=resolution,
            crs=crs,
            align_pixels=align_pixels,
            bounds=bounds,
            bounds_crs=bounds_crs,
            shape=shape,
        )

    @classmethod
    @typecheck_promote(Str)
    def from_dltile_key(cls, key):
        """
        Construct a Workflows GeoContext from a DLTile key.

        Parameters
        ----------
        key: Str

        Returns
        -------
        ~descarteslabs.workflows.GeoContext
        """
        return cls._from_apply("wf.GeoContext.from_dltile_key", key)

    @classmethod
    @typecheck_promote(Int, Int, Int)
    def from_xyz_tile(cls, x, y, z):
        """
        Construct a Workflows GeoContext for an XYZ tile in the OpenStreetMap tiling scheme.

        Parameters
        ----------
        x: Int
        y: Int
        z: Int

        Returns
        -------
        ~descarteslabs.workflows.GeoContext
        """
        return cls._from_apply("wf.GeoContext.from_xyz_tile", x, y, z)

    @classmethod
    def from_scenes(cls, ctx):
        """
        Construct a Workflows GeoContext from a Scenes GeoContext

        Parameters
        ----------
        ctx: ~descarteslabs.scenes.AOI, ~descarteslabs.scenes.DLTile, or ~descarteslabs.scenes.XYZTile

        Returns
        -------
        ~descarteslabs.workflows.GeoContext
        """
        if isinstance(ctx, scenes.AOI):
            resolution = float(ctx.resolution) if ctx.resolution else None
            # ^ often given as an int, but we're stricter here

            return cls(
                geometry=ctx.geometry,
                resolution=resolution,
                shape=ctx.shape,
                crs=ctx.crs,
                align_pixels=ctx.align_pixels,
                bounds=ctx.bounds,
                bounds_crs=ctx.bounds_crs,
            )
        elif isinstance(ctx, scenes.DLTile):
            return cls.from_dltile_key(ctx.key)
        elif isinstance(ctx, scenes.XYZTile):
            return cls.from_xyz_tile(ctx.x, ctx.y, ctx.z)
        else:
            raise TypeError(
                "In GeoContext.from_scenes, expected a `descarteslabs.scenes.GeoContext` "
                "but got {}".format(ctx)
            )

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, scenes.GeoContext):
            return cls.from_scenes(obj)
        else:
            return super(GeoContext, cls)._promote(obj)

    @typecheck_promote(Int, Int)
    def index_to_coords(self, row, col):
        """
        Convert pixel coordinates (row, col) to spatial coordinates (x, y) in the GeoContext's CRS.
        """
        return Tuple[Float, Float]._from_apply(
            "wf.GeoContext.index_to_coords", self, row, col
        )

    @typecheck_promote(Float, Float)
    def coords_to_index(self, x, y):
        """
        Convert spatial coordinates (x, y) in the GeoContext's CRS to pixel coordinates (row, col).
        """
        return Tuple[Int, Int]._from_apply("wf.GeoContext.coords_to_index", self, x, y)
