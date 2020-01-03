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

    Note: You don't often need to construct a Workflows GeoContext yourself. When you call compute(),
    you can pass in any `.scenes.geocontext.GeoContext`, or use
    `wf.map.geocontext() <.interactive.Map.geocontext>` for the current map viewport.
    """

    _constructor = "GeoContext.create"
    _optional = {
        "geometry",
        "resolution",
        "crs",
        "align_pixels",
        "bounds",
        "bounds_crs",
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
        return cls._from_apply("GeoContext.from_dltile_key", key)

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
        return cls._from_apply("GeoContext.from_xyz_tile", x, y, z)

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
            if ctx.shape is not None:
                raise ValueError("AOI shape is not supported.")
            return cls(
                geometry=ctx.geometry,
                resolution=float(ctx.resolution),
                # ^ often given as an int, but we're stricter here
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
