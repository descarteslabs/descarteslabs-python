from typing import Union
import ibis
import ibis.expr.types as ir
from . import operations as ops

GEOSPATIAL_COLUMNS = (
    ir.GeoSpatialColumn,
    ir.PointColumn,
    ir.LineStringColumn,
    ir.PolygonColumn,
    ir.MultiPointColumn,
    ir.MultiLineStringColumn,
    ir.MultiPolygonColumn,
)

GEOSPATIAL_SCALARS = (
    ir.GeoSpatialScalar,
    ir.PointScalar,
    ir.LineStringScalar,
    ir.PolygonScalar,
    ir.MultiPointScalar,
    ir.MultiLineStringScalar,
    ir.MultiPointScalar,
)

GEO = Union[ir.GeoSpatialColumn, ir.GeoSpatialScalar]


def asmvtgeom(
    arg: ir.GeoSpatialColumn,
    tile_envelope: ir.GeoSpatialScalar,
    extent: int = 4096,
    buffer: int = 64,
):
    """
    Reduce a column to an MVT.
    """
    expr = ops.AsMVTGeom(arg, tile_envelope, extent, buffer).to_expr()
    expr = expr.name("mvtgeom")
    return expr


def asmvt(arg: ir.TableExpr):
    """
    Reduce a query to an MVT.
    """
    expr = ops.AsMVT(arg).to_expr()
    # TODO maybe this is ok, I'm not sure
    expr = expr.name("mvt")
    return expr


def bbox_intersects(arg: GEO, other: GEO) -> ir.BooleanColumn:
    return ops.GeometryBboxIntersects(arg, other).to_expr()


def tile_envelope(z: int, x: int, y: int) -> ir.GeoSpatialScalar:
    # TODO how critical are the optional bounds and margin args?
    return ops.TileEnvelope(z, x, y).to_expr()


def simplify(arg: GEO, tolerance: float) -> GEO:
    return ops.SimplifyPreserveTopology(arg, tolerance).to_expr()


def patch_api():
    # configure new column methods
    for col in GEOSPATIAL_COLUMNS:
        setattr(col, "asmvtgeom", asmvtgeom)
        setattr(col, "bbox_intersects", bbox_intersects)
        setattr(col, "simplify", simplify)

    # configure new scalar methods
    for scal in GEOSPATIAL_SCALARS:
        setattr(scal, "asmvtgeom", asmvtgeom)
        setattr(scal, "bbox_intersects", bbox_intersects)
        setattr(scal, "simplify", simplify)

    # configure new table methods
    setattr(ir.TableExpr, "asmvt", asmvt)

    # configure top level elements
    setattr(ibis, "tile_envelope", tile_envelope)
