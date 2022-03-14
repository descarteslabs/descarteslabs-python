import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.rules as rlz
import ibis.expr.types as ir
from ibis.expr.signature import Argument as Arg


class AsMVTGeom(ops.ValueOp):
    arg = Arg(rlz.geospatial)
    tile_envelope = Arg(rlz.geospatial)
    extent = Arg(rlz.integer, default=4096)
    buffer = Arg(rlz.integer, default=64)

    output_type = rlz.shape_like("arg", dt.geometry)


class AsMVT(ops.Reduction):
    arg = Arg(ir.TableExpr)

    def output_type(self):
        return dt.string.scalar_type()


class GeometryBboxIntersects(ops.ValueOp):
    arg = Arg(rlz.geospatial)
    other = Arg(rlz.one_of((rlz.column(rlz.geospatial), rlz.geospatial(), rlz.string)))

    output_type = rlz.shape_like("arg", dt.boolean)


class TileEnvelope(ops.Constant):
    z = Arg(rlz.integer)
    x = Arg(rlz.integer)
    y = Arg(rlz.integer)

    def output_type(self):
        return dt.geometry.scalar_type()


class SimplifyPreserveTopology(ops.GeoSpatialUnOp):
    tolerance = Arg(rlz.floating)

    output_type = rlz.shape_like("arg", dt.geometry)
