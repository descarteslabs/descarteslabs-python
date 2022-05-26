import ibis.backends.base.sql.alchemy as alch
import ibis.backends.postgres.compiler as comp
import sqlalchemy as sa

from . import operations as ops

fixed_arity = alch.registry.fixed_arity


def _translate_args(t, expr):
    return [t.translate(a) for a in expr.op().args]


def _asmvt(t, expr):
    (arg,) = expr.op().args

    ctx = t.context

    sa_table = alch.get_sqla_table(ctx, arg)

    # TODO this sucks, why does sqlalchemy make this construct so difficult?
    return sa.func.ST_AsMVT(sa.sql.literal_column(f"{sa_table.name}.*"))


def _geometry_bbox_intersects(t, expr):
    arg, other = _translate_args(t, expr)

    res = sa.func.ST_Intersects(arg, other)
    return res


def patch_registry():
    comp.PostgreSQLExprTranslator._registry.update(
        {
            ops.AsMVTGeom: fixed_arity(sa.func.ST_AsMVTGeom, 4),
            ops.AsMVT: _asmvt,
            ops.GeometryBboxIntersects: _geometry_bbox_intersects,
            ops.TileEnvelope: fixed_arity(sa.func.ST_TileEnvelope, 3),
            ops.SimplifyPreserveTopology: fixed_arity(
                sa.func.ST_SimplifyPreserveTopology, 2
            ),
        }
    )
