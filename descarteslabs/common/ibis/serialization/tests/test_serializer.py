import pytest

import shapely.geometry

import ibis.expr.operations as ops
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import ibis.expr.types as ir
import ibis.expr.window as win
from ibis.client import Client

from ....proto.ibis import ibis_pb2
from ..compiler import AstSerializer


# RESIST THE URGE! to use the compiler.make_* functions to construct
# what you expect for results, as those are the functions under test here!


def test_schema_serializer():
    names = [
        "aBoolean",
        "anInt64",
        "aFloat64",
        "aDecimal",
        "aString",
        "aTimestamp",
        "aGeography",
        "anArray",
        "aStruct",
        "aPolygon",
    ]
    ast = ibis_pb2.Schema(
        type="schema",
        value=ibis_pb2.SchemaValue(
            names=names,
            types=[
                ibis_pb2.SchemaType(type="Boolean"),
                ibis_pb2.SchemaType(type="Int64"),
                ibis_pb2.SchemaType(type="Float64"),
                ibis_pb2.SchemaType(
                    type="Decimal",
                    decimal=ibis_pb2.DecimalSchemaValue(precision=38, scale=9),
                ),
                ibis_pb2.SchemaType(type="String"),
                ibis_pb2.SchemaType(type="Timestamp"),
                ibis_pb2.SchemaType(
                    type="Geography",
                    geospatial=ibis_pb2.GeospatialSchemaValue(
                        geotype="geography", srid=4326
                    ),
                ),
                ibis_pb2.SchemaType(
                    type="Array",
                    array=ibis_pb2.ArraySchemaValue(
                        value_type=ibis_pb2.SchemaType(type="Int64")
                    ),
                ),
                ibis_pb2.SchemaType(
                    type="Struct",
                    struct=ibis_pb2.SchemaValue(
                        names=["anInt64", "aGeography"],
                        types=[
                            ibis_pb2.SchemaType(type="Int64"),
                            ibis_pb2.SchemaType(
                                type="Geography",
                                geospatial=ibis_pb2.GeospatialSchemaValue(
                                    geotype="geography", srid=4326
                                ),
                            ),
                        ],
                    ),
                ),
                ibis_pb2.SchemaType(
                    type="Polygon",
                    geospatial=ibis_pb2.GeospatialSchemaValue(srid=4326),
                ),
            ],
        ),
    )

    schema = sch.Schema(
        names=names,
        types=[
            dt.Boolean(),
            dt.Int64(),
            dt.Float64(),
            dt.Decimal(precision=38, scale=9),
            dt.String(),
            dt.Timestamp(),
            dt.Geography(srid=4326),
            dt.Array(value_type=dt.Int64()),
            dt.Struct(
                names=["anInt64", "aGeography"],
                types=[dt.Int64(), dt.Geography(srid=4326)],
            ),
            dt.Polygon(srid=4326),
        ],
    )

    ser = AstSerializer(schema, is_query=False)
    res = ser.serialize()

    assert res == ast


def test_ast_serializer():
    column_names = [
        "area",
        "area_square_m",
        "continent_id",
        "country",
        "country_id",
        "county_id",
        "geom",
        "geomhash",
        "locality_id",
        "parent_id",
        "placetype",
        "region_id",
        "wof_id",
        "wof_name",
    ]
    schema_ast = ibis_pb2.Schema(
        type="schema",
        value=ibis_pb2.SchemaValue(
            names=column_names,
            types=[
                ibis_pb2.SchemaType(type="Float64"),
                ibis_pb2.SchemaType(type="Float64"),
                ibis_pb2.SchemaType(type="Int64"),
                ibis_pb2.SchemaType(type="String"),
                ibis_pb2.SchemaType(type="Int64"),
                ibis_pb2.SchemaType(type="Int64"),
                ibis_pb2.SchemaType(
                    type="Geography",
                    geospatial=ibis_pb2.GeospatialSchemaValue(
                        geotype="geography", srid=4326
                    ),
                ),
                ibis_pb2.SchemaType(type="String"),
                ibis_pb2.SchemaType(type="Int64"),
                ibis_pb2.SchemaType(type="Int64"),
                ibis_pb2.SchemaType(type="String"),
                ibis_pb2.SchemaType(type="Int64"),
                ibis_pb2.SchemaType(type="Int64"),
                ibis_pb2.SchemaType(type="String"),
            ],
        ),
    )
    ast = ibis_pb2.Expr(
        opname="Selection",
        type="table",
        value={
            "predicates": ibis_pb2.MapValue(
                list=ibis_pb2.MapValueList(
                    values=[
                        ibis_pb2.MapValue(
                            expr=ibis_pb2.Expr(
                                opname="Greater",
                                type="boolean*",
                                value={
                                    "left": ibis_pb2.MapValue(
                                        expr=ibis_pb2.Expr(
                                            opname="TableColumn",
                                            type="int64*",
                                            value={
                                                "name": ibis_pb2.MapValue(
                                                    primitive=ibis_pb2.Primitive(
                                                        string="wof_id"
                                                    )
                                                ),
                                                "table": ibis_pb2.MapValue(
                                                    primitive=ibis_pb2.Primitive(int=1)
                                                ),
                                            },
                                        )
                                    ),
                                    "right": ibis_pb2.MapValue(
                                        literal=ibis_pb2.Literal(
                                            opname="Literal",
                                            dtype="int64",
                                            int=100,
                                        )
                                    ),
                                },
                            )
                        )
                    ]
                )
            ),
            "selections": ibis_pb2.MapValue(
                list=ibis_pb2.MapValueList(
                    values=[
                        ibis_pb2.MapValue(
                            expr=ibis_pb2.Expr(
                                opname="TableColumn",
                                type="geography*",
                                value={
                                    "name": ibis_pb2.MapValue(
                                        primitive=ibis_pb2.Primitive(string="geom")
                                    ),
                                    "table": ibis_pb2.MapValue(
                                        primitive=ibis_pb2.Primitive(int=1)
                                    ),
                                },
                            )
                        ),
                        ibis_pb2.MapValue(
                            expr=ibis_pb2.Expr(
                                opname="TableColumn",
                                type="int64*",
                                value={
                                    "name": ibis_pb2.MapValue(
                                        primitive=ibis_pb2.Primitive(string="wof_id")
                                    ),
                                    "table": ibis_pb2.MapValue(
                                        primitive=ibis_pb2.Primitive(int=1)
                                    ),
                                },
                            )
                        ),
                    ]
                )
            ),
            "table": ibis_pb2.MapValue(
                expr=ibis_pb2.Expr(
                    opname="DatabaseTable",
                    type="table",
                    alias=1,
                    value={
                        "name": ibis_pb2.MapValue(
                            primitive=ibis_pb2.Primitive(string="whosonfirst")
                        ),
                        "schema": ibis_pb2.MapValue(
                            schema=schema_ast,
                        ),
                    },
                )
            ),
        },
    )

    table_ref = ibis_pb2.Expr(
        opname="DatabaseTable",
        type="table",
        value={
            "name": ibis_pb2.MapValue(
                primitive=ibis_pb2.Primitive(string="whosonfirst")
            ),
            "schema": ibis_pb2.MapValue(
                schema=schema_ast,
            ),
        },
    )

    schema = sch.Schema(
        names=column_names,
        types=[
            dt.Float64(),
            dt.Float64(),
            dt.Int64(),
            dt.String(),
            dt.Int64(),
            dt.Int64(),
            dt.Geography(srid=4326),
            dt.String(),
            dt.Int64(),
            dt.Int64(),
            dt.String(),
            dt.Int64(),
            dt.Int64(),
            dt.String(),
        ],
    )
    table_op = ops.DatabaseTable("whosonfirst", schema, Client())
    table = ir.TableExpr(table_op)
    proj = table[table.geom, table.wof_id][table.wof_id > 100]
    ser = AstSerializer(proj)
    res = ser.serialize()
    assert ser.table_refs == [table_ref]
    assert ast == res


def test_literal_geometry():
    shape = shapely.geometry.Polygon([[0, 1], [1, 1], [1, 0], [0, 1]])

    literal_expr = ir.literal(shape)
    ser = AstSerializer(literal_expr)

    ast = ibis_pb2.Literal(
        opname="Literal",
        dtype="polygon",
        geometry=ibis_pb2.Geometry(
            wkt="POLYGON ((0 1, 1 1, 1 0, 0 1))",
        ),
    )
    actual = ser.serialize()
    assert ast == actual


def test_literal_geometry_crs():
    shape = shapely.geometry.Polygon([[0, 1], [1, 1], [1, 0], [0, 1]])
    shape.crs = "EPSG:4326"

    literal_expr = ir.literal(shape)
    ser = AstSerializer(literal_expr)

    ast = ibis_pb2.Literal(
        opname="Literal",
        dtype="polygon",
        geometry=ibis_pb2.Geometry(
            wkt="POLYGON ((0 1, 1 1, 1 0, 0 1))", crs="EPSG:4326"
        ),
    )
    actual = ser.serialize()
    assert ast == actual


def test_window():
    window = win.Window(preceding=0, how="rows")

    ser = AstSerializer(window, is_query=False)

    ast = ibis_pb2.Window(
        type="window",
        value=ibis_pb2.WindowValue(
            preceding=ibis_pb2.WindowParam(offset=0), how="rows"
        ),
    )

    actual = ser.serialize()
    assert ast == actual


@pytest.mark.parametrize(
    "lit,dtype,proto_type,field_name",
    [
        ([1, 2, 3, 4], "array<int8>", ibis_pb2.IntList, "int_list"),
        (tuple([1, 2, 3, 4]), "array<int8>", ibis_pb2.IntList, "int_list"),
        (set([1, 2, 3, 4]), "set<int8>", ibis_pb2.IntList, "int_list"),
        (frozenset([1, 2, 3, 4]), "set<int8>", ibis_pb2.IntList, "int_list"),
        # bool
        ([True, False], "array<boolean>", ibis_pb2.BoolList, "bool_list"),
        (tuple([True, False]), "array<boolean>", ibis_pb2.BoolList, "bool_list"),
        (set([True, False]), "set<boolean>", ibis_pb2.BoolList, "bool_list"),
        (frozenset([True, False]), "set<boolean>", ibis_pb2.BoolList, "bool_list"),
        # float
        ([1.0, 2.0, 3.0, 4.0], "array<float64>", ibis_pb2.DoubleList, "double_list"),
        (
            tuple([1.0, 2.0, 3.0, 4.0]),
            "array<float64>",
            ibis_pb2.DoubleList,
            "double_list",
        ),
        (set([1.0, 2.0, 3.0, 4.0]), "set<float64>", ibis_pb2.DoubleList, "double_list"),
        (
            frozenset([1.0, 2.0, 3.0, 4.0]),
            "set<float64>",
            ibis_pb2.DoubleList,
            "double_list",
        ),
        # str
        (["a", "b", "c"], "array<string>", ibis_pb2.StringList, "string_list"),
        (tuple(["a", "b", "c"]), "array<string>", ibis_pb2.StringList, "string_list"),
        (set(["a", "b", "c"]), "set<string>", ibis_pb2.StringList, "string_list"),
        (frozenset(["a", "b", "c"]), "set<string>", ibis_pb2.StringList, "string_list"),
    ],
)
def test_literal_list(lit, dtype, proto_type, field_name):
    # normally this isn't called with a type, but tuple can't be inferred automatically
    # but can be specified as a type. Bypass inference to ensure coverage of possible types
    literal_expr = ir.literal(lit, type=dtype)
    ser = AstSerializer(literal_expr, is_query=False)

    ast = ibis_pb2.Literal(
        opname="Literal", dtype=dtype, **{field_name: proto_type(value=lit)}
    )
    actual = ser.serialize()
    assert ast == actual
