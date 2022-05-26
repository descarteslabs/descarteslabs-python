import pytest

import shapely.geometry

import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import ibis.expr.window as win
from ibis.client import Client

from ....proto.ibis import ibis_pb2

from ...serialization.compiler import (
    make_schema,
    make_window,
    make_primitive,
    make_literal,
    make_expr,
    make_query,
)

from ..compiler import (
    all_equal,
    Deserializer,
    LiteralDeserializer,
    SchemaDeserializer,
    WindowDeserializer,
    DatabaseTableDeserializer,
    TableColumnDeserializer,
    QueryDeserializer,
    AstDeserializer,
)
from ..exceptions import (
    LiteralDeserializationError,
    OperationError,
    DataTypeError,
)


def test_expr():
    ast = make_expr(opname="Negate", value={"arg": make_literal("int32", 1)})

    dsr = Deserializer(ast)
    res = dsr.deserialize()

    assert isinstance(res, ir.IntegerScalar)
    assert isinstance(res.op(), ops.Negate)

    # test via AstDeserializer
    dsr = AstDeserializer(ast)
    res = dsr.deserialize()

    assert isinstance(res, ir.IntegerScalar)
    assert isinstance(res.op(), ops.Negate)


def test_no_type():
    ast = make_expr()

    dsr = AstDeserializer(ast)
    with pytest.raises(OperationError):
        dsr.deserialize()


def test_bad_operation():
    ast = make_expr(opname="Foo")

    with pytest.raises(OperationError):
        Deserializer(ast)


def test_bad_operation_astdeserializer():
    ast = make_expr(opname="Foo")

    dsr = AstDeserializer(ast)
    with pytest.raises(OperationError):
        dsr.deserialize()


def test_literal_bool():
    ast = make_literal("boolean", True)
    dsr = LiteralDeserializer(ast)
    res = dsr.deserialize()

    assert res.op().value is True

    # test via AstDeserializer
    dsr = AstDeserializer(ast)
    res = dsr.deserialize()

    assert res.op().value is True


def test_literal_int():
    ast = make_literal("int32", -1)
    dsr = LiteralDeserializer(ast)
    res = dsr.deserialize()

    assert res.op().value is -1


def test_literal_double():
    ast = make_literal("double", 1.23)
    dsr = LiteralDeserializer(ast)
    res = dsr.deserialize()

    assert res.op().value == 1.23


def test_literal_string():
    ast = make_literal("string", "foo")
    dsr = LiteralDeserializer(ast)
    res = dsr.deserialize()

    assert res.op().value == "foo"


def test_literal_geospatial():
    ast = make_literal(
        "polygon",
        shapely.geometry.shape(
            {"type": "Polygon", "coordinates": [[[0, 1], [1, 1], [1, 0], [0, 1]]]}
        ),
    )

    dsr = LiteralDeserializer(ast)
    res = dsr.deserialize()

    op = res.op()
    dtype = op.dtype
    val = op.value
    assert isinstance(val, shapely.geometry.Polygon)
    assert "POLYGON ((0 1, 1 1, 1 0, 0 1))" == val.wkt
    assert dtype.srid is None


def test_literal_geospatial_with_srid():
    ast = make_literal(
        "polygon",
        ibis_pb2.Geometry(wkt="POLYGON ((0 1, 1 1, 1 0, 0 1))", crs="EPSG:4326"),
    )

    dsr = LiteralDeserializer(ast)
    res = dsr.deserialize()

    op = res.op()
    dtype = op.dtype
    val = op.value
    assert isinstance(val, shapely.geometry.Polygon)
    assert "POLYGON ((0 1, 1 1, 1 0, 0 1))" == val.wkt
    assert 4326 == dtype.srid


def test_literal_geospatial_bad_geometry_type():
    ast = make_literal(
        "polygon",
        ibis_pb2.Geometry(
            wkt="""GEOMETRYCOLLECTION (POINT (40 10),
                        LINESTRING (10 10, 20 20, 10 40),
                        POLYGON ((40 40, 20 45, 45 30, 40 40)))""",
        ),
    )

    dsr = LiteralDeserializer(ast)
    with pytest.raises(LiteralDeserializationError):
        dsr.deserialize()


def test_literal_geospatial_bad_wkt():
    ast = make_literal(
        "polygon",
        ibis_pb2.Geometry(
            wkt="DODECAHEDRON",
        ),
    )

    dsr = LiteralDeserializer(ast)
    with pytest.raises(LiteralDeserializationError):
        dsr.deserialize()


def test_literal_good_cast():
    ast = make_literal("double", 10)
    dsr = LiteralDeserializer(ast)
    res = dsr.deserialize()

    op = res.op()
    assert op.value == 10
    assert op.dtype == dt.float64


def test_literal_bad_cast():
    ast = make_literal("int32", "10")
    dsr = LiteralDeserializer(ast)
    with pytest.raises(LiteralDeserializationError):
        dsr.deserialize()


@pytest.mark.parametrize(
    "lit,dtype,result_type",
    [
        ([1, 2, 3, 4], "array<int8>", list),
        (tuple([1, 2, 3, 4]), "array<int8>", list),
        (set([1, 2, 3, 4]), "set<int8>", frozenset),
        (frozenset([1, 2, 3, 4]), "set<int8>", frozenset),
        # bool
        ([True, False], "array<boolean>", list),
        (tuple([True, False]), "array<boolean>", list),
        (set([True, False]), "set<boolean>", frozenset),
        (frozenset([True, False]), "set<boolean>", frozenset),
        # float
        ([1.0, 2.0, 3.0, 4.0], "array<float64>", list),
        (tuple([1.0, 2.0, 3.0, 4.0]), "array<float64>", list),
        (set([1.0, 2.0, 3.0, 4.0]), "set<float64>", frozenset),
        (frozenset([1.0, 2.0, 3.0, 4.0]), "set<float64>", frozenset),
        # str
        (["a", "b", "c"], "array<string>", list),
        (tuple(["a", "b", "c"]), "array<string>", list),
        (set(["a", "b", "c"]), "set<string>", frozenset),
        (frozenset(["a", "b", "c"]), "set<string>", frozenset),
    ],
)
def test_literal_list(lit, dtype, result_type):
    ast = make_literal(dtype, lit)
    dsr = LiteralDeserializer(ast)
    res = dsr.deserialize()
    op = res.op()
    # tuple/sets become list/frozenlist
    assert op.value == result_type(lit)
    assert res._type_display() == dtype


def test_bad_datatype():
    ast = make_expr(type="foo")

    with pytest.raises(DataTypeError):
        Deserializer(ast)


def test_schema():
    schema = sch.Schema(
        names=[
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
        ],
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

    ast = make_schema(schema)

    dsr = SchemaDeserializer(ast)
    res = dsr.deserialize()

    assert res == schema

    # test via AstDeserializer
    dsr = AstDeserializer(ast)
    res = dsr.deserialize()

    assert res == schema


def test_empty_schema():
    schema = sch.Schema(names=[], types=[])

    ast = make_schema(schema)

    dsr = SchemaDeserializer(ast)
    res = dsr.deserialize()

    assert res == schema


def test_table():
    schema = sch.Schema(
        names=["id", "val"],
        types=[dt.String(), dt.Int64()],
    )
    ast = make_expr(
        opname="DatabaseTable",
        value={"name": make_primitive("sometable"), "schema": make_schema(schema)},
    )

    dsr = DatabaseTableDeserializer(ast)
    res = dsr.deserialize()

    assert isinstance(res, ir.TableExpr)
    assert res.schema() == schema

    # test via AstDeserializer
    dsr = AstDeserializer(ast)
    res = dsr.deserialize()

    assert isinstance(res, ir.TableExpr)
    assert res.schema() == schema


def test_table_with_ref():
    schema = sch.Schema(
        names=["id", "val"],
        types=[dt.String(), dt.Int64()],
    )
    table = ops.DatabaseTable(
        name="sometable", schema=schema, source=Client()
    ).to_expr()

    table_refs = [table]

    ast = make_expr(opname="DatabaseTable", alias=1)
    dsr = DatabaseTableDeserializer(ast, table_refs=table_refs)
    res = dsr.deserialize()

    assert isinstance(res, ir.TableExpr)
    assert res.schema() == schema

    # test via AstDeserializer
    dsr = AstDeserializer(ast, table_refs=table_refs)
    res = dsr.deserialize()

    assert isinstance(res, ir.TableExpr)
    assert res.schema() == schema


def test_table_column():
    schema = sch.Schema(
        names=["id", "val"],
        types=[dt.String(), dt.Int64()],
    )

    table_ast = make_expr(
        opname="DatabaseTable",
        value={"name": make_primitive("sometable"), "schema": make_schema(schema)},
    )
    ast = make_expr(
        opname="TableColumn",
        value={"table": table_ast, "name": make_primitive("id")},
    )

    dsr = TableColumnDeserializer(ast)
    res = dsr.deserialize()

    assert isinstance(res, ir.StringColumn)
    assert res.op().name == "id"
    assert isinstance(res.op().table, ir.TableExpr)
    assert res.op().table.schema() == schema

    # test via AstDeserializer
    dsr = AstDeserializer(ast)
    res = dsr.deserialize()

    assert isinstance(res, ir.StringColumn)
    assert res.op().name == "id"
    assert isinstance(res.op().table, ir.TableExpr)
    assert res.op().table.schema() == schema


def test_table_column_mismatch():
    schema = sch.Schema(
        names=["id", "val"],
        types=[dt.String(), dt.Int64()],
    )

    table_ast = make_expr(
        opname="DatabaseTable",
        value={"name": make_primitive("sometable"), "schema": make_schema(schema)},
    )
    ast = make_expr(
        opname="TableColumn",
        value={"table": table_ast, "name": make_primitive("ident")},
    )

    dsr = TableColumnDeserializer(ast)

    with pytest.raises(com.IbisTypeError) as excinfo:
        dsr.deserialize()
    assert "'ident' is not a field" in excinfo.value.args[0]


def test_table_column_with_ref():
    schema = sch.Schema(
        names=["id", "val"],
        types=[dt.String(), dt.Int64()],
    )
    table = ops.DatabaseTable(
        name="sometable", schema=schema, source=Client()
    ).to_expr()

    table_refs = [table]

    ast = make_expr(
        opname="TableColumn",
        value={"table": make_primitive(1), "name": make_primitive("id")},
    )

    dsr = TableColumnDeserializer(ast, table_refs=table_refs)
    res = dsr.deserialize()

    assert isinstance(res, ir.StringColumn)
    assert res.op().name == "id"
    assert isinstance(res.op().table, ir.TableExpr)
    assert res.op().table.schema() == schema

    # test via AstDeserializer
    dsr = AstDeserializer(ast, table_refs=table_refs)
    res = dsr.deserialize()

    assert isinstance(res, ir.StringColumn)
    assert res.op().name == "id"
    assert isinstance(res.op().table, ir.TableExpr)
    assert res.op().table.schema() == schema


def test_table_column_mismatch_with_ref():
    schema = sch.Schema(
        names=["id", "val"],
        types=[dt.String(), dt.Int64()],
    )
    table = ops.DatabaseTable(
        name="sometable", schema=schema, source=Client()
    ).to_expr()

    table_refs = [table]

    ast = make_expr(
        opname="TableColumn",
        value={"table": make_primitive(1), "name": make_primitive("ident")},
    )

    dsr = TableColumnDeserializer(ast, table_refs=table_refs)

    with pytest.raises(com.IbisTypeError) as excinfo:
        dsr.deserialize()
    assert "'ident' is not a field" in excinfo.value.args[0]


def test_window():
    window = win.Window(preceding=0, how="rows")
    ast = make_window(preceding=window.preceding, how=window.how)

    dsr = WindowDeserializer(ast)
    res = dsr.deserialize()

    assert all_equal(res, window)

    # test via AstDeserializer
    dsr = AstDeserializer(ast)
    res = dsr.deserialize()

    assert all_equal(res, window)


def test_query():
    schema = sch.Schema(
        names=["id", "val"],
        types=[dt.String(), dt.Int64()],
    )

    schema_ast = make_schema(schema)
    table_ast = make_expr(
        opname="DatabaseTable",
        value={"name": make_primitive("sometable"), "schema": schema_ast},
    )
    expr_ast = make_expr(
        opname="TableColumn",
        value={"table": make_primitive(1), "name": make_primitive("id")},
    )
    ast = make_query(expr=expr_ast, table_refs=[table_ast])

    dsr = QueryDeserializer(ast)
    res = dsr.deserialize()

    assert isinstance(res, ir.StringColumn)
    assert res.op().name == "id"
    assert isinstance(res.op().table, ir.TableExpr)
    assert res.op().table.schema() == schema

    # test via AstDeserializer
    dsr = AstDeserializer(ast)
    res = dsr.deserialize()

    assert isinstance(res, ir.StringColumn)
    assert res.op().name == "id"
    assert isinstance(res.op().table, ir.TableExpr)
    assert res.op().table.schema() == schema
