from collections import defaultdict
from shapely.geometry.base import BaseGeometry

import ibis.expr.operations as ops
import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import ibis.expr.types as ir
import ibis.expr.window as win
import ibis.util as util

# in order to support both a pinned master commit of ibis-framework
# and the official 1.4.0 tag we need some conditional imports due to
# refactors done between the pinned commit and the 1.4.0 release
# TODO: refactor so that we no longer have to rely on pinned commit:
# b46c60bbb4ee5e9b809a9c83cf4da79a0856b025
try:
    # 1.4.0
    import ibis.backends.base_sqlalchemy.compiler as comp

    adapt_expr = comp._adapt_expr
except ImportError:
    # pinned
    import ibis.backends.base_sql.compiler as comp

    adapt_expr = comp.SelectBuilder._adapt_expr

import pyarrow as pa

from ...proto.ibis import ibis_pb2

COERCES_LITERAL = {set: frozenset, tuple: list}
SUPPORTED_REPEATED_ITEMS = {
    int: ("int_list", ibis_pb2.IntList),
    bool: ("bool_list", ibis_pb2.BoolList),
    float: ("double_list", ibis_pb2.DoubleList),
    str: ("string_list", ibis_pb2.StringList),
}


def make_schema_type(dtype):
    """Helper function to construc a SchemaType message from an ibis datatype."""
    if isinstance(dtype, dt.Struct):
        return ibis_pb2.SchemaType(
            type=dtype.name,
            struct=ibis_pb2.SchemaValue(
                names=dtype.names,
                types=[make_schema_type(t) for t in dtype.pairs.values()],
            ),
        )
    elif isinstance(dtype, dt.Array):
        return ibis_pb2.SchemaType(
            type=dtype.name,
            array=ibis_pb2.ArraySchemaValue(
                value_type=make_schema_type(dtype.value_type)
            ),
        )
    elif isinstance(dtype, dt.GeoSpatial):
        return ibis_pb2.SchemaType(
            type=dtype.name,
            geospatial=ibis_pb2.GeospatialSchemaValue(
                geotype=dtype.geotype, srid=dtype.srid
            ),
        )
    elif isinstance(dtype, dt.Decimal):
        return ibis_pb2.SchemaType(
            type=dtype.name,
            decimal=ibis_pb2.DecimalSchemaValue(
                precision=dtype.precision, scale=dtype.scale
            ),
        )
    else:
        return ibis_pb2.SchemaType(type=dtype.name)


def make_schema(value):
    """Helper function to construct a Schema message from an ibis schema."""
    return ibis_pb2.Schema(
        type="schema",
        value=ibis_pb2.SchemaValue(
            names=value.names,
            types=[make_schema_type(t) for t in value.types],
        ),
    )


def make_window_param(value):
    if value is None:
        return None
    elif isinstance(value, int):
        return ibis_pb2.WindowParam(offset=value)
    elif isinstance(value, (list, tuple)):
        wt = ibis_pb2.WindowTuple()
        if value[0] is not None:
            wt.start_value = value[0]
        if value[1] is not None:
            wt.end_value = value[1]
        return ibis_pb2.WindowParam(tuple=wt)
    elif isinstance(value, ibis_pb2.Expr):
        return ibis_pb2.WindowParam(expr=value)
    elif isinstance(value, ibis_pb2.Literal):
        return ibis_pb2.WindowParam(literal=value)
    else:
        raise TypeError(f"Unsupported window parameter type {type(value)}")


def make_window(
    group_by=None,
    order_by=None,
    preceding=None,
    following=None,
    max_lookback=None,
    how=None,
):
    """Helper function to construct a Window message from a Window object."""
    return ibis_pb2.Window(
        type="window",
        value=ibis_pb2.WindowValue(
            group_by=group_by or [],
            order_by=order_by or [],
            preceding=make_window_param(preceding),
            following=make_window_param(following),
            max_lookback=make_window_param(max_lookback),
            how=how,
        ),
    )


def make_primitive(value):
    """Helper function to construct a Primitive message."""
    if type(value) is bool:
        return ibis_pb2.Primitive(bool=value)
    elif type(value) is int:
        if value >= -(2 ** 63) and value < 2 ** 63:
            return ibis_pb2.Primitive(int=value)
        elif value > 2 ** 63 and value < 2 ** 64:
            return ibis_pb2.Primitive(uint=value)
        else:
            raise ValueError(f"Integer value {value} outside of supported range")
    elif type(value) is float:
        return ibis_pb2.Primitive(double=value)
    elif type(value) is str:
        return ibis_pb2.Primitive(string=value)
    else:
        raise TypeError(f"Unsupported primitive type {type(value)}")


def make_literal(dtype, value):
    """Helper function to construct a Literal message."""
    if type(value) is bool:
        return ibis_pb2.Literal(opname="Literal", dtype=dtype, bool=value)
    elif type(value) is int:
        return ibis_pb2.Literal(opname="Literal", dtype=dtype, int=value)
    elif type(value) is float:
        return ibis_pb2.Literal(opname="Literal", dtype=dtype, double=value)
    elif type(value) is str:
        return ibis_pb2.Literal(opname="Literal", dtype=dtype, string=value)
    elif type(value) == ibis_pb2.Geometry:
        return ibis_pb2.Literal(opname="Literal", dtype=dtype, geometry=value)
    elif isinstance(value, BaseGeometry):
        geometry = ibis_pb2.Geometry(
            wkt=value.wkt,
            crs=getattr(value, "crs", None),
        )
        return ibis_pb2.Literal(opname="Literal", dtype=dtype, geometry=geometry)
    elif isinstance(value, (list, tuple, set, frozenset)):
        # Ibis handles validating that values match the expected type
        item_type = type(next(iter(value)))

        # coerce the value to a python type that ibis handles gracefully
        value_type = type(value)
        coerce_ = COERCES_LITERAL.get(value_type, None)
        if coerce_ is not None:
            value = coerce_(value)

        try:
            field_name, proto_type = SUPPORTED_REPEATED_ITEMS.get(item_type, None)
        except KeyError:
            raise TypeError(f"Unsupported primitive type for sequence {type(value)}")

        new_value = proto_type(value=value)
        return ibis_pb2.Literal(
            opname="Literal", dtype=dtype, **{field_name: new_value}
        )
    else:
        raise TypeError(f"Unsupported primitive type {type(value)}")


def make_map_value(value):
    """Helper to wrap a value as a MapValue."""
    if isinstance(value, (list, tuple)):
        values = [make_map_value(v) for v in value]
        return ibis_pb2.MapValue(list=ibis_pb2.MapValueList(values=values))
    elif isinstance(value, ibis_pb2.Expr):
        return ibis_pb2.MapValue(expr=value)
    elif isinstance(value, ibis_pb2.Literal):
        return ibis_pb2.MapValue(literal=value)
    elif isinstance(value, ibis_pb2.Schema):
        return ibis_pb2.MapValue(schema=value)
    elif isinstance(value, ibis_pb2.Window):
        return ibis_pb2.MapValue(window=value)
    elif isinstance(value, ibis_pb2.Primitive):
        return ibis_pb2.MapValue(primitive=value)
    # and just to be helpful!
    elif isinstance(value, (bool, int, float, str)):
        return ibis_pb2.MapValue(primitive=make_primitive(value))
    else:
        raise TypeError(f"Unsupported map value type {type(value)}")


def make_expr(opname=None, type=None, alias=None, name=None, value=None):
    """Helper function to construct an Expr message.

    The value dict must contain values which can be wrapped as an ibis_pb2.MapValue.
    """
    if value is None:
        value = {}
    kwargs = {}
    for k, v in value.items():
        kwargs[k] = make_map_value(v)
    return ibis_pb2.Expr(opname=opname, value=kwargs, type=type, alias=alias, name=name)


def make_query(expr, table_refs=None):
    """Helper function to construct a Query message."""
    return ibis_pb2.Query(
        type="query", value=ibis_pb2.QueryValue(expr=expr, table_refs=table_refs)
    )


def serialize_record_batch(batch):
    """Generate bytes to stream from a pyarrow RecordBatch."""
    sink = pa.BufferOutputStream()
    writer = pa.ipc.RecordBatchStreamWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    # this mimics what we do in the vektorius service
    return sink.getvalue().to_pybytes()


class FormatMemo:
    """Adaptation of `ibis.expr.format.FormatMemo`.

    Allows for custom formatting of all memoized items.
    """

    def __init__(self):

        self.formatted = {}
        self.aliases = {}
        self.ops = {}
        self.counts = defaultdict(int)
        self._repr_memo = {}
        self.subexprs = {}
        self.visit_memo = set()

    def __contains__(self, obj):
        return obj in self.formatted

    def observe(self, expr, formatter):
        if expr not in self.formatted:
            # encode the list index value with + 1 so that it can
            # be distinguished from the protobuf default value of 0.
            # this is matched by a corresponding - 1 in the
            # deserialization code.
            self.aliases[expr] = len(self.aliases) + 1
            self.formatted[expr] = formatter(expr)
            self.ops[expr] = expr.op()

        self.counts[expr] += 1

    def count(self, expr):
        return self.counts[expr]

    def get_alias(self, expr):
        return self.aliases[expr]

    def get_formatted(self, expr, as_table_ref=False):
        formatted = self.formatted[expr]
        if as_table_ref:
            # remove any alias from the top level Expr
            return ibis_pb2.Expr(
                opname=formatted.opname,
                type=formatted.type,
                name=formatted.name,
                value=formatted.value,
            )
        else:
            return formatted


class AstSerializer:
    __slots__ = ("expr", "is_query", "is_top_level", "_table_refs_memo", "_seen")

    def __init__(self, expr, is_query=True, _table_refs_memo=None):
        self.expr = expr
        self.is_query = is_query
        self.is_top_level = _table_refs_memo is None and self.is_query

        if self.is_top_level:
            # only meaningful at the top level
            self._seen = set()
            self._table_refs_memo = FormatMemo()
            self._memoize_tables()
        else:
            self._table_refs_memo = _table_refs_memo

    @property
    def table_refs(self):
        """Formatted table refs."""
        memo = self._table_refs_memo
        return [
            memo.get_formatted(k, as_table_ref=True)
            for k, _ in sorted(memo.aliases.items(), key=lambda i: i[1])
        ]

    def _memoize_tables(self, expr=None):
        """Find all the table ops and memoize them in _table_refs_memo.

        Since `AstSerializer` is really just a special case of
        `ibis.expr.format.ExprFormatter` this code is copied
        directly from that class's `_memoize_tables` method.
        """
        # initial call uses top level expression
        # recursive calls use the passed in expression
        if expr is None:
            expr = self.expr

        if not hasattr(expr, "op"):
            return

        op = expr.op()
        if op in self._table_refs_memo.visit_memo:
            return

        seen = self._seen
        memo = self._table_refs_memo

        if op in memo.visit_memo or op in seen:
            return

        table_memo_ops = (ops.Aggregation, ops.Selection, ops.SelfReference)
        if isinstance(op, ops.PhysicalTable):
            memo.observe(expr, self._format_table_memo)
        elif isinstance(op, ops.Node):
            # force visiting args for the node prior
            # to trying to formatting this expr
            for arg in reversed(op.args):
                if isinstance(arg, ir.Expr):
                    self._memoize_tables(arg)

            # only catalog this node if it's one of a known set of table args
            # and it's not the top level operation.  This is an optimization
            # to keep `table_refs` small, since top level nodes are, by definition
            # not "refed" by anything.
            if isinstance(op, table_memo_ops) and not self.is_top_level:
                memo.observe(expr, self._format_node)
        elif isinstance(op, ops.TableNode) and op.has_schema():
            memo.observe(expr, self._format_table_memo)
        memo.visit_memo.add(op)

    def _format_table_memo(self, expr):
        table = expr.op()
        opname = type(table).__name__
        type_ = self.expr._type_display()

        alias = self._table_refs_memo.get_alias(expr)
        name = None
        if hasattr(expr, "get_name"):
            name = expr.get_name()
            if name == table.name:
                name = None

        schema = self._format_schema(expr.schema())
        return make_expr(
            opname=opname,
            type=type_,
            alias=alias,
            name=name,
            value=dict(name=table.name, schema=schema),
        )

    def _format_table(self, expr):
        return self._table_refs_memo.get_formatted(expr)

    def _format_column(self, expr):
        col = expr.op()
        parent = col.parent()

        if parent not in self._table_refs_memo:
            self._table_refs_memo.observe(parent, formatter=self._format_node)

        table_formatted = self._table_refs_memo.get_alias(parent)

        opname = type(col).__name__
        type_ = expr._type_display()
        name = expr.get_name()
        if name == col.name:
            name = None
        return make_expr(
            opname=opname,
            type=type_,
            name=name,
            value=dict(name=col.name, table=table_formatted),
        )

    def _format_literal(self, expr):
        return make_literal(expr._type_display(), expr.op().value)

    def _format_node(self, expr):
        """Format a node of the AST.

        Some node types are actually aggregates of other expressions (think
        SELECT statements that use subqueries in the FROM clause). In this
        case we need to recurse through the expression tree to compose the
        sub-elements.
        """
        op = expr.op()
        opname = type(op).__name__
        type_ = expr._type_display()
        formatted_args = {}

        arg_names = op.argnames

        if not arg_names:
            for arg in op.flat_args():
                subexpr = self._format_subexpr(arg)
                formatted_args["unnamed"].append(subexpr)
        else:
            signature = op.signature
            arg_name_pairs = [
                (arg, name)
                for arg, name in zip(op.args, arg_names)
                if signature[name].show
            ]
            for arg, name in arg_name_pairs:
                if util.is_iterable(arg):
                    for x in arg:
                        subexpr = self._format_subexpr(x)
                        formatted_args.setdefault(name, []).append(subexpr)
                elif arg is not None:
                    subexpr = self._format_subexpr(arg)
                    if name in formatted_args:
                        raise ValueError(f"unexpected duplicate expr arg {name}")
                    formatted_args[name] = subexpr

        name = getattr(expr, "_name", None)
        try:
            alias = self._table_refs_memo.get_alias(expr)
        except KeyError:
            alias = None
        return make_expr(
            opname=opname, type=type_, name=name, alias=alias, value=formatted_args
        )

    def _format_window(self, window):
        # Window is not an operation but we serialize it like one
        group_by = [self._format_subexpr(gb) for gb in window._group_by]
        order_by = [self._format_subexpr(ob) for ob in window._order_by]
        preceding = window.preceding
        if isinstance(preceding, ir.Expr):
            preceding = self._format_subexpr(preceding)
        following = window.following
        if isinstance(following, ir.Expr):
            following = self._format_subexpr(following)
        max_lookback = window.max_lookback
        if isinstance(max_lookback, ir.Expr):
            max_lookback = self._format_subexpr(max_lookback)
        return make_window(
            group_by=group_by,
            order_by=order_by,
            preceding=preceding,
            following=following,
            max_lookback=max_lookback,
            how=window.how,
        )

    def _format_schema(self, schema):
        return make_schema(schema)

    def _format_primitive(self, value):
        # some sort of number or string parameter such as limit() value
        return make_primitive(value)

    def _format_subexpr(self, expr):
        serializer = AstSerializer(
            expr, is_query=self.is_query, _table_refs_memo=self._table_refs_memo
        )
        return serializer.serialize()

    def serialize(self):
        expr = self.expr
        op = expr.op() if hasattr(expr, "op") else None

        # TODO the ordering here is complex because of the inheritance
        # hierarchy, this needs to be exercised much more completely than it is
        if isinstance(op, ops.TableNode) and op.has_schema():
            if isinstance(op, ops.PhysicalTable):
                ast = self._format_table(expr)
            else:
                # if the table isn't physical than we have a subquery
                ast = self._format_node(expr)
        elif isinstance(op, ops.TableColumn):
            ast = self._format_column(expr)
        elif isinstance(op, ops.Literal):
            ast = self._format_literal(expr)
        elif isinstance(expr, win.Window):
            ast = self._format_window(expr)
        elif isinstance(expr, sch.Schema):
            ast = self._format_schema(expr)
        elif isinstance(expr, (int, float, str)):
            ast = self._format_primitive(expr)
        elif isinstance(op, ops.Node):
            ast = self._format_node(expr)
        else:
            raise ValueError(
                f"Unable to format expression type `{type(expr)}` from {expr}"
            )

        return ast


class SerializerExprTranslator:
    _registry = {}
    _rewrites = {}


class SerializerDialect(comp.Dialect):
    translator = SerializerExprTranslator

    serializer_class = AstSerializer


dialect = SerializerDialect
