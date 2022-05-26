import functools

# ibis has a glaring bug where all Exprs have an equals() method used
# by the all_equal() function, but then an entirely unrelated
# geo_equals() function (defined in ibis.expr.api) is inserted into
# the GeoSpatialValue and GeoSpatialColumn expr types as an equals()
# method which breaks them when used by all_equal(). Sigh...
import itertools

import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import ibis.expr.window as win
import shapely.wkt
from ...proto.ibis import ibis_pb2
from google.protobuf.message import Message
from ibis import util
from ibis.client import Client
from shapely.errors import WKTReadingError
from shapely.geometry import GeometryCollection, LinearRing

from . import exceptions as exc

LITERAL_LIST_LIKE_TYPES = (
    ibis_pb2.IntList,
    ibis_pb2.BoolList,
    ibis_pb2.DoubleList,
    ibis_pb2.StringList,
)


COERCES_LITERAL_LIST_LIKE = {dt.Set: frozenset, dt.Array: list}
# monkeypatch client to avoid bad equality checks
Client.__eq__ = lambda self, other: self is other or type(self) == type(other)


def all_equal(left, right, cache=None):
    """Check whether two objects `left` and `right` are equal.
    Parameters
    ----------
    left : Union[object, Expr, Node]
    right : Union[object, Expr, Node]
    cache : Optional[Dict[Tuple[Node, Node], bool]]
        A dictionary indicating whether two Nodes are equal
    """
    if cache is None:
        cache = {}

    if util.is_iterable(left):
        # check that left and right are equal length iterables and that all
        # of their elements are equal
        return (
            util.is_iterable(right)
            and len(left) == len(right)
            and all(
                itertools.starmap(
                    functools.partial(all_equal, cache=cache), zip(left, right)
                )
            )
        )

    if hasattr(left, "equals"):
        if isinstance(left, ir.GeoSpatialValue):
            return super(ir.GeoSpatialValue, left).equals(right, cache=cache)
        if isinstance(left, ir.GeoSpatialColumn):
            return super(ir.GeoSpatialColumn, left).equals(right, cache=cache)
        else:
            return left.equals(right, cache=cache)

    return left == right


ops.all_equal = all_equal


# Create a registry of deserializers keyed by opname for ops types,
# and datatype class name for all datatypes. Also a few special types
# such as Schema, Window, and top_level.
_DESERIALIZERS = {}


def register(arg):
    if isinstance(arg, type):
        name = arg.__name__
        if name.endswith("Deserializer"):
            name = name[0:-12]
        _DESERIALIZERS[name] = arg
        return arg
    else:
        # parameter provided, assume a string
        def wrap(cls):
            _DESERIALIZERS[arg] = cls
            return cls

        return wrap


def _get_deserializer(name):
    """Get *Deserializer type for name (opname or type name)."""
    return _DESERIALIZERS.get(name, Deserializer)


def get_oneof(message, field):
    """Get the value of the field of a oneof group which is set."""

    which = message.WhichOneof(field)
    if which is None:
        return None
    else:
        return getattr(message, which)


class Deserializer:
    """Base deserializer type.

    Can handle all normal cases for both ops and data types. Only
    a few special things such as tables, columns, etc. need to be overridden.
    """

    def __init__(
        self,
        ast: Message,
        table_refs: list = None,
        seen: dict = None,
    ):
        self.ast = ast
        self.table_refs = table_refs
        self.klass = self._klass()
        self.seen = seen

    @property
    def opname(self):
        return self.ast.opname

    @property
    def type(self):
        return self.ast.type

    @property
    def name(self):
        return self.ast.name

    @property
    def alias(self):
        # note that integer field defaults to 0, so we use 1-based indexing
        return self.ast.alias

    @property
    def value(self):
        return self.ast.value

    def _klass(self):
        if self.opname:
            klass = getattr(ops, self.opname, None)
            if klass is None:
                raise exc.OperationError("Operation not found: {}".format(self.opname))
        elif self.type:
            klass = getattr(dt, self.type, None)
            if klass is None:
                raise exc.DataTypeError("Data type not found: {}".format(self.type))
        else:
            raise exc.OperationError("No opname or type defined in Expr")

        return klass

    def _maybe_deserialize(self, arg):
        """Deserialize any protobuf Message, otherwise return as-is."""
        if isinstance(arg, ibis_pb2.Primitive):
            # extract its native python value
            return get_oneof(arg, "value")
        elif isinstance(arg, ibis_pb2.Geometry):
            try:
                geom = shapely.wkt.loads(arg.wkt)
            except WKTReadingError:
                raise exc.LiteralDeserializationError(
                    "Not a valid shape:{}".format(arg.wkt)
                )
                # TODO this is a hack to communicate the crs back to LiteralDeserializer so it
                # can create the correct dt.Geography type. Perhaps there is a better way?
            geom.crs = arg.crs
            return geom
        elif isinstance(arg, ibis_pb2.WindowTuple):
            return (get_oneof(arg, "start"), get_oneof(arg, "end"))
        elif isinstance(arg, ibis_pb2.MapValueList):
            return [self._maybe_deserialize(get_oneof(a, "value")) for a in arg.values]
        elif isinstance(arg, LITERAL_LIST_LIKE_TYPES):
            return [self._maybe_deserialize(a) for a in arg.value]
        elif isinstance(arg, Message):
            return AstDeserializer(
                arg, table_refs=self.table_refs, seen=self.seen
            ).deserialize()
        else:
            # already deserialized
            return arg

    def deserialize(self):
        """Implements standard ops/datatype deserialization so we don't need to enumerate everything.

        All ops are deserialized to their corresponding Expr, otherwise returns the deserialized
        instance as-is.
        """

        args = {}
        for n in self.value:
            v = self.value[n]
            if type(v) is ibis_pb2.MapValueList:
                args[n] = [
                    self._maybe_deserialize(get_oneof(i, "value")) for i in v.values
                ]
            else:
                args[n] = self._maybe_deserialize(get_oneof(v, "value"))

        value = self.klass(**args)

        if self.opname:
            # handle ops instances
            expr = value.to_expr()
            if self.name:
                expr = expr.name(self.name)
            return expr
        else:
            return value


@register
class CrossJoinDeserializer(Deserializer):
    """Deserializer for CROSS JOINs.

    Ibis implements CrossJoin differently from other Join operations,
    using *args to hide `left`/`right` because the public API allows you
    to do something like `a.cross_join(b, c, d)`.  In practice, deserialization
    will always only have `left` and `right` and never have n number of tables
    because the implementation of a cross join collapses each successive join into
    a single join operation, something akin to `a.inner_join(b.inner_join(c.inner_join(d)))`.
    """

    def _klass(self):
        klass = super()._klass()

        def wrapper(**kwargs):
            left = kwargs.pop("left")
            right = kwargs.pop("right")
            return klass(left, right, **kwargs)

        return wrapper


@register
class LiteralDeserializer(Deserializer):
    """Deserializer for Literal Expressions.

    We require special handling for deserialization of certain supported literal types.
    """

    def _maybe_coerce_list_like(self, value, dtype):
        """Ibis `literal` can't coerce Python lists to `Set` types, and similarly
        can't convert sets to `Array` types, so we need to convert things prior
        to passing them to `literal`.
        """
        coerce_ = COERCES_LITERAL_LIST_LIKE.get(type(dtype), None)
        if coerce_ is not None:
            value = coerce_(value)
        return value

    def deserialize(self):
        dtype = dt.dtype(self.ast.dtype)
        value = self._maybe_deserialize(get_oneof(self.ast, "value"))
        value = self._maybe_coerce_list_like(value, dtype)
        if isinstance(dtype, dt.GeoSpatial):
            if getattr(value, "crs", None) == "EPSG:4326":
                # force the literal to be interpreted as WKT
                # when compiled to SQL
                dtype = dt.Geography(srid=4326)

            # we need to check here otherwise we get a cryptic failure
            # when running the query
            if isinstance(value, (GeometryCollection, LinearRing)):
                raise exc.LiteralDeserializationError(
                    "Shapes of type {} not supported.".format(type(value))
                )

        try:
            # bypass using `self.klass` to enable validation
            # added with `ir.literal`
            return ir.literal(value, dtype)
        except (com.IbisTypeError, TypeError, ValueError) as e:
            raise exc.LiteralDeserializationError(
                "Unable to deserialize `{}` to data type {}".format(value, dtype)
            ) from e


@register
class TableColumnDeserializer(Deserializer):
    """Deserializer for a TableColumn node of the AST.

    A TableColumn represents a specific column in a projection or predicate, it can be
    a physical column in a table, or a column computed from some expression.

    Example
    ------
    >>> table_col = {"name": "geom", "opname": "TableColumn", "type": "geography*", "type": "ref_0"}
    >>> dsr = TableColumnDeserializer(table_col, table_refs=table_refs)
    >>> res = dsr.deserialize()
    """

    def deserialize(self):
        """Special handling for table refs."""
        table = self._maybe_deserialize(get_oneof(self.value["table"], "value"))
        if not isinstance(table, ir.TableExpr):
            # must be an alias, look up in table_refs
            alias = table
            if alias < 1 or alias > len(self.table_refs):
                raise exc.IbisDeserializationError(
                    "TableColumn refers to unknown table_ref"
                )
            table = self.table_refs[alias - 1]

        colname = self._maybe_deserialize(get_oneof(self.value["name"], "value"))
        expr = self.klass(colname, table).to_expr()
        if self.name:
            expr = expr.name(self.name)

        return expr


@register
class DatabaseTableDeserializer(Deserializer):
    """Deserializer for a Table node of the AST.

    A DatabaseTable represents a physical table in the database.

    For now we include the Schema in the serialized table to avoid
    re-fetching, but we should reconsider this choice.
    """

    @property
    def opname(self):
        return "UnboundTable"

    def deserialize(self):
        if self.table_refs is not None and self.alias:
            # its a table reference
            # alias is 1 based index of table alias in table_refs
            if self.alias > len(self.table_refs):
                raise exc.IbisDeserializationError(
                    "DatabaseTable refers to unknown table_ref"
                )
            return self.table_refs[self.alias - 1]

        # otherwise we need to retrieve the table from the backend
        name = self._maybe_deserialize(get_oneof(self.value["name"], "value"))
        schema = self._maybe_deserialize(get_oneof(self.value["schema"], "value"))

        return self.klass(schema, name).to_expr()


@register("schema")
class SchemaDeserializer(Deserializer):
    """Schema deserializer."""

    def _klass(self):
        return sch.Schema

    def _deserialize_schema_type(self, st):
        """Deserialize a single datatype (ibis_pb2.SchemaType), possibly recursively."""

        dtype = getattr(dt, st.type)
        kwargs = {}
        args = get_oneof(st, "value")
        if isinstance(args, ibis_pb2.SchemaValue):
            # struct
            kwargs["names"] = args.names
            kwargs["types"] = [self._deserialize_schema_type(t) for t in args.types]
        elif isinstance(args, ibis_pb2.ArraySchemaValue):
            # array
            kwargs["value_type"] = self._deserialize_schema_type(args.value_type)
        elif isinstance(args, ibis_pb2.GeospatialSchemaValue):
            # geography
            if args.geotype:
                kwargs["geotype"] = args.geotype
            kwargs["srid"] = args.srid
        elif isinstance(args, ibis_pb2.DecimalSchemaValue):
            # decimal
            kwargs["precision"] = args.precision
            kwargs["scale"] = args.scale

        return dtype(**kwargs)

    def deserialize(self):
        """Schema-specific deserialization."""

        names = self.value.names
        types = [self._deserialize_schema_type(t) for t in self.value.types]

        return self.klass(names=names, types=types)


@register("window")
class WindowDeserializer(Deserializer):
    """Window deserializer."""

    def _klass(self):
        return win.Window

    def deserialize(self):
        """Window-specific deserialization."""

        group_by = [self._maybe_deserialize(gb) for gb in self.value.group_by]
        order_by = [self._maybe_deserialize(ob) for ob in self.value.order_by]
        preceding = self._maybe_deserialize(get_oneof(self.value.preceding, "value"))
        following = self._maybe_deserialize(get_oneof(self.value.following, "value"))
        max_lookback = self._maybe_deserialize(
            get_oneof(self.value.max_lookback, "value")
        )
        how = self.value.how

        return self.klass(
            group_by=group_by,
            order_by=order_by,
            preceding=preceding,
            following=following,
            max_lookback=max_lookback,
            how=how,
        )


@register("query")
class QueryDeserializer(Deserializer):
    """Deserialize a top level query.

    Returns the deserialized expr with all the table references replaced
    with the actual tables.
    """

    def _klass(self):
        return None

    def deserialize(self):
        """Specialized to deserialized the wrapped expression with the table_refs."""
        expr = self.value.expr
        refs = self.value.table_refs

        # This assumes that the refs are sorted correctly, i.e. that
        # refs appear in the list before other refs that depend on them.
        # This is enforced by requiring any encounted reference (alias or table)
        # to already exist in DatabaseTableDeserializer.deserialize().
        table_refs = []
        for ref in refs:
            ref = AstDeserializer(
                ref, table_refs=table_refs, seen=self.seen
            ).deserialize()
            table_refs.append(ref)
        deserializer = AstDeserializer(expr, table_refs=table_refs)
        return deserializer.deserialize()


class AstDeserializer:
    """Deserializer for a serialized query AST."""

    def __init__(
        self,
        ast: Message,
        table_refs: list = None,
        seen: dict = None,
    ):
        self.ast = ast
        self.table_refs = table_refs
        if seen is None:
            seen = dict()

        self.seen = seen

    def deserialize(self):
        """Deserialize arbitrary Expr, looking up appropriate
        Deserializer type by the opname/type.
        """
        if hasattr(self.ast, "alias") and self.ast.alias:
            expr = ibis_pb2.Expr(
                opname=self.ast.opname,
                type=self.ast.type,
                name=self.ast.name,
                value=self.ast.value,
            )
        else:
            expr = self.ast
        # ordering must be deterministic, otherwise we don't correctly memoize
        bytes_ = expr.SerializeToString(deterministic=True)
        if bytes_ in self.seen:
            return self.seen[bytes_]

        typename = getattr(self.ast, "opname", None)
        if not typename:
            typename = getattr(self.ast, "type", None)
            if not typename:
                raise exc.OperationError("No opname or type defined in Expr")

        deserializer = _get_deserializer(typename)
        if not deserializer:
            raise exc.OperationError("Operation not found: {}".format(typename))
        deserializer = deserializer(
            self.ast, table_refs=self.table_refs, seen=self.seen
        )

        result = deserializer.deserialize()
        self.seen[bytes_] = result
        return result
