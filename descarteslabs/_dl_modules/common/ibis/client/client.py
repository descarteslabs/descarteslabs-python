"""Serializer ibis client implementation."""

import grpc

import pyarrow as pa
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import shapely.wkt
import numpy as np

from functools import lru_cache
from ibis.client import Client

from ....client.grpc import GrpcClient
from ...proto.vektorius import vektorius_pb2_grpc, vektorius_pb2
from ...proto.health import health_pb2


from ..serialization import compiler as comp
from ..deserialization import AstDeserializer


JOB_STATUS_DONE = "DONE"
_PARTITIONTIME = "PARTITIONTIME"


def record_batch_to_bytes(batch):
    sink = pa.BufferOutputStream()
    writer = pa.ipc.RecordBatchStreamWriter(sink, batch.schema)
    writer.write_batch(batch)
    writer.close()
    return sink.getvalue().to_pybytes()


class VektoriusGrpcClient(GrpcClient):
    def _populate_api(self):
        self._add_stub("Vektorius", vektorius_pb2_grpc.VektoriusStub)

        self._add_api("Vektorius", "CreateTable")
        self._add_api("Vektorius", "DeleteTable")
        self._add_api("Vektorius", "CreateIngestJob")
        self._add_api("Vektorius", "GetJobStatus")
        self._add_api("Vektorius", "GetSchema")
        self._add_api("Vektorius", "Search")
        self._add_api("Vektorius", "SearchResults")
        self._add_api("Vektorius", "CreateDeleteRowsJob")


class _Client(object):
    def __init__(self, **grpc_client_kwargs):
        self._client = VektoriusGrpcClient(**grpc_client_kwargs)

    def healthcheck(self):
        r = self._client.health()
        return r.status == health_pb2.HealthCheckResponse.ServingStatus.SERVING

    def get_schema(self, table_name, owner, database=None):
        r = self._client.api["GetSchema"](
            vektorius_pb2.SchemaRequest(
                table_name=table_name, database=database, owner=owner
            )
        )
        deserializer = AstDeserializer(r.schema, table_refs={})
        schema = deserializer.deserialize()
        return schema, r.table_name

    def search(self, query):
        responses = self._client.api["Search"](vektorius_pb2.SearchRequest(query=query))

        bytes_ = b""
        batches = []
        for r in responses:
            # async results will return a status immediately
            # sync results will never return a status
            if r.HasField("status"):
                status = r.status
                return status, []

            # sync results
            # accumulate the returned bytes until we reach the last slice in batch
            # size is defined by settings.SLICE_SIZE_KB
            bytes_ += r.slice.data
            if r.slice.final:
                # the record batch is done, append it and reset bytes
                batches.append(pa.RecordBatchStreamReader(bytes_).read_next_batch())
                bytes_ = b""

        return None, batches

    def search_results(self, continuation_token, offset=0):
        """Retrieve a single "dynamic page" of the results via streaming.

        This method should be invoked repeatedly until an error is raised
        or the returned continuation_token is None.
        """

        batches = []
        retries = 3
        # A response is a stream of SearchResultsResponse objects
        # which contain either a StreamingStatus (job, continuation_token
        # and/or error) or a RecordBatch.
        #
        # Note that with the streamed but "dynamically paged" approach here,
        # A complete result from the request is not necessarily the complete
        # results for the entire query. This is determined by the
        # continuation_token in the final status block.
        job = None
        done = False
        bytes_ = b""
        while not done:
            messages = self._client.api["SearchResults"](
                vektorius_pb2.SearchResultsRequest(
                    continuation_token=continuation_token, offset=offset
                )
            )
            while True:
                try:
                    r = next(messages)
                # TODO should this be tightened up a little
                except grpc.RpcError:
                    # an RpcError might occur midstream that is retriable
                    # but not handled by the client's retry configuration
                    # which wraps python function calls, not grpc calls
                    retries -= 1
                    if retries > 0:
                        # if an error occurs in this loop and we have retries left,
                        # break to the outer loop and resend the request with the
                        # last updated continuation token
                        break

                    # something's gone wrong and we reraise the error
                    raise
                except StopIteration:
                    # we read all the messages and should break the loop
                    done = True
                    break

                if r.HasField("slice"):
                    bytes_ += r.slice.data
                    if r.slice.final:
                        # if this is the final slice, read the RecordBatch
                        # and reset the accumulator
                        batch = pa.RecordBatchStreamReader(bytes_).read_next_batch()
                        bytes_ = b""
                        batches.append(batch)

                        # update the offset so that partial reads of the results
                        # can pick up from that point, rather than needing to start over
                        offset += batch.num_rows
                else:
                    # StreamingStatus
                    if r.status.HasField("job"):
                        job = r.status.job

                    bytes_ = b""
                    continuation_token = r.status.continuation_token or None

        return batches, job, continuation_token


def _to_shapely(v):
    """Turn WKT into a Shapely object."""
    # avoid isinstance checking on every value
    # but prevent this blowing up on subsequent runs
    # subsequent runs shouldn't occur, but you never know
    try:
        return shapely.wkt.loads(v)
    except TypeError:
        return v


# vectorized version of `_to_shapely`
to_shapely = np.vectorize(_to_shapely, otypes=[object])


def _struct_to_geospatial(struct, geos, arrs, structs):
    """Convert GeoSpatial struct fields to Shapely objects.

    Parameters
    ----------
    struct : dict
        The struct to potentially apply the Shapely conversion to.
    geos : list(str)
        Names of GeoSpatial fields to apply a shapely conversion to.
    arrs : dict(str, dt.DataType)
        Dictionary of field names:data types for array nested
        fields in the struct which need to be traversed for potential conversion.
    structs : dict(str, dt.DataType)
        Dictionary of field names:data types for struct fields nested
        in the struct which need to be traversed for potential conversion.
    """
    res = {}
    for k, v in struct.items():
        if k in geos:
            v = _to_shapely(v)
        elif k in arrs:
            arr_dtype = arrs[k]
            v = _array_to_geospatial(v, arr_dtype)
        elif k in structs:
            struct_dtype = structs[k]
            # can't use apply_to_struct_column as it's vectorized
            # and we've got a scalar value here.
            geos, arrs, structs = struct_types(struct_dtype)
            v = _struct_to_geospatial(v, geos, arrs, structs)
        res[k] = v
    return res


# vectorized version of `_struct_to_geospatial`
# `geos`, `arrs` and `structs` are not vectorized because
# those values are the same for every row in the array
# and determined by the schema of the array
struct_to_geospatial = np.vectorize(
    _struct_to_geospatial, excluded=[1, 2, 3], otypes=[object]
)


# memoize to avoid recomputation in nested array structures
@lru_cache(None)
def struct_types(struct_dtype):
    """Get the names and dtypes for GeoSpatial, Array, and Struct columns
    of a Struct data type."""
    items = struct_dtype.pairs.items()
    geos = [name for name, dtype in items if isinstance(dtype, dt.GeoSpatial)]
    arrs = {name: dtype for name, dtype in items if isinstance(dtype, dt.Array)}
    structs = {name: dtype for name, dtype in items if isinstance(dtype, dt.Struct)}
    return geos, arrs, structs


def apply_to_struct_column(series, struct_dtype):
    """Apply an Ibis data type to a Struct series.

    Handles complex nesting of structures.
    """
    # these values are computed here to avoid recomputaion for every row
    geos, arrs, structs = struct_types(struct_dtype)

    # there are no potential conversions to make, so don't bother
    if not geos and not arrs and not structs:
        return series

    return struct_to_geospatial(series, geos, arrs, structs)


def _array_to_geospatial(arr, arr_dtype):
    """Apply an Ibis data type to an array-typed value."""
    # we use vectorized versions of functions here since our
    # value is an array
    if isinstance(arr_dtype.value_type, dt.Struct):
        return apply_to_struct_column(arr, arr_dtype.value_type)
    elif isinstance(arr_dtype.value_type, dt.GeoSpatial):
        return to_shapely(arr)
    else:
        # if the value type isn't Struct or GeoSpatial, then we assume that
        # pyarrow did the correct conversion to Python types, and that Ibis did
        # the correct things when applying schema on first pass
        return arr


# vectorized version of `_array_to_geospatial`, think nested for loops
apply_to_array_column = np.vectorize(_array_to_geospatial, otypes=[object])


def _maybe_to_geodataframe(df, schema):
    """
    If the required libraries for geospatial support are installed, and if a
    geospatial column is present in the dataframe, convert it to a
    GeoDataFrame.
    """
    geom_col = None

    geospatial_available = True
    try:
        import geopandas
    except ImportError:
        geospatial_available = False

    for name, dtype in schema.items():
        if isinstance(dtype, dt.GeoSpatial):
            # first geom_col is the indexed column
            geom_col = geom_col or name
            df[name] = to_shapely(df[name])
        # nested data structures can't be indexed, but should
        # be converted to shapely objects if they're present
        # TODO should traverse the nested schema prior to vectorizing over the rows
        # to prevent unnecessary traversal when there are no nested geospatial fields
        # but this adds a certain amount of complexity that we may not need right now
        elif isinstance(dtype, dt.Struct):
            df[name] = apply_to_struct_column(df[name], dtype)
        elif isinstance(dtype, dt.Array):
            df[name] = apply_to_array_column(df[name], dtype)

    if geospatial_available and geom_col:
        df = geopandas.GeoDataFrame(df, geometry=geom_col)

    return df


class SerializerClient(Client):
    """An ibis Serialized client implementation."""

    table_class = ops.DatabaseTable
    table_expr_class = ir.TableExpr
    dialect = comp.dialect

    def __init__(self, database=None, client=None, **grpc_client_kwargs):
        """Construct a SerializerClient."""
        if client is None:
            client = _Client(**grpc_client_kwargs)
        self.client = client
        self.database = database

    def table(self, name, owner=None, database=None):
        if not database:
            database = self.database
        if owner is None:
            # If owner isn't provided, we cast to an empty string
            # that is re-interpretted as None on the server
            owner = ""
        schema, table_name = self.client.get_schema(name, owner, database=database)
        node = self.table_class(table_name, schema, self)
        return self.table_expr_class(node)

    def compile(self, expr, params=None, **kwargs):
        serializer = comp.AstSerializer(expr)
        ast = serializer.serialize()
        return comp.make_query(expr=ast, table_refs=serializer.table_refs)

    def _async_load_data(self, status):
        # consume the results until no more continuation_token
        continuation_token = status.continuation_token
        data = []
        while continuation_token:
            batches, job, continuation_token = self.client.search_results(
                continuation_token
            )
            data.extend(batches)

        return data

    def execute(self, expr, params=None, **kwargs):
        expr, result_wrapper = comp.adapt_expr(expr)
        ast = expr.compile(params=params)

        # initiate the query
        # if status is present, then we fetch the data from the async endpoint
        # otherwise we assume that the search was executed synchronously and returned immediately
        status, data = self.client.search(ast)
        if status:
            data = self._async_load_data(status)

        if not data:
            return None

        df = pa.Table.from_batches(data).to_pandas()
        if not len(df):
            return None

        if isinstance(expr, (ir.TableExpr, ir.ExprList, sch.HasSchema)):
            schema = expr.schema()
        elif isinstance(expr, ir.ValueExpr):
            schema = sch.schema([(expr.get_name(), expr.type())])
        else:
            raise ValueError("no schema!?! no schema...")

        if _PARTITIONTIME in schema:
            # the is a pseudo column, present in the schema to allow
            # for querying the column, but never returned in queries
            schema = schema.delete([_PARTITIONTIME])

        df = _maybe_to_geodataframe(schema.apply_to(df), schema)

        # get the correct arity for the result
        return result_wrapper(df)

    def get_schema(self, name, database=None):
        if not database:
            database = self.database
        schema, table_name = self.client.get_schema(name, database)
        return schema
