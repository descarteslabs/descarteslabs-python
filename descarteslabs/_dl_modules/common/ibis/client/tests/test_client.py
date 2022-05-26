import unittest
from unittest.mock import patch, MagicMock
import pytest
import sys

import ibis.expr.datatypes as dt
import ibis.expr.schema as sch
import ibis.expr.types as ir

import pyarrow as pa
import pandas as pd
from pandas.util.testing import assert_frame_equal

from shapely.geometry import Point

import grpc

from ....proto.vektorius import vektorius_pb2
from ...serialization.compiler import (
    make_schema,
    serialize_record_batch,
)
from ..client import (
    SerializerClient,
    _maybe_to_geodataframe,
)


def slice_(df):
    chunksize = 64 * 1024
    bytes_ = serialize_record_batch(pa.RecordBatch.from_pandas(df))
    for i in range(0, len(bytes_), chunksize):
        end = i + chunksize
        yield vektorius_pb2.RecordBatchSlice(
            data=bytes_[i : i + chunksize], final=end >= len(bytes_)
        )


def slice_response(df):
    yield from (vektorius_pb2.SearchResultsResponse(slice=s) for s in slice_(df))


def search_slice_response(df):
    yield from (vektorius_pb2.SearchResponse(slice=s) for s in slice_(df))


class TestRpcError(grpc.RpcError):
    def __init__(self, code, details):
        super().__init__()
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


class ClientTestCase(unittest.TestCase):
    def setUp(self):
        self.client = SerializerClient(host="localhost", port=443)

    def test_get_schema(self):
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
            ],
            types=[
                dt.boolean,
                dt.int64,
                dt.float64,
                dt.Decimal(38, 9),
                dt.string,
                dt.timestamp,
                dt.Geography(srid=4326),
                dt.Array(dt.int64),
                dt.Struct(
                    names=["anInt64", "aGeography"],
                    types=[dt.int64, dt.Geography(srid=4326)],
                ),
            ],
        )

        with patch.dict(
            self.client.client._client.api,
            {
                "GetSchema": MagicMock(
                    return_value=vektorius_pb2.SchemaResponse(
                        schema=make_schema(schema),
                        table_name="somedatabase.sometable",
                    )
                ),
            },
        ):
            result = self.client.get_schema("sometable")

        assert type(result) is sch.Schema
        assert result == schema

    @patch("descarteslabs.common.ibis.client.client.VektoriusGrpcClient.api")
    def test_table(self, mock_api):
        schema = sch.Schema(names=["anInt64", "aString"], types=[dt.int64, dt.string])

        mock_api["GetSchema"].return_value = vektorius_pb2.SchemaResponse(
            schema=make_schema(schema),
            table_name="somedatabase.sometable",
        )

        result = self.client.table("sometable")
        assert type(result) is ir.TableExpr
        assert result.op().name == "somedatabase.sometable"
        assert result.schema() == schema

    def test_execute(self):
        # mock table
        schema = sch.Schema(names=["ints", "strings"], types=[dt.int64, dt.string])

        # mock query
        job = vektorius_pb2.Job(job_id="some-job-id", status="DONE")

        # mock results
        ints = [5, 6, 7]
        strings = ["Five", "Six", "Seven"]
        df = pd.DataFrame({"ints": ints, "strings": strings})

        def search_results():
            yield vektorius_pb2.SearchResultsResponse(
                status=vektorius_pb2.SearchStatus(
                    job=job,
                    results_available=True,
                    total_rows=3,
                    continuation_token="blah",
                ),
            )
            yield from slice_response(df)
            yield vektorius_pb2.SearchResultsResponse(
                status=vektorius_pb2.SearchStatus()
            )

        with patch.dict(
            self.client.client._client.api,
            {
                "GetSchema": MagicMock(
                    return_value=vektorius_pb2.SchemaResponse(
                        schema=make_schema(schema),
                        table_name="somedatabase.sometable",
                    )
                ),
                "Search": MagicMock(
                    return_value=[
                        vektorius_pb2.SearchResponse(
                            status=vektorius_pb2.SearchStatus(
                                job=job, continuation_token="blah"
                            )
                        )
                    ]
                ),
                "SearchResults": MagicMock(return_value=search_results()),
            },
        ):
            table = self.client.table("sometable")

            result = table[table.ints >= 5].execute()

        assert type(result) is pd.DataFrame
        assert result.shape == df.shape
        assert (result.columns == schema.names).all()
        assert_frame_equal(result, df)

    def test_execute_paged(self):
        # mock table
        schema = sch.Schema(names=["ints", "strings"], types=[dt.int64, dt.string])

        # mock query
        job = vektorius_pb2.Job(job_id="some-job-id", status="DONE")

        # mock results
        strings = ["A", "B", "C"]
        df1 = pd.DataFrame({"ints": [5, 6, 7], "strings": strings})
        df2 = pd.DataFrame({"ints": [8, 9, 10], "strings": strings})

        def search_results_1():
            yield vektorius_pb2.SearchResultsResponse(
                status=vektorius_pb2.SearchStatus(
                    job=job,
                    results_available=True,
                    total_rows=6,
                    continuation_token="blah",
                ),
            )
            yield from slice_response(df1)
            yield vektorius_pb2.SearchResultsResponse(
                status=vektorius_pb2.SearchStatus(
                    job=job,
                    results_available=True,
                    total_rows=6,
                    continuation_token="blah",
                ),
            )

        def search_results_2():
            yield vektorius_pb2.SearchResultsResponse(
                status=vektorius_pb2.SearchStatus(
                    job=job,
                    results_available=True,
                    total_rows=6,
                    continuation_token="blah",
                ),
            )

            yield from slice_response(df2)
            yield vektorius_pb2.SearchResultsResponse(
                status=vektorius_pb2.SearchStatus(
                    job=job,
                    results_available=True,
                    total_rows=6,
                ),
            )

        with patch.dict(
            self.client.client._client.api,
            {
                "GetSchema": MagicMock(
                    return_value=vektorius_pb2.SchemaResponse(
                        schema=make_schema(schema),
                        table_name="somedatabase.sometable",
                    )
                ),
                "Search": MagicMock(
                    return_value=[
                        vektorius_pb2.SearchResponse(
                            status=vektorius_pb2.SearchStatus(
                                job=job, continuation_token="blah"
                            )
                        )
                    ]
                ),
                "SearchResults": MagicMock(
                    side_effect=[search_results_1(), search_results_2()]
                ),
            },
        ):
            table = self.client.table("sometable")
            result = table[table.ints >= 5].execute()

        assert type(result) is pd.DataFrame
        assert result.shape == (6, 2)
        assert (result.columns == schema.names).all()
        assert_frame_equal(result.iloc[0:3], df1)
        assert_frame_equal(
            result.iloc[3:6].reset_index(drop=True), df2.reset_index(drop=True)
        )

    def test_execute_stream_error(self):
        # mock table
        schema = sch.Schema(names=["ints", "strings"], types=[dt.int64, dt.string])

        # mock query
        job = vektorius_pb2.Job(job_id="some-job-id", status="DONE")

        # mock results
        def search_results():
            yield vektorius_pb2.SearchResultsResponse(
                status=vektorius_pb2.SearchStatus(
                    job=job,
                    results_available=True,
                    total_rows=3,
                    continuation_token="blah",
                ),
            )
            raise TestRpcError(grpc.StatusCode.INTERNAL, "blarf!")

        with patch.dict(
            self.client.client._client.api,
            {
                "GetSchema": MagicMock(
                    return_value=vektorius_pb2.SchemaResponse(
                        schema=make_schema(schema),
                        table_name="somedatabase.sometable",
                    )
                ),
                "Search": MagicMock(
                    return_value=[
                        vektorius_pb2.SearchResponse(
                            status=vektorius_pb2.SearchStatus(
                                job=job, continuation_token="blah"
                            )
                        )
                    ]
                ),
                # client will retry 3 times
                "SearchResults": MagicMock(
                    side_effect=[search_results(), search_results(), search_results()]
                ),
            },
        ):
            table = self.client.table("sometable")

            with pytest.raises(grpc.RpcError) as excinfo:
                table[table.ints >= 5].execute()
            assert excinfo.value.code() == grpc.StatusCode.INTERNAL
            assert "blarf!" in excinfo.value.details()

    def test_execute_stream_eof(self):
        # mock table
        schema = sch.Schema(names=["ints", "strings"], types=[dt.int64, dt.string])

        # mock query
        job = vektorius_pb2.Job(job_id="some-job-id", status="DONE")

        # mock results
        ints = [5, 6, 7]
        strings = ["Five", "Six", "Seven"]
        df = pd.DataFrame({"ints": ints, "strings": strings})

        def search_results_1():
            yield vektorius_pb2.SearchResultsResponse(
                status=vektorius_pb2.SearchStatus(
                    job=job,
                    results_available=True,
                    total_rows=3,
                    continuation_token="blah",
                ),
            )

        def search_results_2():
            yield vektorius_pb2.SearchResultsResponse(
                status=vektorius_pb2.SearchStatus(
                    job=job,
                    results_available=True,
                    total_rows=3,
                    continuation_token="blah",
                ),
            )
            yield from slice_response(df)
            yield vektorius_pb2.SearchResultsResponse(
                status=vektorius_pb2.SearchStatus()
            )

        with patch.dict(
            self.client.client._client.api,
            {
                "GetSchema": MagicMock(
                    return_value=vektorius_pb2.SchemaResponse(
                        schema=make_schema(schema),
                        table_name="somedatabase.sometable",
                    )
                ),
                "Search": MagicMock(
                    return_value=[
                        vektorius_pb2.SearchResponse(
                            status=vektorius_pb2.SearchStatus(
                                job=job, continuation_token="blah"
                            )
                        )
                    ]
                ),
                "SearchResults": MagicMock(
                    side_effect=[search_results_1(), search_results_2()]
                ),
            },
        ):
            table = self.client.table("sometable")
            result = table[table.ints >= 5].execute()

        assert type(result) is pd.DataFrame
        assert result.shape == df.shape
        assert (result.columns == schema.names).all()
        assert_frame_equal(result, df)

    def test_execute_fail(self):

        # mock table
        schema = sch.Schema(names=["ints", "strings"], types=[dt.int64, dt.string])

        # mock query
        job = vektorius_pb2.Job(job_id="some-job-id", status="DONE")

        with patch.dict(
            self.client.client._client.api,
            {
                "GetSchema": MagicMock(
                    return_value=vektorius_pb2.SchemaResponse(
                        schema=make_schema(schema),
                        table_name="somedatabase.sometable",
                    )
                ),
                "Search": MagicMock(
                    return_value=[
                        vektorius_pb2.SearchResponse(
                            status=vektorius_pb2.SearchStatus(
                                job=job, continuation_token="blah"
                            )
                        )
                    ]
                ),
                "SearchResults": MagicMock(side_effect=grpc.RpcError("You lose!")),
            },
        ):
            table = self.client.table("sometable")

            with pytest.raises(grpc.RpcError):
                table[table.ints >= 5].execute()

    def test_maybe_to_geodataframe_no_geo(self):
        pa_schema = pa.schema([pa.field("id", pa.int32())])

        ids = pa.array([1, 2, 3, 4, 5], type=pa_schema[0].type)
        batch = pa.RecordBatch.from_arrays([ids], schema=pa_schema)

        df = batch.to_pandas()

        schema = sch.schema([["id", dt.int32]])
        df = _maybe_to_geodataframe(df, schema)

        assert isinstance(df, pd.DataFrame)

    @pytest.mark.skipif(
        "geopandas" not in sys.modules, reason="requires the geopandas library"
    )
    def test_maybe_to_geodataframe_geo(self):
        import geopandas as gpd

        pa_schema = pa.schema([pa.field("geom", pa.string())])

        geoms = pa.array(["POINT(0 0)", "POINT(1 1)"], type=pa_schema[0].type)
        batch = pa.RecordBatch.from_arrays([geoms], schema=pa_schema)

        df = batch.to_pandas()

        schema = sch.schema([["geom", dt.GeoSpatial()]])
        df = _maybe_to_geodataframe(df, schema)

        assert isinstance(df, gpd.GeoDataFrame)
        assert df.iloc[0]["geom"] == Point(0, 0)
        assert df.iloc[1]["geom"] == Point(1, 1)

    @pytest.mark.skipif(
        "geopandas" not in sys.modules, reason="requires the geopandas library"
    )
    def test_maybe_to_geodataframe_multiple_geo(self):
        import geopandas as gpd

        pa_schema = pa.schema(
            [pa.field("geom1", pa.string()), pa.field("geom2", pa.string())]
        )

        geoms1 = pa.array(["POINT(0 0)", "POINT(1 1)"], type=pa_schema[0].type)
        geoms2 = pa.array(["POINT(2 2)", "POINT(3 3)"], type=pa_schema[1].type)
        batch = pa.RecordBatch.from_arrays([geoms1, geoms2], schema=pa_schema)

        df = batch.to_pandas()

        schema = sch.schema([["geom1", dt.GeoSpatial()], ["geom2", dt.GeoSpatial()]])
        df = _maybe_to_geodataframe(df, schema)

        assert isinstance(df, gpd.GeoDataFrame)
        assert df.geometry.name == "geom1"
        assert df.iloc[0]["geom1"] == Point(0, 0)
        assert df.iloc[0]["geom2"] == Point(2, 2)
        assert df.iloc[1]["geom1"] == Point(1, 1)
        assert df.iloc[1]["geom2"] == Point(3, 3)

    def test_maybe_to_geodataframe_geo_array(self):
        pa_schema = pa.schema([pa.field("geom", pa.list_(pa.string()))])

        geoms = pa.array(
            [["POINT(0 0)", "POINT(1 1)"], ["POINT(2 2)", "POINT(3 3)"]],
            type=pa_schema[0].type,
        )
        batch = pa.RecordBatch.from_arrays([geoms], schema=pa_schema)

        df = batch.to_pandas()

        schema = sch.schema([["geom", dt.Array(dt.GeoSpatial())]])
        df = _maybe_to_geodataframe(df, schema)

        # won't index array-like geospatial columns
        assert isinstance(df, pd.DataFrame)
        assert df.iloc[0]["geom"][0] == Point(0, 0)
        assert df.iloc[0]["geom"][1] == Point(1, 1)
        assert df.iloc[1]["geom"][0] == Point(2, 2)
        assert df.iloc[1]["geom"][1] == Point(3, 3)

    def test_maybe_to_geodataframe_geo_struct(self):
        pa_schema = pa.schema(
            [pa.field("geom", pa.struct([pa.field("value", pa.string())]))]
        )

        geoms = pa.array(
            [dict(value="POINT(0 0)"), dict(value="POINT(1 1)")],
            type=pa_schema[0].type,
        )
        batch = pa.RecordBatch.from_arrays([geoms], schema=pa_schema)

        df = batch.to_pandas()

        schema = sch.schema(
            [["geom", dt.Struct.from_tuples([("value", dt.GeoSpatial())])]]
        )
        df = _maybe_to_geodataframe(df, schema)

        # won't index array-like geospatial columns
        assert isinstance(df, pd.DataFrame)
        assert df.iloc[0]["geom"]["value"] == Point(0, 0)
        assert df.iloc[1]["geom"]["value"] == Point(1, 1)

    def test_maybe_to_geodataframe_geo_array_struct(self):
        struct_type = pa.struct([pa.field("value", pa.string())])
        pa_schema = pa.schema([pa.field("geom", pa.list_(struct_type))])

        geoms = pa.array(
            [
                [dict(value="POINT(0 0)"), dict(value="POINT(1 1)")],
                [dict(value="POINT(2 2)"), dict(value="POINT(3 3)")],
            ]
        )

        batch = pa.RecordBatch.from_arrays([geoms], schema=pa_schema)
        df = batch.to_pandas()

        schema = sch.schema(
            [["geom", dt.Array(dt.Struct.from_tuples([("value", dt.GeoSpatial())]))]]
        )
        df = _maybe_to_geodataframe(df, schema)

        # won't index array-like geospatial columns
        assert isinstance(df, pd.DataFrame)
        assert df.iloc[0]["geom"][0]["value"] == Point(0, 0)
        assert df.iloc[0]["geom"][1]["value"] == Point(1, 1)
        assert df.iloc[1]["geom"][0]["value"] == Point(2, 2)
        assert df.iloc[1]["geom"][1]["value"] == Point(3, 3)

    def test_maybe_to_geodataframe_annoyingly_nested(self):
        struct_type = pa.struct(
            [
                pa.field("value", pa.string()),
                pa.field("nested_value", pa.list_(pa.string())),
            ]
        )
        array_type = pa.list_(struct_type)
        pa_schema = pa.schema(
            [pa.field("geom", pa.struct([pa.field("array", array_type)]))]
        )

        geoms = pa.array(
            [
                dict(
                    array=[
                        dict(
                            value="POINT(0 0)",
                            nested_value=["POINT(4 4)"],
                        ),
                        dict(
                            value="POINT(1 1)",
                            nested_value=["POINT(5 5)"],
                        ),
                    ]
                ),
                dict(
                    array=[
                        dict(
                            value="POINT(2 2)",
                            nested_value=["POINT(6 6)"],
                        ),
                        dict(
                            value="POINT(3 3)",
                            nested_value=["POINT(7 7)"],
                        ),
                    ]
                ),
            ],
            type=pa_schema[0].type,
        )

        batch = pa.RecordBatch.from_arrays([geoms], schema=pa_schema)
        df = batch.to_pandas()

        schema = sch.schema(
            [
                [
                    "geom",
                    dt.Struct.from_tuples(
                        [
                            (
                                "array",
                                dt.Array(
                                    dt.Struct.from_tuples(
                                        [
                                            ("value", dt.GeoSpatial()),
                                            ("nested_value", dt.GeoSpatial()),
                                        ]
                                    )
                                ),
                            )
                        ]
                    ),
                ]
            ]
        )
        df = _maybe_to_geodataframe(df, schema)

        # won't index array-like geospatial columns
        assert isinstance(df, pd.DataFrame)
        df.iloc[0]["geom"]["array"][0]["value"] == Point(0, 0)
        df.iloc[0]["geom"]["array"][1]["value"] == Point(1, 1)
        df.iloc[1]["geom"]["array"][0]["value"] == Point(2, 2)
        df.iloc[1]["geom"]["array"][1]["value"] == Point(3, 3)

        df.iloc[0]["geom"]["array"][0]["nested_value"][0] == Point(4, 4)
        df.iloc[0]["geom"]["array"][1]["nested_value"][0] == Point(5, 5)
        df.iloc[1]["geom"]["array"][0]["nested_value"][0] == Point(6, 6)
        df.iloc[1]["geom"]["array"][1]["nested_value"][0] == Point(7, 7)

    def test_execute_sync_query(self):
        # mock table
        schema = sch.Schema(names=["ints", "strings"], types=[dt.int64, dt.string])

        # mock results

        strings = ["A", "B", "C"]
        df1 = pd.DataFrame({"ints": [5, 6, 7], "strings": strings})
        df2 = pd.DataFrame({"ints": [8, 9, 10], "strings": strings})

        def search_results():
            yield from slice_response(pd.concat([df1, df2], axis=0))

        with patch.dict(
            self.client.client._client.api,
            {
                "GetSchema": MagicMock(
                    return_value=vektorius_pb2.SchemaResponse(
                        schema=make_schema(schema),
                        table_name="somedatabase.sometable",
                    )
                ),
                "Search": MagicMock(return_value=search_results()),
            },
        ):
            table = self.client.table("sometable")

            result = table[table.ints >= 5].execute()

        assert type(result) is pd.DataFrame
        assert result.shape == (6, 2)
        assert (result.columns == schema.names).all()
        assert_frame_equal(result.iloc[0:3], df1)
        assert_frame_equal(
            result.iloc[3:6].reset_index(drop=True), df2.reset_index(drop=True)
        )

    def test_execute_sync_query_error(self):
        # mock table
        schema = sch.Schema(names=["ints", "strings"], types=[dt.int64, dt.string])

        # mock results

        strings = ["A", "B", "C"]
        df1 = pd.DataFrame({"ints": [5, 6, 7], "strings": strings})
        df2 = pd.DataFrame({"ints": [8, 9, 10], "strings": strings})

        def search_results():
            yield from slice_response(df1)
            yield from slice_response(df2)
            raise TestRpcError(grpc.StatusCode.INTERNAL, "blarf!")

        with patch.dict(
            self.client.client._client.api,
            {
                "GetSchema": MagicMock(
                    return_value=vektorius_pb2.SchemaResponse(
                        schema=make_schema(schema),
                        table_name="somedatabase.sometable",
                    )
                ),
                "Search": MagicMock(return_value=search_results()),
            },
        ):
            table = self.client.table("sometable")

            with pytest.raises(grpc.RpcError) as excinfo:
                table[table.ints >= 5].execute()

            assert excinfo.value.code() == grpc.StatusCode.INTERNAL
            assert "blarf!" in excinfo.value.details()

    def test_execute_sync_query_fail(self):
        # mock table
        schema = sch.Schema(names=["ints", "strings"], types=[dt.int64, dt.string])

        # mock results

        with patch.dict(
            self.client.client._client.api,
            {
                "GetSchema": MagicMock(
                    return_value=vektorius_pb2.SchemaResponse(
                        schema=make_schema(schema),
                        table_name="somedatabase.sometable",
                    )
                ),
                "Search": MagicMock(side_effect=grpc.RpcError("You lose!")),
            },
        ):
            table = self.client.table("sometable")

            with pytest.raises(grpc.RpcError):
                table[table.ints >= 5].execute()

    # tests without geopandas / geo libraries ----------------------------------

    def test_maybe_to_geodataframe_no_geo_1(self):
        pa_schema = pa.schema([pa.field("id", pa.int32())])

        ids = pa.array([1, 2, 3, 4, 5], type=pa_schema[0].type)
        batch = pa.RecordBatch.from_arrays([ids], schema=pa_schema)

        df = batch.to_pandas()

        schema = sch.schema([["id", dt.int32]])

        with patch.dict("sys.modules", geopandas=None):
            df = _maybe_to_geodataframe(df, schema)

        assert isinstance(df, pd.DataFrame)
        assert df.iloc[2]["id"] == 3

    def test_maybe_to_geodataframe_no_geo_2(self):
        pa_schema = pa.schema([pa.field("geom", pa.string())])

        geoms = pa.array(["POINT(0 0)", "POINT(1 1)"], type=pa_schema[0].type)
        batch = pa.RecordBatch.from_arrays([geoms], schema=pa_schema)

        df = batch.to_pandas()

        schema = sch.schema([["geom", dt.GeoSpatial()]])
        with patch.dict("sys.modules", geopandas=None):
            df = _maybe_to_geodataframe(df, schema)

        assert isinstance(df, pd.DataFrame)
        assert df.iloc[0]["geom"] == Point(0, 0)
        assert df.iloc[1]["geom"] == Point(1, 1)

    def test_maybe_to_geodataframe_multiple_no_geo_3(self):
        pa_schema = pa.schema(
            [pa.field("geom1", pa.string()), pa.field("geom2", pa.string())]
        )

        geoms1 = pa.array(["POINT(0 0)", "POINT(1 1)"], type=pa_schema[0].type)
        geoms2 = pa.array(["POINT(2 2)", "POINT(3 3)"], type=pa_schema[1].type)
        batch = pa.RecordBatch.from_arrays([geoms1, geoms2], schema=pa_schema)

        df = batch.to_pandas()
        schema = sch.schema([["geom1", dt.GeoSpatial()], ["geom2", dt.GeoSpatial()]])

        with patch.dict("sys.modules", geopandas=None):
            df = _maybe_to_geodataframe(df, schema)

        assert isinstance(df, pd.DataFrame)
        assert df.iloc[0]["geom1"] == Point(0, 0)
        assert df.iloc[0]["geom2"] == Point(2, 2)
        assert df.iloc[1]["geom1"] == Point(1, 1)
        assert df.iloc[1]["geom2"] == Point(3, 3)
