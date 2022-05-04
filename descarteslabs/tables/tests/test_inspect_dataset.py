import json
import sys

import pytest
import pandas as pd
import ibis.expr.schema as sch
import ibis.expr.datatypes as dt

from ..client import Tables
from ...common.ibis.client.client import _maybe_to_geodataframe


@pytest.fixture
def t():
    return Tables()


# We want to only want to test on systems that do  have geopandas (linux)
# and skip these tests on systems that don't (windows). The tests will pass through
# but until the user also installs Tables they won't get the dependencies.
@pytest.fixture
def df():

    df = pd.DataFrame(
        [
            [
                "POINT (-0.01943 5.88169)",
                123,
                12.3,
                "Cave",
                {"type": "cave_entrance"},
                "2020-12-02",
                "2021-09-16T18:24:48Z",
                [1, 2, 3],
            ],
            [
                "POINT (-0.01993 5.81137)",
                124,
                12.4,
                "Cave2",
                {},
                "2020-12-02",
                "2021-09-16T18:24:48Z",
                [],
            ],
        ],
        columns=[
            "geometry",
            "id",
            "ele",
            "name",
            "metadata",
            "date",
            "timestamp",
            "array",
        ],
    )
    schema = sch.schema(
        [
            ["geometry", dt.GeoSpatial()],
            ["id", dt.int16],
            ["ele", dt.float16],
            ["name", dt.string],
            ["metadata", dt.jsonb],
            ["date", dt.date],
            ["timestamp", dt.timestamp],
            ["array", dt.Array(dt.int16)],
        ]
    )

    # use geopandas if available, otherwise use pandas
    df = _maybe_to_geodataframe(df, schema)

    return df.astype(
        {
            "timestamp": "datetime64[ns]",
            "date": "datetime64",
            "name": "str",
            "array": "object",
            "metadata": "object",
        }
    )


@pytest.mark.skipif(
    "geopandas" not in sys.modules, reason="requires the geopandas library"
)
def test_inspect_dataset_df(t, df):
    schema, srid = t.inspect_dataset(df)

    # Caveats:
    # There's no reliable way to determine if a column with dtype(object)
    # is a string, a list or a dict without iterating through the data.
    # We have to default to string/text, otherwise strings could be misinterpretted
    # as lists of chars. Instead, JSON objects get misinterpreted as strings, a lesser evil.
    # This is why we should always manually confirm the schema before creating table!!

    expected_schema = {
        "properties": {
            "id": "int",
            "ele": "float",
            "name": "text",
            "timestamp": "datetime",
            # TODO:
            # known limitations of running inspect_dataset on pandas dataframes
            # date should be a date-only (not a full precision datetime)
            "date": "datetime",
            # dicts and lists should be json but can't tell based on dtype alone
            "metadata": "text",  # json
            "array": "text",  # json
        },
        "geometry": "Point",
    }

    # TODO, crs for dataframes is not reported correctly, resulting in srid=0
    # assert srid == 4326

    assert schema == expected_schema


@pytest.mark.skipif(
    "geopandas" not in sys.modules, reason="requires the geopandas library"
)
def test_inspect_dataset_file(t, df):
    # Create a geojson file
    # Hacks for timestamp
    df = df.astype({"date": "str", "timestamp": "str"})
    path = "/tmp/test.geojson"
    with open(path, mode="w") as fh:
        fh.write(json.dumps(df.__geo_interface__))

    schema, srid = t.inspect_dataset(path)
    expected_schema = {
        "properties": {
            "id": "int",
            "ele": "float",
            "name": "str",
            "date": "date",
            "timestamp": "datetime",
            # TODO:
            # Bug in fiona: properties with dicts are reported as str
            # Note, because this comes from fiona, we accept 'str' as an alias for 'text'
            "metadata": "str",
            # Bug in fiona: properties with lists are dropped from the schema
            # "array": "json",
        },
        "geometry": "Point",
    }
    assert srid == 4326
    assert schema == expected_schema
