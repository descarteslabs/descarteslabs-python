import json
import sys

import pytest
import geopandas as gpd

from descarteslabs.tables.client import Tables


@pytest.fixture
def t():
    return Tables()


@pytest.fixture
def df():

    collection = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "id": 123,
                    "ele": 12.3,
                    "name": "Cave",
                    "metadata": {"type": "cave_entrance"},
                    "date": "2020-12-02",
                    "timestamp": "2021-09-16T18:24:48Z",
                    "array": [1, 2, 3],
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-0.01943, 5.881694],
                },
            },
            {
                "type": "Feature",
                "properties": {
                    "id": 124,
                    "ele": 12.4,
                    "name": "Cave2",
                    "metadata": {},
                    "date": "2020-12-02",
                    "timestamp": "2021-09-16T18:24:48Z",
                    "array": [],
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [-0.01993, 5.81137],
                },
            },
        ],
    }
    df = gpd.GeoDataFrame.from_features(collection)
    return df.astype(
        {
            "timestamp": "datetime64[ns]",
            "date": "datetime64",
            "name": "str",
            "array": "object",
            "metadata": "object",
        }
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


def test_inspect_dataset_no_geo(t, df):
    sys.modules["fiona"] = None
    with pytest.raises(ImportError):
        t.inspect_dataset(df)
