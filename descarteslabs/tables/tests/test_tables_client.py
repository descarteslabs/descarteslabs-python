import datetime
import json
import sys

import pytest
import pandas as pd

from descarteslabs.tables.client import Tables
from descarteslabs.client.exceptions import BadRequestError
from descarteslabs.common.proto.vektorius import vektorius_pb2


@pytest.fixture
def tables_client():
    return Tables()


@pytest.fixture
def feature():
    return {
        "type": "Feature",
        "properties": {
            "ele": None,
            "name": "Whispering Cave",
            "osm_id": 3958471845,
            "osm_type": "cave_entrance",
        },
        "geometry": {
            "type": "Point",
            "coordinates": [-12133160.01929433, 4941225.888116944],
        },
    }


def test_normalize_features(tables_client, feature):
    # Single feature
    features = list(tables_client._normalize_features(feature))
    assert features[0] == feature

    # Iterable of features
    features = list(tables_client._normalize_features([feature]))
    assert features[0] == feature

    # FeatureCollection mapping
    collection = {"type": "FeatureCollection", "features": [feature]}
    features = list(tables_client._normalize_features(collection))
    assert features[0] == feature

    # Object with a __geo_interface__ object
    class MockDf(object):
        @property
        def __geo_interface__(self):
            return collection

    features = list(tables_client._normalize_features(MockDf()))
    assert features[0] == feature


@pytest.mark.skipif(
    "geopandas" not in sys.modules, reason="requires the geopandas library"
)
def test_normalize_features_gdf(tables_client, feature):
    # Include the geometry, geopandas dataframe
    import geopandas as gpd

    gdf = gpd.GeoDataFrame.from_features([feature])
    features = list(tables_client._normalize_features(gdf))
    res = features[0]
    # gpd adds these, we strip them out when serializing to JSON
    del res["bbox"]
    del res["id"]
    # ensure they serialize to the same thing
    assert json.dumps(res) == json.dumps(feature)


def test_normalize_features_df(tables_client, feature):
    # Exclude the geometry, properties only, standard pandas dataframe
    df = pd.DataFrame.from_records([feature["properties"]])
    features = list(tables_client._normalize_features(df))
    res = features[0]
    assert res["geometry"] is None

    # expected
    nonspatial_feature = feature
    nonspatial_feature["geometry"] = None

    # ensure they serialize to the same thing
    assert json.dumps(res) == json.dumps(nonspatial_feature)


def test_encode_feature(tables_client, feature):
    bytes_ = tables_client._encode_feature_line(feature)
    assert bytes_.startswith(b"{")
    assert bytes_.endswith(b"}\n")

    with pytest.raises(BadRequestError):
        del feature["properties"]
        tables_client._encode_feature_line(feature)


def test_encode_feature_dates(tables_client, feature):
    feature["properties"]["date"] = datetime.date.fromisoformat("2020-01-01")
    feature["properties"]["datetime"] = datetime.datetime.fromisoformat(
        "2020-01-01T00:05:23"
    )
    bytes_ = tables_client._encode_feature_line(feature)
    assert bytes_.startswith(b"{")
    assert b"2020-01-01" in bytes_
    assert bytes_.endswith(b"}\n")


def test_encode_feature_error(tables_client, feature):
    # Some things are not JSON serializable (yet) and should fail
    feature["properties"]["function"] = dir
    with pytest.raises(TypeError):
        tables_client._encode_feature_line(feature)


def test_wait_until_completion(tables_client):
    status = vektorius_pb2.JobStatus.SUCCESS
    tables_client.check_status = lambda x: (status, "test")
    (status, msg) = tables_client.wait_until_completion("1234", poll_interval=0.0)
    assert status
