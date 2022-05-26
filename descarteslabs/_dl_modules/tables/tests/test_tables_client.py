import datetime
import json
import sys
from unittest.mock import MagicMock, Mock

import pytest
import pandas as pd

from descarteslabs.exceptions import BadRequestError
from ...discover.client import AccessGrant, Asset, SymLink
from ..client import Tables, JobStatus


def mock_dict(d: dict) -> MagicMock:
    mock = MagicMock()
    mock.__getitem__.side_effect = d.__getitem__
    return mock


@pytest.fixture
def tables_client():
    auth = Mock(payload=None)
    discover_client = tables_client.discover_client = Mock(
        spec=[
            "list_assets",
            "list_access_grants",
        ],
    )

    return Tables(discover_client=discover_client, auth=auth)


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
    tables_client.check_status = lambda _: (JobStatus.SUCCESS, "test message")
    (status, msg) = tables_client.wait_until_completion(1234, poll_interval=0.0)
    assert status == JobStatus.SUCCESS


def test_wait_until_completion_fail(tables_client):
    tables_client.check_status = lambda _: (JobStatus.FAILURE, "test message")

    with pytest.raises(RuntimeError):
        tables_client.wait_until_completion(
            1234, poll_interval=0.0, raise_on_failure=True
        )


def test_list_tables(tables_client: Tables):
    assets = [
        Asset(asset_name="asset/vector/some_table.tbl", display_name="Table 1"),
        Asset(asset_name="asset/vector/some_table2.tbl", display_name="Table 2"),
        Asset(
            asset_name="asset/sym_link/link_to_some_table2.tbl",
            display_name="Link to Table 2",
            sym_link=SymLink(
                target_asset_display_name="Table 2",
                target_asset_name="asset/vector/some_table2.tbl",
            ),
        ),
    ]

    grants = [
        [
            AccessGrant(
                asset_name=assets[0].asset_name,
                access="storage/role/owner",
                target_id="test@descarteslabs.com",
            )
        ],
        [
            AccessGrant(
                asset_name=assets[1].asset_name,
                access="storage/role/editor",
                target_id="test@descarteslabs.com",
            ),
            AccessGrant(
                asset_name=assets[1].asset_name,
                access="storage/role/owner",
                target_id="not-you@descarteslabs.com",
            ),
        ],
        [
            AccessGrant(
                asset_name=assets[2].sym_link.target_asset_name,
                access="storage/role/viewer",
                target_id="test@descarteslabs.com",
            ),
            AccessGrant(
                asset_name=assets[2].sym_link.target_asset_name,
                access="storage/role/owner",
                target_id="not-you@descarteslabs.com",
            ),
        ],
    ]

    user = {"email": "test@descarteslabs.com"}
    tables_client.auth.payload = mock_dict(user)
    tables_client.discover_client.list_assets.side_effect = [assets]
    tables_client.discover_client.list_access_grants.side_effect = grants

    response = tables_client.list_tables()
    assert response["owner"] == ["Table 1"]
    assert response["editor"] == {"not-you@descarteslabs.com": ["Table 2"]}
    assert response["viewer"] == {"not-you@descarteslabs.com": ["Table 2"]}
