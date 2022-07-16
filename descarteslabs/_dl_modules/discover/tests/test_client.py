import base64
import json
import re
import time
import unittest
import warnings
from unittest.mock import Mock

import pytest
import responses
from descarteslabs.auth import Auth
from descarteslabs.exceptions import ServerError

from ...client.grpc.exceptions import BadRequest
from ...common.proto.discover import discover_pb2
from ..client import (
    AccessGrant,
    Asset,
    Discover,
    DiscoverGrpcClient,
    Organization,
    UserEmail,
    _IamClient,
)


@pytest.fixture
def discover_grpc_client():
    return Mock(
        spec=[
            "CreateAccessGrant",
            "GetAccessGrant",
            "DeleteAccessGrant",
            "ListAccessGrants",
            "ListAccessGrantsStream",
            "ReplaceAccessGrant",
            "MoveAsset",
            "GetAsset",
            "ListAssets",
            "CreateAsset",
            "DeleteAsset",
        ],
        auth=Mock(namespace=None),
    )


def test_discover_client(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)
    assert client._discover_client == discover_grpc_client


def test_discover_client_default_client():
    client = Discover()
    assert isinstance(client._discover_client, DiscoverGrpcClient)


def test_add_access_grant(discover_grpc_client):
    expected_res = discover_pb2.CreateAccessGrantResponse()
    expected_res.access_grant.asset_name = (
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    )
    expected_res.access_grant.entity.id = "name@fake.com"
    expected_res.access_grant.access = "storage/role/viewer"
    discover_grpc_client.CreateAccessGrant.return_value = expected_res

    client = Discover(discover_client=discover_grpc_client)
    res = client.add_access_grant(
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        UserEmail("name@fake.com"),
        "storage/role/viewer",
    )
    assert res == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="name@fake.com",
        access="storage/role/viewer",
    )


def test_add_access_grant_error(discover_grpc_client):
    discover_grpc_client.CreateAccessGrant.side_effect = BadRequest(
        "Failed to add access grant!"
    )
    client = Discover(discover_client=discover_grpc_client)
    with pytest.raises(BadRequest):
        client.add_access_grant(
            "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
            UserEmail("name@fake.com"),
            "storage/role/viewer",
        )


def test_remove_access_grant(discover_grpc_client):
    discover_grpc_client.DeleteAccessGrant.return_value = (
        discover_pb2.DeleteAccessGrantResponse()
    )
    client = Discover(discover_client=discover_grpc_client)
    res = client.remove_access_grant(
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        "name@fake.com",
        "storage/role/viewer",
    )
    assert res is None


def test_remove_access_grant_error(discover_grpc_client):
    discover_grpc_client.DeleteAccessGrant.side_effect = BadRequest(
        "Failed to remove access grant!"
    )
    client = Discover(discover_client=discover_grpc_client)
    with pytest.raises(BadRequest):
        client.remove_access_grant(
            "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
            "name@fake.com",
            "storage/role/viewer",
        )


def test_replace_access_grant(discover_grpc_client):
    expected_res = discover_pb2.ReplaceAccessGrantResponse()
    expected_res.access_grant.asset_name = (
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    )
    expected_res.access_grant.entity.id = "name@fake.com"
    expected_res.access_grant.access = "storage/role/editor"
    discover_grpc_client.ReplaceAccessGrant.return_value = expected_res

    client = Discover(discover_client=discover_grpc_client)
    res = client.replace_access_grant(
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        "name@fake.com",
        "storage/role/viewer",
        "storage/role/editor",
    )
    assert res == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="name@fake.com",
        access="storage/role/editor",
    )


def test_replace_access_grant_error(discover_grpc_client):
    discover_grpc_client.ReplaceAccessGrant.side_effect = BadRequest(
        "Failed to replace access grant!"
    )
    client = Discover(discover_client=discover_grpc_client)
    with pytest.raises(BadRequest):
        client.replace_access_grant(
            "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
            "name@fake.com",
            "storage/role/viewer",
            "storage/role/editor",
        )


def test_list_access_grants(discover_grpc_client):
    discover_grpc_client.ListAccessGrants.side_effect = [
        discover_pb2.ListAccessGrantsResponse(
            access_grants=[
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test1@fake.com"),
                    access="storage/role/editor",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test2@fake.com"),
                    access="storage/role/viewer",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test3@fake.com"),
                    access="storage/role/editor",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test4@fake.com"),
                    access="storage/role/viewer",
                ),
            ]
        ),
    ]
    client = Discover(discover_client=discover_grpc_client)
    res = client.list_access_grants(
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
    )

    assert len(res) == 4
    assert res[0] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test1@fake.com",
        access="storage/role/editor",
    )
    assert res[1] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test2@fake.com",
        access="storage/role/viewer",
    )
    assert res[2] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test3@fake.com",
        access="storage/role/editor",
    )
    assert res[3] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test4@fake.com",
        access="storage/role/viewer",
    )


def test_list_access_grants_paging(discover_grpc_client):
    discover_grpc_client.ListAccessGrants.side_effect = [
        discover_pb2.ListAccessGrantsResponse(
            access_grants=[
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test1@fake.com"),
                    access="storage/role/editor",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test2@fake.com"),
                    access="storage/role/viewer",
                ),
            ],
            next_page="foo",
        ),
        discover_pb2.ListAccessGrantsResponse(
            access_grants=[
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test3@fake.com"),
                    access="storage/role/editor",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test4@fake.com"),
                    access="storage/role/viewer",
                ),
            ],
        ),
    ]
    client = Discover(discover_client=discover_grpc_client)
    res = client.list_access_grants(
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
    )
    assert len(res) == 4
    assert res[0] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test1@fake.com",
        access="storage/role/editor",
    )
    assert res[1] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test2@fake.com",
        access="storage/role/viewer",
    )
    assert res[2] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test3@fake.com",
        access="storage/role/editor",
    )
    assert res[3] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test4@fake.com",
        access="storage/role/viewer",
    )


def test_list_access_grants_error(discover_grpc_client):
    discover_grpc_client.ListAccessGrants.side_effect = BadRequest(
        "Failed to list access grants!"
    )
    client = Discover(discover_client=discover_grpc_client)
    with pytest.raises(BadRequest):
        client.list_access_grants(
            "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        )


def test_list_access_grants_paging_error(discover_grpc_client):
    discover_grpc_client.ListAccessGrants.side_effect = [
        discover_pb2.ListAccessGrantsResponse(
            access_grants=[
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test1@fake.com"),
                    access="storage/role/editor",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test2@fake.com"),
                    access="storage/role/viewer",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test3@fake.com"),
                    access="storage/role/editor",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test4@fake.com"),
                    access="storage/role/viewer",
                ),
            ],
            next_page="foo",
        ),
        BadRequest("Failed to list access grants!"),
    ]
    client = Discover(discover_client=discover_grpc_client)
    with pytest.raises(BadRequest):
        client.list_access_grants(
            "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        )


def test_blob_request_builder_type(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)
    client._discover_client.auth.namespace = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"
    assert (
        client.blob(
            "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
        )._type()
        == "blob"
    )


def test_resolve_name_blob(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)
    client._discover_client.auth.namespace = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"

    expected_asset_name = (
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    )

    # fully-resolved path
    asset_name = "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    assert client.blob(asset_name)._resolve_name(asset_name) == expected_asset_name

    # just file name
    asset_name = "foo.txt"
    assert client.blob(asset_name)._resolve_name(asset_name) == expected_asset_name

    # user SHA plus file name
    asset_name = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    assert client.blob(asset_name)._resolve_name(asset_name) == expected_asset_name

    expected_asset_name_in_folder = (
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/data/foo.txt"
    )

    # fully-resolved path with folder
    asset_name = "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/data/foo.txt"
    assert (
        client.blob(asset_name)._resolve_name(asset_name)
        == expected_asset_name_in_folder
    )

    # user SHA plus path with folder
    asset_name = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/data/foo.txt"
    assert (
        client.blob(asset_name)._resolve_name(asset_name)
        == expected_asset_name_in_folder
    )

    # fully-resolved with different user SHA
    asset_name = "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16cd90:~/foo.txt"
    assert client.blob(asset_name)._resolve_name(asset_name) == asset_name


def test_resolve_blob_name_error(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)
    client._discover_client.auth.namespace = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"

    # too many :~/
    asset_name = "12345:~/myfolder:~/myfile"
    with pytest.raises(ValueError):
        client.blob(asset_name)._resolve_name(asset_name)

    # no user SHA plus path (with folder)
    asset_name = "asset/blob:~/data/foo.txt"
    with pytest.raises(ValueError):
        client.blob(asset_name)._resolve_name(asset_name)

    # no type plus path
    asset_name = "asset/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    with pytest.raises(ValueError):
        client.blob(asset_name)._resolve_name(asset_name)


def test_folder_request_builder_type(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)
    assert (
        client.folder("asset/folder/f050081dad10d31faf16bd43c377ead5")._type()
        == "folder"
    )


def test_resolve_name_folder(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)

    expected_folder_name = "asset/folder/f050081dad10d31faf16bd43c377ead5"

    # fully-resolved folder name
    asset_name = "asset/folder/f050081dad10d31faf16bd43c377ead5"
    assert client.folder(asset_name)._resolve_name(asset_name) == expected_folder_name

    # partially-resolved with resource name
    asset_name = "folder/f050081dad10d31faf16bd43c377ead5"
    assert client.folder(asset_name)._resolve_name(asset_name) == expected_folder_name

    # just resource name
    asset_name = "f050081dad10d31faf16bd43c377ead5"
    assert client.folder(asset_name)._resolve_name(asset_name) == expected_folder_name


def test_resolve_folder_name_error(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)

    # bad UUID, not long enough
    asset_name = "c6cdbf1cb7c84519ae6f"
    with pytest.raises(ValueError):
        client.folder(asset_name)._resolve_name(asset_name)

    # bad UUID, not hexadecimal
    asset_name = "fhkdjsaf&$sadsecretcodefdsffdsaahihihihi"
    with pytest.raises(ValueError):
        client.folder(asset_name)._resolve_name(asset_name)

    # no type
    asset_name = "asset/f050081dad10d31faf16bd43c377ead5"
    with pytest.raises(ValueError):
        client.folder(asset_name)._resolve_name(asset_name)


def test_request_builder_share_blob(discover_grpc_client):
    expected_res = discover_pb2.CreateAccessGrantResponse()
    expected_res.access_grant.asset_name = (
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    )
    expected_res.access_grant.entity.id = "name@fake.com"
    expected_res.access_grant.access = "storage/role/viewer"
    discover_grpc_client.CreateAccessGrant.return_value = expected_res
    client = Discover(discover_client=discover_grpc_client)
    client._discover_client.auth.namespace = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"
    res = client.blob(
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    ).share(with_="name@fake.com", as_="storage/role/viewer")

    assert res == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="name@fake.com",
        access="storage/role/viewer",
    )


def test_request_builder_revoke_blob(discover_grpc_client):
    discover_grpc_client.DeleteAccessGrant.return_value = (
        discover_pb2.DeleteAccessGrantResponse()
    )
    client = Discover(discover_client=discover_grpc_client)
    client._discover_client.auth.namespace = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"
    res = client.blob(
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    ).revoke(
        from_="name@fake.com",
        as_="storage/role/viewer",
    )
    assert res is None


def test_request_builder_replace_blob(discover_grpc_client):
    expected_res = discover_pb2.ReplaceAccessGrantResponse()
    expected_res.access_grant.asset_name = (
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    )
    expected_res.access_grant.entity.id = "name@fake.com"
    expected_res.access_grant.access = "storage/role/editor"
    discover_grpc_client.ReplaceAccessGrant.return_value = expected_res

    client = Discover(discover_client=discover_grpc_client)
    client._discover_client.auth.namespace = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"
    res = client.blob(
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    ).replace_shares(
        user="name@fake.com",
        from_role="storage/role/viewer",
        to_role="storage/role/editor",
    )
    assert res == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="name@fake.com",
        access="storage/role/editor",
    )


def test_request_builder_list_blob(discover_grpc_client):
    discover_grpc_client.ListAccessGrants.side_effect = [
        discover_pb2.ListAccessGrantsResponse(
            access_grants=[
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test1@fake.com"),
                    access="storage/role/editor",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test2@fake.com"),
                    access="storage/role/viewer",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test3@fake.com"),
                    access="storage/role/editor",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
                    entity=discover_pb2.Entity(type="user-email", id="test4@fake.com"),
                    access="storage/role/viewer",
                ),
            ]
        ),
    ]
    client = Discover(discover_client=discover_grpc_client)
    client._discover_client.auth.namespace = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"
    res = client.blob(
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    ).list_shares()

    assert len(res) == 4
    assert res[0] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test1@fake.com",
        access="storage/role/editor",
    )
    assert res[1] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test2@fake.com",
        access="storage/role/viewer",
    )
    assert res[2] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test3@fake.com",
        access="storage/role/editor",
    )
    assert res[3] == AccessGrant(
        asset_name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        target_id="test4@fake.com",
        access="storage/role/viewer",
    )


def test_request_builder_share_folder(discover_grpc_client):
    expected_res = discover_pb2.CreateAccessGrantResponse()
    expected_res.access_grant.asset_name = (
        "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    )
    expected_res.access_grant.entity.id = "name@fake.com"
    expected_res.access_grant.access = "discover/role/viewer"
    discover_grpc_client.CreateAccessGrant.return_value = expected_res

    client = Discover(discover_client=discover_grpc_client)
    res = client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").share(
        with_="name@fake.com", as_="discover/role/viewer"
    )

    assert res == AccessGrant(
        asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
        target_id="name@fake.com",
        access="discover/role/viewer",
    )


def test_request_builder_revoke_folder(discover_grpc_client):
    discover_grpc_client.DeleteAccessGrant.return_value = (
        discover_pb2.DeleteAccessGrantResponse()
    )
    client = Discover(discover_client=discover_grpc_client)
    res = client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").revoke(
        from_="name@fake.com",
        as_="discover/role/viewer",
    )
    assert res is None


def test_request_builder_replace_folder(discover_grpc_client):
    expected_res = discover_pb2.ReplaceAccessGrantResponse()
    expected_res.access_grant.asset_name = (
        "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    )
    expected_res.access_grant.entity.id = "name@fake.com"
    expected_res.access_grant.access = "discover/role/editor"
    discover_grpc_client.ReplaceAccessGrant.return_value = expected_res

    client = Discover(discover_client=discover_grpc_client)
    res = client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").replace_shares(
        user="name@fake.com",
        from_role="discover/role/viewer",
        to_role="discover/role/editor",
    )
    assert res == AccessGrant(
        asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
        target_id="name@fake.com",
        access="discover/role/editor",
    )


def test_request_builder_list_folder(discover_grpc_client):
    discover_grpc_client.ListAccessGrants.side_effect = [
        discover_pb2.ListAccessGrantsResponse(
            access_grants=[
                discover_pb2.AccessGrant(
                    asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
                    entity=discover_pb2.Entity(type="user-email", id="test1@fake.com"),
                    access="discover/role/editor",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
                    entity=discover_pb2.Entity(type="user-email", id="test2@fake.com"),
                    access="discover/role/viewer",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
                    entity=discover_pb2.Entity(type="user-email", id="test3@fake.com"),
                    access="discover/role/editor",
                ),
                discover_pb2.AccessGrant(
                    asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
                    entity=discover_pb2.Entity(type="user-email", id="test4@fake.com"),
                    access="discover/role/viewer",
                ),
            ]
        ),
    ]
    client = Discover(discover_client=discover_grpc_client)
    res = client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").list_shares()

    assert len(res) == 4
    assert res[0] == AccessGrant(
        asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
        target_id="test1@fake.com",
        access="discover/role/editor",
    )
    assert res[1] == AccessGrant(
        asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
        target_id="test2@fake.com",
        access="discover/role/viewer",
    )
    assert res[2] == AccessGrant(
        asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
        target_id="test3@fake.com",
        access="discover/role/editor",
    )
    assert res[3] == AccessGrant(
        asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
        target_id="test4@fake.com",
        access="discover/role/viewer",
    )


def test_move_asset(discover_grpc_client):
    asset_name = "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    display_name = "My foo asset"
    description = "it's an asset!"
    parent_asset_name = "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    discover_grpc_client.GetAsset.return_value = discover_pb2.GetAssetResponse(
        asset=discover_pb2.Asset(
            name=asset_name,
            display_name=display_name,
            description=description,
            parent_name=parent_asset_name,
        )
    )

    client = Discover(discover_client=discover_grpc_client)

    blob = client.move_asset(asset_name, parent_asset_name)

    assert asset_name == blob.asset_name
    assert display_name == blob.display_name
    assert description == blob.description
    assert parent_asset_name == blob.parent_asset_name

    assert 1 == discover_grpc_client.MoveAsset.call_count


def test_move_asset_bad_request(discover_grpc_client):
    asset_name = "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    parent_asset_name = "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    discover_grpc_client.MoveAsset.side_effect = BadRequest("bad parent")

    client = Discover(discover_client=discover_grpc_client)

    with pytest.raises(BadRequest):
        client.move_asset(asset_name, parent_asset_name)


def test_list_assets_single_page(discover_grpc_client):
    asset_name = "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    exp_asset = discover_pb2.Asset(
        name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        display_name="asset 1",
        description="asset 1 desc",
        parent_name=asset_name,
    )
    discover_grpc_client.ListAssets.side_effect = [
        discover_pb2.ListAssetsResponse(assets=[exp_asset]),
        discover_pb2.ListAssetsResponse(assets=[]),
    ]

    client = Discover(discover_client=discover_grpc_client)

    assets = client.list_assets(asset_name)
    assert 1 == len(assets)

    asset = assets[0]
    assert asset.asset_name == exp_asset.name
    assert asset.display_name == exp_asset.display_name
    assert asset.description == exp_asset.description
    assert asset.parent_asset_name == exp_asset.parent_name

    # annoying
    assert 2 == discover_grpc_client.ListAssets.call_count


def test_list_assets_with_filters(discover_grpc_client):
    asset_name = "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    exp_asset = discover_pb2.Asset(
        name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt",
        display_name="asset 1",
        description="asset 1 desc",
        parent_name=asset_name,
    )

    discover_grpc_client.ListAssets.side_effect = [
        discover_pb2.ListAssetsResponse(assets=[exp_asset]),
        discover_pb2.ListAssetsResponse(assets=[]),
    ]
    client = Discover(discover_client=discover_grpc_client)

    filters = {"type": "blob"}
    assets = client.list_assets(asset_name, filters)

    discover_grpc_client.ListAssets.assert_called_with(
        discover_pb2.ListAssetsRequest(
            parent_name=asset_name,
            page_token=None,
            filter="type=blob",
        )
    )
    assert 1 == len(assets)

    discover_grpc_client.ListAssets.side_effect = [
        discover_pb2.ListAssetsResponse(assets=[exp_asset]),
        discover_pb2.ListAssetsResponse(assets=[]),
    ]
    filters = {"type": ["blob", "vector"], "name": "as?et *"}
    assets = client.list_assets(asset_name, filters)

    discover_grpc_client.ListAssets.assert_called_with(
        discover_pb2.ListAssetsRequest(
            parent_name=asset_name,
            page_token=None,
            filter="type=blob&type=vector&name=as?et *",
        )
    )
    assert 1 == len(assets)


def test_list_assets_with_invalid_filters(discover_grpc_client):
    asset_name = "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    client = Discover(discover_client=discover_grpc_client)

    with pytest.raises(KeyError, match="Allowed fields are: type,name"):
        client.list_assets(asset_name, filters={"non existent": "not here"})

    with pytest.raises(ValueError, match="Type must be one or more of"):
        client.list_assets(asset_name, filters={"type": "asdf"})


def test_list_assets_multiple_page(discover_grpc_client):
    asset_name = "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    exp_asset1 = discover_pb2.Asset(
        name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo1.txt",
        display_name="asset 1",
        description="asset 1 desc",
        parent_name=asset_name,
    )
    exp_asset2 = discover_pb2.Asset(
        name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo2.txt",
        display_name="asset 2",
        description="asset 2 desc",
        parent_name=asset_name,
    )
    discover_grpc_client.ListAssets.side_effect = [
        discover_pb2.ListAssetsResponse(assets=[exp_asset1]),
        discover_pb2.ListAssetsResponse(assets=[exp_asset2]),
        discover_pb2.ListAssetsResponse(assets=[]),
    ]

    client = Discover(discover_client=discover_grpc_client)

    assets = client.list_assets(asset_name)
    assert 2 == len(assets)

    assert assets[0].asset_name == exp_asset1.name
    assert assets[0].display_name == exp_asset1.display_name
    assert assets[0].description == exp_asset1.description
    assert assets[0].parent_asset_name == exp_asset1.parent_name
    assert assets[1].asset_name == exp_asset2.name
    assert assets[1].display_name == exp_asset2.display_name
    assert assets[1].description == exp_asset2.description
    assert assets[1].parent_asset_name == exp_asset2.parent_name

    # annoying
    assert 3 == discover_grpc_client.ListAssets.call_count


def test_list_assets_error(discover_grpc_client):
    asset_name = "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    exp_asset1 = discover_pb2.Asset(
        name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo1.txt",
        display_name="asset 1",
        description="asset 1 desc",
        parent_name=asset_name,
    )
    exp_asset2 = discover_pb2.Asset(
        name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo2.txt",
        display_name="asset 2",
        description="asset 2 desc",
        parent_name=asset_name,
    )
    discover_grpc_client.ListAssets.side_effect = [
        discover_pb2.ListAssetsResponse(assets=[exp_asset1]),
        BadRequest("oh no!"),
        discover_pb2.ListAssetsResponse(assets=[exp_asset2]),
    ]

    client = Discover(discover_client=discover_grpc_client)

    with pytest.raises(BadRequest):
        client.list_assets(asset_name)


def test_list_assets_no_asset_name(discover_grpc_client):
    namespace = "asset/namespace/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"

    expected_asset1 = discover_pb2.Asset(
        name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo1.txt",
        display_name="asset 1",
        description="asset 1 desc",
        parent_name=namespace,
    )

    expected_asset2 = discover_pb2.Asset(
        name="asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo2.txt",
        display_name="asset 2",
        description="asset 2 desc",
        parent_name=namespace,
    )

    discover_grpc_client.ListAssets.side_effect = [
        discover_pb2.ListAssetsResponse(assets=[expected_asset1]),
        discover_pb2.ListAssetsResponse(assets=[expected_asset2]),
        discover_pb2.ListAssetsResponse(assets=[]),
        discover_pb2.ListAssetsResponse(assets=[expected_asset1]),
        discover_pb2.ListAssetsResponse(assets=[expected_asset2]),
        discover_pb2.ListAssetsResponse(assets=[]),
    ]

    expected_assets = [
        Asset._from_proto_asset(expected_asset1),
        Asset._from_proto_asset(expected_asset2),
    ]

    def asset_names(assets):
        return {asset.asset_name for asset in assets}

    client = Discover(discover_client=discover_grpc_client)
    assert asset_names(client.list_assets()) == asset_names(expected_assets)
    assert asset_names(client.list_assets(asset_name=namespace)) == asset_names(
        expected_assets
    )


def test_create_folder(discover_grpc_client):
    asset_name = "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"
    parent_name = "asset/folder/c9285cea44d14a70879099dc98c39b6cd6a1875f"
    description = "Just another folder, yo."
    display_name = "JUST_ANOTHER_FOLDER_YO"

    expected_response = discover_pb2.CreateAssetResponse(
        asset=discover_pb2.Asset(
            name=asset_name,
            display_name=display_name,
            description=description,
            parent_name=parent_name,
        )
    )

    discover_grpc_client.CreateAsset.return_value = expected_response

    client = Discover(discover_client=discover_grpc_client)
    asset = client.create_folder(
        display_name="JUST_ANOTHER_FOLDER_YO",
        parent_asset_name="asset/folder/c9285cea44d14a70879099dc98c39b6cd6a1875f",
        description="Just another folder, yo.",
    )

    assert asset.asset_name == expected_response.asset.name
    assert asset.parent_asset_name == expected_response.asset.parent_name
    assert asset.description == expected_response.asset.description
    assert asset.display_name == expected_response.asset.display_name


def test_create_folder_raises_on_none_display_name(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)

    with pytest.raises(ValueError):
        client.create_folder(display_name=None)


def test_create_folder_raises_on_empty_display_name(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)

    with pytest.raises(ValueError):
        client.create_folder(display_name="")


def test_delete_asset_raises_on_none_asset_name(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)

    with pytest.raises(ValueError):
        client.delete_asset(None)


def test_delete_asset_raises_on_empty_asset_name(discover_grpc_client):
    client = Discover(discover_client=discover_grpc_client)

    with pytest.raises(ValueError):
        client.delete_asset("")


def test_delete_asset(discover_grpc_client):
    asset_name = "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"

    discover_grpc_client.DeleteAsset.return_value = discover_pb2.DeleteAssetResponse()

    client = Discover(discover_client=discover_grpc_client)
    assert client.delete_asset(asset_name) is None


def test_symlink_assets_have_target_asset_name_and_display_name(discover_grpc_client):
    parent_name = "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    exp_asset = discover_pb2.Asset(
        name="asset/sym_link/1ab460e3aa2845bdd970e9cbc5dab306",
        display_name="",
        shared=True,
        sym_link=discover_pb2.SymLink(
            target_name="asset/blob/5d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea2:~/20210603.A_STAGE_FILE.txt",
            target_display_name="A Stage File",
        ),
        description="symlink 1 desc",
        parent_name=parent_name,
    )
    discover_grpc_client.ListAssets.side_effect = [
        discover_pb2.ListAssetsResponse(assets=[exp_asset]),
        discover_pb2.ListAssetsResponse(assets=[]),
    ]

    client = Discover(discover_client=discover_grpc_client)
    symlinks = [
        x for x in client.list_assets(asset_name="") if "sym_link" in x.asset_name
    ]
    symlink = symlinks[0]

    assert len(symlinks) == 1
    assert symlink.asset_name == exp_asset.name
    assert symlink.display_name == exp_asset.display_name
    assert symlink.is_shared == exp_asset.shared
    assert symlink.sym_link.target_asset_name == exp_asset.sym_link.target_name
    assert symlink.description == exp_asset.description
    assert symlink.parent_asset_name == exp_asset.parent_name
    assert (
        symlink.sym_link.target_asset_display_name
        == exp_asset.sym_link.target_display_name
    )

    assert discover_grpc_client.ListAssets.call_count == 2


def test_share_folder_with_shortcut_roles(discover_grpc_client):
    expected_res = discover_pb2.CreateAccessGrantResponse()
    expected_res.access_grant.asset_name = (
        "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    )
    expected_res.access_grant.entity.id = "name@fake.com"
    expected_res.access_grant.access = "discover/role/viewer"
    discover_grpc_client.CreateAccessGrant.return_value = expected_res

    client = Discover(discover_client=discover_grpc_client)
    res = client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").share(
        with_="name@fake.com", as_="viewer"
    )

    assert res == AccessGrant(
        asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
        target_id="name@fake.com",
        access="discover/role/viewer",
    )


def test_share_folder_fails_with_wrong_shortcut_role(discover_grpc_client):
    discover_grpc_client.CreateAccessGrant.return_value = None

    with pytest.raises(ValueError):
        client = Discover(discover_client=discover_grpc_client)
        client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").share(
            with_="name@fake.com", as_="foo/bar/baz"
        )
    with pytest.raises(ValueError):
        client = Discover(discover_client=discover_grpc_client)
        client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").share(
            with_="name@fake.com", as_="owner"
        )
    with pytest.raises(ValueError):
        client = Discover(discover_client=discover_grpc_client)
        client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").share(
            with_="name@fake.com", as_="storage/role/viewer"
        )


def test_replace_folder_with_shortcut_roles(discover_grpc_client):
    expected_res = discover_pb2.ReplaceAccessGrantResponse()
    expected_res.access_grant.asset_name = (
        "asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de"
    )
    expected_res.access_grant.entity.id = "name@fake.com"
    expected_res.access_grant.access = "discover/role/editor"
    discover_grpc_client.ReplaceAccessGrant.return_value = expected_res

    client = Discover(discover_client=discover_grpc_client)
    res = client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").replace_shares(
        user="name@fake.com",
        from_role="viewer",
        to_role="editor",
    )
    assert res == AccessGrant(
        asset_name="asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de",
        target_id="name@fake.com",
        access="discover/role/editor",
    )


def test_replace_folder_fails_with_wrong_shortcut_role(discover_grpc_client):
    discover_grpc_client.ReplaceAccessGrant.return_value = None

    with pytest.raises(ValueError):
        client = Discover(discover_client=discover_grpc_client)
        client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").replace_shares(
            user="name@fake.com",
            from_role="foo",
            to_role="bar",
        )
    with pytest.raises(ValueError):
        client = Discover(discover_client=discover_grpc_client)
        client.folder("asset/folder/3d7bf4b0b1f4e6283e5cbeaadddbc6de").replace_shares(
            user="name@fake.com",
            from_role="viewer",
            to_role="owner",
        )


def test_revoke_folder_with_shortcut_role(discover_grpc_client):
    discover_grpc_client.DeleteAccessGrant.return_value = (
        discover_pb2.DeleteAccessGrantResponse()
    )
    client = Discover(discover_client=discover_grpc_client)
    client._discover_client.auth.namespace = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"
    res = client.blob(
        "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
    ).revoke(
        from_="name@fake.com",
        as_="viewer",
    )
    assert res is None


def test_revoke_folder_fails_with_wrong_shortcut_role(discover_grpc_client):
    discover_grpc_client.DeleteAccessGrant.return_value = (
        discover_pb2.DeleteAccessGrantResponse()
    )
    client = Discover(discover_client=discover_grpc_client)
    client._discover_client.auth.namespace = "3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1"
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        client.blob(
            "asset/blob/3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1:~/foo.txt"
        ).revoke(
            from_="name@fake.com",
            as_="discover/role/viewer",
        )
        assert len(w) == 1
        assert issubclass(w[-1].category, UserWarning)


class TestListOrgUsers(unittest.TestCase):
    payload = (
        base64.b64encode(
            json.dumps(
                {
                    "aud": "ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
                    "exp": time.time() + 3600,
                }
            ).encode()
        )
        .decode()
        .strip("=")
    )
    public_token = f"header.{payload}.signature"

    url = "https://foo.com"
    url_match = re.compile(url)

    def setUp(self):
        _IamClient.set_default_client(
            _IamClient(
                url=self.url,
                auth=Auth(jwt_token=self.public_token, token_info_path=None),
            )
        )

    @responses.activate
    def test_list_org_users_empty(self):
        responses.add(responses.GET, self.url_match, json=[], status=200)
        users = Discover().list_org_users()
        assert users == []

    @responses.activate
    def test_list_org_users(self):
        json = [
            {"name": "Foo", "email": "foo@bar.com"},
            {"name": "Bar", "email": "fubar@fubar.com"},
            {"name": "Else", "email": "else@else.com"},
        ]
        responses.add(
            responses.GET,
            self.url_match,
            json=json,
            status=200,
        )
        users = Discover().list_org_users()
        assert users == json

    @responses.activate
    def test_list_org_users_search(self):
        responses.add(
            responses.GET,
            self.url_match,
            json=[],
            status=200,
        )
        Discover().list_org_users(search="bar")
        assert responses.calls[0].request.url.endswith("q=bar")

    @responses.activate
    def test_list_org_users_unauthorized(self):
        responses.add(
            responses.GET,
            self.url_match,
            status=401,
        )

        with self.assertRaises(ServerError) as e:
            Discover().list_org_users()
            assert e.original_status == 401


class TestOrgUsers(unittest.TestCase):
    def test_email_and_org(self):
        assert "test@foo.bar" == UserEmail("test@foo.bar")
        assert "email:test@foo.bar" == UserEmail("test@foo.bar")
        assert "test@foo.bar" == UserEmail("email:test@foo.bar")
        assert "email:test@foo.bar" == UserEmail("email:test@foo.bar")

        assert "myorg" == Organization("myorg")
        assert "org:myorg" == Organization("myorg")
        assert "myorg" == Organization("org:myorg")
        assert "org:myorg" == Organization("org:myorg")

        with self.assertRaises(ValueError):
            UserEmail("test@")
        with self.assertRaises(ValueError):
            UserEmail("test@.")
        with self.assertRaises(ValueError):
            UserEmail("foo.bar")
        with self.assertRaises(ValueError):
            UserEmail("@foo.bar")
        with self.assertRaises(ValueError):
            UserEmail("test")

        with self.assertRaises(ValueError):
            Organization("x@y.z")
        with self.assertRaises(ValueError):
            Organization("SomeOrg")

        a = Organization("aaa")
        b = Organization("bbb")

        assert not (a == b)
        assert a != b
        assert not (a > b)
        assert not (a >= b)
        assert a < b
        assert a <= b
        assert a >= a
        assert a <= a
        assert not ("aaa" > b)
        assert "aaa" >= a
