# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# -*- coding: utf-8 -*-
import copy
import json
import pytest
import responses

import textwrap

import shapely.geometry

from datetime import datetime
from tempfile import NamedTemporaryFile
from unittest.mock import patch

from descarteslabs.exceptions import BadRequestError
from .base import ClientTestCase
from ..attributes import AttributeValidationError
from ..blob import Blob, BlobCollection, BlobSearch, StorageType
from ..blob_upload import BlobUpload
from ..catalog_base import DocumentState, DeletedObjectError
from ...common.property_filtering import Properties


def _blob_do_download(_, dest=None, range=None):
    mock_data = b"This is mock download data. It can be any binary data."

    if range:
        if isinstance(range, str):
            range_str = range
        elif isinstance(range, (list, tuple)) and all(
            map(lambda x: isinstance(x, int), range)
        ):
            if len(range) == 1:
                range_str = f"bytes={range[0]}"
            elif len(range) == 2:
                range_str = f"bytes={range[0]}-{range[1]}"
            else:
                raise ValueError("invalid range value")
        else:
            raise ValueError("invalid range value")

        if len(range_str.split("-")) == 2:
            start_byte = int(range_str.split("-")[0].split("=")[-1])
            end_byte = int(range_str.split("-")[-1])
            mock_data = mock_data[start_byte:end_byte]

        elif len(range_str.split("-")) == 1:
            start_byte = int(range_str.split("=")[-1])
            mock_data = mock_data[start_byte:]

    if dest is None:
        return mock_data
    else:
        dest.write(mock_data)
        return dest.name


class TestBlob(ClientTestCase):
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [-95.2989209, 42.7999878],
                [-93.1167728, 42.3858464],
                [-93.7138666, 40.703737],
                [-95.8364984, 41.1150618],
                [-95.2989209, 42.7999878],
            ]
        ],
    }

    test_combinations = [
        {"value": b"This is mock download data. It can be any binary data."},
        {"range": (0, 4), "value": b"This"},
        {"range": "bytes=0-4", "value": b"This"},
        {"range": (49,), "value": b"data."},
        {"range": "bytes=49", "value": b"data."},
    ]

    def test_constructor(self):
        b = Blob(
            name="test-blob",
            id="data/descarteslabs:test-namespace/test-blob",
            storage_type="data",
            storage_state="available",
            description="a description",
            expires="2023-01-01",
            tags=["TESTING BLOB"],
        )

        assert b.name == "test-blob"
        assert b.id == "data/descarteslabs:test-namespace/test-blob"
        assert b.storage_type == StorageType.DATA
        assert b.storage_state == "available"
        assert b.description == "a description"
        assert b.tags == ["TESTING BLOB"]
        assert b.state == DocumentState.UNSAVED

    def test_repr(self):
        b = Blob(
            name="test-blob",
            id="data/descarteslabs:test-namespace/test-blob",
            storage_type="data",
            storage_state="available",
            description="a description",
            expires="2023-01-01",
            tags=["TESTING BLOB"],
        )
        b_repr = repr(b)
        match_str = """\
            Blob: test-blob
              id: data/descarteslabs:test-namespace/test-blob
            * Not up-to-date in the Descartes Labs catalog. Call `.save()` to save or update this record."""
        assert b_repr.strip("\n") == textwrap.dedent(match_str)

    def test_set_geometry(self):
        shape = shapely.geometry.shape(self.geometry)
        b = Blob(id="data/descarteslabs:test/test-blob", name="test-blob")
        b.geometry = self.geometry
        assert shape == b.geometry

        b.geometry = shape
        assert shape == b.geometry

        with pytest.raises(AttributeValidationError):
            b.geometry = {"type": "Lollipop"}
        with pytest.raises(AttributeValidationError):
            b.geometry = 2

    def test_storage_type_new(self):
        b = Blob(
            id="data/descarteslabs:test-namespace/test-blob",
            name="test-blob",
            storage_type="nodata",
            _saved=True,
        )
        assert b.description is None
        assert b.storage_type == "nodata"

        with pytest.raises(ValueError):
            StorageType("nodata")

    def test_search_intersects(self):
        search = Blob.search().intersects(self.geometry).filter(Properties().id == "b1")
        _, request_params = search._to_request()
        assert self.geometry == json.loads(request_params["intersects"])

    @responses.activate
    def test_get(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "expires": None,
                        "extra_properties": {},
                        "geometry": self.geometry,
                        "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                        "href": "s3://super/long/uri/data/descarteslabs:test-namespace/test-blob",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-blob",
                        "namespace": "descarteslabs:test-namespace",
                        "owners": ["org:descarteslabs"],
                        "readers": ["org:descarteslabs"],
                        "writers": [],
                        "size_bytes": 1008,
                        "storage_state": "available",
                        "storage_type": "data",
                        "tags": ["TESTING BLOB"],
                    },
                    "id": "data/descarteslabs:test-namespace/test-blob",
                    "type": "storage",
                }
            },
            status=200,
        )

        b = Blob.get(
            id="data/descarteslabs:test-namespace/test-blob", client=self.client
        )
        assert isinstance(b.created, datetime)
        assert b.description == "a generic description"
        assert b.expires is None
        assert b.geometry == shapely.geometry.shape(self.geometry)
        assert b.hash == "28495fde1c101c01f2d3ae92d1af85a5"
        assert (
            b.href == "s3://super/long/uri/data/descarteslabs:test-namespace/test-blob"
        )
        assert isinstance(b.modified, datetime)
        assert b.name == "test-blob"
        assert b.namespace == "descarteslabs:test-namespace"
        assert b.owners == ["org:descarteslabs"]
        assert b.readers == ["org:descarteslabs"]
        assert b.writers == []
        assert b.size_bytes == 1008
        assert b.storage_type == StorageType.DATA
        assert b.tags == ["TESTING BLOB"]

    @responses.activate
    def test_get_unknown_attribute(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "expires": None,
                        "extra_properties": {},
                        "geometry": self.geometry,
                        "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                        "href": "s3://super/long/uri/data/descarteslabs:test-namespace/test-blob",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-blob",
                        "namespace": "descarteslabs:test-namespace",
                        "owners": ["org:descarteslabs"],
                        "readers": ["org:descarteslabs"],
                        "writers": [],
                        "size_bytes": 1008,
                        "storage_state": "available",
                        "storage_type": "data",
                        "tags": ["TESTING BLOB"],
                        "foobar": "unknown",
                    },
                    "id": "data/descarteslabs:test-namespace/test-blob",
                    "type": "storage",
                }
            },
            status=200,
        )

        b = Blob.get(
            id="data/descarteslabs:test-namespace/test-blob", client=self.client
        )
        assert not hasattr(b, "foobar")

    @responses.activate
    def test_get_many(self):
        self.mock_response(
            responses.PUT,
            {
                "data": [
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "expires": None,
                            "extra_properties": {},
                            "geometry": self.geometry,
                            "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                            "href": "s3://super/long/uri/data/descarteslabs:test-namespace/test-blob-1",
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-blob-1",
                            "namespace": "descarteslabs:test-namespace",
                            "owners": ["org:descarteslabs"],
                            "readers": ["org:descarteslabs"],
                            "writers": [],
                            "size_bytes": 1008,
                            "storage_state": "available",
                            "storage_type": "data",
                            "tags": ["TESTING BLOB"],
                        },
                        "id": "data/descarteslabs:test-namespace/test-blob-1",
                        "type": "storage",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "expires": None,
                            "extra_properties": {},
                            "geometry": self.geometry,
                            "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                            "href": "s3://super/long/uri/data/descarteslabs:test-namespace/test-blob-2",
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-blob-2",
                            "namespace": "descarteslabs:test-namespace",
                            "owners": ["org:descarteslabs"],
                            "readers": ["org:descarteslabs"],
                            "writers": [],
                            "size_bytes": 1008,
                            "storage_state": "available",
                            "storage_type": "data",
                            "tags": ["TESTING BLOB"],
                        },
                        "id": "data/descarteslabs:test-namespace/test-blob-2",
                        "type": "storage",
                    },
                ],
            },
            status=200,
        )

        blobs = Blob.get_many(
            [
                "data/descarteslabs:test-namespace/test-blob-1",
                "data/descarteslabs:test-namespace/test-blob-2",
            ],
            client=self.client,
        )

        for i, b in enumerate(blobs):
            assert isinstance(b, Blob)
            assert b.id == f"data/descarteslabs:test-namespace/test-blob-{i + 1}"

    @responses.activate
    def test_get_or_create(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "expires": None,
                        "extra_properties": {},
                        "geometry": self.geometry,
                        "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                        "href": "s3://super/long/uri/data/descarteslabs:test-namespace/test-blob",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-blob",
                        "namespace": "descarteslabs:test-namespace",
                        "owners": ["org:descarteslabs"],
                        "readers": ["org:descarteslabs"],
                        "writers": [],
                        "size_bytes": 1008,
                        "storage_state": "available",
                        "storage_type": "data",
                        "tags": ["TESTING BLOB"],
                    },
                    "id": "data/descarteslabs:test-namespace/test-blob",
                    "type": "storage",
                }
            },
            status=200,
        )

        b = Blob.get_or_create(
            id="data/descarteslabs:test-namespace/test-blob", client=self.client
        )
        assert b.id == "data/descarteslabs:test-namespace/test-blob"

    @responses.activate
    def test_list(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 2},
                "links": {"self": "https://example.com/catalog/v2/storage"},
                "data": [
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "expires": None,
                            "extra_properties": {},
                            "geometry": self.geometry,
                            "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                            "href": "s3://super/long/uri/data/descarteslabs:test-namespace/test-blob-1",
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-blob-1",
                            "namespace": "descarteslabs:test-namespace",
                            "owners": ["org:descarteslabs"],
                            "readers": ["org:descarteslabs"],
                            "writers": [],
                            "size_bytes": 1008,
                            "storage_state": "available",
                            "storage_type": "data",
                            "tags": ["TESTING BLOB"],
                        },
                        "id": "data/descarteslabs:test-namespace/test-blob-1",
                        "type": "storage",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "expires": None,
                            "extra_properties": {},
                            "geometry": self.geometry,
                            "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                            "href": "s3://super/long/uri/data/descarteslabs:test-namespace/test-blob-2",
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-blob-2",
                            "namespace": "descarteslabs:test-namespace",
                            "owners": ["org:descarteslabs"],
                            "readers": ["org:descarteslabs"],
                            "writers": [],
                            "size_bytes": 1008,
                            "storage_state": "available",
                            "storage_type": "data",
                            "tags": ["TESTING BLOB"],
                        },
                        "id": "data/descarteslabs:test-namespace/test-blob-2",
                        "type": "storage",
                    },
                ],
            },
            status=200,
        )

        search = Blob.search(client=self.client)
        assert search.count() == 2
        assert isinstance(search, BlobSearch)
        bc = search.collect()
        assert isinstance(bc, BlobCollection)

    @responses.activate
    def test_list_no_results(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 0},
                "data": [],
            },
        )

        r = list(Blob.search(client=self.client))
        assert r == []

    @responses.activate
    def test_save(self):
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "expires": None,
                        "extra_properties": {},
                        "geometry": self.geometry,
                        "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                        "href": "s3://super/long/uri/data/descarteslabs:test-namespace/test-blob",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-blob",
                        "namespace": "descarteslabs:test-namespace",
                        "owners": ["org:descarteslabs"],
                        "readers": ["org:descarteslabs"],
                        "writers": [],
                        "size_bytes": 1008,
                        "storage_state": "available",
                        "storage_type": "data",
                        "tags": ["TESTING BLOB"],
                    },
                    "id": "data/descarteslabs:test-namespace/test-blob",
                    "type": "storage",
                }
            },
            status=200,
        )

        b = Blob(
            id="data/descarteslabs:test-namespace/test-blob",
            name="test-blob",
            storage_state="available",
            client=self.client,
        )
        assert b.state == DocumentState.UNSAVED
        b.save()
        assert responses.calls[0].request.url == self.url + "/storage"
        assert b.state == DocumentState.SAVED

    @responses.activate
    def test_save_dupe(self):
        self.mock_response(
            responses.POST,
            {
                "errors": [
                    {
                        "status": "400",
                        "detail": "A document with id `data/descarteslabs:test-namespace/test-blob` already exists.",
                        "title": "Bad request",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=400,
        )
        b = Blob(id="data/descarteslabs:test-namespace/test-blob", client=self.client)
        with pytest.raises(BadRequestError):
            b.save()

    @responses.activate
    def test_exists(self):
        self.mock_response(responses.HEAD, {}, status=200)
        assert Blob.exists(
            "data/descarteslabs:test-namespace/test-blob", client=self.client
        )
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/storage/data/descarteslabs:test-namespace/test-blob"
        )

    @responses.activate
    def test_exists_false(self):
        self.mock_response(responses.HEAD, self.not_found_json, status=404)
        assert not Blob.exists(
            "data/descarteslabs:test-namespace/nonexistent-blob", client=self.client
        )
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/storage/data/descarteslabs:test-namespace/nonexistent-blob"
        )

    @responses.activate
    def test_update(self):
        self.mock_response(
            responses.POST,
            {
                "meta": {"count": 1},
                "data": {
                    "attributes": {
                        "owners": ["org:descarteslabs"],
                        "name": "test-blob",
                        "namespace": "descarteslabs:test-namespace",
                        "geometry": self.geometry,
                        "storage_type": "data",
                        "storage_state": "available",
                        "readers": [],
                        "modified": "2019-06-10T18:48:13.066192Z",
                        "created": "2019-06-10T18:48:13.066192Z",
                        "writers": [],
                        "description": "a description",
                    },
                    "type": "storage",
                    "id": "data/descarteslabs:test-namespace/test-blob",
                },
            },
            status=200,
        )

        b = Blob(
            id="data/descarteslabs:test-namespace/test-blob",
            name="test-blob",
            storage_state="available",
            client=self.client,
        )
        b.save()
        assert b.state == DocumentState.SAVED
        b.readers = ["org:acme-corp"]
        assert b.state == DocumentState.MODIFIED
        self.mock_response(
            responses.PATCH,
            {
                "meta": {"count": 1},
                "data": {
                    "attributes": {
                        "readers": ["org:acme-corp"],
                    },
                    "type": "storage",
                    "id": "data/descarteslabs:test-namespace/test-blob",
                },
            },
            status=200,
        )
        b.save()
        assert b.readers == ["org:acme-corp"]

    @responses.activate
    def test_reload(self):
        self.mock_response(
            responses.POST,
            {
                "meta": {"count": 1},
                "data": {
                    "attributes": {
                        "owners": ["org:descarteslabs"],
                        "name": "test-blob",
                        "namespace": "descarteslabs:test-namespace",
                        "geometry": self.geometry,
                        "storage_type": "data",
                        "storage_state": "available",
                        "readers": [],
                        "modified": "2019-06-10T18:48:13.066192Z",
                        "created": "2019-06-10T18:48:13.066192Z",
                        "writers": [],
                        "description": "a description",
                    },
                    "type": "storage",
                    "id": "data/descarteslabs:test/test-blob",
                },
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/storage"},
            },
        )

        b = Blob(
            id="data/descarteslabs:test/test-blob",
            name="test-blob",
            storage_state="available",
            client=self.client,
        )
        b.save()
        assert b.state == DocumentState.SAVED
        b.readers = ["org:acme-corp"]
        with pytest.raises(ValueError):
            b.reload()

    @responses.activate
    def test_delete(self):
        b = Blob(
            id="data/descarteslabs:test-namespace/test-blob",
            name="test-blob",
            client=self.client,
            _saved=True,
        )
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )

        b.delete()
        assert b.state == DocumentState.DELETED

    @responses.activate
    def test_delete_non_existent(self):
        b = Blob(
            id="data/descarteslabs:test-namespace/nonexistent-blob",
            name="nonexistent-blob",
            client=self.client,
            _saved=True,
        )
        self.mock_response(responses.DELETE, self.not_found_json, status=404)

        with pytest.raises(DeletedObjectError):
            b.delete()

    @responses.activate
    def test_delete_many(self):
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "ids": [
                            "data/descarteslabs:test-namespace/test-blob-0",
                            "data/descarteslabs:test-namespace/test-blob-1",
                        ]
                    },
                    "id": "unique-test-id",
                    "type": "storage_delete",
                }
            },
        )

        deleted_blobs = Blob.delete_many(
            [
                "data/descarteslabs:test-namespace/test-blob-0",
                "data/descarteslabs:test/test-blob-1",
                "data/descarteslabs:test/nonexistent-blob",
            ],
            client=self.client,
        )

        assert "data/descarteslabs:test-namespace/test-blob-0" in deleted_blobs
        assert "data/descarteslabs:test-namespace/test-blob-1" in deleted_blobs
        assert "data/descarteslabs:test-namespace/nonexistent-blob" not in deleted_blobs

    def test_serialize(self):
        u = BlobUpload(
            storage=Blob(
                name="test-blob",
                id="data/descarteslabs:test-namespace/test-blob",
                storage_type="data",
                storage_state="available",
                description="a description",
                expires="2023-01-01",
                tags=["TESTING BLOB"],
                client=self.client,
            )
        )
        serialized = u.serialize(jsonapi_format=True)

        self.assertDictEqual(
            dict(
                data=dict(
                    type=BlobUpload._doc_type,
                    attributes=dict(
                        storage=dict(
                            data=dict(
                                type="storage",
                                attributes=dict(
                                    name="test-blob",
                                    storage_type="data",
                                    storage_state="available",
                                    description="a description",
                                    expires="2023-01-01",
                                    tags=["TESTING BLOB"],
                                ),
                                id="data/descarteslabs:test-namespace/test-blob",
                            )
                        )
                    ),
                )
            ),
            serialized,
        )

    @patch.object(Blob, "_do_download", _blob_do_download)
    def test_data(self):
        b = Blob(
            name="test-blob",
            id="data/descarteslabs:test-namespace/test-blob",
            storage_type="data",
            storage_state="available",
            description="a description",
            expires="2023-01-01",
            tags=["tag"],
            _saved=True,
            client=self.client,
        )

        for test in self.test_combinations:
            test_copy = copy.deepcopy(test)
            value = test_copy.pop("value")
            mock_data = b.data(**test_copy)
            assert mock_data == value

        with pytest.raises(ValueError):
            mock_data = b.data(range=(1, "a"))

        b._saved = False

        with pytest.raises(ValueError):
            b.data()

    @patch.object(Blob, "_do_download", _blob_do_download)
    def test_get_data(self):
        mock_data = Blob.get_data(id="data/descarteslabs:test-namespace/test-blob")

        for test in self.test_combinations:
            test_copy = copy.deepcopy(test)
            value = test_copy.pop("value")
            mock_data = Blob.get_data(
                id="data/descarteslabs:test-namespace/test-blob",
                client=self.client,
                **test_copy,
            )
            assert mock_data == value

        with pytest.raises(ValueError):
            mock_data = Blob.get_data(
                id="data/descarteslabs:test-namespace/test-blob", range=(1, "a")
            )

    @patch.object(Blob, "_do_download", _blob_do_download)
    def test_download(self):
        b = Blob(
            name="test-blob",
            id="data/descarteslabs:test-namespace/test-blob",
            storage_type="data",
            storage_state="available",
            description="a description",
            expires="2023-01-01",
            tags=["TESTING BLOB"],
            _saved=True,
            client=self.client,
        )

        with NamedTemporaryFile(delete=True) as temp:
            result = b.download(temp.name)
            assert result == temp.name

            with open(temp.name, "r") as handle:
                line = handle.readlines()[0]

            assert line == "This is mock download data. It can be any binary data."

        with NamedTemporaryFile(delete=True) as temp:
            with open(temp.name, "wb") as temp:
                result = b.download(temp)

            assert result == temp.name

            with open(temp.name, "r") as handle:
                line = handle.readlines()[0]

            assert line == "This is mock download data. It can be any binary data."

        with pytest.raises(ValueError):
            b.download(1)

        b._saved = False
        with pytest.raises(ValueError):
            b.download("wrong")

    def test_invalid_upload_data(self):
        b = Blob(
            name="test-blob",
            id="data/descarteslabs:test-namespace/test-blob",
            storage_type="data",
            storage_state="available",
            description="a description",
            expires="2023-01-01",
            tags=["TESTING BLOB"],
            _saved=True,
            client=self.client,
        )

        b.name = None
        with pytest.raises(ValueError):
            b.upload_data(data="")

        b.name = "test-blob"
        b._saved = False
        with pytest.raises(DeletedObjectError):
            b.upload_data(data="")

    def test_invalid_upload(self):
        b = Blob(
            name="test-blob",
            id="data/descarteslabs:test-namespace/test-blob",
            storage_type="data",
            storage_state="available",
            description="a description",
            expires="2023-01-01",
            tags=["TESTING BLOB"],
            _saved=True,
            client=self.client,
        )

        with pytest.raises(ValueError):
            with NamedTemporaryFile(delete=True) as temp:
                _ = b.upload(temp.name)

        b.name = None
        b._save = False
        with pytest.raises(ValueError):
            with NamedTemporaryFile(delete=True) as temp:
                _ = b.upload(temp)
