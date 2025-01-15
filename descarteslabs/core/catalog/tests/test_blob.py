# © 2025 EarthDaily Analytics Corp.
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
import os
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
from ..blob import Blob, BlobCollection, BlobDeletionTaskStatus, BlobSearch, StorageType
from ..blob_upload import BlobUpload
from ..catalog_base import DocumentState, DeletedObjectError
from ...common.property_filtering import Properties


def _namespace_id(namespace_id, client=None):
    return "someorg:test-namespace"


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
    polygon_geometry = {
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
    multipolygon_geometry = {
        "type": "MultiPolygon",
        "coordinates": [
            [
                [
                    [-95.2989209, 42.7999878],
                    [-93.1167728, 42.3858464],
                    [-93.7138666, 40.703737],
                    [-95.8364984, 41.1150618],
                    [-95.2989209, 42.7999878],
                ]
            ],
            [
                [
                    [-95.3989209, 42.7999878],
                    [-93.4167728, 42.3858464],
                    [-93.7138666, 40.703737],
                    [-95.6364984, 41.1150618],
                    [-95.3989209, 42.7999878],
                ]
            ],
        ],
    }
    point_geometry = {
        "type": "Point",
        "coordinates": [-95.2989209, 42.7999878],
    }
    multipoint_geometry = {
        "type": "MultiPoint",
        "coordinates": [
            [-95.2989209, 42.7999878],
            [-96.2989209, 43.7999878],
        ],
    }
    multipoint_all_coincident_geometry = {
        "type": "MultiPoint",
        "coordinates": [
            [-95.2989209, 42.7999878],
            [-95.2989209, 42.7999878],
        ],
    }
    line_geometry = {
        "type": "LineString",
        "coordinates": [
            [-122.17523623224433, 47.90651694142758],
            [-122.13437682048007, 47.88564432387702],
        ],
    }
    horizontal_line_geometry = {
        "type": "LineString",
        "coordinates": [
            [-122.17523623224433, 47.88564432387702],
            [-122.13437682048007, 47.88564432387702],
        ],
    }
    vertical_line_geometry = {
        "type": "LineString",
        "coordinates": [
            [-122.17523623224433, 47.90651694142758],
            [-122.17523623224433, 47.88564432387702],
        ],
    }
    multiline_geometry = {
        "type": "MultiLineString",
        "coordinates": [
            [
                [-122.13826819210863, 47.90599522815964],
                [-122.12931803524592, 47.91303790851427],
            ],
            [
                [-122.13865732936327, 47.920340416616256],
                [-122.12892889799097, 47.912777085591244],
            ],
            [
                [-122.17601450675424, 47.91277722269507],
                [-122.1293180361663, 47.91277722269507],
            ],
        ],
    }

    test_geometries = [
        polygon_geometry,
        multipolygon_geometry,
        point_geometry,
        multipoint_geometry,
        line_geometry,
        horizontal_line_geometry,
        vertical_line_geometry,
        multiline_geometry,
    ]

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
            id="data/someorg:test-namespace/test-blob",
            storage_type="data",
            storage_state="available",
            description="a description",
            expires="2023-01-01",
            tags=["TESTING BLOB"],
        )

        assert b.name == "test-blob"
        assert b.id == "data/someorg:test-namespace/test-blob"
        assert b.storage_type == StorageType.DATA
        assert b.storage_state == "available"
        assert b.description == "a description"
        assert b.tags == ["TESTING BLOB"]
        assert b.state == DocumentState.UNSAVED

    def test_repr(self):
        b = Blob(
            name="test-blob",
            id="data/someorg:test-namespace/test-blob",
            storage_type="data",
            storage_state="available",
            description="a description",
            expires="2023-01-01",
            tags=["TESTING BLOB"],
        )
        b_repr = repr(b)
        match_str = """\
            Blob: test-blob
              id: data/someorg:test-namespace/test-blob
            * Not up-to-date in the Descartes Labs catalog. Call `.save()` to save or update this record."""
        assert b_repr.strip("\n") == textwrap.dedent(match_str)

    def test_set_geometry(self):
        b = Blob(id="data/someorg:test/test-blob", name="test-blob")
        for test_geometry in self.test_geometries:
            shape = shapely.geometry.shape(test_geometry)

            b.geometry = test_geometry
            assert shape == b.geometry

            b.geometry = shape
            assert shape == b.geometry

        with pytest.raises(AttributeValidationError):
            b.geometry = {"type": "Lollipop"}
        with pytest.raises(AttributeValidationError):
            b.geometry = 2

    def test_storage_type_new(self):
        b = Blob(
            id="data/someorg:test-namespace/test-blob",
            name="test-blob",
            storage_type="nodata",
            _saved=True,
        )
        assert b.description is None
        assert b.storage_type == "nodata"

        with pytest.raises(ValueError):
            StorageType("nodata")

    def test_search_intersects(self):
        search = (
            Blob.search()
            .intersects(self.polygon_geometry)
            .filter(Properties().id == "b1")
        )
        _, request_params = search._to_request()
        assert self.polygon_geometry == json.loads(request_params["intersects"])
        assert "intersects_none" not in request_params

    def test_search_intersects_none(self):
        search = (
            Blob.search()
            .intersects(self.polygon_geometry, match_null_geometry=True)
            .filter(Properties().id == "b1")
        )
        _, request_params = search._to_request()
        assert self.polygon_geometry == json.loads(request_params["intersects"])
        assert request_params["intersects_none"] is True

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
                        "geometry": self.polygon_geometry,
                        "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                        "href": "s3://super/long/uri/data/someorg:test-namespace/test-blob",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-blob",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "writers": [],
                        "size_bytes": 1008,
                        "storage_state": "available",
                        "storage_type": "data",
                        "tags": ["TESTING BLOB"],
                    },
                    "id": "data/someorg:test-namespace/test-blob",
                    "type": "storage",
                }
            },
            status=200,
        )

        b = Blob.get(id="data/someorg:test-namespace/test-blob", client=self.client)
        assert isinstance(b.created, datetime)
        assert b.description == "a generic description"
        assert b.expires is None
        assert b.geometry == shapely.geometry.shape(self.polygon_geometry)
        assert b.hash == "28495fde1c101c01f2d3ae92d1af85a5"
        assert b.href == "s3://super/long/uri/data/someorg:test-namespace/test-blob"
        assert isinstance(b.modified, datetime)
        assert b.name == "test-blob"
        assert b.namespace == "someorg:test-namespace"
        assert b.owners == ["org:someorg"]
        assert b.readers == ["org:someorg"]
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
                        "geometry": self.polygon_geometry,
                        "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                        "href": "s3://super/long/uri/data/someorg:test-namespace/test-blob",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-blob",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "writers": [],
                        "size_bytes": 1008,
                        "storage_state": "available",
                        "storage_type": "data",
                        "tags": ["TESTING BLOB"],
                        "foobar": "unknown",
                    },
                    "id": "data/someorg:test-namespace/test-blob",
                    "type": "storage",
                }
            },
            status=200,
        )

        b = Blob.get(id="data/someorg:test-namespace/test-blob", client=self.client)
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
                            "geometry": self.polygon_geometry,
                            "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                            "href": "s3://super/long/uri/data/someorg:test-namespace/test-blob-1",
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-blob-1",
                            "namespace": "someorg:test-namespace",
                            "owners": ["org:someorg"],
                            "readers": ["org:someorg"],
                            "writers": [],
                            "size_bytes": 1008,
                            "storage_state": "available",
                            "storage_type": "data",
                            "tags": ["TESTING BLOB"],
                        },
                        "id": "data/someorg:test-namespace/test-blob-1",
                        "type": "storage",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "expires": None,
                            "extra_properties": {},
                            "geometry": self.polygon_geometry,
                            "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                            "href": "s3://super/long/uri/data/someorg:test-namespace/test-blob-2",
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-blob-2",
                            "namespace": "someorg:test-namespace",
                            "owners": ["org:someorg"],
                            "readers": ["org:someorg"],
                            "writers": [],
                            "size_bytes": 1008,
                            "storage_state": "available",
                            "storage_type": "data",
                            "tags": ["TESTING BLOB"],
                        },
                        "id": "data/someorg:test-namespace/test-blob-2",
                        "type": "storage",
                    },
                ],
            },
            status=200,
        )

        blobs = Blob.get_many(
            [
                "data/someorg:test-namespace/test-blob-1",
                "data/someorg:test-namespace/test-blob-2",
            ],
            client=self.client,
        )

        for i, b in enumerate(blobs):
            assert isinstance(b, Blob)
            assert b.id == f"data/someorg:test-namespace/test-blob-{i + 1}"

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
                        "geometry": self.polygon_geometry,
                        "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                        "href": "s3://super/long/uri/data/someorg:test-namespace/test-blob",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-blob",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "writers": [],
                        "size_bytes": 1008,
                        "storage_state": "available",
                        "storage_type": "data",
                        "tags": ["TESTING BLOB"],
                    },
                    "id": "data/someorg:test-namespace/test-blob",
                    "type": "storage",
                }
            },
            status=200,
        )

        b = Blob.get_or_create(
            id="data/someorg:test-namespace/test-blob", client=self.client
        )
        assert b.id == "data/someorg:test-namespace/test-blob"

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
                            "geometry": self.polygon_geometry,
                            "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                            "href": "s3://super/long/uri/data/someorg:test-namespace/test-blob-1",
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-blob-1",
                            "namespace": "someorg:test-namespace",
                            "owners": ["org:someorg"],
                            "readers": ["org:someorg"],
                            "writers": [],
                            "size_bytes": 1008,
                            "storage_state": "available",
                            "storage_type": "data",
                            "tags": ["TESTING BLOB"],
                        },
                        "id": "data/someorg:test-namespace/test-blob-1",
                        "type": "storage",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "expires": None,
                            "extra_properties": {},
                            "geometry": self.polygon_geometry,
                            "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                            "href": "s3://super/long/uri/data/someorg:test-namespace/test-blob-2",
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-blob-2",
                            "namespace": "someorg:test-namespace",
                            "owners": ["org:someorg"],
                            "readers": ["org:someorg"],
                            "writers": [],
                            "size_bytes": 1008,
                            "storage_state": "available",
                            "storage_type": "data",
                            "tags": ["TESTING BLOB"],
                        },
                        "id": "data/someorg:test-namespace/test-blob-2",
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
                        "geometry": self.polygon_geometry,
                        "hash": "28495fde1c101c01f2d3ae92d1af85a5",
                        "href": "s3://super/long/uri/data/someorg:test-namespace/test-blob",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-blob",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "writers": [],
                        "size_bytes": 1008,
                        "storage_state": "available",
                        "storage_type": "data",
                        "tags": ["TESTING BLOB"],
                    },
                    "id": "data/someorg:test-namespace/test-blob",
                    "type": "storage",
                }
            },
            status=200,
        )

        b = Blob(
            id="data/someorg:test-namespace/test-blob",
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
                        "detail": "A document with id `data/someorg:test-namespace/test-blob` already exists.",
                        "title": "Bad request",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=400,
        )
        b = Blob(id="data/someorg:test-namespace/test-blob", client=self.client)
        with pytest.raises(BadRequestError):
            b.save()

    @responses.activate
    def test_exists(self):
        self.mock_response(responses.HEAD, {}, status=200)
        assert Blob.exists("data/someorg:test-namespace/test-blob", client=self.client)
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/storage/data/someorg:test-namespace/test-blob"
        )

    @responses.activate
    def test_exists_false(self):
        self.mock_response(responses.HEAD, self.not_found_json, status=404)
        assert not Blob.exists(
            "data/someorg:test-namespace/nonexistent-blob", client=self.client
        )
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/storage/data/someorg:test-namespace/nonexistent-blob"
        )

    @responses.activate
    def test_update(self):
        self.mock_response(
            responses.POST,
            {
                "meta": {"count": 1},
                "data": {
                    "attributes": {
                        "owners": ["org:someorg"],
                        "name": "test-blob",
                        "namespace": "someorg:test-namespace",
                        "geometry": self.polygon_geometry,
                        "storage_type": "data",
                        "storage_state": "available",
                        "readers": [],
                        "modified": "2019-06-10T18:48:13.066192Z",
                        "created": "2019-06-10T18:48:13.066192Z",
                        "writers": [],
                        "description": "a description",
                    },
                    "type": "storage",
                    "id": "data/someorg:test-namespace/test-blob",
                },
            },
            status=200,
        )

        b = Blob(
            id="data/someorg:test-namespace/test-blob",
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
                    "id": "data/someorg:test-namespace/test-blob",
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
                        "owners": ["org:someorg"],
                        "name": "test-blob",
                        "namespace": "someorg:test-namespace",
                        "geometry": self.polygon_geometry,
                        "storage_type": "data",
                        "storage_state": "available",
                        "readers": [],
                        "modified": "2019-06-10T18:48:13.066192Z",
                        "created": "2019-06-10T18:48:13.066192Z",
                        "writers": [],
                        "description": "a description",
                    },
                    "type": "storage",
                    "id": "data/someorg:test/test-blob",
                },
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/storage"},
            },
        )

        b = Blob(
            id="data/someorg:test/test-blob",
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
            id="data/someorg:test-namespace/test-blob",
            name="test-blob",
            client=self.client,
            _saved=True,
        )
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "id": "123",
                    "attributes": {
                        "status": "RUNNING",
                        "ids": [b.id],
                    },
                    "type": "storage_delete",
                },
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
            status=201,
        )

        task = b.delete()
        assert isinstance(task, BlobDeletionTaskStatus)
        assert b.state == DocumentState.DELETED

    @responses.activate
    def test_class_delete(self):
        blob_id = "data/someorg:test-namespace/test-blob"
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "id": "123",
                    "attributes": {
                        "status": "RUNNING",
                        "ids": [blob_id],
                    },
                    "type": "storage_delete",
                },
                "jsonapi": {"version": "1.0"},
            },
            status=201,
        )

        task = Blob.delete(blob_id, client=self.client)
        assert isinstance(task, BlobDeletionTaskStatus)
        assert task.id == "123"
        assert task.status == "RUNNING"
        assert task.ids == [blob_id]

        self.mock_response(
            responses.GET,
            {
                "data": {
                    "id": "123",
                    "attributes": {
                        "status": "SUCCESS",
                        "objects_deleted": 1,
                        "errors": None,
                    },
                    "type": "storage_delete",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        task.wait_for_completion()
        assert task.status == "SUCCESS"
        assert task.objects_deleted == 1
        assert task.ids == [blob_id]

    @responses.activate
    def test_delete_non_existent(self):
        b = Blob(
            id="data/someorg:test-namespace/nonexistent-blob",
            name="nonexistent-blob",
            client=self.client,
            _saved=True,
        )

        self.mock_response(
            responses.POST,
            self.not_found_json,
            status=404,
        )

        with pytest.raises(DeletedObjectError):
            b.delete()

    @responses.activate
    def test_delete_many(self):
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "status": "RUNNING",
                        "start_datetime": "2024-01-01T00:00:00Z",
                        "ids": [
                            "data/someorg:test-namespace/test-blob-0",
                            "data/someorg:test-namespace/test-blob-1",
                        ],
                    },
                    "id": "123",
                    "type": "storage_delete",
                }
            },
            201,
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "status": "SUCCESS",
                        "start_datetime": "2024-01-01T00:00:00Z",
                        "duration_in_seconds": 1.0,
                        "objects_deleted": 2,
                    },
                    "id": "123",
                    "type": "storage_delete",
                }
            },
            201,
        )

        deleted_blobs = Blob.delete_many(
            [
                "data/someorg:test-namespace/test-blob-0",
                "data/someorg:test/test-blob-1",
                "data/someorg:test/nonexistent-blob",
            ],
            wait_for_completion=True,
            client=self.client,
        )

        assert "data/someorg:test-namespace/test-blob-0" in deleted_blobs
        assert "data/someorg:test-namespace/test-blob-1" in deleted_blobs
        assert "data/someorg:test-namespace/nonexistent-blob" not in deleted_blobs

    def test_serialize(self):
        u = BlobUpload(
            storage=Blob(
                name="test-blob",
                id="data/someorg:test-namespace/test-blob",
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
                                id="data/someorg:test-namespace/test-blob",
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
            id="data/someorg:test-namespace/test-blob",
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
        mock_data = Blob.get_data(id="data/someorg:test-namespace/test-blob")

        for test in self.test_combinations:
            test_copy = copy.deepcopy(test)
            value = test_copy.pop("value")
            mock_data = Blob.get_data(
                id="data/someorg:test-namespace/test-blob",
                client=self.client,
                **test_copy,
            )
            assert mock_data == value

        with pytest.raises(ValueError):
            mock_data = Blob.get_data(
                id="data/someorg:test-namespace/test-blob", range=(1, "a")
            )

    @patch.object(Blob, "_do_download", _blob_do_download)
    def test_download(self):
        b = Blob(
            name="test-blob",
            id="data/someorg:test-namespace/test-blob",
            storage_type="data",
            storage_state="available",
            description="a description",
            expires="2023-01-01",
            tags=["TESTING BLOB"],
            _saved=True,
            client=self.client,
        )

        with NamedTemporaryFile(delete=False) as f1:
            with NamedTemporaryFile(delete=False) as f2:
                try:
                    f1.close()
                    f2.close()

                    result = b.download(f1.name)

                    assert result == f1.name

                    with open(f1.name, "r") as handle:
                        line = handle.readlines()[0]

                    assert (
                        line == "This is mock download data. It can be any binary data."
                    )

                    with open(f2.name, "wb") as temp:
                        result = b.download(temp)

                    assert result == f2.name

                    with open(f2.name, "r") as handle:
                        line = handle.readlines()[0]

                    assert (
                        line == "This is mock download data. It can be any binary data."
                    )

                    with pytest.raises(ValueError):
                        b.download(1)

                    b._saved = False
                    with pytest.raises(ValueError):
                        b.download("wrong")

                finally:
                    os.unlink(f1.name)
                    os.unlink(f2.name)

    @patch.object(Blob, "namespace_id", _namespace_id)
    def test_invalid_upload_data(self):
        b = Blob(
            name="test-blob",
            id="data/someorg:test-namespace/test-blob",
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

    @patch.object(Blob, "namespace_id", _namespace_id)
    def test_invalid_upload(self):
        b = Blob(
            name="test-blob",
            id="data/someorg:test-namespace/test-blob",
            storage_type="data",
            storage_state="available",
            description="a description",
            expires="2023-01-01",
            tags=["TESTING BLOB"],
            _saved=True,
            client=self.client,
        )

        with NamedTemporaryFile(delete=False) as f1:
            try:
                f1.close()

                with pytest.raises(ValueError):
                    _ = b.upload(f1.name)

            finally:
                os.unlink(f1.name)
