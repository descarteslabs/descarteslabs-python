# Copyright 2018-2024 Descartes Labs.
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
import json
import pytest
import responses

import textwrap

import shapely.geometry

from datetime import datetime

from descarteslabs.exceptions import ConflictError
from .base import ClientTestCase
from ..attributes import AttributeValidationError
from ..event_subscription import (
    EventSubscription,
    EventSubscriptionCollection,
    EventSubscriptionSearch,
    EventSubscriptionTarget,
    EventType,
)
from ..catalog_base import DocumentState, DeletedObjectError
from ...common.property_filtering import Properties


class TestEventSubscription(ClientTestCase):
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

    def test_constructor(self):
        s = EventSubscription(
            namespace="descarteslabs:test-namespace",
            name="test-sub",
            id="descarteslabs:test-namespace:test-sub",
            description="a description",
            geometry=self.geometry,
            expires="2023-01-01",
            event_type=[EventType.NEW_IMAGE],
            event_source=["metadata"],
            event_namespace=["some-product-id"],
            event_filters=[
                (
                    (Properties().cloud_fraction > 0.5)
                    & (Properties().cloud_fraction < 0.9)
                ),
            ],
            targets=[
                EventSubscriptionTarget(
                    rule_id="descarteslabs:some-rule",
                    detail_template="some-template",
                ),
            ],
            enabled=True,
            tags=["TESTING"],
        )

        assert s.namespace == "descarteslabs:test-namespace"
        assert s.name == "test-sub"
        assert s.id == "descarteslabs:test-namespace:test-sub"
        assert s.description == "a description"
        assert s.geometry == shapely.geometry.shape(self.geometry)
        assert s.expires == "2023-01-01"
        assert s.event_type == [EventType.NEW_IMAGE]
        assert s.event_source == ["metadata"]
        assert s.event_namespace == ["some-product-id"]
        assert len(s.event_filters) == 1
        assert s.event_filters[0].is_same(
            (Properties().cloud_fraction > 0.5) & (Properties().cloud_fraction < 0.9)
        )
        assert len(s.targets) == 1
        assert s.targets[0].rule_id == "descarteslabs:some-rule"
        assert s.targets[0].detail_template == "some-template"
        assert s.enabled is True
        assert s.tags == ["TESTING"]
        assert s.state == DocumentState.UNSAVED

    def test_repr(self):
        s = EventSubscription(
            name="test-sub",
            id="descarteslabs:test-namespace:test-sub",
            description="a description",
            expires="2023-01-01",
            tags=["TESTING BLOB"],
        )
        s_repr = repr(s)
        match_str = """\
            EventSubscription: test-sub
              id: descarteslabs:test-namespace:test-sub
            * Not up-to-date in the Descartes Labs catalog. Call `.save()` to save or update this record."""
        assert s_repr.strip("\n") == textwrap.dedent(match_str)

    def test_set_geometry(self):
        shape = shapely.geometry.shape(self.geometry)
        s = EventSubscription(id="descarteslabs:test:test-sub", name="test-sub")
        s.geometry = self.geometry
        assert shape == s.geometry

        s.geometry = shape
        assert shape == s.geometry

        with pytest.raises(AttributeValidationError):
            s.geometry = {"type": "Lollipop"}
        with pytest.raises(AttributeValidationError):
            s.geometry = 2

    def test_search_intersects(self):
        search = (
            EventSubscription.search()
            .intersects(self.geometry)
            .filter(Properties().id == "s1")
        )
        _, request_params = search._to_request()
        assert self.geometry == json.loads(request_params["intersects"])
        assert "intersects_none" not in request_params

    def test_search_intersects_none(self):
        search = (
            EventSubscription.search()
            .intersects(self.geometry, match_null_geometry=True)
            .filter(Properties().id == "s1")
        )
        _, request_params = search._to_request()
        assert self.geometry == json.loads(request_params["intersects"])
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
                        "event_filters": [
                            {
                                "and": [
                                    {"name": "cloud_fraction", "op": "gt", "val": 0.5},
                                    {"name": "cloud_fraction", "op": "lt", "val": 0.9},
                                ]
                            }
                        ],
                        "event_namespace": ["some-product-id"],
                        "event_source": ["metadata"],
                        "event_type": ["new-image"],
                        "expires": None,
                        "extra_properties": {},
                        "geometry": self.geometry,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sub",
                        "namespace": "descarteslabs:test-namespace",
                        "owner": "user:somehash",
                        "owners": ["org:descarteslabs"],
                        "readers": ["org:descarteslabs"],
                        "tags": ["TESTING"],
                        "targets": [
                            {
                                "rule_id": "descarteslabs:some-rule",
                                "detail_template": "some-template",
                            }
                        ],
                        "writers": [],
                    },
                    "id": "descarteslabs:test-namespace:test-sub",
                    "type": "event_subscription",
                }
            },
            status=200,
        )

        s = EventSubscription.get(
            id="descarteslabs:test-namespace:test-sub", client=self.client
        )
        assert isinstance(s.created, datetime)
        assert isinstance(s.modified, datetime)
        assert s.id == "descarteslabs:test-namespace:test-sub"
        assert s.name == "test-sub"
        assert s.namespace == "descarteslabs:test-namespace"
        assert s.description == "a generic description"
        assert s.owner == "user:somehash"
        assert s.expires is None
        assert s.geometry == shapely.geometry.shape(self.geometry)
        assert s.event_type == [EventType.NEW_IMAGE]
        assert s.event_source == ["metadata"]
        assert s.event_namespace == ["some-product-id"]
        assert len(s.event_filters) == 1
        assert s.event_filters[0].is_same(
            (Properties().cloud_fraction > 0.5) & (Properties().cloud_fraction < 0.9)
        )
        assert len(s.targets) == 1
        assert type(s.targets[0]) is EventSubscriptionTarget
        assert s.targets[0].rule_id == "descarteslabs:some-rule"
        assert s.targets[0].detail_template == "some-template"
        assert s.owners == ["org:descarteslabs"]
        assert s.readers == ["org:descarteslabs"]
        assert s.writers == []
        assert s.tags == ["TESTING"]

    @responses.activate
    def test_get_unknown_attribute(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "event_filters": [
                            {
                                "and": [
                                    {"name": "cloud_fraction", "op": "gt", "val": 0.5},
                                    {"name": "cloud_fraction", "op": "lt", "val": 0.9},
                                ]
                            }
                        ],
                        "event_namespace": ["some-product-id"],
                        "event_source": ["metadata"],
                        "event_type": ["new-image"],
                        "expires": None,
                        "extra_properties": {},
                        "geometry": self.geometry,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sub",
                        "namespace": "descarteslabs:test-namespace",
                        "owner": "user:somehash",
                        "owners": ["org:descarteslabs"],
                        "readers": ["org:descarteslabs"],
                        "writers": [],
                        "tags": ["TESTING"],
                        "foobar": "unknown",
                    },
                    "id": "descarteslabs:test-namespace:test-sub",
                    "type": "event_subscription",
                }
            },
            status=200,
        )

        s = EventSubscription.get(
            id="descarteslabs:test-namespace:test-sub", client=self.client
        )
        assert not hasattr(s, "foobar")

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
                            "event_filters": [],
                            "event_namespace": ["some-product-id"],
                            "event_source": ["metadata"],
                            "event_type": ["new-image"],
                            "expires": None,
                            "extra_properties": {},
                            "geometry": None,
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-sub-1",
                            "namespace": "descarteslabs:test-namespace",
                            "owner": "user:somehash",
                            "owners": ["org:descarteslabs"],
                            "readers": ["org:descarteslabs"],
                            "writers": [],
                            "tags": ["TESTING"],
                        },
                        "id": "descarteslabs:test-namespace:test-sub-1",
                        "type": "event_subscription",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "event_filters": [],
                            "event_namespace": ["some-product-id"],
                            "event_source": ["metadata"],
                            "event_type": ["new-image"],
                            "expires": None,
                            "extra_properties": {},
                            "geometry": None,
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-sub-2",
                            "namespace": "descarteslabs:test-namespace",
                            "owner": "user:somehash",
                            "owners": ["org:descarteslabs"],
                            "readers": ["org:descarteslabs"],
                            "writers": [],
                            "tags": ["TESTING"],
                        },
                        "id": "descarteslabs:test-namespace:test-sub-2",
                        "type": "event_subscription",
                    },
                ],
            },
            status=200,
        )

        subs = EventSubscription.get_many(
            [
                "descarteslabs:test-namespace:test-sub-1",
                "descarteslabs:test-namespace:test-sub-2",
            ],
            client=self.client,
        )

        for i, r in enumerate(subs):
            assert isinstance(r, EventSubscription)
            assert r.id == f"descarteslabs:test-namespace:test-sub-{i + 1}"

    @responses.activate
    def test_get_or_create(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "event_filters": [],
                        "event_namespace": ["some-product-id"],
                        "event_source": ["metadata"],
                        "event_type": ["new-image"],
                        "expires": None,
                        "extra_properties": {},
                        "geometry": None,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sub",
                        "namespace": "descarteslabs:test-namespace",
                        "owner": "user:somehash",
                        "owners": ["org:descarteslabs"],
                        "readers": ["org:descarteslabs"],
                        "writers": [],
                        "tags": ["TESTING"],
                    },
                    "id": "descarteslabs:test-namespace:test-sub",
                    "type": "event_subscription",
                },
            },
            status=200,
        )

        s = EventSubscription.get_or_create(
            id="descarteslabs:test-namespace:test-sub", client=self.client
        )
        assert s.id == "descarteslabs:test-namespace:test-sub"

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
                            "event_filters": [],
                            "event_namespace": ["some-product-id"],
                            "event_source": ["metadata"],
                            "event_type": ["new-image"],
                            "expires": None,
                            "extra_properties": {},
                            "geometry": None,
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-sub-1",
                            "namespace": "descarteslabs:test-namespace",
                            "owner": "user:somehash",
                            "owners": ["org:descarteslabs"],
                            "readers": ["org:descarteslabs"],
                            "writers": [],
                            "tags": ["TESTING"],
                        },
                        "id": "descarteslabs:test-namespace:test-sub-1",
                        "type": "event_subscription",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "event_filters": [],
                            "event_namespace": ["some-product-id"],
                            "event_source": ["metadata"],
                            "event_type": ["new-image"],
                            "expires": None,
                            "extra_properties": {},
                            "geometry": None,
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-sub-2",
                            "namespace": "descarteslabs:test-namespace",
                            "owner": "user:somehash",
                            "owners": ["org:descarteslabs"],
                            "readers": ["org:descarteslabs"],
                            "writers": [],
                            "tags": ["TESTING"],
                        },
                        "id": "descarteslabs:test-namespace:test-sub-2",
                        "type": "event_subscription",
                    },
                ],
            },
            status=200,
        )

        search = EventSubscription.search(client=self.client)
        assert search.count() == 2
        assert isinstance(search, EventSubscriptionSearch)
        sc = search.collect()
        assert isinstance(sc, EventSubscriptionCollection)

    @responses.activate
    def test_list_no_results(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 0},
                "data": [],
            },
        )

        r = list(EventSubscription.search(client=self.client))
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
                        "event_filters": [],
                        "event_namespace": ["some-product-id"],
                        "event_source": ["metadata"],
                        "event_type": ["new-image"],
                        "expires": None,
                        "extra_properties": {},
                        "geometry": None,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sub",
                        "namespace": "descarteslabs:test-namespace",
                        "owner": "user:somehash",
                        "owners": ["org:descarteslabs"],
                        "readers": ["org:descarteslabs"],
                        "writers": [],
                        "tags": ["TESTING"],
                    },
                    "id": "descarteslabs:test-namespace:test-sub",
                    "type": "event_subscription",
                }
            },
            status=201,
        )

        s = EventSubscription(
            id="descarteslabs:test-namespace:test-sub",
            name="test-sub",
            client=self.client,
        )
        assert s.state == DocumentState.UNSAVED
        s.save()
        assert responses.calls[0].request.url == self.url + "/event_subscriptions"
        assert s.state == DocumentState.SAVED

    @responses.activate
    def test_save_dupe(self):
        self.mock_response(
            responses.POST,
            {
                "errors": [
                    {
                        "status": "409",
                        "detail": "A document with id `descarteslabs:test-namespace:test-sub` already exists.",
                        "title": "Conflict",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=409,
        )
        s = EventSubscription(
            id="descarteslabs:test-namespace:test-sub", client=self.client
        )
        with pytest.raises(ConflictError):
            s.save()

    @responses.activate
    def test_exists(self):
        self.mock_response(responses.HEAD, {}, status=200)
        assert EventSubscription.exists(
            "descarteslabs:test-namespace:test-sub", client=self.client
        )
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/event_subscriptions/descarteslabs:test-namespace:test-sub"
        )

    @responses.activate
    def test_exists_false(self):
        self.mock_response(responses.HEAD, self.not_found_json, status=404)
        assert not EventSubscription.exists(
            "descarteslabs:test-namespace:nonexistent-sub", client=self.client
        )
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/event_subscriptions/descarteslabs:test-namespace:nonexistent-sub"
        )

    @responses.activate
    def test_update(self):
        self.mock_response(
            responses.POST,
            {
                "meta": {"count": 1},
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": None,
                        "event_filters": [],
                        "event_namespace": [],
                        "event_source": [],
                        "event_type": [],
                        "expires": None,
                        "extra_properties": {},
                        "geometry": None,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sub",
                        "namespace": "descarteslabs:test-namespace",
                        "owner": "user:somehash",
                        "owners": ["org:descarteslabs", "user:somehash"],
                        "readers": [],
                        "writers": [],
                        "tags": [],
                    },
                    "id": "descarteslabs:test-namespace:test-sub",
                    "type": "event_subscription",
                },
            },
            status=200,
        )

        s = EventSubscription(
            id="descarteslabs:test-namespace:test-sub",
            name="test-sub",
            client=self.client,
        )
        s.save()
        assert s.state == DocumentState.SAVED
        s.readers = ["org:acme-corp"]
        assert s.state == DocumentState.MODIFIED
        self.mock_response(
            responses.PATCH,
            {
                "meta": {"count": 1},
                "data": {
                    "attributes": {
                        "readers": ["org:acme-corp"],
                    },
                    "type": "event_subscription",
                    "id": "descarteslabs:test-namespace:test-sub",
                },
            },
            status=200,
        )
        s.save()
        assert s.readers == ["org:acme-corp"]

    @responses.activate
    def test_reload(self):
        self.mock_response(
            responses.POST,
            {
                "meta": {"count": 1},
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": None,
                        "event_filters": [],
                        "event_namespace": [],
                        "event_source": [],
                        "event_type": [],
                        "expires": None,
                        "extra_properties": {},
                        "geometry": None,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sub",
                        "namespace": "descarteslabs:test-namespace",
                        "owner": "user:somehash",
                        "owners": ["org:descarteslabs", "user:somehash"],
                        "readers": [],
                        "writers": [],
                        "tags": [],
                    },
                    "id": "descarteslabs:test-namespace:test-sub",
                    "type": "event_subscription",
                },
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/storage"},
            },
        )

        s = EventSubscription(
            id="descarteslabs:test-namespace:test-sub",
            name="test-sub",
            client=self.client,
        )
        s.save()
        assert s.state == DocumentState.SAVED
        s.readers = ["org:acme-corp"]
        with pytest.raises(ValueError):
            s.reload()

    @responses.activate
    def test_delete(self):
        s = EventSubscription(
            id="descarteslabs:test-namespace:test-sub",
            name="test-sub",
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

        s.delete()
        assert s.state == DocumentState.DELETED

    @responses.activate
    def test_class_delete(self):
        sub_id = "descarteslabs:test-namespace:test-sub"
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )

        assert EventSubscription.delete(sub_id, client=self.client)

    @responses.activate
    def test_delete_non_existent(self):
        s = EventSubscription(
            id="descarteslabs:test-namespace:nonexistent-sub",
            name="nonexistent-sub",
            client=self.client,
            _saved=True,
        )

        self.mock_response(
            responses.DELETE,
            self.not_found_json,
            status=404,
        )

        with pytest.raises(DeletedObjectError):
            s.delete()
