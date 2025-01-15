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
import pytest
import responses

import textwrap

from datetime import datetime

from descarteslabs.exceptions import ConflictError
from .base import ClientTestCase
from ..event_api_destination import (
    EventApiDestination,
    EventApiDestinationCollection,
    EventApiDestinationSearch,
    EventConnectionParameter,
)
from ..catalog_base import DocumentState, DeletedObjectError


class TestEventApiDestination(ClientTestCase):
    def test_constructor(self):
        d = EventApiDestination(
            namespace="someorg:test-namespace",
            name="test-api-destination",
            id="someorg:test-namespace:test-api-destination",
            description="a description",
            endpoint="https://some.endpoint",
            method="POST",
            invocation_rate=1,
            arn="some-arn",
            connection_name="some-connection",
            connection_description="a connection description",
            connection_header_parameters=[
                EventConnectionParameter(
                    Key="some-header", Value="some-value", IsValueSecret=False
                )
            ],
            connection_query_string_parameters=[
                EventConnectionParameter(
                    Key="some-query", Value="some-value", IsValueSecret=False
                )
            ],
            connection_body_parameters=[
                EventConnectionParameter(
                    Key="some-body", Value="some-value", IsValueSecret=False
                )
            ],
            connection_authorization_type="OAUTH_CLIENT_CREDENTIALS",
            connection_oauth_endpoint="https://some.oauth.endpoint",
            connection_oauth_method="POST",
            connection_oauth_client_id="some-client-id",
            connection_oauth_client_secret="some-secret",
            connection_oauth_header_parameters=[
                EventConnectionParameter(
                    Key="some-oauth-header",
                    Value="some-oauth-value",
                    IsValueSecret=False,
                )
            ],
            connection_oauth_query_string_parameters=[
                EventConnectionParameter(
                    Key="some-oauth-query",
                    Value="some-oauth-value",
                    IsValueSecret=False,
                )
            ],
            connection_oauth_body_parameters=[
                EventConnectionParameter(
                    Key="some-oauth-body", Value="some-oauth-value", IsValueSecret=False
                )
            ],
            connection_arn="some-connection-arn",
            tags=["TESTING"],
        )

        assert d.namespace == "someorg:test-namespace"
        assert d.name == "test-api-destination"
        assert d.id == "someorg:test-namespace:test-api-destination"
        assert d.description == "a description"
        assert d.endpoint == "https://some.endpoint"
        assert d.method == "POST"
        assert d.invocation_rate == 1
        assert d.arn == "some-arn"
        assert d.connection_name == "some-connection"
        assert d.connection_description == "a connection description"
        assert d.connection_header_parameters == [
            EventConnectionParameter(
                Key="some-header", Value="some-value", IsValueSecret=False
            )
        ]
        assert d.connection_query_string_parameters == [
            EventConnectionParameter(
                Key="some-query", Value="some-value", IsValueSecret=False
            )
        ]
        assert d.connection_body_parameters == [
            EventConnectionParameter(
                Key="some-body", Value="some-value", IsValueSecret=False
            )
        ]
        assert d.connection_authorization_type == "OAUTH_CLIENT_CREDENTIALS"
        assert d.connection_oauth_endpoint == "https://some.oauth.endpoint"
        assert d.connection_oauth_method == "POST"
        assert d.connection_oauth_client_id == "some-client-id"
        assert d.connection_oauth_client_secret == "some-secret"
        assert d.connection_oauth_header_parameters == [
            EventConnectionParameter(
                Key="some-oauth-header", Value="some-oauth-value", IsValueSecret=False
            )
        ]
        assert d.connection_oauth_query_string_parameters == [
            EventConnectionParameter(
                Key="some-oauth-query", Value="some-oauth-value", IsValueSecret=False
            )
        ]
        assert d.connection_oauth_body_parameters == [
            EventConnectionParameter(
                Key="some-oauth-body", Value="some-oauth-value", IsValueSecret=False
            )
        ]
        assert d.connection_arn == "some-connection-arn"
        assert d.tags == ["TESTING"]
        assert d.state == DocumentState.UNSAVED

    def test_repr(self):
        d = EventApiDestination(
            name="test-api-destination",
            id="someorg:test-namespace:test-api-destination",
        )
        d_repr = repr(d)
        match_str = """\
            EventApiDestination: test-api-destination
              id: someorg:test-namespace:test-api-destination
            * Not up-to-date in the Descartes Labs catalog. Call `.save()` to save or update this record."""
        assert d_repr.strip("\n") == textwrap.dedent(match_str)

    @responses.activate
    def test_get(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "arn": "some-arn",
                        "connection_arn": "some-connection-arn",
                        "connection_authorization_type": "OAUTH_CLIENT_CREDENTIALS",
                        "connection_body_parameters": [
                            {
                                "Key": "some-body",
                                "Value": "some-value",
                                "IsValueSecret": False,
                            }
                        ],
                        "connection_description": "a connection description",
                        "connection_header_parameters": [
                            {
                                "Key": "some-header",
                                "Value": "some-value",
                                "IsValueSecret": False,
                            }
                        ],
                        "connection_oauth_body_parameters": [
                            {
                                "Key": "some-oauth-body",
                                "Value": "some-oauth-value",
                                "IsValueSecret": False,
                            }
                        ],
                        "connection_oauth_client_id": "some-client-id",
                        "connection_oauth_client_secret": "some-secret",
                        "connection_oauth_endpoint": "https://some.oauth.endpoint",
                        "connection_oauth_header_parameters": [
                            {
                                "Key": "some-oauth-header",
                                "Value": "some-oauth-value",
                                "IsValueSecret": False,
                            }
                        ],
                        "connection_oauth_method": "POST",
                        "connection_oauth_query_string_parameters": [
                            {
                                "Key": "some-oauth-query",
                                "Value": "some-oauth-value",
                                "IsValueSecret": False,
                            }
                        ],
                        "connection_name": "some-connection",
                        "connection_query_string_parameters": [
                            {
                                "Key": "some-query",
                                "Value": "some-value",
                                "IsValueSecret": False,
                            }
                        ],
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "endpoint": "https://some.endpoint",
                        "invocation_rate": 1,
                        "method": "POST",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-api-destination",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "writers": [],
                        "tags": ["TESTING"],
                    },
                    "id": "someorg:test-namespace:test-api-destination",
                    "type": "event_api_destination",
                }
            },
            status=200,
        )

        d = EventApiDestination.get(
            id="someorg:test-namespace:test-api-destination", client=self.client
        )
        assert isinstance(d.created, datetime)
        assert isinstance(d.modified, datetime)
        assert d.id == "someorg:test-namespace:test-api-destination"
        assert d.name == "test-api-destination"
        assert d.namespace == "someorg:test-namespace"
        assert d.description == "a generic description"
        assert d.endpoint == "https://some.endpoint"
        assert d.method == "POST"
        assert d.invocation_rate == 1
        assert d.arn == "some-arn"
        assert d.connection_name == "some-connection"
        assert d.connection_description == "a connection description"
        assert d.connection_header_parameters == [
            EventConnectionParameter(
                Key="some-header", Value="some-value", IsValueSecret=False
            )
        ]
        assert d.connection_query_string_parameters == [
            EventConnectionParameter(
                Key="some-query", Value="some-value", IsValueSecret=False
            )
        ]
        assert d.connection_body_parameters == [
            EventConnectionParameter(
                Key="some-body", Value="some-value", IsValueSecret=False
            )
        ]
        assert d.connection_authorization_type == "OAUTH_CLIENT_CREDENTIALS"
        assert d.connection_oauth_endpoint == "https://some.oauth.endpoint"
        assert d.connection_oauth_method == "POST"
        assert d.connection_oauth_client_id == "some-client-id"
        assert d.connection_oauth_client_secret == "some-secret"
        assert d.connection_oauth_header_parameters == [
            EventConnectionParameter(
                Key="some-oauth-header", Value="some-oauth-value", IsValueSecret=False
            )
        ]
        assert d.connection_oauth_query_string_parameters == [
            EventConnectionParameter(
                Key="some-oauth-query", Value="some-oauth-value", IsValueSecret=False
            )
        ]
        assert d.connection_oauth_body_parameters == [
            EventConnectionParameter(
                Key="some-oauth-body", Value="some-oauth-value", IsValueSecret=False
            )
        ]
        assert d.connection_arn == "some-connection-arn"
        assert d.owners == ["org:someorg"]
        assert d.readers == ["org:someorg"]
        assert d.writers == []
        assert d.tags == ["TESTING"]

    @responses.activate
    def test_get_unknown_attribute(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-api-destination",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "writers": [],
                        "tags": ["TESTING"],
                        "foobar": "baz",
                    },
                    "id": "someorg:test-namespace:test-api-destination",
                    "type": "event_api_destination",
                },
            },
            status=200,
        )

        d = EventApiDestination.get(
            id="someorg:test-namespace:test-api-destination", client=self.client
        )
        assert not hasattr(d, "foobar")

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
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-api-destination-1",
                            "namespace": "someorg:test-namespace",
                        },
                        "id": "someorg:test-namespace:test-api-destination-1",
                        "type": "event_api_destination",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-api-destination-2",
                            "namespace": "someorg:test-namespace",
                        },
                        "id": "someorg:test-namespace:test-api-destination-2",
                        "type": "event_api_destination",
                    },
                ],
            },
            status=200,
        )

        api_destinations = EventApiDestination.get_many(
            [
                "someorg:test-namespace:test-api-destination-1",
                "someorg:test-namespace:test-api-destination-2",
            ],
            client=self.client,
        )

        for i, d in enumerate(api_destinations):
            assert isinstance(d, EventApiDestination)
            assert d.id == f"someorg:test-namespace:test-api-destination-{i + 1}"

    @responses.activate
    def test_get_or_create(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-api-destination",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "writers": [],
                        "tags": ["TESTING"],
                    },
                    "id": "someorg:test-namespace:test-api-destination",
                    "type": "event_api_destination",
                },
            },
            status=200,
        )

        d = EventApiDestination.get_or_create(
            id="someorg:test-namespace:test-api-destination", client=self.client
        )
        assert d.id == "someorg:test-namespace:test-api-destination"

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
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-api-destination-1",
                            "namespace": "someorg:test-namespace",
                        },
                        "id": "someorg:test-namespace:test-api-destination-1",
                        "type": "event_api_destination",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-api-destination-2",
                            "namespace": "someorg:test-namespace",
                        },
                        "id": "someorg:test-namespace:test-api-destination-2",
                        "type": "event_api_destination",
                    },
                ],
            },
            status=200,
        )

        search = EventApiDestination.search(client=self.client)
        assert search.count() == 2
        assert isinstance(search, EventApiDestinationSearch)
        dc = search.collect()
        assert isinstance(dc, EventApiDestinationCollection)

    @responses.activate
    def test_list_no_results(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 0},
                "data": [],
            },
        )

        d = list(EventApiDestination.search(client=self.client))
        assert d == []

    @responses.activate
    def test_save(self):
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-api-destination",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "writers": [],
                        "tags": ["TESTING"],
                    },
                    "id": "someorg:test-namespace:test-api-destination",
                    "type": "event_api_destination",
                }
            },
            status=201,
        )

        d = EventApiDestination(
            id="someorg:test-namespace:test-api-destination",
            name="test-api-destination",
            client=self.client,
        )
        assert d.state == DocumentState.UNSAVED
        d.save()
        assert responses.calls[0].request.url == self.url + "/event_api_destinations"
        assert d.state == DocumentState.SAVED

    @responses.activate
    def test_save_dupe(self):
        self.mock_response(
            responses.POST,
            {
                "errors": [
                    {
                        "status": "409",
                        "detail": "A document with id `someorg:test-namespace:test-api-destination` already exists.",  # noqa: E501
                        "title": "Conflict",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=409,
        )
        d = EventApiDestination(
            id="someorg:test-namespace:test-api-destination", client=self.client
        )
        with pytest.raises(ConflictError):
            d.save()

    @responses.activate
    def test_exists(self):
        self.mock_response(responses.HEAD, {}, status=200)
        assert EventApiDestination.exists(
            "someorg:test-namespace:test-api-destination", client=self.client
        )
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/event_api_destinations/someorg:test-namespace:test-api-destination"
        )

    @responses.activate
    def test_exists_false(self):
        self.mock_response(responses.HEAD, self.not_found_json, status=404)
        assert not EventApiDestination.exists(
            "someorg:test-namespace:nonexistent-api_destination",
            client=self.client,
        )
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/event_api_destinations/someorg:test-namespace:nonexistent-api_destination"  # noqa: E501
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
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-api-destination",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "writers": [],
                        "tags": ["TESTING"],
                    },
                    "id": "someorg:test-namespace:test-api-destination",
                    "type": "event_api_destination",
                },
            },
            status=201,
        )

        d = EventApiDestination(
            id="someorg:test-namespace:test-api-destination",
            name="test-api-destination",
            client=self.client,
        )
        d.save()
        assert d.state == DocumentState.SAVED
        d.readers = ["org:acme-corp"]
        assert d.state == DocumentState.MODIFIED
        self.mock_response(
            responses.PATCH,
            {
                "meta": {"count": 1},
                "data": {
                    "attributes": {
                        "readers": ["org:acme-corp"],
                    },
                    "type": "event_api_destination",
                    "id": "someorg:test-namespace:test-api-destination",
                },
            },
            status=200,
        )
        d.save()
        assert d.readers == ["org:acme-corp"]

    @responses.activate
    def test_delete(self):
        d = EventApiDestination(
            id="someorg:test-namespace:test-api-destination",
            name="test-api-destination",
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

        d.delete()
        assert d.state == DocumentState.DELETED

    @responses.activate
    def test_class_delete(self):
        api_destination_id = "someorg:test-namespace:test-api-destination"
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )

        assert EventApiDestination.delete(api_destination_id, client=self.client)

    @responses.activate
    def test_delete_non_existent(self):
        d = EventApiDestination(
            id="someorg:test-namespace:nonexistent-api_destination",
            name="nonexistent-api_destination",
            client=self.client,
            _saved=True,
        )

        self.mock_response(
            responses.DELETE,
            self.not_found_json,
            status=404,
        )

        with pytest.raises(DeletedObjectError):
            d.delete()
