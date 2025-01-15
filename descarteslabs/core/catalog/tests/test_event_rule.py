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
from ..event_rule import (
    EventRule,
    EventRuleCollection,
    EventRuleSearch,
    EventRuleTarget,
)
from ..catalog_base import DocumentState, DeletedObjectError


class TestEventRule(ClientTestCase):
    def test_constructor(self):
        r = EventRule(
            namespace="someorg:test-namespace",
            name="test-rule",
            id="someorg:test-namespace:test-rule",
            description="a description",
            event_pattern="""{"some": "pattern"}""",
            targets=[
                EventRuleTarget(
                    name="test-target",
                    arn="some-destination-arn",
                    role_arn="some-role-arn",
                    input="some-input",
                    ttl=123,
                    retries=2,
                    dead_letter_arn="some-dead-letter-arn",
                    path_parameter_values=["some-path"],
                    header_parameters={
                        "some-header": "some-value",
                    },
                    query_string_parameters={
                        "some-query": "some-value",
                    },
                    event_api_destination_id="someorg:test-namespace:test-destination",
                ),
            ],
            event_bus_arn="some-event-bus-arn",
            rule_arn="some-rule-arn",
            tags=["TESTING"],
        )

        assert r.namespace == "someorg:test-namespace"
        assert r.name == "test-rule"
        assert r.id == "someorg:test-namespace:test-rule"
        assert r.description == "a description"
        assert r.event_pattern == """{"some": "pattern"}"""
        assert len(r.targets) == 1
        assert r.targets[0].name == "test-target"
        assert r.targets[0].arn == "some-destination-arn"
        assert r.targets[0].role_arn == "some-role-arn"
        assert r.targets[0].input == "some-input"
        assert r.targets[0].ttl == 123
        assert r.targets[0].retries == 2
        assert r.targets[0].dead_letter_arn == "some-dead-letter-arn"
        assert r.targets[0].path_parameter_values == ["some-path"]
        assert r.targets[0].header_parameters == {"some-header": "some-value"}
        assert r.targets[0].query_string_parameters == {"some-query": "some-value"}
        assert (
            r.targets[0].event_api_destination_id
            == "someorg:test-namespace:test-destination"
        )
        assert r.event_bus_arn == "some-event-bus-arn"
        assert r.rule_arn == "some-rule-arn"
        assert r.tags == ["TESTING"]
        assert r.state == DocumentState.UNSAVED

    def test_repr(self):
        r = EventRule(
            name="test-rule",
            id="someorg:test-namespace:test-rule",
        )
        r_repr = repr(r)
        match_str = """\
            EventRule: test-rule
              id: someorg:test-namespace:test-rule
            * Not up-to-date in the Descartes Labs catalog. Call `.save()` to save or update this record."""
        assert r_repr.strip("\n") == textwrap.dedent(match_str)

    @responses.activate
    def test_get(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "event_pattern": """{"some": "pattern"}""",
                        "extra_properties": {},
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-rule",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "writers": [],
                        "tags": ["TESTING"],
                        "targets": [
                            {
                                "arn": "some-destination-arn",
                                "dead_letter_arn": "some-dead-letter-arn",
                                "event_api_destination_id": "someorg:test-namespace:test-destination",
                                "header_parameters": {
                                    "some-header": "some-value",
                                },
                                "input": "some-input",
                                "name": "test-target",
                                "path_parameter_values": ["some-path"],
                                "retries": 2,
                                "role_arn": "some-role-arn",
                                "query_string_parameters": {
                                    "some-query": "some-value",
                                },
                                "ttl": 123,
                            },
                        ],
                    },
                    "id": "someorg:test-namespace:test-rule",
                    "type": "event_rule",
                }
            },
            status=200,
        )

        r = EventRule.get(id="someorg:test-namespace:test-rule", client=self.client)
        assert isinstance(r.created, datetime)
        assert isinstance(r.modified, datetime)
        assert r.id == "someorg:test-namespace:test-rule"
        assert r.name == "test-rule"
        assert r.namespace == "someorg:test-namespace"
        assert r.description == "a generic description"
        assert r.event_pattern == """{"some": "pattern"}"""
        assert len(r.targets) == 1
        assert type(r.targets[0]) is EventRuleTarget
        assert r.targets[0].name == "test-target"
        assert r.targets[0].arn == "some-destination-arn"
        assert r.targets[0].role_arn == "some-role-arn"
        assert r.targets[0].input == "some-input"
        assert r.targets[0].ttl == 123
        assert r.targets[0].retries == 2
        assert r.targets[0].dead_letter_arn == "some-dead-letter-arn"
        assert r.targets[0].path_parameter_values == ["some-path"]
        assert r.targets[0].header_parameters == {"some-header": "some-value"}
        assert r.targets[0].query_string_parameters == {"some-query": "some-value"}
        assert (
            r.targets[0].event_api_destination_id
            == "someorg:test-namespace:test-destination"
        )
        assert r.owners == ["org:someorg"]
        assert r.readers == ["org:someorg"]
        assert r.writers == []
        assert r.tags == ["TESTING"]

    @responses.activate
    def test_get_unknown_attribute(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "event_pattern": """{"some": "pattern"}""",
                        "extra_properties": {},
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-rule",
                        "namespace": "someorg:test-namespace",
                        "targets": [],
                        "foobar": "baz",
                    },
                    "id": "someorg:test-namespace:test-rule",
                    "type": "event_rule",
                },
            },
            status=200,
        )

        r = EventRule.get(id="someorg:test-namespace:test-rule", client=self.client)
        assert not hasattr(r, "foobar")

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
                            "event_pattern": """{"some": "pattern"}""",
                            "extra_properties": {},
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-rule-1",
                            "namespace": "someorg:test-namespace",
                            "targets": [],
                        },
                        "id": "someorg:test-namespace:test-rule-1",
                        "type": "event_rule",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "event_pattern": """{"some": "pattern"}""",
                            "extra_properties": {},
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-rule-2",
                            "namespace": "someorg:test-namespace",
                            "targets": [],
                        },
                        "id": "someorg:test-namespace:test-rule-2",
                        "type": "event_rule",
                    },
                ],
            },
            status=200,
        )

        rules = EventRule.get_many(
            [
                "someorg:test-namespace:test-rule-1",
                "someorg:test-namespace:test-rule-2",
            ],
            client=self.client,
        )

        for i, r in enumerate(rules):
            assert isinstance(r, EventRule)
            assert r.id == f"someorg:test-namespace:test-rule-{i + 1}"

    @responses.activate
    def test_get_or_create(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "event_pattern": """{"some": "pattern"}""",
                        "extra_properties": {},
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-rule",
                        "namespace": "someorg:test-namespace",
                        "targets": [],
                    },
                    "id": "someorg:test-namespace:test-rule",
                    "type": "event_rule",
                },
            },
            status=200,
        )

        r = EventRule.get_or_create(
            id="someorg:test-namespace:test-rule", client=self.client
        )
        assert r.id == "someorg:test-namespace:test-rule"

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
                            "event_pattern": """{"some": "pattern"}""",
                            "extra_properties": {},
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-rule-1",
                            "namespace": "someorg:test-namespace",
                            "targets": [],
                        },
                        "id": "someorg:test-namespace:test-rule-1",
                        "type": "event_rule",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "event_pattern": """{"some": "pattern"}""",
                            "extra_properties": {},
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-rule-2",
                            "namespace": "someorg:test-namespace",
                            "targets": [],
                        },
                        "id": "someorg:test-namespace:test-rule-2",
                        "type": "event_rule",
                    },
                ],
            },
            status=200,
        )

        search = EventRule.search(client=self.client)
        assert search.count() == 2
        assert isinstance(search, EventRuleSearch)
        sc = search.collect()
        assert isinstance(sc, EventRuleCollection)

    @responses.activate
    def test_list_no_results(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 0},
                "data": [],
            },
        )

        r = list(EventRule.search(client=self.client))
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
                        "event_pattern": """{"some": "pattern"}""",
                        "extra_properties": {},
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-rule",
                        "namespace": "someorg:test-namespace",
                        "targets": [],
                    },
                    "id": "someorg:test-namespace:test-rule",
                    "type": "event_rule",
                }
            },
            status=201,
        )

        r = EventRule(
            id="someorg:test-namespace:test-rule",
            name="test-rule",
            client=self.client,
        )
        assert r.state == DocumentState.UNSAVED
        r.save()
        assert responses.calls[0].request.url == self.url + "/event_rules"
        assert r.state == DocumentState.SAVED

    @responses.activate
    def test_save_dupe(self):
        self.mock_response(
            responses.POST,
            {
                "errors": [
                    {
                        "status": "409",
                        "detail": "A document with id `someorg:test-namespace:test-rule` already exists.",
                        "title": "Conflict",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=409,
        )
        r = EventRule(id="someorg:test-namespace:test-rule", client=self.client)
        with pytest.raises(ConflictError):
            r.save()

    @responses.activate
    def test_exists(self):
        self.mock_response(responses.HEAD, {}, status=200)
        assert EventRule.exists("someorg:test-namespace:test-rule", client=self.client)
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/event_rules/someorg:test-namespace:test-rule"
        )

    @responses.activate
    def test_exists_false(self):
        self.mock_response(responses.HEAD, self.not_found_json, status=404)
        assert not EventRule.exists(
            "someorg:test-namespace:nonexistent-rule", client=self.client
        )
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/event_rules/someorg:test-namespace:nonexistent-rule"
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
                        "description": "a generic description",
                        "event_pattern": """{"some": "pattern"}""",
                        "extra_properties": {},
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-rule",
                        "namespace": "someorg:test-namespace",
                        "targets": [],
                    },
                    "id": "someorg:test-namespace:test-rule",
                    "type": "event_rule",
                },
            },
            status=200,
        )

        r = EventRule(
            id="someorg:test-namespace:test-rule",
            name="test-rule",
            client=self.client,
        )
        r.save()
        assert r.state == DocumentState.SAVED
        r.readers = ["org:acme-corp"]
        assert r.state == DocumentState.MODIFIED
        self.mock_response(
            responses.PATCH,
            {
                "meta": {"count": 1},
                "data": {
                    "attributes": {
                        "readers": ["org:acme-corp"],
                    },
                    "type": "event_rule",
                    "id": "someorg:test-namespace:test-rule",
                },
            },
            status=200,
        )
        r.save()
        assert r.readers == ["org:acme-corp"]

    @responses.activate
    def test_delete(self):
        r = EventRule(
            id="someorg:test-namespace:test-rule",
            name="test-rule",
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

        r.delete()
        assert r.state == DocumentState.DELETED

    @responses.activate
    def test_class_delete(self):
        rule_id = "someorg:test-namespace:test-rule"
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )

        assert EventRule.delete(rule_id, client=self.client)

    @responses.activate
    def test_delete_non_existent(self):
        r = EventRule(
            id="someorg:test-namespace:nonexistent-rule",
            name="nonexistent-rule",
            client=self.client,
            _saved=True,
        )

        self.mock_response(
            responses.DELETE,
            self.not_found_json,
            status=404,
        )

        with pytest.raises(DeletedObjectError):
            r.delete()
