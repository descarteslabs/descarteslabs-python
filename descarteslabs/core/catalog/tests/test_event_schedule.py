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
import pytest
import responses

import textwrap

from datetime import datetime, timezone

from descarteslabs.exceptions import ConflictError
from .base import ClientTestCase
from ..event_schedule import (
    EventSchedule,
    EventScheduleCollection,
    EventScheduleSearch,
)
from ..catalog_base import DocumentState, DeletedObjectError


class TestEventSchedule(ClientTestCase):
    def test_constructor(self):
        s = EventSchedule(
            namespace="someorg:test-namespace",
            name="test-sched",
            id="someorg:test-namespace:test-sched",
            description="a description",
            schedule="cron(* * * * * *)",
            schedule_timezone="America/New_York",
            start_datetime="2023-01-01",
            end_datetime="2024-01-01",
            flexible_time_window=10,
            enabled=True,
            tags=["TESTING"],
        )

        assert s.namespace == "someorg:test-namespace"
        assert s.name == "test-sched"
        assert s.id == "someorg:test-namespace:test-sched"
        assert s.description == "a description"
        assert s.schedule == "cron(* * * * * *)"
        assert s.schedule_timezone == "America/New_York"
        assert s.start_datetime == "2023-01-01"
        assert s.end_datetime == "2024-01-01"
        assert s.flexible_time_window == 10
        assert s.enabled is True
        assert s.tags == ["TESTING"]
        assert s.state == DocumentState.UNSAVED

    def test_repr(self):
        s = EventSchedule(
            name="test-sched",
            id="someorg:test-namespace:test-sched",
            schedule="rate(1 days)",
        )
        s_repr = repr(s)
        match_str = """\
            EventSchedule: test-sched
              id: someorg:test-namespace:test-sched
            * Not up-to-date in the Descartes Labs catalog. Call `.save()` to save or update this record."""
        assert s_repr.strip("\n") == textwrap.dedent(match_str)

    @responses.activate
    def test_get(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "enabled": True,
                        "end_datetime": "2024-01-01T00:00:00Z",
                        "extra_properties": {},
                        "flexible_time_window": 10,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sched",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "schedule": "cron(* * * * * *)",
                        "schedule_timezone": "America/New_York",
                        "start_datetime": "2023-01-01T00:00:00Z",
                        "tags": ["TESTING"],
                        "writers": [],
                    },
                    "id": "someorg:test-namespace:test-sched",
                    "type": "event_schedule",
                }
            },
            status=200,
        )

        s = EventSchedule.get(
            id="someorg:test-namespace:test-sched", client=self.client
        )
        assert isinstance(s.created, datetime)
        assert isinstance(s.modified, datetime)
        assert s.id == "someorg:test-namespace:test-sched"
        assert s.name == "test-sched"
        assert s.namespace == "someorg:test-namespace"
        assert s.description == "a generic description"
        assert s.schedule == "cron(* * * * * *)"
        assert s.schedule_timezone == "America/New_York"
        assert s.start_datetime == datetime(2023, 1, 1, tzinfo=timezone.utc)
        assert s.end_datetime == datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert s.flexible_time_window == 10
        assert s.enabled is True
        assert s.owners == ["org:someorg"]
        assert s.readers == ["org:someorg"]
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
                        "enabled": True,
                        "end_datetime": "2024-01-01T00:00:00Z",
                        "extra_properties": {},
                        "flexible_time_window": 10,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sched",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "schedule": "cron(* * * * * *)",
                        "schedule_timezone": "America/New_York",
                        "start_datetime": "2024-01-01T00:00:00Z",
                        "tags": ["TESTING"],
                        "writers": [],
                        "foobar": "unknown",
                    },
                    "id": "someorg:test-namespace:test-sched",
                    "type": "event_schedule",
                }
            },
            status=200,
        )

        s = EventSchedule.get(
            id="someorg:test-namespace:test-sched", client=self.client
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
                            "enabled": True,
                            "end_datetime": "2024-01-01T00:00:00Z",
                            "extra_properties": {},
                            "flexible_time_window": 10,
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-sched",
                            "namespace": "someorg:test-namespace",
                            "owners": ["org:someorg"],
                            "readers": ["org:someorg"],
                            "schedule": "cron(* * * * * *)",
                            "schedule_timezone": "America/New_York",
                            "start_datetime": "2024-01-01T00:00:00Z",
                            "tags": ["TESTING"],
                            "writers": [],
                        },
                        "id": "someorg:test-namespace:test-sched-1",
                        "type": "event_schedule",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "enabled": True,
                            "end_datetime": "2024-01-01T00:00:00Z",
                            "extra_properties": {},
                            "flexible_time_window": 10,
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-sched",
                            "namespace": "someorg:test-namespace",
                            "owners": ["org:someorg"],
                            "readers": ["org:someorg"],
                            "schedule": "cron(* * * * * *)",
                            "schedule_timezone": "America/New_York",
                            "start_datetime": "2024-01-01T00:00:00Z",
                            "tags": ["TESTING"],
                            "writers": [],
                        },
                        "id": "someorg:test-namespace:test-sched-2",
                        "type": "event_schedule",
                    },
                ],
            },
            status=200,
        )

        scheds = EventSchedule.get_many(
            [
                "someorg:test-namespace:test-sched-1",
                "someorg:test-namespace:test-sched-2",
            ],
            client=self.client,
        )

        for i, r in enumerate(scheds):
            assert isinstance(r, EventSchedule)
            assert r.id == f"someorg:test-namespace:test-sched-{i + 1}"

    @responses.activate
    def test_get_or_create(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": "a generic description",
                        "enabled": True,
                        "end_datetime": "2024-01-01T00:00:00Z",
                        "extra_properties": {},
                        "flexible_time_window": 10,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sched",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": ["org:someorg"],
                        "schedule": "cron(* * * * * *)",
                        "schedule_timezone": "America/New_York",
                        "start_datetime": "2024-01-01T00:00:00Z",
                        "tags": ["TESTING"],
                        "writers": [],
                    },
                    "id": "someorg:test-namespace:test-sched",
                    "type": "event_schedule",
                },
            },
            status=200,
        )

        s = EventSchedule.get_or_create(
            id="someorg:test-namespace:test-sched", client=self.client
        )
        assert s.id == "someorg:test-namespace:test-sched"

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
                            "enabled": True,
                            "end_datetime": "2024-01-01T00:00:00Z",
                            "extra_properties": {},
                            "flexible_time_window": 10,
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-sched",
                            "namespace": "someorg:test-namespace",
                            "owners": ["org:someorg"],
                            "readers": ["org:someorg"],
                            "schedule": "cron(* * * * * *)",
                            "schedule_timezone": "America/New_York",
                            "start_datetime": "2024-01-01T00:00:00Z",
                            "tags": ["TESTING"],
                            "writers": [],
                        },
                        "id": "someorg:test-namespace:test-sched-1",
                        "type": "event_schedule",
                    },
                    {
                        "attributes": {
                            "created": "2023-09-29T15:54:37.006769Z",
                            "description": "a generic description",
                            "enabled": True,
                            "end_datetime": "2024-01-01T00:00:00Z",
                            "extra_properties": {},
                            "flexible_time_window": 10,
                            "modified": "2023-09-29T15:54:37.006769Z",
                            "name": "test-sched",
                            "namespace": "someorg:test-namespace",
                            "owners": ["org:someorg"],
                            "readers": ["org:someorg"],
                            "schedule": "cron(* * * * * *)",
                            "schedule_timezone": "America/New_York",
                            "start_datetime": "2024-01-01T00:00:00Z",
                            "tags": ["TESTING"],
                            "writers": [],
                        },
                        "id": "someorg:test-namespace:test-sched-2",
                        "type": "event_schedule",
                    },
                ],
            },
            status=200,
        )

        search = EventSchedule.search(client=self.client)
        assert search.count() == 2
        assert isinstance(search, EventScheduleSearch)
        sc = search.collect()
        assert isinstance(sc, EventScheduleCollection)

    @responses.activate
    def test_list_no_results(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 0},
                "data": [],
            },
        )

        r = list(EventSchedule.search(client=self.client))
        assert r == []

    @responses.activate
    def test_save(self):
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "created": "2023-09-29T15:54:37.006769Z",
                        "description": None,
                        "enabled": True,
                        "end_datetime": None,
                        "extra_properties": {},
                        "flexible_time_window": 0,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sched",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": [],
                        "schedule": "rate(1 days)",
                        "schedule_timezone": None,
                        "start_datetime": None,
                        "tags": [],
                        "writers": [],
                    },
                    "id": "someorg:test-namespace:test-sched",
                    "type": "event_schedule",
                }
            },
            status=201,
        )

        s = EventSchedule(
            id="someorg:test-namespace:test-sched",
            name="test-sched",
            schedule="rate(1 days)",
            client=self.client,
        )
        assert s.state == DocumentState.UNSAVED
        s.save()
        assert responses.calls[0].request.url == self.url + "/event_schedules"
        assert s.state == DocumentState.SAVED

    @responses.activate
    def test_save_dupe(self):
        self.mock_response(
            responses.POST,
            {
                "errors": [
                    {
                        "status": "409",
                        "detail": "A document with id `someorg:test-namespace:test-sched` already exists.",
                        "title": "Conflict",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=409,
        )
        s = EventSchedule(id="someorg:test-namespace:test-sched", client=self.client)
        with pytest.raises(ConflictError):
            s.save()

    @responses.activate
    def test_exists(self):
        self.mock_response(responses.HEAD, {}, status=200)
        assert EventSchedule.exists(
            "someorg:test-namespace:test-sched", client=self.client
        )
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/event_schedules/someorg:test-namespace:test-sched"
        )

    @responses.activate
    def test_exists_false(self):
        self.mock_response(responses.HEAD, self.not_found_json, status=404)
        assert not EventSchedule.exists(
            "someorg:test-namespace:nonexistent-sched", client=self.client
        )
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/event_schedules/someorg:test-namespace:nonexistent-sched"
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
                        "enabled": True,
                        "end_datetime": None,
                        "extra_properties": {},
                        "flexible_time_window": 0,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sched",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": [],
                        "schedule": "rate(1 days)",
                        "schedule_timezone": None,
                        "start_datetime": None,
                        "tags": [],
                        "writers": [],
                    },
                    "id": "someorg:test-namespace:test-sched",
                    "type": "event_schedule",
                },
            },
            status=200,
        )

        s = EventSchedule(
            id="someorg:test-namespace:test-sched",
            name="test-sched",
            schedule="rate(1 days)",
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
                    "type": "event_schedule",
                    "id": "someorg:test-namespace:test-sched",
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
                        "enabled": True,
                        "end_datetime": None,
                        "extra_properties": {},
                        "flexible_time_window": 0,
                        "modified": "2023-09-29T15:54:37.006769Z",
                        "name": "test-sched",
                        "namespace": "someorg:test-namespace",
                        "owners": ["org:someorg"],
                        "readers": [],
                        "schedule": "rate(1 days)",
                        "schedule_timezone": None,
                        "start_datetime": None,
                        "tags": [],
                        "writers": [],
                    },
                    "id": "someorg:test-namespace:test-sched",
                    "type": "event_schedule",
                },
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/storage"},
            },
        )

        s = EventSchedule(
            id="someorg:test-namespace:test-sched",
            name="test-sched",
            schedule="rate(1 days)",
            client=self.client,
        )
        s.save()
        assert s.state == DocumentState.SAVED
        s.readers = ["org:acme-corp"]
        with pytest.raises(ValueError):
            s.reload()

    @responses.activate
    def test_delete(self):
        s = EventSchedule(
            id="someorg:test-namespace:test-sched",
            name="test-sched",
            schedule="rate(1 days)",
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
        sched_id = "someorg:test-namespace:test-sched"
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )

        assert EventSchedule.delete(sched_id, client=self.client)

    @responses.activate
    def test_delete_non_existent(self):
        s = EventSchedule(
            id="someorg:test-namespace:nonexistent-sched",
            name="nonexistent-sched",
            schedule="rate(1 days)",
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
