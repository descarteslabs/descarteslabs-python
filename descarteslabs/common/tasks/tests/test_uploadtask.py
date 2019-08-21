try:
    import mock
except ImportError:
    from unittest import mock

import re
import responses
import unittest

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.vector import Vector
from descarteslabs.common.tasks import UploadTask, TransientResultError, TimeoutError

# flake8: noqa
public_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJncm91cHMiOlsicHVibGljIl0sImlzcyI6Imh0dHBzOi8vZGVzY2FydGVzbGFicy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMTExMzg1NTY1MjQ4MTIzOTU3MTIiLCJhdWQiOiJaT0JBaTRVUk9sNWdLWklweHhsd09FZng4S3BxWGYyYyIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxNDc4MjAxNDE5fQ.sbSzD9ACNZvaxSgClZCnZMpee_p5MBaKV9uHZQonD6Q"


class ClientTestCase(unittest.TestCase):
    def setUp(self):
        url = "http://example.com"
        self.client = Vector(
            url=url, auth=Auth(jwt_token=public_token, token_info_path=None)
        )
        self.match_url = re.compile(url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)


group_id = "group-id"
upload_id = "upload-id"
task_id = "task-id"


class TestUploadTask(ClientTestCase):
    def test_from_guid_upload_id(self):
        task = UploadTask(group_id, upload_id=upload_id, client=self.client)

        assert task.upload_id == upload_id

    def test_from_guid_tuid(self):
        task = UploadTask(group_id, tuid=task_id, client=self.client)

        assert task.tuid == task_id

    @responses.activate
    def test_ready(self):
        task = UploadTask(group_id, upload_id=upload_id, client=self.client)

        self.mock_response(
            responses.GET,
            {"data": {"id": upload_id, "attributes": {"status": "PENDING"}}},
        )
        self.mock_response(
            responses.GET,
            {"data": {"id": upload_id, "attributes": {"status": "PENDING"}}},
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "id": task_id,
                    "attributes": {"status": "SUCCESS", "load": {"state": "RUNNING"}},
                }
            },
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "id": task_id,
                    "attributes": {"status": "SUCCESS", "load": {"state": "RUNNING"}},
                }
            },
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "id": task_id,
                    "attributes": {"status": "SUCCESS", "load": {"state": "SUCCESS"}},
                }
            },
        )

        assert not task.ready
        assert task.status == "PENDING"
        assert task.tuid is None

        assert not task.ready
        assert task.status == "RUNNING"
        assert task.tuid == task_id

        assert task.ready
        assert task.status == "SUCCESS"

    @responses.activate
    def test_ready_failure(self):
        task = UploadTask(group_id, upload_id=upload_id, client=self.client)

        self.mock_response(
            responses.GET,
            {"data": {"id": upload_id, "attributes": {"status": "PENDING"}}},
        )
        self.mock_response(
            responses.GET,
            {"data": {"id": upload_id, "attributes": {"status": "PENDING"}}},
        )
        self.mock_response(
            responses.GET,
            {"data": {"id": task_id, "attributes": {"status": "FAILURE"}}},
        )

        assert not task.ready
        assert task.status == "PENDING"
        assert task.tuid is None

        assert task.ready
        assert task.status == "FAILURE"
        assert task.tuid == task_id

    @responses.activate
    def test_ready_skipped(self):
        task = UploadTask(group_id, upload_id=upload_id, client=self.client)

        self.mock_response(
            responses.GET,
            {"data": {"id": upload_id, "attributes": {"status": "PENDING"}}},
        )
        self.mock_response(
            responses.GET,
            {"data": {"id": upload_id, "attributes": {"status": "PENDING"}}},
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "id": task_id,
                    "attributes": {"status": "SUCCESS", "load": {"state": "SKIPPED"}},
                }
            },
        )

        assert not task.ready
        assert task.status == "PENDING"
        assert task.tuid is None

        assert task.ready
        assert task.status == "SUCCESS"
        assert task.tuid == task_id

    @responses.activate
    def test_ready_failure_bq(self):
        task = UploadTask(group_id, upload_id=upload_id, client=self.client)

        self.mock_response(
            responses.GET,
            {"data": {"id": upload_id, "attributes": {"status": "PENDING"}}},
        )
        self.mock_response(
            responses.GET,
            {"data": {"id": upload_id, "attributes": {"status": "PENDING"}}},
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "id": task_id,
                    "attributes": {"status": "SUCCESS", "load": {"state": "PENDING"}},
                }
            },
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "id": task_id,
                    "attributes": {"status": "SUCCESS", "load": {"state": "PENDING"}},
                }
            },
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "id": task_id,
                    "attributes": {"status": "SUCCESS", "load": {"state": "FAILURE"}},
                }
            },
        )

        assert not task.ready
        assert task.status == "PENDING"
        assert task.tuid is None

        assert not task.ready
        assert task.status == "PENDING"
        assert task.tuid == task_id

        assert task.ready
        assert task.status == "FAILURE"

    @responses.activate
    def test_results(self):
        task = UploadTask(group_id, upload_id=upload_id, client=self.client)

        self.mock_response(
            responses.GET,
            {
                "data": {
                    "id": upload_id,
                    "attributes": {
                        "status": "SUCCESS",
                        "result": {
                            "errors": ["invalid geometry"],
                            "input_features": 1,
                            "input_rows": 1,
                        },
                        "load": {
                            "state": "DONE",
                            "errors": ["some BQ error"],
                            "output_rows": 1,
                        },
                    },
                }
            },
        )

        assert task.ready
        assert task.status == "SUCCESS"
        assert task.error_rows == 2
        assert len(task.errors) == 2
        assert task.input_features == 1
        assert task.input_rows == 1
        assert task.output_rows == 1
