try:
    import mock
except ImportError:
    from unittest import mock

import re
import responses
import unittest

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.vector import Vector
from descarteslabs.common.tasks import ExportTask, TransientResultError, TimeoutError

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
export_id = "export-id"
task_id = "task-id"


class TestExportTask(ClientTestCase):
    def test_from_key_export_id(self):
        key = "my_key"
        task = ExportTask(group_id, tuid=task_id, key=key, client=self.client)

        assert task.key == key

    @responses.activate
    def test_from_attributes(self):
        key = "my_key"
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "id": export_id,
                    "attributes": {"status": "SUCCESS", "labels": [1, 2, 3, key]},
                }
            },
        )
        task = ExportTask(group_id, tuid=task_id, client=self.client)
        assert task.status == "SUCCESS"
        assert task.key == key
