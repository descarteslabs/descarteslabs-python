try:
    import mock
except ImportError:
    from unittest import mock

import pytest
import re
import responses
import unittest

from descarteslabs.auth import Auth
from ....client.services.tasks import (
    Tasks,
    as_completed,
    GroupTerminalException,
)
from .. import FutureTask, TransientResultError, TimeoutError

# flake8: noqa
public_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJncm91cHMiOlsicHVibGljIl0sImlzcyI6Imh0dHBzOi8vZGVzY2FydGVzbGFicy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMTExMzg1NTY1MjQ4MTIzOTU3MTIiLCJhdWQiOiJaT0JBaTRVUk9sNWdLWklweHhsd09FZng4S3BxWGYyYyIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxNDc4MjAxNDE5fQ.sbSzD9ACNZvaxSgClZCnZMpee_p5MBaKV9uHZQonD6Q"


class ClientTestCase(unittest.TestCase):
    def setUp(self):
        url = "http://example.com"
        self.client = Tasks(
            url=url, auth=Auth(jwt_token=public_token, token_info_path=None)
        )
        self.match_url = re.compile(url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)


guid = "some_guid"
tuid = "some_tuid"


class TestFutureTask(ClientTestCase):
    def test_from_guid_tuid(self):
        ft = FutureTask(guid, tuid)
        ft2 = FutureTask(guid, tuid)

        assert ft == ft2
        assert isinstance(ft, FutureTask)
        assert isinstance(ft, object)

    def test_getattr_access(self):
        ft = FutureTask(guid=guid, tuid=tuid)

        assert ft.guid == guid
        assert ft.tuid == tuid

        with pytest.raises(AttributeError):
            ft.nonexistent

    def test_without_guid(self):
        with pytest.raises(TypeError):
            ft = FutureTask(tuid=tuid)

    def test_without_tuid(self):
        with pytest.raises(TypeError):
            ft = FutureTask(guid=guid)

    def test_without_guid_tuid(self):
        with pytest.raises(TypeError):
            ft = FutureTask()

    @responses.activate
    def test_transient_result(self):
        ft = FutureTask(guid=guid, tuid=tuid, client=self.client)

        self.mock_response(responses.GET, {}, status=404)

        with pytest.raises(TransientResultError):
            ft.get_result(wait=False)

    @responses.activate
    def test_get_result(self):
        ft = FutureTask(guid=guid, tuid=tuid, client=self.client)

        self.mock_response(responses.GET, {}, status=404)

        with pytest.raises(TimeoutError):
            ft.get_result(wait=True, timeout=1)

    @responses.activate
    def test_ready(self):
        ft = FutureTask(guid=guid, tuid=tuid, client=self.client)

        self.mock_response(responses.GET, {}, status=404)
        self.mock_response(responses.GET, {"id": tuid, "result_type": "json"})

        assert not ft.ready
        assert ft.ready

    @responses.activate
    @mock.patch.object(Tasks, "COMPLETION_POLL_INTERVAL_SECONDS", 0)
    @mock.patch.object(Tasks, "TASK_RESULT_BATCH_SIZE", 3)
    def test_as_completed(self):
        tasks = [FutureTask("group_id", str(n), client=self.client) for n in range(5)]

        response1 = [{"id": str(n), "result_type": "json"} for n in range(3)]
        response2 = [{"id": str(n), "result_type": "json"} for n in range(3, 5)]

        self.mock_response(
            responses.GET,
            {
                "id": "foo",
                "queue": {"pending": 3, "successes": 0, "failures": 0},
                "status": "running",
            },
        )
        self.mock_response(
            responses.GET,
            {
                "id": "foo",
                "queue": {"pending": 3, "successes": 0, "failures": 0},
                "status": "running",
            },
        )

        self.mock_response(responses.POST, {"results": response1})
        self.mock_response(responses.POST, {"results": response2})

        completed_tasks = list(as_completed(tasks, show_progress=False))

        assert 5 == len(completed_tasks)
        assert list(range(5)) == [int(r._task_result["id"]) for r in completed_tasks]

    @responses.activate
    @mock.patch.object(Tasks, "COMPLETION_POLL_INTERVAL_SECONDS", 0)
    @mock.patch.object(Tasks, "TASK_RESULT_BATCH_SIZE", 3)
    def test_as_completed_exception(self):
        tasks = [FutureTask("group_id", str(n), client=self.client) for n in range(5)]

        response1 = [{"id": str(n), "result_type": "json"} for n in range(3)]
        response2 = [{"id": str(n), "result_type": "json"} for n in range(3, 5)]

        self.mock_response(
            responses.GET,
            {
                "id": "foo",
                "queue": {"pending": 3, "successes": 0, "failures": 0},
                "status": "running",
            },
        )
        self.mock_response(
            responses.GET,
            {
                "id": "foo",
                "queue": {"pending": 3, "successes": 0, "failures": 0},
                "status": "terminated",
            },
        )

        self.mock_response(responses.POST, {"results": response1})
        self.mock_response(responses.POST, {"results": response2})

        with pytest.raises(GroupTerminalException):
            list(as_completed(tasks, show_progress=False))


if __name__ == "__main__":
    unittest.main()
