import mock
import re
import responses
import unittest

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.tasks import Tasks, as_completed
from descarteslabs.common.tasks import FutureTask, TransientResultError, TimeoutError

# flake8: noqa
public_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJncm91cHMiOlsicHVibGljIl0sImlzcyI6Imh0dHBzOi8vZGVzY2FydGVzbGFicy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMTExMzg1NTY1MjQ4MTIzOTU3MTIiLCJhdWQiOiJaT0JBaTRVUk9sNWdLWklweHhsd09FZng4S3BxWGYyYyIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxNDc4MjAxNDE5fQ.sbSzD9ACNZvaxSgClZCnZMpee_p5MBaKV9uHZQonD6Q"


class ClientTestCase(unittest.TestCase):
    def setUp(self):
        url = "http://example.com"
        self.client = Tasks(url=url,
                            auth=Auth(jwt_token=public_token,
                                      token_info_path=None))
        self.match_url = re.compile(url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status,
            **kwargs)


guid = "some_guid"
tuid = "some_tuid"


class TestFutureTask(ClientTestCase):
    def test_from_guid_tuid(self):
        ft = FutureTask(guid, tuid)
        ft2 = FutureTask(guid, tuid)

        self.assertEqual(ft, ft2)
        self.assertIsInstance(ft, FutureTask)
        self.assertIsInstance(ft, object)

    def test_getattr_access(self):
        ft = FutureTask(guid=guid, tuid=tuid)

        self.assertEqual(ft.guid, guid)
        self.assertEqual(ft.tuid, tuid)

        with self.assertRaises(AttributeError):
            ft.nonexistent

    def test_without_guid(self):
        with self.assertRaises(TypeError):
            ft = FutureTask(tuid=tuid)

    def test_without_tuid(self):
        with self.assertRaises(TypeError):
            ft = FutureTask(guid=guid)

    def test_without_guid_tuid(self):
        with self.assertRaises(TypeError):
            ft = FutureTask()

    def test_transient_result(self):
        ft = FutureTask(guid=guid, tuid=tuid, client=self.client)

        with self.assertRaises(TransientResultError):
            ft.get_result(wait=False)

    def test_getattr_access(self):
        ft = FutureTask(guid=guid, tuid=tuid, client=self.client)

        with self.assertRaises(TimeoutError):
            ft.get_result(wait=True, timeout=1)

    @responses.activate
    @mock.patch.object(Tasks, "COMPLETION_POLL_INTERVAL_SECONDS", 0)
    @mock.patch.object(Tasks, "TASK_RESULT_BATCH_SIZE", 3)
    def test_as_completed(self):
        tasks = [FutureTask("group_id", str(n), client=self.client)
            for n in range(5)]

        response1 = [{'id': str(n), 'result_type': 'json'} for n in range(3)]
        response2 = [{'id': str(n), 'result_type': 'json'} for n in range(3, 5)]

        self.mock_response(responses.POST, {'results': response1})
        self.mock_response(responses.POST, {'results': response2})

        completed_tasks = list(as_completed(tasks, show_progress=False))

        self.assertEqual(5, len(completed_tasks))
        self.assertEqual(list(range(5)), [int(r._task_result['id'])
            for r in completed_tasks])


if __name__ == '__main__':
    unittest.main()
