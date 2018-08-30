# Copyright 2018 Descartes Labs.
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

import re
import unittest

import mock
import responses

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.tasks import CloudFunction, FutureTask, Tasks, as_completed

# flake8: noqa
public_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJncm91cHMiOlsicHVibGljIl0sImlzcyI6Imh0dHBzOi8vZGVzY2FydGVzbGFicy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMTExMzg1NTY1MjQ4MTIzOTU3MTIiLCJhdWQiOiJaT0JBaTRVUk9sNWdLWklweHhsd09FZng4S3BxWGYyYyIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxNDc4MjAxNDE5fQ.sbSzD9ACNZvaxSgClZCnZMpee_p5MBaKV9uHZQonD6Q"


class ClientTestCase(unittest.TestCase):

    def setUp(self):
        url = "http://example.com"
        self.client = Tasks(url=url, auth=Auth(jwt_token=public_token, token_info_path=None))
        self.match_url = re.compile(url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)


class TasksTest(ClientTestCase):

    @responses.activate
    def test_new_group(self):
        def f():
            return True
        self.mock_response(
            responses.POST,
            {'error': 409, 'message': 'namespace is missing authentication'},
            status=409
        )
        self.mock_response(responses.POST, {}, status=201)
        self.mock_response(responses.POST, {'id': 'foo'})
        group = self.client.new_group(f)
        self.assertEqual('foo', group.id)

    @responses.activate
    def test_iter_groups(self):
        self.mock_response(responses.GET, {'groups': [{'id': 'foo'}], 'continuation_token': 'continue'})
        self.mock_response(responses.GET, {'groups': [{'id': 'bar'}], 'continuation_token': None})
        groups = self.client.iter_groups()
        self.assertEqual(['foo', 'bar'], [group.id for group in groups])

    @responses.activate
    def test_new_task(self):
        self.mock_response(responses.POST, {'tasks': [{'id': 'foo'}]})
        tasks = self.client.new_task("group_id", arguments=('foo'), parameters={'bar': 'baz'})
        self.assertEqual('foo', tasks.tasks[0].id)

    @responses.activate
    def test_iter_task_results(self):
        self.mock_response(responses.GET, {'results': [{'id': 'foo'}], 'continuation_token': 'continue'})
        self.mock_response(responses.GET, {'results': [{'id': 'bar'}], 'continuation_token': None})
        results = self.client.iter_task_results("group_id")
        self.assertEqual(['foo', 'bar'], [result.id for result in results])

    @responses.activate
    @mock.patch.object(Tasks, "COMPLETION_POLL_INTERVAL_SECONDS", 0)
    def test_wait_for_completion(self):
        self.mock_response(responses.GET, {'id': 'foo', 'queue': {'pending': 3, 'successes': 0, 'failures': 0}})
        self.mock_response(responses.GET, {'id': 'foo', 'queue': {'pending': 0, 'successes': 2, 'failures': 1}})
        self.client.wait_for_completion('foo', show_progress=False)


class CloudFunctionTest(ClientTestCase):

    def setUp(self):
        super(CloudFunctionTest, self).setUp()
        self.function = CloudFunction("group_id", client=self.client)

    @responses.activate
    def test_call(self):
        self.mock_response(responses.POST, {'tasks': [{'id': 'foo'}]})
        task = self.function("foo", bar="baz")
        self.assertEqual(self.function.group_id, task.guid)
        self.assertEqual("foo", task.tuid)
        self.assertEqual(("foo",), task.args)
        self.assertEqual({"bar": "baz"}, task.kwargs)

    @responses.activate
    def test_map(self):
        self.mock_response(responses.POST, {'tasks': [{'id': 'foo'}, {'id': 'bar'}]})
        tasks = self.function.map(iter(["foo", "bar"]))
        self.assertEqual(["foo", "bar"], [task.tuid for task in tasks])
        self.assertEqual([("foo",), ("bar",)], [task.args for task in tasks])

    @responses.activate
    def test_map_multi(self):
        self.mock_response(responses.POST, {'tasks': [{'id': 'foo'}, {'id': 'bar'}]})
        tasks = self.function.map(iter(["foo", "bar"]), iter(["baz"]))
        self.assertEqual(["foo", "bar"], [task.tuid for task in tasks])
        self.assertEqual([("foo", "baz"), ("bar", None)], [task.args for task in tasks])


class FutureTaskTest(ClientTestCase):

    @responses.activate
    @mock.patch.object(Tasks, "COMPLETION_POLL_INTERVAL_SECONDS", 0)
    @mock.patch.object(Tasks, "TASK_RESULT_BATCH_SIZE", 3)
    def test_as_completed(self):
        tasks = [FutureTask("group_id", str(n), client=self.client) for n in range(5)]
        response1 = [{'id': str(n), 'result_type': 'json'} for n in range(3)]
        response2 = [{'id': str(n), 'result_type': 'json'} for n in range(3, 5)]
        self.mock_response(responses.POST, {'results': response1})
        self.mock_response(responses.POST, {'results': response2})
        completed_tasks = list(as_completed(tasks, show_progress=False))
        self.assertEqual(5, len(completed_tasks))
        self.assertEqual(list(range(5)), [int(r._task_result['id']) for r in completed_tasks])


if __name__ == "__main__":
    unittest.main()
