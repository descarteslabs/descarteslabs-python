# Copyright 2018-2019 Descartes Labs.
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
from __future__ import unicode_literals

import io
import json
import os
import re
import runpy
import shutil
import six
import sys
import tempfile
import unittest
import warnings
from zipfile import ZipFile

import responses
try:
    import mock
except ImportError:
    from unittest import mock

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.tasks import BoundGlobalError, CloudFunction, \
    Tasks, as_completed, GroupTerminalException
from descarteslabs.common.services.tasks.constants import DIST, DATA, ENTRYPOINT, FunctionType, REQUIREMENTS

from descarteslabs.common.tasks import FutureTask

# flake8: noqa
public_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJncm91cHMiOlsicHVibGljIl0sImlzcyI6Imh0dHBzOi8vZGVzY2FydGVzbGFicy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMTExMzg1NTY1MjQ4MTIzOTU3MTIiLCJhdWQiOiJaT0JBaTRVUk9sNWdLWklweHhsd09FZng4S3BxWGYyYyIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxNDc4MjAxNDE5fQ.sbSzD9ACNZvaxSgClZCnZMpee_p5MBaKV9uHZQonD6Q"

# Used in tests that need to reference a global
a_global = "A Global"


class ClientTestCase(unittest.TestCase):

    def setUp(self):
        url = "http://example.com"
        self.url = url
        self.client = Tasks(url=url, auth=Auth(jwt_token=public_token, token_info_path=None))
        self.match_url = re.compile(url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)


class TasksTest(ClientTestCase):

    @responses.activate
    @mock.patch.object(sys.modules['cloudpickle'], '__version__', '0.3.0')
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
        with warnings.catch_warnings(record=True) as w:
            group = self.client.new_group(f, 'task-image')
            self.assertEqual('foo', group.id)
            self.assertEqual(1, len(w))

    @responses.activate
    @mock.patch.object(sys.modules['cloudpickle'], '__version__', None)
    def test_cloudpickle_not_found(self):
        def f():
            return True
        self.mock_response(responses.POST, {}, status=201)
        with warnings.catch_warnings(record=True) as w:
            group = self.client.new_group(f, 'task-image')
            self.assertEqual(1, len(w))

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
        self.mock_response(responses.GET, {'id': 'foo', 'queue': {'pending': 3,
                                                                  'successes': 0, 'failures': 0}, 'status': 'running'})
        self.mock_response(responses.GET, {'id': 'foo', 'queue': {'pending': 0,
                                                                  'successes': 2, 'failures': 1}, 'status': 'running'})
        self.client.wait_for_completion('foo', show_progress=False)

    @responses.activate
    @mock.patch.object(Tasks, "COMPLETION_POLL_INTERVAL_SECONDS", 0)
    def test_wait_for_completion_group_terminated(self):

        self.mock_response(responses.GET, {'id': 'foo', 'queue': {'pending': 3,
                                                                  'successes': 0, 'failures': 0}, 'status': 'running'})
        self.mock_response(responses.GET, {'id': 'foo', 'queue': {'pending': 3,
                                                                  'successes': 0, 'failures': 0}, 'status': 'terminated'})

        with self.assertRaises(GroupTerminalException):
            self.client.wait_for_completion('foo', show_progress=False)


class TasksPackagingTest(ClientTestCase):

    TEST_DATA_PATH = os.path.join(os.path.dirname(__file__), "data")
    TEST_PACKAGE_NAME = "dl_test_package"
    DATA_FILE_RELATIVE_PATH = os.path.join(TEST_PACKAGE_NAME, "data.json")
    DATA_FILE_ZIP_PATH = "{}/data.json".format(TEST_PACKAGE_NAME)
    DATA_FILE_PATH = os.path.join(TEST_DATA_PATH, DATA_FILE_RELATIVE_PATH)
    TEST_MODULE = "{}.package.module".format(TEST_PACKAGE_NAME)
    TEST_MODULE_ZIP_PATH = "{}/package/module.py".format(TEST_PACKAGE_NAME)
    GLOBAL_STRING = 'A global var'
    LOCAL_STRING = 'A local var'

    def setUp(self):
        super(TasksPackagingTest, self).setUp()
        self._sys_path = sys.path
        sys.path += [self.TEST_DATA_PATH]

    def tearDown(self):
        sys.path = self._sys_path
        super(TasksPackagingTest, self).tearDown()

    @staticmethod
    def a_function():
        # Used as a function referenced by name in a test
        print("a_function")

    def call_function(self, zipfile, expected_return_value):
        # Remember settings
        curdir = os.getcwd()
        tempdir = None
        sys_path = sys.path
        sys_modules = sys.modules.copy()

        try:
            # Create a temp directory to extract the sources into
            tempdir = tempfile.mkdtemp()
            zipfile.extractall(tempdir)

            # Set the env to only look at the temp directory
            os.chdir(tempdir)
            sys.path = ["{}/{}".format(tempdir, DIST)]
            sys.modules.clear()

            # Import the module using a clean environment
            env = runpy.run_module(os.path.splitext(ENTRYPOINT)[0])

            # Run the resulting imported main function
            value = env['main']()

            # And compare the return value
            self.assertEqual(expected_return_value, value)
        finally:
            # Restore environment
            sys.modules.update(sys_modules)
            os.chdir(curdir)
            sys.path = sys_path

            if tempdir and os.path.exists(tempdir):
                shutil.rmtree(tempdir)

    @responses.activate
    def test_new_group(self):
        def foo():
            pass
        upload_url = 'https://storage.google.com/upload/b/dl-pex-storage/o/12345343'
        resp_json = {'id': 12345343, 'upload_url': upload_url}
        self.mock_response(responses.POST, status=201, json=resp_json)
        responses.add(responses.PUT, upload_url, status=200)
        with mock.patch("os.remove"):  # Don't delete bundle so we can read it back below
            self.client.new_group(foo, 'task-image', include_data=[self.DATA_FILE_PATH], include_modules=[self.TEST_MODULE])

        body = responses.calls[0].request.body.decode("utf-8")  # prior to 3.6, json does not accept bytes
        call_args = json.loads(body)
        bundle = responses.calls[1].request.body
        try:
            with ZipFile(bundle.name, mode='r') as zf:
                self.assertGreater(len(zf.namelist()), 0)
        finally:
            os.remove(bundle.name)

        self.assertEqual(call_args['function_type'], FunctionType.PY_BUNDLE)

    def test_write_main_function_exceptions(self):
        self.assertRaises(ValueError, self.client._write_main_function, map, None)
        self.assertRaises(ValueError, self.client._write_main_function, lambda x: x, None)

    def test_write_main_function(self):
        def foo():
            print("foo")

        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            with ZipFile(f, mode='w') as arc:
                self.client._write_main_function(foo, arc)
            f.seek(0)
            with ZipFile(f, mode='r') as arc:
                entrypoint_path = '{}/{}'.format(DIST, ENTRYPOINT)
                self.assertIn(entrypoint_path, arc.namelist())
                with arc.open(entrypoint_path) as entrypoint:
                    source = entrypoint.read()
                    self.assertIn(b'main = foo', source)

    def test_find_data_files_glob(self):
        pattern = os.path.join(self.TEST_DATA_PATH, 'dl_test_package/*.json')
        data_files = self.client._find_data_files([pattern])
        self.assertEqual([(self.DATA_FILE_PATH, os.path.join(DATA, self.DATA_FILE_RELATIVE_PATH))], data_files)

    def test_find_data_files(self):
        data_files = self.client._find_data_files([self.DATA_FILE_PATH])
        self.assertEqual([(self.DATA_FILE_PATH, os.path.join(DATA, self.DATA_FILE_RELATIVE_PATH))], data_files)

    def test_find_data_files_directory(self):
        self.assertRaises(ValueError, self.client._find_data_files, ["descarteslabs"])

    def test_find_data_files_missing(self):
        self.assertRaises(ValueError, self.client._find_data_files, ["descarteslabs/foobar.txt"])

    def test_find_data_files_glob_missing(self):
        with warnings.catch_warnings(record=True) as w:
            data_files = self.client._find_data_files(["descarteslabs/foobar/*.txt"])
            self.assertEqual([], data_files)
            self.assertEqual(1, len(w))

    def test_include_modules_exceptions(self):
        self.assertRaises(ImportError, self.client._write_include_modules, ['doesnt.exist'], None)

    def test_include_modules(self):
        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            with ZipFile(f, mode='w') as arc:
                self.client._write_include_modules([self.TEST_MODULE], arc)
            f.seek(0)
            with ZipFile(f, mode='r') as arc:
                path = "{}/{}".format(DIST, self.TEST_MODULE_ZIP_PATH)
                init_path = "{}/dl_test_package/package/__init__.py".format(DIST)
                pkg_init_path = "{}/dl_test_package/__init__.py".format(DIST)
                self.assertIn(path, arc.namelist())
                self.assertIn(init_path, arc.namelist())
                self.assertIn(pkg_init_path, arc.namelist())
                with arc.open(path) as fixture_data:
                    self.assertIn(b'def foo()', fixture_data.read())

    @mock.patch.object(sys, "path", new=[os.path.relpath(TEST_DATA_PATH)])
    def test_include_modules_relative_sys_path(self):
        with tempfile.NamedTemporaryFile(suffix='.zip') as f:
            with ZipFile(f, mode='w') as arc:
                self.client._write_include_modules([self.TEST_MODULE], arc)
            f.seek(0)
            with ZipFile(f, mode='r') as arc:
                path = "{}/{}".format(DIST, self.TEST_MODULE_ZIP_PATH)
                self.assertIn(path, arc.namelist())

    def test_build_bundle(self):
        module_path = "{}/{}".format(DIST, self.TEST_MODULE_ZIP_PATH)
        data_path = "{}/{}".format(DATA, self.DATA_FILE_ZIP_PATH)

        def foo():
            pass

        zf = self.client._build_bundle(
            foo,
            [self.DATA_FILE_PATH],
            [self.TEST_MODULE]
        )

        try:
            with ZipFile(zf) as arc:
                self.assertIn(module_path, arc.namelist())
                self.assertIn(data_path, arc.namelist())
                self.assertNotIn(REQUIREMENTS, arc.namelist())
        finally:
            if os.path.exists(zf):
                os.remove(zf)

    def test_build_bundle_with_globals(self):

        def foo():
            print(a_global)

        class Foo:
            @staticmethod
            def bar():
                print(a_global)

        with self.assertRaises(BoundGlobalError):
            self.client._build_bundle(
                foo,
                [self.DATA_FILE_PATH],
                [self.TEST_MODULE]
            )

        with self.assertRaises(BoundGlobalError):
            self.client._build_bundle(
                Foo.bar,
                [self.DATA_FILE_PATH],
                [self.TEST_MODULE]
            )

    def test_build_bundle_with_named_function(self):
        zf = self.client._build_bundle(
            self.TEST_MODULE + ".func_foo",
            [self.DATA_FILE_PATH],
            [self.TEST_MODULE]
        )

        try:
            with ZipFile(zf) as arc:
                self.call_function(arc, self.LOCAL_STRING + self.GLOBAL_STRING)
        finally:
            if os.path.exists(zf):
                os.remove(zf)

        # And a nested function
        zf = self.client._build_bundle(
            self.TEST_MODULE + ".outer_class.inner_class.func_bar",
            [self.DATA_FILE_PATH],
            [self.TEST_MODULE]
        )

        try:
            with ZipFile(zf) as arc:
                self.call_function(arc, self.LOCAL_STRING + self.GLOBAL_STRING)
        finally:
            if os.path.exists(zf):
                os.remove(zf)

    def test_build_bundle_with_named_function_bad(self):
        with self.assertRaises(NameError):
            zf = self.client._build_bundle(
                "func.func_foo",
                [self.DATA_FILE_PATH],
                [self.TEST_MODULE]
            )

        zf = self.client._build_bundle(
            "descarteslabs.client.services.tasks.tests.test_tasks.TasksPackagingTest.a_function",
            [self.DATA_FILE_PATH],
            [self.TEST_MODULE]
        )

        try:
            with ZipFile(zf) as arc:
                with self.assertRaises(ImportError):
                    self.call_function(arc, self.LOCAL_STRING + self.GLOBAL_STRING)
        finally:
            if os.path.exists(zf):
                os.remove(zf)

    def test_build_bundle_requirements(self):
        def foo():
            pass
        zf = self.client._build_bundle(foo, None, None, ["foo", "bar"])
        try:
            with ZipFile(zf) as arc:
                self.assertEqual(b"foo\nbar", arc.read(REQUIREMENTS))
        finally:
            os.remove(zf)

    def test_requirements_string(self):
        self.assertEqual("requests", self.client._requirements_string(["requests"]))
        self.assertEqual(
            "foo>=1.2\nbar[foo]\nbaz;python_version<\"2.7\"",
            self.client._requirements_string(["foo>=1.2", "bar[foo]", 'baz;python_version<"2.7"'])
        )

    def test_requirements_string_file(self):
        good_requirements = os.path.join(self.TEST_DATA_PATH, "good_requirements.txt")
        self.assertEqual(open(good_requirements).read(), self.client._requirements_string(good_requirements))

    def test_requirements_string_bad(self):
        self.assertRaises(ValueError, self.client._requirements_string, ["foo\nbar"])
        self.assertRaises(ValueError, self.client._requirements_string, ["foo", ""])
        self.assertRaises(ValueError, self.client._requirements_string, ["foo >>> 1.0"])

    def test_requirements_string_file_bad(self):
        self.assertRaises(ValueError, self.client._requirements_string, "non-existent.txt")
        bad_requirements = os.path.join(self.TEST_DATA_PATH, "bad_requirements.txt")
        self.assertTrue(os.path.exists(bad_requirements))
        self.assertRaises(ValueError, self.client._requirements_string, bad_requirements)


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


if __name__ == "__main__":
    unittest.main()
