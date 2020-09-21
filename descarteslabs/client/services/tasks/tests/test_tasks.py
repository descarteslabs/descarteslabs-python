# Copyright 2018-2020 Descartes Labs.
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

import copy
import io
import json
import os
import pytest
import re
import runpy
import shutil
import six
import sys
import tempfile
import unittest
import warnings
from zipfile import ZipFile
from pathlib import Path, PurePosixPath

import responses

try:
    import mock
except ImportError:
    from unittest import mock

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.tasks import (
    BoundGlobalError,
    CloudFunction,
    Tasks,
    as_completed,
    GroupTerminalException,
)
from descarteslabs.common.services.tasks.constants import (
    DIST,
    DATA,
    ENTRYPOINT,
    FunctionType,
    REQUIREMENTS,
)

from descarteslabs.common.tasks import FutureTask

# flake8: noqa
public_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJncm91cHMiOlsicHVibGljIl0sImlzcyI6Imh0dHBzOi8vZGVzY2FydGVzbGFicy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMTExMzg1NTY1MjQ4MTIzOTU3MTIiLCJhdWQiOiJaT0JBaTRVUk9sNWdLWklweHhsd09FZng4S3BxWGYyYyIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxNDc4MjAxNDE5fQ.sbSzD9ACNZvaxSgClZCnZMpee_p5MBaKV9uHZQonD6Q"

# Used in tests that need to reference a global
a_global = "A Global"


class ClientTestCase(unittest.TestCase):
    def setUp(self):
        url = "http://example.com"
        self.url = url
        self.client = Tasks(
            url=url, auth=Auth(jwt_token=public_token, token_info_path=None)
        )
        self.match_url = re.compile(url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)


class TasksTest(ClientTestCase):
    @pytest.mark.skipif(
        sys.version_info >= (3, 8), reason="requires python3.7 or lower"
    )
    @responses.activate
    @mock.patch.object(sys.modules.get("cloudpickle", {}), "__version__", "0.3.0")
    def test_new_group(self):
        def f():
            # force pickling
            return sys.version_info

        self.mock_response(
            responses.POST,
            {"error": 409, "message": "namespace is missing authentication"},
            status=409,
        )
        self.mock_response(responses.POST, {}, status=201)
        self.mock_response(responses.POST, {"id": "foo"})
        with warnings.catch_warnings(record=True) as w:
            group = self.client.new_group(f, "task-image")
            assert "foo" == group.id
            assert 2 == len(w)
            assert "cloudpickle" in str(w[0].message)

    @pytest.mark.skipif(
        sys.version_info >= (3, 8), reason="requires python3.7 or lower"
    )
    @responses.activate
    @mock.patch.object(sys.modules.get("cloudpickle", {}), "__version__", None)
    def test_cloudpickle_not_found(self):
        def f():
            # force pickling
            return sys.version_info

        self.mock_response(responses.POST, {}, status=201)
        with warnings.catch_warnings(record=True) as w:
            group = self.client.new_group(f, "task-image")
            assert 2 == len(w)
            assert "cloudpickle" in str(w[0].message)

    @responses.activate
    def test_iter_groups(self):
        self.mock_response(
            responses.GET, {"groups": [{"id": "foo"}], "continuation_token": "continue"}
        )
        self.mock_response(
            responses.GET, {"groups": [{"id": "bar"}], "continuation_token": None}
        )
        groups = self.client.iter_groups()
        assert ["foo", "bar"] == [group.id for group in groups]

    @responses.activate
    def test_new_task(self):
        self.mock_response(responses.POST, {"tasks": [{"id": "foo"}]})
        tasks = self.client.new_task(
            "group_id", arguments=("foo"), parameters={"bar": "baz"}
        )
        assert "foo" == tasks.tasks[0].id

    @responses.activate
    def test_iter_task_results(self):
        self.mock_response(
            responses.GET,
            {"results": [{"id": "foo"}], "continuation_token": "continue"},
        )
        self.mock_response(
            responses.GET, {"results": [{"id": "bar"}], "continuation_token": None}
        )
        results = self.client.iter_task_results("group_id")
        assert ["foo", "bar"] == [result.id for result in results]

    @responses.activate
    @mock.patch.object(Tasks, "COMPLETION_POLL_INTERVAL_SECONDS", 0)
    def test_wait_for_completion(self):
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
                "queue": {"pending": 0, "successes": 2, "failures": 1},
                "status": "running",
            },
        )
        self.client.wait_for_completion("foo", show_progress=False)

    @responses.activate
    @mock.patch.object(Tasks, "COMPLETION_POLL_INTERVAL_SECONDS", 0)
    def test_wait_for_completion_group_terminated(self):

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

        with pytest.raises(GroupTerminalException):
            self.client.wait_for_completion("foo", show_progress=False)

    @responses.activate
    def test_get_function_by_id(self):
        self.mock_response(responses.GET, {"id": "foo", "name": "bar"})

        f = self.client.get_function_by_id("foo")
        assert isinstance(f, CloudFunction)


class TasksPackagingTest(ClientTestCase):

    TEST_DATA_PATH = os.path.join(tempfile.gettempdir(), "data")
    TEST_PACKAGE_NAME = "dl_test_package"
    DATA_FILE_RELATIVE_PATH = os.path.join(TEST_PACKAGE_NAME, "data.json")
    DATA_FILE_ZIP_PATH = os.path.join(TEST_PACKAGE_NAME, "data.json")
    DATA_FILE_PATH = os.path.join(TEST_DATA_PATH, DATA_FILE_RELATIVE_PATH)
    TEST_MODULE = "{}.package.module".format(TEST_PACKAGE_NAME)
    TEST_MODULE_ZIP_PATH = os.path.join(TEST_PACKAGE_NAME, "package", "module.py")
    TEST_MODULE_CYTHON = "{}.package.cython_module".format(TEST_PACKAGE_NAME)
    TEST_MODULE_CYTHON_ZIP_PATH = os.path.join(
        TEST_PACKAGE_NAME, "package", "cython_module.pyx"
    )
    TEST_MODULE_LIST = [TEST_MODULE, TEST_MODULE_CYTHON]
    TEST_MODULE_ZIP_PATH_LIST = [TEST_MODULE_ZIP_PATH, TEST_MODULE_CYTHON_ZIP_PATH]

    GLOBAL_STRING = "A global var"
    LOCAL_STRING = "A local var"

    def setUp(self):
        super(TasksPackagingTest, self).setUp()
        self._sys_path = copy.copy(sys.path)
        sys.path = [self.TEST_DATA_PATH] + sys.path

        # copy data directory into temporary directory
        shutil.copytree(
            os.path.join(os.path.dirname(__file__), "data"),
            os.path.join(tempfile.gettempdir(), "data"),
        )

    def tearDown(self):
        # remove temporary data directory
        shutil.rmtree(self.TEST_DATA_PATH)
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
        # python 3.8 ends up with funky imports of the core library if you remove
        # everything; we only depend on descarteslabs being removed.
        sys_modules = {
            k: v for k, v in sys.modules.items() if k.startswith("descarteslabs")
        }

        try:
            # Create a temp directory to extract the sources into
            tempdir = tempfile.mkdtemp()
            zipfile.extractall(tempdir)

            # Set the env to only look at the temp directory
            os.chdir(tempdir)
            sys.path = ["{}/{}".format(tempdir, DIST)]
            # remove descarteslabs modules
            for k in sys_modules:
                sys.modules.pop(k)

            # Import the module using a clean environment
            env = runpy.run_module(os.path.splitext(ENTRYPOINT)[0])

            # Run the resulting imported main function
            value = env["main"]()

            # And compare the return value
            assert expected_return_value == value
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

        upload_url = "https://storage.google.com/upload/b/dl-pex-storage/o/12345343"
        resp_json = {"id": 12345343, "upload_url": upload_url}
        self.mock_response(responses.POST, status=201, json=resp_json)
        responses.add(responses.PUT, upload_url, status=200)
        with mock.patch(
            "os.remove"
        ):  # Don't delete bundle so we can read it back below
            self.client.new_group(
                foo,
                "task-image",
                include_data=[self.DATA_FILE_PATH],
                include_modules=[self.TEST_MODULE],
            )

        body = responses.calls[0].request.body.decode(
            "utf-8"
        )  # prior to 3.6, json does not accept bytes
        call_args = json.loads(body)
        bundle = responses.calls[1].request.body
        try:
            with ZipFile(bundle.name, mode="r") as zf:
                assert len(zf.namelist()) > 0
        finally:
            os.remove(bundle.name)

        assert call_args["function_type"] == FunctionType.PY_BUNDLE

    @responses.activate
    def test_new_group_default_bundle(self):
        def foo():
            pass

        upload_url = "https://storage.google.com/upload/b/dl-pex-storage/o/12345343"
        resp_json = {"id": 12345343, "upload_url": upload_url}
        self.mock_response(responses.POST, status=201, json=resp_json)
        responses.add(responses.PUT, upload_url, status=200)
        with mock.patch(
            "os.remove"
        ):  # Don't delete bundle so we can read it back below
            self.client.new_group(
                foo, "task-image",
            )

        body = responses.calls[0].request.body.decode(
            "utf-8"
        )  # prior to 3.6, json does not accept bytes
        call_args = json.loads(body)
        bundle = responses.calls[1].request.body
        try:
            with ZipFile(bundle.name, mode="r") as zf:
                assert len(zf.namelist()) > 0
        finally:
            os.remove(bundle.name)

        assert call_args["function_type"] == FunctionType.PY_BUNDLE

    def test_write_main_function_exceptions(self):
        with pytest.raises(ValueError):
            self.client._write_main_function(map, None)

        with pytest.raises(ValueError):
            self.client._write_main_function(lambda x: x, None)

    def test_write_main_function(self):
        def foo():
            print("foo")

        with tempfile.NamedTemporaryFile(suffix=".zip") as f:
            with ZipFile(f, mode="w") as arc:
                self.client._write_main_function(foo, arc)
            f.seek(0)
            with ZipFile(f, mode="r") as arc:
                entrypoint_path = "{}/{}".format(DIST, ENTRYPOINT)
                assert entrypoint_path in arc.namelist()
                # open file in `arc` with a consistent posixpath (windows and linux compat)
                with arc.open(str(PurePosixPath(Path(entrypoint_path)))) as entrypoint:
                    source = entrypoint.read()
                    assert b"main = foo" in source

    def test_find_data_files_glob(self):
        pattern = os.path.join(
            self.TEST_DATA_PATH, "{}/*.json".format(self.TEST_PACKAGE_NAME)
        )
        data_files = self.client._find_data_files([pattern])
        assert [
            (self.DATA_FILE_PATH, os.path.join(DATA, self.DATA_FILE_RELATIVE_PATH))
        ] == data_files

    def test_find_data_files(self):
        data_files = self.client._find_data_files([self.DATA_FILE_PATH])
        assert [
            (self.DATA_FILE_PATH, os.path.join(DATA, self.DATA_FILE_RELATIVE_PATH))
        ] == data_files

    def test_find_data_files_directory(self):
        with pytest.raises(ValueError):
            self.client._find_data_files([self.TEST_PACKAGE_NAME])

    def test_find_data_files_missing(self):
        with pytest.raises(ValueError):
            self.client._find_data_files(
                ["{}/foobar.txt".format(self.TEST_PACKAGE_NAME)]
            )

    def test_find_data_files_glob_missing(self):
        with warnings.catch_warnings(record=True) as w:
            data_files = self.client._find_data_files(
                ["{}/foobar/*.txt".format(self.TEST_PACKAGE_NAME)]
            )
            assert [] == data_files
            assert 1 == len(w)

    def test_include_modules_exceptions(self):
        with pytest.raises(ImportError):
            self.client._write_include_modules(["doesnt.exist"], None)

    def test_include_modules(self):
        with tempfile.NamedTemporaryFile(suffix=".zip") as f:
            with ZipFile(f, mode="w") as arc:
                self.client._write_include_modules(self.TEST_MODULE_LIST, arc)
            f.seek(0)
            with ZipFile(f, mode="r") as arc:
                init_path = os.path.join(
                    DIST, self.TEST_PACKAGE_NAME, "package", "__init__.py"
                )
                pkg_init_path = os.path.join(
                    DIST, self.TEST_PACKAGE_NAME, "__init__.py"
                )
                arc_namelist = [os.path.abspath(name) for name in arc.namelist()]
                assert os.path.abspath(init_path) in arc_namelist
                assert os.path.abspath(pkg_init_path) in arc_namelist
                for mod_zip_path in self.TEST_MODULE_ZIP_PATH_LIST:
                    path = os.path.join(DIST, mod_zip_path)
                    assert os.path.abspath(path) in arc_namelist
                    # open file in `arc` with a consistent posixpath (windows and linux compat)
                    with arc.open(
                        str(DIST / PurePosixPath(Path(mod_zip_path)))
                    ) as fixture_data:
                        assert b"def foo()" in fixture_data.read()

    @mock.patch.object(sys, "path", new=[os.path.relpath(TEST_DATA_PATH)])
    def test_include_modules_relative_sys_path(self):
        with tempfile.NamedTemporaryFile(suffix=".zip") as f:
            with ZipFile(f, mode="w") as arc:
                self.client._write_include_modules(self.TEST_MODULE_LIST, arc)
            f.seek(0)
            with ZipFile(f, mode="r") as arc:
                for mod_zip_path in self.TEST_MODULE_ZIP_PATH_LIST:
                    path = os.path.join(DIST, mod_zip_path)
                    arc_namelist = [os.path.abspath(name) for name in arc.namelist()]
                    assert os.path.abspath(path) in arc_namelist

    def test_build_bundle(self):
        module_path = os.path.join(DIST, self.TEST_MODULE_ZIP_PATH)
        cython_module_path = os.path.join(DIST, self.TEST_MODULE_CYTHON_ZIP_PATH)
        data_path = os.path.join(DATA, self.DATA_FILE_ZIP_PATH)

        def foo():
            pass

        zf = self.client._build_bundle(
            foo, [self.DATA_FILE_PATH], self.TEST_MODULE_LIST
        )

        try:
            with ZipFile(zf) as arc:
                arc_namelist = [os.path.abspath(name) for name in arc.namelist()]
                assert os.path.abspath(module_path) in arc_namelist
                assert os.path.abspath(cython_module_path) in arc_namelist
                assert os.path.abspath(data_path) in arc_namelist
                assert os.path.abspath(REQUIREMENTS) not in arc_namelist
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

        with pytest.raises(BoundGlobalError):
            self.client._build_bundle(foo, [self.DATA_FILE_PATH], self.TEST_MODULE_LIST)

        with pytest.raises(BoundGlobalError):
            self.client._build_bundle(
                Foo.bar, [self.DATA_FILE_PATH], self.TEST_MODULE_LIST
            )

    def test_build_bundle_with_named_function(self):
        zf = self.client._build_bundle(
            self.TEST_MODULE + ".func_foo", [self.DATA_FILE_PATH], [self.TEST_MODULE],
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
            [self.TEST_MODULE],
        )

        try:
            with ZipFile(zf) as arc:
                self.call_function(arc, self.LOCAL_STRING + self.GLOBAL_STRING)
        finally:
            if os.path.exists(zf):
                os.remove(zf)

    def test_build_bundle_with_named_function_bad(self):
        with pytest.raises(NameError):
            zf = self.client._build_bundle(
                "func.func_foo", [self.DATA_FILE_PATH], [self.TEST_MODULE],
            )

        zf = self.client._build_bundle(
            "descarteslabs.client.services.tasks.tests.test_tasks.TasksPackagingTest.a_function",
            [self.DATA_FILE_PATH],
            [self.TEST_MODULE],
        )

        try:
            with ZipFile(zf) as arc:
                with pytest.raises(ImportError):
                    self.call_function(arc, None)
        finally:
            if os.path.exists(zf):
                os.remove(zf)

    def test_build_bundle_requirements(self):
        def foo():
            pass

        zf = self.client._build_bundle(foo, None, None, ["foo", "bar"])
        try:
            with ZipFile(zf) as arc:
                assert b"foo\nbar" == arc.read(REQUIREMENTS)
        finally:
            os.remove(zf)

    def test_requirements_string(self):
        assert "requests" == self.client._requirements_string(["requests"])
        assert (
            'foo>=1.2\nbar[foo]\nbaz;python_version<"2.7"'
            == self.client._requirements_string(
                ["foo>=1.2", "bar[foo]", 'baz;python_version<"2.7"']
            )
        )

    def test_requirements_string_file(self):
        good_requirements = os.path.join(self.TEST_DATA_PATH, "good_requirements.txt")
        with open(good_requirements) as requirements:
            assert requirements.read() == self.client._requirements_string(
                good_requirements
            )

    def test_requirements_string_bad(self):
        with pytest.raises(ValueError):
            self.client._requirements_string(["foo\nbar"])

        with pytest.raises(ValueError):
            self.client._requirements_string(["foo", ""])

        with pytest.raises(ValueError):
            self.client._requirements_string(["foo >>> 1.0"])

    def test_requirements_string_file_bad(self):
        with pytest.raises(ValueError):
            self.client._requirements_string("non-existent.txt")

        bad_requirements = os.path.join(self.TEST_DATA_PATH, "bad_requirements.txt")
        assert os.path.exists(bad_requirements) == True
        with pytest.raises(ValueError):
            self.client._requirements_string(bad_requirements)


class CloudFunctionTest(ClientTestCase):
    def setUp(self):
        super(CloudFunctionTest, self).setUp()
        self.function = CloudFunction("group_id", client=self.client)

    @responses.activate
    def test_call(self):
        self.mock_response(responses.POST, {"tasks": [{"id": "foo"}]})
        task = self.function("foo", bar="baz")
        assert self.function.group_id == task.guid
        assert "foo" == task.tuid
        assert ("foo",) == task.args
        assert {"bar": "baz"} == task.kwargs

    @responses.activate
    def test_map(self):
        self.mock_response(responses.POST, {"tasks": [{"id": "foo"}, {"id": "bar"}]})
        tasks = self.function.map(iter(["foo", "bar"]))
        assert ["foo", "bar"] == [task.tuid for task in tasks]
        assert [("foo",), ("bar",)] == [task.args for task in tasks]

    @responses.activate
    def test_map_multi(self):
        self.mock_response(responses.POST, {"tasks": [{"id": "foo"}, {"id": "bar"}]})
        tasks = self.function.map(iter(["foo", "bar"]), iter(["baz"]))
        assert ["foo", "bar"] == [task.tuid for task in tasks]
        assert [("foo", "baz"), ("bar", None)] == [task.args for task in tasks]


if __name__ == "__main__":
    unittest.main()
