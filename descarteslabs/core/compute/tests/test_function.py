import gzip
import json
import os
import random
import string
import zipfile
from collections.abc import Iterable
from datetime import timezone

import responses

from descarteslabs import exceptions
from descarteslabs.compute import Function, FunctionStatus, Job, JobStatus

from .base import BaseTestCase, make_uuid

token = "header.e30.signature"

s3_url = "https://bucket.region.amazonaws.com"


class TestCreateFunctionValidation(BaseTestCase):
    def test_cpu_and_memory(self):
        def test():
            pass

        tests = [
            ("1", "8Gb", 1.0, 8192),
            ("8VCPUs", "8GB", 8.0, 8192),
            ("0.25", "8Gi", 0.25, 8192),
            (4, "512Mb", 4.0, 512),
            (4, "512", 4.0, 512),
        ]

        for cpus, memory, expected_cpus, expected_memory in tests:
            fn = Function(test, cpus=cpus, memory=memory)
            assert fn.cpus == expected_cpus
            assert fn.memory == expected_memory

        with self.assertRaises(ValueError) as ctx:
            Function(test, cpus=1, memory="100TB")
        assert "Unable to convert memory to megabytes" in str(ctx.exception)


class FunctionTestCase(BaseTestCase):
    def mock_function_create(self, data: dict):
        function = self.make_function(**data)
        self.mock_response(
            responses.POST,
            "/functions",
            json={
                "function": function,
                "bundle_upload_url": s3_url,
            },
        )

        response = {**function, "status": FunctionStatus.BUILDING}
        self.mock_response(
            responses.POST,
            f"/functions/{function['id']}/bundle",
            json=response,
        )

    def mock_s3_upload_url(self):
        responses.add(responses.PUT, s3_url)

    def generate_function(self, **params):
        function = {
            "id": make_uuid(),
            "creation_date": self.now.replace(tzinfo=timezone.utc).isoformat(),
            "name": "".join(
                random.choices(string.ascii_lowercase + string.digits, k=10)
            ),
            "image": "".join(
                random.choices(string.ascii_lowercase + string.digits, k=10)
            ),
            "cpus": random.choice([1, 2]),
            "memory": random.choice([512, 1024, 2048]),
            "maximum_concurrency": random.randint(1, 10),
            "timeout": random.randint(60, 900),
            "retry_count": random.randint(1, 10),
            "status": str(FunctionStatus.RUNNING),
        }
        return function


class TestCreateFunction(FunctionTestCase):
    def setUp(self):
        super().setUp()
        self.mock_credentials()

    @responses.activate
    def test_requires_function(self):
        with self.assertRaises(ValueError) as ctx:
            fn = Function()
            fn.save()
        assert fn.state == "new"
        assert "Function not provided" in str(ctx.exception)

    @responses.activate
    def test_server_validation_errors(self):
        self.mock_response(
            responses.POST,
            "/functions",
            status=422,
            json={
                "detail": "Validation error",
                "errors": {
                    "image": ["Field is required", "Must be a valid docker image"]
                },
            },
        )

        def test():
            pass

        with self.assertRaises(exceptions.ValidationError) as ctx:
            fn = Function(test)
            fn.save()
        assert fn.state == "new"
        assert "image" in str(ctx.exception)
        assert "Field is required" in str(ctx.exception)
        assert "Must be a valid docker image" in str(ctx.exception)

    @responses.activate
    def test_create_function(self):
        params = {
            "image": "python3.8",
            "cpus": 1,
            "memory": 8 * 1024,
            "maximum_concurrency": 1,
            "timeout": 60,
            "retry_count": 1,
        }

        self.mock_function_create(params)
        self.mock_s3_upload_url()

        def test_compute_fn(a, b):
            print(f"{a} to the power of {b}")
            return a**b

        fn = Function(test_compute_fn, **params)
        fn.save()
        assert fn.state == "saved"
        assert fn.name == "test_compute_fn"
        assert fn.status == FunctionStatus.BUILDING
        assert fn.creation_date == self.now.replace(tzinfo=timezone.utc)
        assert fn.id
        self.assertDictContainsSubset(params, fn.to_dict())

    @responses.activate
    def test_call_creates_function(self):
        params = {
            "image": "python3.8",
            "cpus": 1,
            "memory": 8 * 1024,
            "maximum_concurrency": 1,
            "timeout": 60,
            "retry_count": 1,
        }

        job_params = {
            "args": [2, 8],
            "kwargs": None,
        }

        self.mock_job_create(job_params)
        self.mock_function_create(params)
        self.mock_s3_upload_url()

        def test_compute_fn(a, b):
            print(f"{a} to the power of {b}")
            return a**b

        fn = Function(test_compute_fn, **params)
        job = fn(2, 8)
        assert fn.state == "saved"
        assert isinstance(job, Job)
        assert job.id
        assert job.status == JobStatus.PENDING
        assert job.creation_date == self.now.replace(tzinfo=timezone.utc)
        self.assertDictContainsSubset(job_params, job.to_dict())

    @responses.activate
    def test_bundle_lambda(self):
        fn = Function(lambda: 1)
        with self.assertRaises(ValueError) as ctx:
            fn.save()
        assert "Compute main function cannot be a lambda expression" in str(
            ctx.exception
        )

    @responses.activate
    def test_function_nested_globals(self):
        params = {
            "image": "python3.8",
            "cpus": 1,
            "memory": 8 * 1024,
            "maximum_concurrency": 1,
            "timeout": 60,
            "retry_count": 1,
        }

        def test_compute_fn(a, b):
            def catch_this():
                print("failed to prevent nested global:", make_uuid())
                print(f"{a} to the power of {b}")

            return a**b

        fn = Function(test_compute_fn, **params)
        with self.assertRaises(NameError) as ctx:
            fn.save()
        assert str(ctx.exception) == (
            "Illegal reference to one or more global variables in your function:"
            " {'make_uuid'}"
        )


class TestFunctionBundle(FunctionTestCase):
    def get_module_paths(self):
        # Get the path to the module

        parts = ["descarteslabs"] + __file__.split("descarteslabs")[-1].strip(
            "/"
        ).split("/")

        # If the OS is Windows, the path will be different
        if os.name == "nt":
            parts = ["descarteslabs"] + __file__.split("descarteslabs")[-1].split("\\")
            # remove empty elements
            parts = [i for i in parts if i]

        # Construct the module path and module in dot notation
        module_path = os.path.join(*parts[:-1])
        module_dot = ".".join(parts[:-1])

        # Return the module path, module in dot notation and the parts of the path
        return module_path, module_dot, parts

    def get_init_files(self, parts):
        # Return list of paths to the __init__.py files

        init_files = []

        # Don't need the last part of the path because it's the current file name
        parts.remove(parts[-1])

        # Construct list of paths to the __init__.py files for each sub-module
        for i in range(len(parts)):
            init_files.append(os.path.join(*parts[: i + 1], "__init__.py"))

        return init_files

    def test_function_bundling(self):
        # Test with list of requirements, explicitly specified modules and explicitly
        # specified data file

        module_path, module_dot, parts = self.get_module_paths()

        # Construct the module path with forward slashes, regardless of OS
        # This is needed for the bundle check to work with ZipFile
        module_path_forward_slash = "/".join(parts[:-1])

        # Add the paths to the __init__.py files
        files_to_be_bundled = [
            "__dlentrypoint__.py",
            # os.path.join(module_path, "data", "test_data1.csv"),
            # os.path.join(module_path, "test_function.py"),
            # os.path.join(module_path, "test_job.py"),
            f"{module_path_forward_slash}/data/test_data1.csv",
            f"{module_path_forward_slash}/test_function.py",
            f"{module_path_forward_slash}/test_job.py",
            "requirements.txt",
        ] + self.get_init_files(parts)

        params = {
            "image": "python3.8:latest",
            "cpus": 1,
            "memory": 8 * 1024,
            "maximum_concurrency": 1,
            "timeout": 60,
            "retry_count": 1,
            "requirements": [
                "descarteslabs[complete]>=2.0.3",
                "geopandas==0.13.2",
            ],
            "include_modules": [
                f"{module_dot}.test_function",
                f"{module_dot}.test_job",
            ],
            "include_data": [os.path.join(module_path, "data", "test_data1.csv")],
        }

        def test_compute_fn(a, b):
            print(f"{a} to the power of {b}")
            return a**b

        fn = Function(test_compute_fn, **params)
        bundle_path = fn._bundle()

        with zipfile.ZipFile(os.path.abspath(bundle_path)):
            contents = zipfile.ZipFile(bundle_path).namelist()

        # expected = {path.replace(r"\\\\", "/") for path in files_to_be_bundled}
        import pprint

        pprint.pprint(contents)
        pprint.pprint(files_to_be_bundled)

        assert sorted(files_to_be_bundled) == sorted(set(contents))
        assert os.path.join(module_path, "base.py") not in contents

    def test_function_bundling_requirements_file(self):
        # Test with requirements file, full module and all (*) contents of data folder

        module_path, module_dot, parts = self.get_module_paths()

        # Construct the module path with forward slashes, regardless of OS
        # This is needed for the bundle check to work with ZipFile
        module_path_forward_slash = "/".join(parts[:-1])

        # Add the paths to the __init__.py files
        files_to_be_bundled = [
            "__dlentrypoint__.py",
            # os.path.join(module_path, "data", "test_data1.csv"),
            # os.path.join(module_path, "data", "test_data2.json"),
            # os.path.join(module_path, "base.py"),
            # os.path.join(module_path, "test_function.py"),
            # os.path.join(module_path, "test_job.py"),
            f"{module_path_forward_slash}/data/test_data1.csv",
            f"{module_path_forward_slash}/data/test_data2.json",
            f"{module_path_forward_slash}/base.py",
            f"{module_path_forward_slash}/test_function.py",
            f"{module_path_forward_slash}/test_job.py",
            "requirements.txt",
        ] + self.get_init_files(parts)

        params = {
            "image": "python3.8:latest",
            "cpus": 1,
            "memory": 8 * 1024,
            "maximum_concurrency": 1,
            "timeout": 60,
            "retry_count": 1,
            "requirements": os.path.join(module_path, "requirements.txt"),
            "include_modules": [module_dot],
            "include_data": [os.path.join(module_path, "data", "*")],
        }

        def test_compute_fn(a, b):
            print(f"{a} to the power of {b}")
            return a**b

        fn = Function(test_compute_fn, **params)
        bundle_path = fn._bundle()

        with zipfile.ZipFile(os.path.abspath(bundle_path)):
            contents = zipfile.ZipFile(bundle_path).namelist()

        expected = {path.replace(r"\\\\", "/") for path in files_to_be_bundled}

        contents = set(contents)

        # Remove the _main_tests.py file from the expected contents because it is
        # dynamically generated
        if os.path.join(module_path, "_main_tests.py") in contents:
            contents.remove(os.path.join(module_path, "_main_tests.py"))
        import pprint

        pprint.pprint(contents)
        pprint.pprint(expected)
        assert expected == contents


class TestListFunctions(FunctionTestCase):
    @responses.activate
    def test_list_function_empty(self):
        self.mock_response(responses.GET, "/functions", json=self.make_page([]))
        fn_iter = Function.list()
        assert isinstance(fn_iter, Iterable)
        assert list(fn_iter) == []

    @responses.activate
    def test_list_function(self):
        self.mock_response(
            responses.GET,
            "/functions",
            json=self.make_page(
                [self.generate_function(), self.generate_function()],
                page_cursor="page2",
            ),
        )
        self.mock_response(
            responses.GET,
            "/functions",
            json=self.make_page([self.generate_function()]),
        )
        functions = list(Function.list())

        for function in functions:
            assert isinstance(function, Function)
            assert function.state == "saved"

        assert len(functions) == 3
        self.assert_url_called("/functions?page_size=100", 1)
        self.assert_url_called("/functions?page_cursor=page2", 1)

    @responses.activate
    def test_list_function_filters(self):
        self.mock_response(
            responses.GET,
            "/functions",
            json=self.make_page([self.generate_function()]),
        )
        list(Function.list(status=FunctionStatus.BUILDING))
        self.assert_url_called("/functions?page_size=100&status=building", 1)

        list(
            Function.list(
                status=[FunctionStatus.BUILDING, FunctionStatus.AWAITING_BUNDLE]
            )
        )
        self.assert_url_called(
            "/functions?page_size=100&status=building&status=awaiting_bundle", 1
        )


class TestGetFunction(FunctionTestCase):
    @responses.activate
    def test_get_missing(self):
        self.mock_response(responses.GET, "/functions/missing-id", status=404)

        with self.assertRaises(exceptions.NotFoundError):
            Function.get("missing-id")

    @responses.activate
    def test_get_by_id(self):
        expected = self.generate_function()
        self.mock_response(responses.GET, "/functions/some-id", json=expected)

        fn = Function.get("some-id")
        assert fn.state == "saved"
        self.assertDictContainsSubset(expected, fn.to_dict())


class TestFunction(FunctionTestCase):
    @responses.activate
    def test_build_log(self):
        log_lines = ["test", "log"]
        log = "\n".join(log_lines)
        log_bytes = (log + "\n").encode()
        buffer = gzip.compress(log_bytes)
        self.mock_response(responses.GET, "/functions/some-id/log", body=buffer)

        fn = Function(id="some-id", saved=True)
        fn.build_log()

    @responses.activate
    def test_delete(self):
        self.mock_response(
            responses.GET,
            "/jobs",
            json=self.make_page(
                [
                    self.make_job(id="1", status=JobStatus.SUCCESS),
                    self.make_job(id="2", status=JobStatus.SUCCESS),
                    self.make_job(id="3", status=JobStatus.RUNNING),
                    self.make_job(id="4", status=JobStatus.FAILURE),
                ]
            ),
        )
        self.mock_response(responses.DELETE, "/jobs/1", status=204)
        self.mock_response(responses.DELETE, "/jobs/2", status=204)
        self.mock_response(responses.DELETE, "/jobs/3", status=204)
        self.mock_response(responses.DELETE, "/jobs/4", status=204)
        self.mock_response(responses.DELETE, "/functions/some-id", status=204)

        fn = Function(id="some-id", saved=True)
        fn.delete()
        self.assert_url_called("/functions/some-id")
        assert fn._deleted is True
        assert fn.state == "deleted"

        with self.assertRaises(AttributeError) as ctx:
            fn.id
        assert "Function has been deleted" in str(ctx.exception)

    @responses.activate
    def test_delete_new(self):
        fn = Function(id="some-id", saved=True)
        fn._saved = False

        with self.assertRaises(ValueError) as ctx:
            fn.delete()
        assert "has not been saved" in str(ctx.exception)
        assert fn._deleted is False
        assert fn.state == "new"

    @responses.activate
    def test_delete_no_jobs(self):
        self.mock_response(responses.GET, "/jobs", json=self.make_page([]))
        self.mock_response(responses.DELETE, "/functions/some-id", status=204)

        fn = Function(id="some-id", saved=True)
        fn.delete()
        self.assert_url_called("/functions/some-id")
        assert fn._deleted is True
        assert fn.state == "deleted"

        with self.assertRaises(AttributeError) as ctx:
            fn.id
        assert "Function has been deleted" in str(ctx.exception)

    @responses.activate
    def test_delete_failed(self):
        self.mock_response(
            responses.GET,
            "/jobs",
            json=self.make_page(
                [
                    self.make_job(id="1", status=JobStatus.SUCCESS),
                ]
            ),
        )
        self.mock_response(responses.DELETE, "/jobs/1", status=400)

        fn = Function(id="some-id", saved=True)

        with self.assertRaises(Exception):
            fn.delete()

        self.assert_url_called("/jobs/1")
        assert fn._deleted is False
        assert fn.state == "saved"
        assert fn.id == "some-id"

    @responses.activate
    def test_rerun(self):
        self.mock_response(
            responses.POST,
            "/jobs/rerun",
            json=[self.make_job(function_id="some-id", args=[1, 2])],
        )

        fn = Function(id="some-id", saved=True)
        jobs = fn.rerun()
        assert isinstance(jobs, list)
        job = jobs[0]
        assert job.state == "saved"
        assert job.id
        assert job.args == [1, 2]
        assert job.kwargs is None
        assert job.creation_date == self.now.replace(tzinfo=timezone.utc)
        assert job.function_id == "some-id"
        assert job.status == JobStatus.PENDING
        self.assert_url_called(
            "/jobs/rerun",
            json={"filter": [{"op": "eq", "name": "function_id", "val": "some-id"}]},
        )

    @responses.activate
    def test_refresh(self):
        params = {
            "id": "some-id",
            "name": "compute-test",
            "image": "image:tag",
            "cpus": 1,
            "memory": 2048,
            "maximum_concurrency": 1,
            "retry_count": 1,
            "status": FunctionStatus.RUNNING,
            "timeout": 60,
        }

        self.mock_response(
            responses.GET,
            "/functions/some-id",
            json=self.make_function(**params),
        )

        fn = Function(id="some-id", saved=True)
        fn.refresh()
        assert fn.state == "saved"
        assert fn.creation_date == self.now.replace(tzinfo=timezone.utc)
        self.assertDictContainsSubset(params, fn.to_dict())

    @responses.activate
    def test_map(self):
        self.mock_response(
            responses.POST,
            "/jobs/bulk",
            json=[self.make_job(args=[1, 2]), self.make_job(args=[3, 4])],
        )

        fn = Function(id="some-id", saved=True)
        fn.map(
            [[1, 2], [3, 4]],
            iterargs=[{"first": 1, "second": 2}, {"first": 1.0, "second": 2.0}],
        )
        request = responses.calls[-1].request
        assert json.loads(request.body) == {
            "bulk_args": [[1, 2], [3, 4]],
            "bulk_kwargs": [{"first": 1, "second": 2}, {"first": 1.0, "second": 2.0}],
            "function_id": "some-id",
        }

    @responses.activate
    def test_map_with_tags(self):
        self.mock_response(
            responses.POST,
            "/jobs/bulk",
            json=[self.make_job(args=[1, 2]), self.make_job(args=[3, 4])],
        )

        fn = Function(id="some-id", saved=True)
        fn.map(
            [[1, 2], [3, 4]],
            iterargs=[{"first": 1, "second": 2}, {"first": 1.0, "second": 2.0}],
            tags=["tag1", "tag2"],
        )
        request = responses.calls[-1].request
        assert json.loads(request.body) == {
            "bulk_args": [[1, 2], [3, 4]],
            "bulk_kwargs": [{"first": 1, "second": 2}, {"first": 1.0, "second": 2.0}],
            "function_id": "some-id",
            "tags": ["tag1", "tag2"],
        }

    @responses.activate
    def test_results(self):
        self.mock_response(
            responses.GET,
            "/jobs",
            json=self.make_page(
                [
                    self.make_job(id="1", status=JobStatus.SUCCESS),
                    self.make_job(id="2", status=JobStatus.SUCCESS),
                ]
            ),
        )
        self.mock_response(responses.GET, "/jobs/1/result", body=json.dumps(1))
        self.mock_response(responses.GET, "/jobs/2/result", body=json.dumps(2))

        fn = Function(id="some-id", saved=True)
        assert fn.results() == [1, 2]
        self.assert_url_called(
            "/jobs",
            params={
                "filter": [
                    {"op": "eq", "name": "function_id", "val": "some-id"},
                    {"op": "eq", "name": "status", "val": "success"},
                ]
            },
        )

    @responses.activate
    def test_wait_for_completion(self):
        self.mock_response(
            responses.GET,
            "/functions/some-id",
            json=self.make_function(
                id="some-id",
                name="compute-test",
                status=FunctionStatus.RUNNING,
            ),
        )
        self.mock_response(
            responses.GET,
            "/functions/some-id",
            json=self.make_function(
                id="some-id",
                name="compute-test",
                status=FunctionStatus.SUCCESS,
            ),
        )

        fn = Function(
            id="some-id",
            name="compute-test",
            status=FunctionStatus.BUILDING,
            saved=True,
        )
        fn.wait_for_completion(interval=0.1, timeout=5)
        assert fn.state == "saved"
        assert fn.status == FunctionStatus.SUCCESS

    @responses.activate
    def test_wait_for_completion_timeout(self):
        self.mock_response(
            responses.GET,
            "/functions/some-id",
            json=self.make_function(
                id="some-id",
                name="compute-test",
                status=FunctionStatus.RUNNING,
            ),
        )

        fn = Function(
            id="some-id",
            name="compute-test",
            status=FunctionStatus.BUILDING,
            saved=True,
        )

        with self.assertRaises(TimeoutError) as ctx:
            fn.wait_for_completion(interval=0.1, timeout=5)
        assert "did not complete before timeout" in str(ctx.exception)

    @responses.activate
    def test_modified_patch(self):
        self.mock_response(
            responses.PATCH,
            "/functions/some-id",
            json=self.make_function(id="some-id", cpus=16, memory=16 * 1024),
        )

        fn = Function(id="some-id", saved=True)
        fn.cpus = 16
        fn.memory = "16GB"
        fn.save()
        assert fn.state == "saved"
        self.assert_url_called(
            "/functions/some-id", json={"cpus": 16, "memory": 16 * 1024}
        )


class TestFunctionNoApi(BaseTestCase):
    @responses.activate
    def test_no_request_when_saved(self):
        fn = Function(id="some-id", saved=True)
        fn.save()
        assert len(responses.calls) == 0

    @responses.activate
    def test_deleted(self):
        fn = Function(id="some-id", saved=True)
        fn._deleted = True

        with self.assertRaises(AttributeError) as ctx:
            fn.save()
        assert "Function has been deleted" in str(ctx.exception)
