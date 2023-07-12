import json
from datetime import timezone

import responses

from descarteslabs.compute import Job, JobStatus

from .base import BaseTestCase


class TestCreateJob(BaseTestCase):
    @responses.activate
    def test_create(self):
        params = dict(function_id="some-fn", args=[1, 2], kwargs={"key": "blah"})
        self.mock_job_create(params)

        job = Job(**params)
        assert job.state == "new"
        job.save()
        assert job.state == "saved"
        assert job.id
        assert job.creation_date == self.now.replace(tzinfo=timezone.utc)
        self.assertDictContainsSubset(params, job.to_dict())


class TestListJobs(BaseTestCase):
    @responses.activate
    def test_list_jobs(self):
        self.mock_response(
            responses.GET,
            "/jobs",
            json=self.make_page(
                [self.make_job(), self.make_job()],
                page_cursor="page2",
            ),
        )
        self.mock_response(
            responses.GET,
            "/jobs",
            json=self.make_page([self.make_job()]),
        )
        jobs = list(Job.list())

        for job in jobs:
            assert isinstance(job, Job)
            assert job.state == "saved"
            assert job.to_dict()

        assert len(jobs) == 3
        self.assert_url_called("/jobs?page_size=100", 1)
        self.assert_url_called("/jobs?page_cursor=page2", 1)

    @responses.activate
    def test_list_jobs_compatible(self):
        self.mock_response(
            responses.GET,
            "/jobs",
            json=self.make_page([self.make_job()]),
        )
        list(Job.list(status=JobStatus.PENDING, function_id="some-fn"))
        self.assert_url_called(
            "/jobs",
            params={
                "page_size": 100,
                "status": "pending",
                "function_id": "some-fn",
            },
        )

        list(Job.list(status=[JobStatus.PENDING, JobStatus.RUNNING]))
        self.assert_url_called(
            "/jobs",
            params={
                "page_size": 100,
                "status": ["pending", "running"],
            },
        )


class TestJob(BaseTestCase):
    @responses.activate
    def test_get(self):
        self.mock_response(
            responses.GET,
            "/jobs/some-id",
            json=self.make_job(
                id="some-id",
                function_id="function-id",
                args=[1, 2],
                kwargs={"first": "blah", "second": "blah"},
            ),
        )
        job = Job.get("some-id")
        assert job.state == "saved"
        assert job.to_dict() == {
            "args": [1, 2],
            "creation_date": self.now.replace(tzinfo=timezone.utc).isoformat(),
            "function_id": "function-id",
            "id": "some-id",
            "kwargs": {"first": "blah", "second": "blah"},
            "runtime": None,
            "status": JobStatus.PENDING,
        }

    @responses.activate
    def test_delete(self):
        self.mock_response(responses.DELETE, "/jobs/some-id")
        job = Job(id="some-id", function_id="some-fn", saved=True)
        job.delete()
        self.assert_url_called("/jobs/some-id")
        assert job._deleted is True
        assert job.state == "deleted"

    @responses.activate
    def test_delete_new(self):
        job = Job(id="some-id", function_id="some-fn", saved=True)
        job._saved = False

        with self.assertRaises(ValueError) as ctx:
            job.delete()
        assert "has not been saved" in str(ctx.exception)
        assert job._deleted is False
        assert job.state == "new"

    @responses.activate
    def test_delete_failed(self):
        self.mock_response(responses.DELETE, "/jobs/some-id", status=400)
        job = Job(id="some-id", function_id="some-fn", saved=True)

        with self.assertRaises(Exception):
            job.delete()

        self.assert_url_called("/jobs/some-id")
        assert job._deleted is False
        assert job.state == "saved"

    @responses.activate
    def test_result_empty(self):
        self.mock_response(responses.GET, "/jobs/some-id/result", body=None)
        job = Job(id="some-id", function_id="some-fn", saved=True)
        assert job.result() is None

    @responses.activate
    def test_result_json(self):
        body = json.dumps({"test": "blah"}).encode()
        self.mock_response(responses.GET, "/jobs/some-id/result", body=body)
        job = Job(id="some-id", function_id="some-fn", saved=True)
        assert job.result() == {"test": "blah"}

    @responses.activate
    def test_result_float(self):
        body = json.dumps(15.68).encode()
        self.mock_response(responses.GET, "/jobs/some-id/result", body=body)
        job = Job(id="some-id", function_id="some-fn", saved=True)
        assert job.result() == 15.68

    @responses.activate
    def test_result_cast(self):
        class CustomString:
            @classmethod
            def deserialize(cls, data: bytes):
                return "custom"

        self.mock_response(responses.GET, "/jobs/some-id/result", body="blah")
        job = Job(id="some-id", function_id="some-fn", saved=True)
        assert job.result(CustomString) == "custom"

        with self.assertRaises(ValueError) as ctx:
            job.result(bool)
        assert "must implement Serializable" in str(ctx.exception)

    @responses.activate
    def test_log(self):
        log_lines = ["test", "log"]
        log = "\n".join(
            [
                json.dumps({"date": self.now.isoformat() + "Z", "log": log})
                for log in log_lines
            ]
        )
        log_bytes = (log + "\n").encode()
        self.mock_response(responses.GET, "/jobs/some-id/log", body=log_bytes)

        job = Job(id="some-id", function_id="some-fn", saved=True)
        job.log()

    @responses.activate
    def test_wait_for_complete(self):
        self.mock_response(
            responses.GET,
            "/jobs/some-id",
            json=self.make_job(
                id="some-id",
                function_id="function-id",
                args=[1, 2],
                kwargs={},
            ),
        )
        self.mock_response(
            responses.GET,
            "/jobs/some-id",
            json=self.make_job(
                id="some-id",
                function_id="function-id",
                args=[1, 2],
                kwargs={},
                status=JobStatus.SUCCESS,
            ),
        )
        job = Job(id="some-id", function_id="function-id", saved=True)
        job.wait_for_completion(interval=0.1, timeout=5)
        assert job.status == JobStatus.SUCCESS

    @responses.activate
    def test_wait_for_complete_timeout(self):
        self.mock_response(
            responses.GET,
            "/jobs/some-id",
            json=self.make_job(
                id="some-id",
                function_id="function-id",
                args=[1, 2],
                kwargs={},
            ),
        )
        job = Job(id="some-id", function_id="function-id", saved=True)
        with self.assertRaises(TimeoutError):
            job.wait_for_completion(interval=0.1, timeout=5)

    @responses.activate
    def test_modified_patch(self):
        self.mock_response(
            responses.PATCH,
            "/jobs/some-id",
            json=self.make_job(id="some-id", function_id="some-fn", args=[1, 2]),
        )

        job = Job(id="some-id", function_id="some-fn", saved=True)
        job.args = [1, 2]
        job.save()
        assert job.state == "saved"
        self.assert_url_called("/jobs/some-id", json={"args": [1, 2]})


class TestJobNoApi(BaseTestCase):
    @responses.activate
    def test_no_request_when_saved(self):
        job = Job(id="some-id", function_id="some-fn", saved=True)
        job.save()
        assert len(responses.calls) == 0

    @responses.activate
    def test_deleted(self):
        job = Job(id="some-id", function_id="some-fn", saved=True)
        job._deleted = True

        with self.assertRaises(AttributeError) as ctx:
            job.save()
        assert "Job has been deleted" in str(ctx.exception)
