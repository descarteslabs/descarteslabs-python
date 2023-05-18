import json as jsonlib
import uuid
from datetime import datetime
from unittest import TestCase

import responses
from requests import PreparedRequest

from descarteslabs.compute import FunctionStatus, JobStatus


def make_uuid():
    return str(uuid.uuid4())


class BaseTestCase(TestCase):
    compute_url = "https://platform.dev.aws.descarteslabs.com/compute/v1"
    iam_url = "https://iam.dev.aws.descarteslabs.com"

    def setUp(self):
        responses.mock.assert_all_requests_are_fired = True
        self.now = datetime.utcnow()

    def tearDown(self):
        responses.mock.assert_all_requests_are_fired = False

    def mock_token(self):
        responses.add(
            responses.POST,
            f"{self.iam_url}/token",
            '{"access_token":"h.e30.s"}',
        )

    def mock_credentials(self):
        responses.add(responses.POST, f"{self.compute_url}/credentials")

    def mock_response(self, method, uri, json=None, status=200, **kwargs):
        if json is not None:
            kwargs["json"] = json

        responses.add(
            method,
            f"{self.compute_url}{uri}",
            status=status,
            **kwargs,
        )

    def mock_job_create(self, data: dict):
        job = self.make_job(**data)
        self.mock_response(responses.POST, "/jobs", json=job)

    def make_page(
        self,
        data: list,
        page: int = 1,
        page_size: int = 100,
        page_cursor: str = None,
        last_page: int = None,
    ):
        return {
            "meta": {
                "current_page": page,
                "last_page": last_page or page,
                "page_size": page_size,
                "next_page": page + 1,
                "page_cursor": page_cursor,
            },
            "data": data,
        }

    def make_job(self, **data):
        job = {
            "id": make_uuid(),
            "function_id": make_uuid(),
            "args": None,
            "kwargs": None,
            "creation_date": self.now.isoformat(),
            "status": JobStatus.PENDING,
        }
        job.update(data)

        return job

    def make_function(self, **data):
        if "cpus" in data:
            data["cpus"] = float(data["cpus"])

        if "memory" in data:
            data["memory"] = int(data["memory"])

        function = {
            "id": make_uuid(),
            "creation_date": self.now.isoformat(),
            "status": FunctionStatus.AWAITING_BUNDLE,
        }
        function.update(data)

        return function

    def assert_url_called(self, uri, times=1, json=None, body=None):
        if json and body:
            raise ValueError("Using json and body together does not make sense")

        calls = [call for call in responses.calls if call.request.url.endswith(uri)]
        assert calls, f"No requests were made to uri: {uri}"

        data = json or body
        matches = []
        for call in calls:
            request: PreparedRequest = call.request

            if json is not None:
                request_data = jsonlib.loads(request.body)
            else:
                request_data = request.body

            if data is None or request_data == data:
                matches.append(call)

        count = len(matches)
        msg = f"Expected {times} calls found {count} for {uri}"

        if data is not None:
            msg += f" with data: {data}"

        assert count == times, msg
