import unittest

try:
    import mock
except ImportError:
    import unittest.mock as mock

from descarteslabs.common.dotdict import DotDict
from descarteslabs.vectors.async_job import CopyJob, DeleteJob
from descarteslabs.vectors.exceptions import FailedJobError


@mock.patch("descarteslabs.vectors.async_job.Vector")
class AsyncJobsTestCase(unittest.TestCase):

    get_delete_features_status_response = DotDict(
        {
            "data": {
                "attributes": {
                    "created": "2019-01-03T20:07:51.720000+00:00",
                    "started": "2019-01-03T20:07:51.903000+00:00",
                    "ended": "2019-01-03T20:07:53.903000+00:00",
                    "state": "DONE",
                },
                "id": "foo",
                "type": "delete_job",
            }
        }
    )

    get_product_from_query_status_response = DotDict(
        {
            "data": {
                "attributes": {
                    "created": "2019-01-03T20:07:51.720000+00:00",
                    "started": "2019-01-03T20:07:51.903000+00:00",
                    "ended": "2019-01-03T20:07:53.903000+00:00",
                    "state": "DONE",
                },
                "id": "foo",
                "type": "copy_query",
            }
        }
    )

    def test_delete_job(self, vector_client):
        vector_client.get_delete_features_status.return_value = (
            self.get_delete_features_status_response
        )

        job = DeleteJob("product_id", vector_client)

        self.assertEqual(job.id, "product_id")
        self.assertEqual(job.state, "DONE")
        self.assertEqual(job.created, "2019-01-03T20:07:51.720000+00:00")
        self.assertEqual(job.started, "2019-01-03T20:07:51.903000+00:00")
        self.assertEqual(job.ended, "2019-01-03T20:07:53.903000+00:00")

    def test_delete_job_check_complete(self, vector_client):
        vector_client.get_delete_features_status.return_value = (
            self.get_delete_features_status_response
        )
        job = DeleteJob("product_id", vector_client)

        self.assertTrue(job._check_complete())

        job.properties["state"] = "RUNNING"
        self.assertFalse(job._check_complete())

    def test_delete_job_check_complete_exception(self, vector_client):
        vector_client.get_delete_features_status.return_value = (
            self.get_delete_features_status_response
        )
        job = DeleteJob("product_id", vector_client)
        job.properties["state"] = "FAILURE"

        with self.assertRaises(FailedJobError):
            job._check_complete()

        job.properties["state"] = "DONE"
        job.properties["errors"] = ["some error description"]
        with self.assertRaises(FailedJobError):
            job._check_complete()

    def test_copy_job(self, vector_client):
        vector_client.get_product_from_query_status.return_value = (
            self.get_product_from_query_status_response
        )

        job = CopyJob("product_id", vector_client)

        self.assertEqual(job.id, "product_id")
        self.assertEqual(job.state, "DONE")
        self.assertEqual(job.created, "2019-01-03T20:07:51.720000+00:00")
        self.assertEqual(job.started, "2019-01-03T20:07:51.903000+00:00")
        self.assertEqual(job.ended, "2019-01-03T20:07:53.903000+00:00")

    def test_copy_job_check_complete(self, vector_client):
        vector_client.get_product_from_query_status.return_value = (
            self.get_product_from_query_status_response
        )
        job = CopyJob("product_id", vector_client)

        self.assertTrue(job._check_complete())

        job.properties["state"] = "RUNNING"
        self.assertFalse(job._check_complete())

    def test_copy_job_check_complete_exception(self, vector_client):
        vector_client.get_product_from_query_status.return_value = (
            self.get_product_from_query_status_response
        )
        job = CopyJob("product_id", vector_client)
        job.properties["state"] = "FAILURE"

        with self.assertRaises(FailedJobError):
            job._check_complete()

        job.properties["state"] = "DONE"
        job.properties["errors"] = ["some error description"]
        with self.assertRaises(FailedJobError):
            job._check_complete()
