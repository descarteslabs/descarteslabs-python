from ....common.proto.errors import errors_pb2
from ....common.proto.job import job_pb2

from ..exceptions import JobComputeError
from ..job import Job


def test_init():
    message = job_pb2.Job(
        id="foo",
        state=job_pb2.Job.State(
            error=job_pb2.Job.Error(code=errors_pb2.ERROR_DEADLINE, message="bar")
        ),
    )

    job = Job._from_proto(message)

    e = JobComputeError(job)
    assert e.code == job.error.code
    assert e.message == job.error.message
    assert e.id == job.id
