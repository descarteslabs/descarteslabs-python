import mock
import pytest

from ....client.grpc.exceptions import ResourceExhausted
from ....common.retry import RetryError
from ... import _channel
from ...models.toplevel import compute
from ...types import Int

from .. import Client


@mock.patch("descarteslabs.common.proto.health.health_pb2_grpc.HealthStub")
def test_client_health_called_with_default_metadata(stub):
    stub.return_value.Check.return_value = True
    client = Client(auth=mock.Mock())

    assert client.health() == stub.return_value.Check.return_value
    assert (
        "x-wf-channel",
        _channel.__channel__,
    ) in stub.return_value.Check.call_args_list[0][1]["metadata"]


def _grpc_resource_exhausted_exception():
    e = ResourceExhausted(message="You have exceeded your outstanding job limit of 5.")
    setattr(e, "code", lambda: ResourceExhausted.grpc_status_code)
    setattr(e, "trailing_metadata", lambda: {})
    return e


@mock.patch("descarteslabs.common.proto.job.job_pb2_grpc.JobAPIStub")
def test_client_compute_resource_exhausted_does_not_retry(stub):
    expected = _grpc_resource_exhausted_exception()
    stub.return_value.CreateJob.side_effect = expected
    client = Client(auth=mock.Mock())
    result = Int(1) + 1

    with pytest.raises(ResourceExhausted):
        compute(result, client=client)

    assert stub.return_value.CreateJob.call_count == 1


@mock.patch("descarteslabs.common.proto.job.job_pb2_grpc.JobAPIStub")
def test_client_compute_retries_when_num_retries_is_specified(stub):
    stub.return_value.CreateJob.side_effect = [
        _grpc_resource_exhausted_exception(),
        _grpc_resource_exhausted_exception(),
    ]
    client = Client(auth=mock.Mock())
    result = Int(1) + 1

    with pytest.raises(RetryError):
        compute(result, client=client, num_retries=1)

    assert stub.return_value.CreateJob.call_count == 2
