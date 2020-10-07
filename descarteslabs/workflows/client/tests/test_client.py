import mock

from descarteslabs.workflows import _channel

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
