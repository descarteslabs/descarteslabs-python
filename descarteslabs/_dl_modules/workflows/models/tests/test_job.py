import json

import mock
import pytest
import responses

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from .... import scenes
from ...client import Client

from ....client.version import __version__
from ....common.proto.errors import errors_pb2
from ....common.proto.job import job_pb2
from ....common.proto.types import types_pb2
from ....common.workflows.arrow_serialization import serialize_pyarrow
from ....common.workflows.outputs import (
    user_format_to_proto,
    user_destination_to_proto,
)
from ....common.workflows.proto_munging import has_proto_to_user_dict
from ....common.graft import client as graft_client

from ... import _channel

from ... import cereal, types
from ..exceptions import JobInvalid, JobComputeError, JobTerminated, JobTimeoutError
from ..job import Job, download
from ..utils import pb_milliseconds_to_datetime, pb_timestamp_to_datetime

from . import utils


class MockRpcError(grpc.RpcError, grpc.Call):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


@mock.patch(
    "descarteslabs.workflows.models.job.get_global_grpc_client",
    new=lambda: utils.MockedClient(),
)
@mock.patch("descarteslabs.common.proto.job.job_pb2_grpc.JobAPIStub")
class TestJob:
    def test_create_full(self, stub):
        x = types.Int(1)
        param = types.parameter("bar", types.Int)
        obj = x + param
        arguments = {"bar": 2}
        format = {"type": "pyarrow", "compression": "brotli"}
        destination = {"type": "download", "result_url": ""}

        response_message = job_pb2.Job()
        stub.return_value.CreateJob.return_value = response_message

        job = Job(
            obj,
            format=format,
            destination=destination,
            **arguments,
        )

        assert job._message is response_message
        assert isinstance(job._client, Client)
        assert isinstance(
            job._object, types.Function[{param._name: type(param)}, type(obj)]
        )
        assert job._arguments is None

        rpc = stub.return_value.CreateJob
        rpc.assert_called_once()
        assert rpc.call_args.kwargs["timeout"] == Client.DEFAULT_TIMEOUT

        request = rpc.call_args.args[0]
        assert isinstance(request, job_pb2.CreateJobRequest)

        graft = json.loads(request.serialized_graft)
        assert graft_client.is_function_graft(graft)

        assert (
            cereal.deserialize_typespec(request.typespec)
            is types.Function[{param._name: type(param)}, type(obj)]
        )

        request_args = {name: json.loads(v) for name, v in request.arguments.items()}
        assert request_args == arguments

        assert request.geoctx_graft == ""
        assert request.no_ruster is False
        assert request.no_cache is False
        assert request.channel == _channel.__channel__
        assert request.trace is False
        assert request.type == types_pb2.ResultType.Value(type(obj).__name__)
        assert request.client_version == __version__
        assert has_proto_to_user_dict(request.format) == format
        assert has_proto_to_user_dict(request.destination) == destination

    @pytest.mark.parametrize(
        "ctx",
        [scenes.XYZTile(30, 40, 8), types.GeoContext.from_dltile_key("fake"), None],
    )
    def test_create_geoctx(self, stub, ctx):
        obj = types.Int(1)

        rpc = stub.return_value.CreateJob
        rpc.side_effect = lambda req, **kwargs: job_pb2.Job(
            geoctx_graft=req.geoctx_graft
        )

        with graft_client.consistent_guid():
            job = Job(obj, ctx)

        rpc.assert_called_once()
        ctx_json = rpc.call_args.args[0].geoctx_graft
        if ctx is None:
            assert ctx_json == ""
            assert job.geoctx is None
        else:
            with graft_client.consistent_guid():
                expected = types.GeoContext._promote(ctx)

            graft = json.loads(ctx_json)
            assert graft == expected.graft

            assert isinstance(job.geoctx, types.GeoContext)
            utils.assert_graft_is_scope_isolated_equvalent(job.geoctx.graft, graft)

    @pytest.mark.parametrize("client", [utils.MockedClient(), None])
    def test_create_client(self, stub, client):
        obj = types.Int(1)

        job = Job(obj, client=client)

        if client is not None:
            assert job._client is client
        else:
            assert isinstance(job._client, Client)

    @pytest.mark.parametrize("cache", [False, True])
    def test_create_cache(self, stub, cache):
        id_ = "foo"
        obj = types.Int(1)

        stub.return_value.CreateJob.side_effect = lambda req, **kwargs: job_pb2.Job(
            id=id_, no_cache=req.no_cache
        )

        job = Job(obj, cache=cache)

        stub.return_value.CreateJob.assert_called_once()
        assert stub.return_value.CreateJob.call_args[0][0].no_cache == (not cache)
        assert job.cache_enabled == cache

    @pytest.mark.parametrize("ruster", [None, False, True])
    def test_create_ruster(self, stub, ruster):
        rpc = stub.return_value.CreateJob
        rpc.return_value = job_pb2.Job()

        Job(types.Int(1), _ruster=ruster)

        rpc.assert_called_once()
        request_no_ruster = rpc.call_args.args[0].no_ruster

        if ruster is False:
            assert request_no_ruster is True
        else:
            assert request_no_ruster is False

    @pytest.mark.parametrize("trace", [False, True])
    def test_create_trace(self, stub, trace):
        rpc = stub.return_value.CreateJob
        rpc.return_value = job_pb2.Job()

        Job(types.Int(1), _trace=trace)

        rpc.assert_called_once()
        assert rpc.call_args.args[0].trace is trace

    @pytest.mark.parametrize("client", [mock.Mock(), None])
    def test_from_proto(self, stub, client):
        message = job_pb2.Job(id="foo")
        job = Job._from_proto(message, client=client)

        assert job._message == message
        if client is not None:
            assert job._client == client
        else:
            assert isinstance(job._client, Client)

        assert job._object is None
        assert job._arguments is None

    @pytest.mark.parametrize("client", [Client(), None])
    def test_get(self, stub, client):
        id_ = "foo"
        message = job_pb2.Job(id=id_)
        stub.return_value.GetJob.return_value = message

        job = Job.get(id_, client=client)
        assert job._message == message
        stub.return_value.GetJob.assert_called_with(
            job_pb2.GetJobRequest(id=id_),
            timeout=Client.DEFAULT_TIMEOUT,
            metadata=mock.ANY,
        )

        if client is not None:
            assert job._client == client
        else:
            assert isinstance(job._client, Client)

    def test_refresh(self, stub):
        message = job_pb2.Job(id="foo")
        refresh_message = job_pb2.Job(
            id="foo", state=job_pb2.Job.State(stage=job_pb2.Job.Stage.QUEUED)
        )

        job = Job._from_proto(message)

        stub.return_value.GetJob.return_value = refresh_message
        job.refresh()
        stub.return_value.GetJob.assert_called_with(
            job_pb2.GetJobRequest(id=job.id),
            timeout=Client.DEFAULT_TIMEOUT,
            metadata=mock.ANY,
        )
        assert job._message == refresh_message

    def test_cancel(self, stub):
        message = job_pb2.Job(id="foo")
        job = Job._from_proto(message)
        stub.return_value.CancelJob.return_value = job_pb2.CancelJobResponse()

        job.cancel()

        stub.return_value.CancelJob.assert_called_with(
            job_pb2.CancelJobRequest(id=job.id),
            timeout=Client.DEFAULT_TIMEOUT,
            metadata=mock.ANY,
        )

    def test_watch(self, stub):
        id_ = "foo"
        message = job_pb2.Job(id=id_)
        job = Job._from_proto(message)

        stub.return_value.WatchJob.return_value = [
            job_pb2.Job.State(stage=job_pb2.Job.Stage.QUEUED),
            job_pb2.Job.State(stage=job_pb2.Job.Stage.RUNNING),
            job_pb2.Job.State(stage=job_pb2.Job.Stage.SUCCEEDED),
        ]

        state_messages = []
        for job_ in job.watch():
            state = job_pb2.Job.State()
            state.CopyFrom(job_._message.state)
            state_messages.append(state)

        assert state_messages == stub.return_value.WatchJob.return_value

    def test_properties(self, stub):
        id_ = "foo"
        obj = types.Int(1)
        format = "geotiff"
        destination = {"type": "email"}
        expires_timestamp = Timestamp()
        expires_timestamp.FromJsonString("2003-01-02T04:05:06.789+00:00")

        job_state = job_pb2.Job.State(stage=job_pb2.Job.Stage.QUEUED)

        def create_side_effect(req, **kwargs):
            return job_pb2.Job(
                id=id_,
                serialized_graft=req.serialized_graft,
                typespec=req.typespec,
                arguments=req.arguments,
                geoctx_graft=req.geoctx_graft,
                no_ruster=req.no_ruster,
                channel=req.channel,
                client_version=__version__,
                expires_timestamp=expires_timestamp,
                no_cache=req.no_cache,
                trace=req.trace,
                state=job_state,
                type=req.type,
                format=user_format_to_proto(format),
                destination=user_destination_to_proto(destination),
            )

        stub.return_value.CreateJob.side_effect = create_side_effect

        job = Job(obj, format=format, destination=destination)
        job_from_msg = Job._from_proto(job._message, client=job._client)

        assert job.object is obj
        utils.assert_graft_is_scope_isolated_equvalent(
            job_from_msg.object.graft, obj.graft
        )
        assert job_from_msg.type is type(job_from_msg.object) is type(obj)  # noqa: E721
        assert job.result_type == "Int"

        assert job.id == id_
        assert job.arguments == {}
        assert job.geoctx is None
        assert job.channel == _channel.__channel__
        assert job.stage == "QUEUED"
        assert job.created_datetime is None
        assert job.updated_datetime is None
        assert job.expires_datetime == pb_timestamp_to_datetime(expires_timestamp)
        assert job.runtime is None
        assert job.error is None
        assert job.done is False
        assert job.cache_enabled is True
        assert job.version == __version__
        assert job.format == has_proto_to_user_dict(job._message.format)
        assert job.destination == has_proto_to_user_dict(job._message.destination)

        job._message.state.stage = job_pb2.Job.Stage.SUCCEEDED
        job._message.timestamp = 1
        job._message.state.timestamp = 2

        assert job.stage == "SUCCEEDED"
        assert job.created_datetime == pb_milliseconds_to_datetime(1)
        assert job.updated_datetime == pb_milliseconds_to_datetime(2)
        assert job.runtime == job.updated_datetime - job.created_datetime
        assert job.error is None
        assert job.done is True

        job._message.state.stage = job_pb2.Job.Stage.FAILED
        job._message.state.error.code = errors_pb2.ERROR_INVALID
        job._message.state.error.message = "test"

        assert job.stage == "FAILED"
        assert isinstance(job.error, JobInvalid)
        assert job.done is True

    @pytest.mark.parametrize(
        "proxify",
        [False, True],
    )
    def test_arguments(self, stub, proxify):
        x = types.parameter("x", types.Int)
        lst = types.parameter("lst", types.List[types.Int])
        obj = lst.map(lambda v: v + x)
        args = {"x": 1, "lst": [0, 1, 2]}

        def create_side_effect(req, **kwargs):
            return job_pb2.Job(
                serialized_graft=req.serialized_graft,
                typespec=req.typespec,
                arguments=req.arguments,
                client_version=__version__,
                type=req.type,
            )

        stub.return_value.CreateJob.side_effect = create_side_effect

        with graft_client.consistent_guid():
            proxy_lst = types.proxify(args["lst"])
            proxy_args = dict(args, lst=proxy_lst)

        with graft_client.consistent_guid():
            job = Job(obj, **proxy_args if proxify else args)

        job_args = job.arguments
        assert job_args.keys() == args.keys()
        assert job_args["x"] == 1
        assert isinstance(job_args["lst"], type(lst))
        utils.assert_graft_is_scope_isolated_equvalent(
            job_args["lst"].graft, proxy_lst.graft
        )

        # check it's cached
        assert job.arguments is job_args

    def test_error_with_version_mismatch(self, stub):
        job = Job._from_proto(job_pb2.Job(client_version="foo"))

        with pytest.raises(
            NotImplementedError, match="was created by client version 'foo'"
        ):
            job.arguments

        with pytest.raises(
            NotImplementedError, match="was created by client version 'foo'"
        ):
            job.object

        with pytest.raises(
            NotImplementedError, match="was created by client version 'foo'"
        ):
            job.resubmit()

    def test_resubmit(self, stub):
        rpc = stub.return_value.CreateJob
        rpc.side_effect = lambda req, **kwargs: job_pb2.Job(
            **{field: getattr(req, field) for field in req.DESCRIPTOR.fields_by_name}
        )

        obj = types.Int(1) + types.parameter("bar", types.Int)

        job = Job(
            obj,
            bar=2,
            format={"type": "pyarrow", "compression": "brotli"},
            destination="email",
            cache=False,
            _ruster=True,
            _trace=True,
        )

        new_job = job.resubmit()

        assert new_job.object is job.object
        assert new_job.geoctx is job.geoctx
        assert new_job.format == job.format
        assert new_job.destination == job.destination
        assert new_job._client is job._client
        assert new_job.cache_enabled == job.cache_enabled
        assert new_job._message.no_ruster == job._message.no_ruster
        assert new_job._message.trace == job._message.trace
        assert new_job.arguments == job.arguments

    def test_wait_success(self, stub):
        id_ = "foo"
        destination = user_destination_to_proto({"type": "download"})
        message = job_pb2.Job(id=id_, destination=destination)
        j = Job._from_proto(message)
        job_state = job_pb2.Job.State(stage=job_pb2.Job.Stage.SUCCEEDED)

        stub.return_value.WatchJob.return_value = [job_state]

        j.wait()
        assert j._message.state.stage == job_state.stage

    def test_wait_failure(self, stub):
        id_ = "foo"
        destination = user_destination_to_proto({"type": "download"})
        message = job_pb2.Job(id=id_, destination=destination)
        j = Job._from_proto(message)

        job_state = job_pb2.Job.State(
            stage=job_pb2.Job.Stage.FAILED,
            error=job_pb2.Job.Error(code=errors_pb2.ERROR_UNKNOWN),
        )

        stub.return_value.WatchJob.return_value = [job_state]

        with pytest.raises(JobComputeError):
            j.wait()
        assert j._message.state.stage == job_state.stage

    def test_wait_terminated(self, stub):
        id_ = "foo"
        destination = user_destination_to_proto({"type": "download"})
        message = job_pb2.Job(id=id_, destination=destination)
        j = Job._from_proto(message)

        job_state = job_pb2.Job.State(
            stage=job_pb2.Job.Stage.FAILED,
            error=job_pb2.Job.Error(code=errors_pb2.ERROR_TERMINATED),
        )

        stub.return_value.WatchJob.return_value = [job_state]

        with pytest.raises(JobTerminated):
            j.wait()
        assert j._message.state.stage == job_state.stage

    def test_wait_timeout(self, stub):
        id_ = "foo"
        destination = user_destination_to_proto({"type": "download"})
        message = job_pb2.Job(id=id_, destination=destination)
        j = Job._from_proto(message)

        job_state = job_pb2.Job.State(stage=job_pb2.Job.Stage.QUEUED)

        def side_effect(*args, **kwargs):
            yield job_state
            raise MockRpcError(grpc.StatusCode.DEADLINE_EXCEEDED)

        stub.return_value.WatchJob.side_effect = side_effect

        with pytest.raises(JobTimeoutError):
            j.wait(timeout=1)

        stub.return_value.WatchJob.assert_called()
        assert j._message.state.stage == job_state.stage

    @responses.activate
    def test_result(self, stub):
        format_proto = user_format_to_proto(
            {"type": "pyarrow", "compression": "brotli"}
        )
        destination_proto = user_destination_to_proto("download")
        destination_proto.download.result_url = (
            "https://storage.googleapis.com/dl-compute-dev-results"
        )

        job = Job._from_proto(
            job_pb2.Job(
                id="foo",
                state=job_pb2.Job.State(stage=job_pb2.Job.Stage.SUCCEEDED),
                type=9,
                format=format_proto,
                destination=destination_proto,
            )
        )

        result = 2
        codec = "lz4"
        serialized = serialize_pyarrow(result, codec)

        responses.add(
            responses.GET,
            job.url,
            body=serialized,
            headers={
                "x-goog-stored-content-encoding": "application/vnd.pyarrow",
                "x-goog-meta-X-Arrow-Codec": codec,
            },
            status=200,
        )

        assert download(job) == result

    @pytest.mark.parametrize("file_path", [True, False])
    @responses.activate
    def test_result_to_file(self, stub, file_path, tmpdir):
        format_proto = user_format_to_proto("json")
        destination_proto = user_destination_to_proto("download")
        destination_proto.download.result_url = (
            "https://storage.googleapis.com/dl-compute-dev-results"
        )

        job = Job._from_proto(
            job_pb2.Job(
                id="foo",
                state=job_pb2.Job.State(stage=job_pb2.Job.Stage.SUCCEEDED),
                format=format_proto,
                destination=destination_proto,
            )
        )

        result = [1, 2, 3, 4]
        responses.add(
            responses.GET,
            job.url,
            body=json.dumps(result),
            headers={"x-goog-stored-content-encoding": "application/json"},
            status=200,
            stream=True,
        )

        path = tmpdir.join("test.json")
        file_arg = str(path) if file_path else path.open("wb")

        job.result_to_file(file_arg)

        if not file_path:
            assert not file_arg.closed
            file_arg.flush()

        with open(str(path), "r") as f:
            assert result == json.load(f)

        if not file_path:
            file_arg.close()
