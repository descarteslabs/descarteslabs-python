import json

import freezegun
import pytest
import mock
import pyarrow as pa
import responses

from descarteslabs.workflows.client import Client

from descarteslabs.common.proto.errors import errors_pb2
from descarteslabs.common.proto.job import job_pb2
from descarteslabs.common.proto.types import types_pb2
from descarteslabs.common.workflows.arrow_serialization import serialization_context
from descarteslabs.common.graft import client as graft_client

from descarteslabs.workflows import _channel

from ... import cereal, types
from ..exceptions import JobInvalid, JobComputeError, JobTerminated, JobTimeoutError
from ..job import Job
from ..parameters import parameters_to_grafts
from ..utils import pb_milliseconds_to_datetime

from . import utils


class TestTypespecToUnmarshalStr(object):
    def test_nonparametric(self):
        typespec = cereal.serialize_typespec(types.Int)
        assert cereal.typespec_to_unmarshal_str(typespec) == "Int"

    def test_parametric(self):
        typespec = cereal.serialize_typespec(types.List[types.Int])
        assert cereal.typespec_to_unmarshal_str(typespec) == "List"

    def test_non_marshallable(self):
        typespec = cereal.serialize_typespec(types.Function[{}, types.Int])
        with pytest.raises(TypeError, match="'Function' is not a computable type"):
            cereal.typespec_to_unmarshal_str(typespec)


@mock.patch(
    "descarteslabs.workflows.models.job.get_global_grpc_client",
    new=lambda: utils.MockedClient(),
)
@mock.patch("descarteslabs.common.proto.job.job_pb2_grpc.JobAPIStub")
class TestJob(object):
    def test_create(self, stub):
        obj = types.Int(1)
        parameters = {"foo": types.Str("bar")}

        typespec = cereal.serialize_typespec(type(obj))
        create_job_request_message = job_pb2.CreateJobRequest(
            parameters=json.dumps(parameters_to_grafts(**parameters)),
            serialized_graft=json.dumps(obj.graft),
            typespec=typespec,
            type=types_pb2.ResultType.Value(cereal.typespec_to_unmarshal_str(typespec)),
            no_cache=False,
            channel=_channel.__channel__,
        )

        message = job_pb2.Job(
            id="foo",
            parameters=create_job_request_message.parameters,
            serialized_graft=create_job_request_message.serialized_graft,
            typespec=create_job_request_message.typespec,
            type=create_job_request_message.type,
            no_cache=create_job_request_message.no_cache,
            channel=create_job_request_message.channel,
        )
        stub.return_value.CreateJob.return_value = message

        job = Job(obj, parameters)

        stub.return_value.CreateJob.assert_called_once_with(
            create_job_request_message,
            timeout=Client.DEFAULT_TIMEOUT,
            metadata=(("x-wf-channel", create_job_request_message.channel),),
        )

        assert job._message is message

    @pytest.mark.parametrize("client", [utils.MockedClient(), None])
    def test_create_client(self, stub, client):
        obj = types.Int(1)
        parameters = {"foo": types.Str("bar")}

        job = Job(obj, parameters, client=client)

        if client is not None:
            assert job._client is client
        else:
            assert isinstance(job._client, Client)

    @pytest.mark.parametrize("cache", [False, True])
    def test_create_cache(self, stub, cache):
        id_ = "foo"
        obj = types.Int(1)
        parameters = {"foo": types.Str("bar")}

        stub.return_value.CreateJob.side_effect = lambda req, **kwargs: job_pb2.Job(
            id=id_, no_cache=req.no_cache
        )

        job = Job(obj, parameters, cache=cache)

        stub.return_value.CreateJob.assert_called_once()
        assert stub.return_value.CreateJob.call_args[0][0].no_cache == (not cache)
        assert job.cache_enabled == cache

    @pytest.mark.parametrize("client", [mock.Mock(), None])
    def test_from_proto(self, stub, client):
        message = job_pb2.Job(id="foo")
        job = Job._from_proto(message, client=client)

        assert job._message == message
        if client is not None:
            assert job._client == client
        else:
            assert isinstance(job._client, Client)

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
            metadata=(("x-wf-channel", _channel.__channel__),),
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
            metadata=(("x-wf-channel", _channel.__channel__),),
        )
        assert job._message == refresh_message

    # def test_cancel(self, stub):
    #     message = job_pb2.Job(id="foo")
    #     job = Job._from_proto(message)
    #     cancel_message = job_pb2.Job.State(stage=job_pb2.Job.Stage.CANCELLED)
    #     stub.return_value.CancelJob.return_value = cancel_message
    #     job.cancel()
    #     stub.return_value.CancelJob.assert_called_with(
    #         job_pb2.CancelJobRequest(id=job.id), timeout=Client.DEFAULT_TIMEOUT
    #     )
    #     assert job._message.state == cancel_message

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
        parameters = {"foo": types.Str("bar")}

        job_state = job_pb2.Job.State(stage=job_pb2.Job.Stage.QUEUED)

        def create_side_effect(req, **kwargs):
            return job_pb2.Job(
                id=id_,
                parameters=req.parameters,
                serialized_graft=req.serialized_graft,
                typespec=req.typespec,
                type=req.type,
                channel=req.channel,
                state=job_state,
            )

        stub.return_value.CreateJob.side_effect = create_side_effect

        job = Job(obj, parameters)
        job_from_msg = Job._from_proto(job._message, client=job._client)

        assert job.object is obj
        utils.assert_graft_is_scope_isolated_equvalent(
            job_from_msg.object.graft, obj.graft
        )
        assert job_from_msg.type is type(job_from_msg.object) is type(obj)  # noqa: E721
        assert job.result_type == "Int"
        assert job.parameters == {"foo": graft_client.value_graft(parameters["foo"])}

        assert job.id == id_
        assert job.channel == _channel.__channel__
        assert job.stage == "QUEUED"
        assert job.created_datetime is None
        assert job.updated_datetime is None
        assert job.runtime is None
        assert job.error is None
        assert job.done is False

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

    def test_wait_for_result_success(self, stub):
        id_ = "foo"
        message = job_pb2.Job(id=id_)
        j = Job._from_proto(message)
        j._load_result = mock.Mock()
        job_state = job_pb2.Job.State(stage=job_pb2.Job.Stage.SUCCEEDED)

        stub.return_value.WatchJob.return_value = [job_state]

        j._wait_for_result()
        j._load_result.assert_called_once_with()
        assert j._message.state.stage == job_state.stage

    def test_wait_for_result_failure(self, stub):
        id_ = "foo"
        message = job_pb2.Job(id=id_)
        j = Job._from_proto(message)

        job_state = job_pb2.Job.State(
            stage=job_pb2.Job.Stage.FAILED,
            error=job_pb2.Job.Error(code=errors_pb2.ERROR_UNKNOWN),
        )

        stub.return_value.WatchJob.return_value = [job_state]

        with pytest.raises(JobComputeError):
            j._wait_for_result()
        assert j._message.state.stage == job_state.stage

    def test_wait_for_result_terminated(self, stub):
        id_ = "foo"
        message = job_pb2.Job(id=id_)
        j = Job._from_proto(message)

        job_state = job_pb2.Job.State(
            stage=job_pb2.Job.Stage.FAILED,
            error=job_pb2.Job.Error(code=errors_pb2.ERROR_TERMINATED),
        )

        stub.return_value.WatchJob.return_value = [job_state]

        with pytest.raises(JobTerminated):
            j._wait_for_result()
        assert j._message.state.stage == job_state.stage

    def test_wait_for_result_timeout(self, stub):
        id_ = "foo"
        message = job_pb2.Job(id=id_)
        j = Job._from_proto(message)

        job_state = job_pb2.Job.State(stage=job_pb2.Job.Stage.QUEUED)

        with pytest.raises(JobTimeoutError), freezegun.freeze_time(
            "2020-01-01"
        ) as freezer:

            def side_effect(*args, **kwargs):
                freezer.tick()  # ticks by 1 second
                return [job_state]

            stub.return_value.WatchJob.side_effect = side_effect

            j._wait_for_result(1e-4)

        stub.return_value.WatchJob.assert_called()
        assert j._message.state.stage == job_state.stage

    def test_load_result_error(self, stub):
        message = job_pb2.Job(
            id="foo",
            state=job_pb2.Job.State(
                stage=job_pb2.Job.Stage.FAILED,
                error=job_pb2.Job.Error(code=errors_pb2.ERROR_INVALID),
            ),
        )

        job = Job._from_proto(message)
        with pytest.raises(JobInvalid):
            job._load_result()

    @responses.activate
    def test_download_result(self, stub):
        job = Job._from_proto(
            job_pb2.Job(
                id="foo", state=job_pb2.Job.State(stage=job_pb2.Job.Stage.SUCCEEDED),
            )
        )

        result = {}
        buffer = pa.serialize(result, context=serialization_context).to_buffer()
        codec = "lz4"

        responses.add(
            responses.GET,
            Job.BUCKET_PREFIX.format(job.id),
            body=pa.compress(buffer, codec=codec, asbytes=True),
            headers={
                "x-goog-meta-codec": codec,
                "x-goog-meta-decompressed_size": str(len(buffer)),
            },
            status=200,
        )

        assert job._download_result() == result

    def test_unmarshal_primitive(self, stub):
        marshalled = (1, 2, True, None)
        job = Job._from_proto(
            job_pb2.Job(
                id="foo",
                state=job_pb2.Job.State(stage=job_pb2.Job.Stage.SUCCEEDED),
                type=types_pb2.List,
            )
        )

        result = job._unmarshal(marshalled)
        assert result == list(marshalled)

    def test_unmarshal_image(self, stub):
        marshalled = {
            "ndarray": {"red": []},
            "properties": {
                "foo": "bar",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
            },
            "bandinfo": {"red": {}},
            "geocontext": {
                "geometry": None,
                "crs": "EPSG:4326",
                "bounds": (-98, 40, -90, 44),
            },
        }
        job = Job._from_proto(
            job_pb2.Job(
                id="foo",
                state=job_pb2.Job.State(stage=job_pb2.Job.Stage.SUCCEEDED),
                type=types_pb2.Image,
            )
        )

        result = job._unmarshal(marshalled)
        # NOTE(gabe): we check the class name, versus `isinstance(result, results.ImageResult)`,
        # because importing results in this test would register its unmarshallers,
        # and part of what we're testing for is that the unmarshallers are getting registered correctly.
        assert result.__class__.__name__ == "ImageResult"

        assert result.ndarray == marshalled["ndarray"]
        assert result.properties == marshalled["properties"]
        assert result.bandinfo == marshalled["bandinfo"]
        assert result.geocontext == marshalled["geocontext"]
