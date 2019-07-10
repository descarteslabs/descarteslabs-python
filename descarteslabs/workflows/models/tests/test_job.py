import collections
import json

import pytest
import mock
import pyarrow as pa
import responses

from descarteslabs.workflows.client import Client

from descarteslabs.common.proto import job_pb2, types_pb2
from descarteslabs.common.workflows.arrow_serialization import serialization_context
from descarteslabs.common.graft import client as graft_client

from ... import cereal, types
from ..exceptions import JobInvalid
from ..job import Job, _typespec_to_unmarshal_str
from ..utils import pb_milliseconds_to_datetime

from .utils import json_normalize


class TestTypespecToUnmarshalStr(object):
    def test_nonparametric(self):
        typespec = cereal.serialize_typespec(types.Int)
        assert _typespec_to_unmarshal_str(typespec) == "Int"

    def test_parametric(self):
        typespec = cereal.serialize_typespec(types.List[types.Int])
        assert _typespec_to_unmarshal_str(typespec) == "List"

    def test_non_marshallable(self):
        typespec = cereal.serialize_typespec(types.Function[{}, types.Int])
        with pytest.raises(TypeError, match="'Function' is not a computable type"):
            _typespec_to_unmarshal_str(typespec)


@mock.patch("descarteslabs.common.proto.job_pb2_grpc.JobAPIStub")
class TestJob(object):
    @pytest.mark.parametrize("client", [mock.Mock(), None])
    def test_instantiate(self, stub, client):
        message = job_pb2.Job(id="foo")
        job = Job(message, client=client)

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
            job_pb2.GetJobRequest(id=id_), timeout=Client.DEFAULT_TIMEOUT
        )

        if client is not None:
            assert job._client == client
        else:
            assert isinstance(job._client, Client)

    @pytest.mark.parametrize("client", [mock.Mock(), None])
    def test_build(self, stub, client):
        obj = types.Int(1)
        parameters = {"foo": types.Str("bar")}

        job = Job.build(obj, parameters, channel="foo", client=client)
        message = job._message

        assert message.serialized_proxy_object == ""
        assert message.workflow_id == ""
        assert message.channel == "foo"

        assert json.loads(message.parameters) == json_normalize(
            {
                "foo": graft_client.value_graft(parameters["foo"]),
            }
        )
        assert json.loads(message.serialized_graft) == json_normalize(obj.graft)
        assert message.serialized_typespec == json.dumps("Int")
        assert message.type == types_pb2.Int

        if client is not None:
            assert job._client == client
        else:
            assert isinstance(job._client, Client)

    def test_execute(self, stub):
        obj = types.Int(1)
        parameters = {"foo": types.Str("bar")}

        job = Job.build(obj, parameters, channel="foo")

        new_message = job_pb2.Job(
            id="foo",
            parameters=job._message.parameters,
            serialized_graft=job._message.serialized_graft,
            serialized_typespec=job._message.serialized_typespec,
            type=job._message.type,
            channel="foo",
        )
        stub.return_value.CreateJob.return_value = new_message

        job.execute()
        job.execute()  # if it has an id short circuit execution, create should only be called once

        stub.return_value.CreateJob.assert_called_once_with(
            job_pb2.CreateJobRequest(
                parameters=job._message.parameters,
                serialized_graft=job._message.serialized_graft,
                serialized_typespec=job._message.serialized_typespec,
                type=job._message.type,
                channel="foo",
            ),
            timeout=Client.DEFAULT_TIMEOUT,
        )
        assert job._message is new_message

    def test_refresh(self, stub):
        message = job_pb2.Job(id="foo")
        refresh_message = job_pb2.Job(id="foo", status=job_pb2.STATUS_UNKNOWN)

        job = Job(message)

        stub.return_value.GetJob.return_value = refresh_message
        job.refresh()
        stub.return_value.GetJob.assert_called_with(
            job_pb2.GetJobRequest(id=job.id), timeout=Client.DEFAULT_TIMEOUT
        )
        assert job._message == refresh_message

    def test_cancel(self, stub):
        message = job_pb2.Job(id="foo")
        cancel_message = job_pb2.Job(
            id="foo", status=job_pb2.STATUS_FAILURE, terminated=True
        )

        job = Job(message)
        stub.return_value.CancelJob.return_value = cancel_message
        job.cancel()
        stub.return_value.CancelJob.assert_called_with(
            job_pb2.CancelJobRequest(id=job.id), timeout=Client.DEFAULT_TIMEOUT
        )
        assert job._message == cancel_message

    def test_watch(self, stub):
        id_ = "foo"
        message = job_pb2.Job(id=id_)
        job = Job(message)

        stub.return_value.WatchJob.return_value = [
            job_pb2.Job(
                id=id_, stage=job_pb2.STAGE_PENDING, status=job_pb2.STATUS_UNKNOWN
            ),
            job_pb2.Job(
                id=id_, stage=job_pb2.STAGE_RUNNING, status=job_pb2.STATUS_UNKNOWN
            ),
            job_pb2.Job(
                id=id_, stage=job_pb2.STAGE_DONE, status=job_pb2.STATUS_SUCCESS
            ),
        ]

        state_messages = [state._message for state in job.watch()]

        assert state_messages == stub.return_value.WatchJob.return_value

    def test_properties(self, stub):
        obj = types.Int(1)
        parameters = {"foo": types.Str("bar")}

        job = Job.build(obj, parameters, channel="foo")
        job_from_msg = Job(job._message, client=job._client)

        assert job.object is obj
        assert json_normalize(job_from_msg.object.graft) == json_normalize(obj.graft)
        assert job_from_msg.type is type(job_from_msg.object) is type(obj)  # noqa: E721
        assert job.result_type == "Int"
        assert job.parameters == {
            "foo": graft_client.value_graft(parameters["foo"]),
        }

        assert job.id is None
        assert job.channel == "foo"
        assert job.status == "STATUS_UNKNOWN"
        assert job.stage == "STAGE_UNKNOWN"
        assert job.created_datetime is None
        assert job.updated_datetime is None
        assert job.runtime is None
        assert job.error is None
        assert job.done is False

        job._message.id = "foo"
        job._message.status = job_pb2.STATUS_SUCCESS
        job._message.stage = job_pb2.STAGE_DONE
        job._message.created_timestamp = 1
        job._message.updated_timestamp = 2

        assert job.id == "foo"
        assert job.status == "STATUS_SUCCESS"
        assert job.stage == "STAGE_DONE"
        assert job.created_datetime == pb_milliseconds_to_datetime(1)
        assert job.updated_datetime == pb_milliseconds_to_datetime(2)
        assert job.runtime == job.updated_datetime - job.created_datetime
        assert job.error is None
        assert job.done is True

        job._message.status = job_pb2.STATUS_FAILURE
        job._message.error.code = job_pb2.ERROR_INVALID
        job._message.error.message = "test"

        assert job.status == "STATUS_FAILURE"
        assert job.stage == "STAGE_DONE"
        assert isinstance(job.error, JobInvalid)
        assert job.done is True

    def test_wait_for_result_success(self, stub):
        id_ = "foo"
        message = job_pb2.Job(id=id_)
        j = Job(message)
        j._load_result = mock.Mock()
        status = job_pb2.STATUS_SUCCESS

        stub.return_value.GetJob.return_value = job_pb2.Job(id=id_, status=status)

        j._wait_for_result()
        j._load_result.assert_called_once_with()
        assert j._message.status == status

    def test_wait_for_result_failure(self, stub):
        id_ = "foo"
        message = job_pb2.Job(id=id_)
        j = Job(message)

        status = job_pb2.STATUS_FAILURE

        stub.return_value.GetJob.return_value = job_pb2.Job(id=id_, status=status)

        with pytest.raises(Exception):
            # TODO(justin) fix exception type
            j._wait_for_result()
        assert j._message.status == status

    def test_wait_for_result_terminated(self, stub):
        id_ = "foo"
        message = job_pb2.Job(id=id_)
        j = Job(message)

        status = job_pb2.STATUS_FAILURE

        stub.return_value.GetJob.return_value = job_pb2.Job(
            id=id_,
            status=status,
            stage=job_pb2.STAGE_DONE,
            error=job_pb2.JobError(code=job_pb2.ERROR_TERMINATED),
        )

        with pytest.raises(Exception):
            # TODO(justin) fix exception type
            j._wait_for_result()
        assert j._message.status == status

    def test_wait_for_result_timeout(self, stub):
        id_ = "foo"
        status = job_pb2.STATUS_UNKNOWN

        message = job_pb2.Job(id=id_, status=status, stage=job_pb2.STAGE_PENDING)
        j = Job(message)

        stub.return_value.GetJob.return_value = message

        with pytest.raises(Exception):
            # TODO(justin) fix exception type
            j._wait_for_result(1e-4)
        assert j._message.status == status
        stub.return_value.GetJob.assert_called()

    def test_load_result_error(self, stub):
        message = job_pb2.Job(
            id="foo",
            status=job_pb2.STATUS_FAILURE,
            error=job_pb2.JobError(code=job_pb2.ERROR_INVALID),
        )

        job = Job(message)
        with pytest.raises(JobInvalid):
            job._load_result()

    @responses.activate
    def test_download_result(self, stub):
        job = Job(
            job_pb2.Job(
                id="foo",
                status=job_pb2.STATUS_SUCCESS,
                error=job_pb2.JobError(code=job_pb2.ERROR_NONE),
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
        job = Job(
            job_pb2.Job(id="foo", status=job_pb2.STATUS_SUCCESS, type=types_pb2.List)
        )

        result = job._unmarshal(marshalled)
        assert result == list(marshalled)

    def test_unmarshal_image(self, stub):
        marshalled = {
            "bands": collections.OrderedDict([("red", [])]),
            "properties": {
                "foo": "bar",
                "geometry": {"type": "Point", "coordinates": [0, 0]},
            },
            "bandinfo": collections.OrderedDict([("red", {})]),
        }
        job = Job(
            job_pb2.Job(id="foo", status=job_pb2.STATUS_SUCCESS, type=types_pb2.Image)
        )

        result = job._unmarshal(marshalled)
        # NOTE(gabe): we check the class name, versus `isinstance(result, containers.Image)`,
        # because importing containers in this test would register its unmarshallers,
        # and part of what we're testing for is that the unmarshallers are getting registered correctly.
        assert result.__class__.__name__ == "Image"

        assert result.bands == marshalled["bands"]
        assert result.properties == marshalled["properties"]
        assert result.bandinfo == marshalled["bandinfo"]
