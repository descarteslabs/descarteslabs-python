import json

import hypothesis.strategies as st
import mock
import pytest
from descarteslabs.common.proto import workflow_pb2
from hypothesis import given

from ... import _channel, cereal
from ...client import Client
from .. import Workflow
from ..utils import pb_milliseconds_to_datetime
from . import utils


@mock.patch("descarteslabs.common.proto.workflow_pb2_grpc.WorkflowAPIStub")
class TestWorkflow(object):
    def test_build(self, stub):
        obj = utils.Foo(1)
        wf = Workflow.build(obj, name="foo", description="a foo")

        assert wf._object is obj
        assert wf._client is not None
        message = wf._message

        assert json.loads(message.serialized_graft) == utils.json_normalize(obj.graft)
        assert "Foo" in message.serialized_typespec
        assert message.name == "foo"
        assert message.description == "a foo"
        assert message.channel == _channel.__channel__

    def test_roundtrip_from_proto(self, stub):
        obj = utils.Bar(utils.Foo(1))
        wf = Workflow.build(obj, name="bar", description="a bar")
        message = wf._message

        wf_from_proto = Workflow._from_proto(message)

        new_obj = wf_from_proto.object
        assert type(new_obj) == type(obj)
        utils.assert_graft_is_scope_isolated_equvalent(new_obj.graft, obj.graft)

    def test_roundtrip_from_proto_no_graft(self, stub):
        obj = utils.Bar(utils.Foo(1))
        wf = Workflow.build(obj, name="bar", description="a bar")
        message = wf._message
        message.id = "bar_id"
        message.serialized_graft = ""

        wf_from_proto = Workflow._from_proto(message)

        new_obj = wf_from_proto.object
        assert type(new_obj) == type(obj)

        use_application = new_obj.graft[new_obj.graft["returns"]]
        assert use_application[0] == "Workflow.use"
        assert "workflow_id" in use_application[1]
        assert new_obj.graft[use_application[1]["workflow_id"]] == message.id

    def test_get(self, stub):
        message = "foo"
        stub.return_value.GetWorkflow.return_value = message

        with mock.patch.object(Workflow, "_from_proto") as _from_proto:
            Workflow.get("fake_id")
            _from_proto.assert_called_once()
            assert _from_proto.call_args[0][0] is message
            stub.return_value.GetWorkflow.assert_called_once_with(
                workflow_pb2.GetWorkflowRequest(id="fake_id"),
                timeout=Client.DEFAULT_TIMEOUT,
            )

    @pytest.mark.skip(
        reason=(
            "this test is flaky for an inscrutable reason. "
            "possibly some race condition with the stub mock? but hypothesis doesn't "
            "parallelize as far as I can tell."
        )
    )
    @given(
        st.just(utils.Bar(1)) | st.none(), st.text() | st.none(), st.text() | st.none()
    )
    def test_update(self, stub, new_obj, new_name, new_description):
        old_obj = utils.Bar(utils.Foo(1))
        old_name = "bar"
        old_description = "a bar"
        wf = Workflow.build(old_obj, name=old_name, description=old_description)
        wf.update(new_obj, new_name, new_description)

        message = stub.return_value.UpdateWorkflow.call_args[0][0].workflow

        should_be_obj = new_obj if new_obj is not None else old_obj
        assert json.loads(message.serialized_graft) == utils.json_normalize(
            should_be_obj.graft
        )
        assert json.loads(message.serialized_typespec) == utils.json_normalize(
            cereal.serialize_typespec(type(should_be_obj))
        )

        assert message.name == new_name if new_name is not None else old_name
        assert (
            message.description == new_description
            if new_description is not None
            else old_description
        )

    def test_save(self, stub):
        new_message = "fake message"
        stub.return_value.CreateWorkflow.return_value = new_message

        obj = utils.Bar(utils.Foo(1))
        wf = Workflow.build(obj, name="bar", description="a bar")
        old_message = wf._message

        wf.save()
        assert wf._message is new_message
        stub.return_value.CreateWorkflow.assert_called_once_with(
            workflow_pb2.CreateWorkflowRequest(workflow=old_message),
            timeout=Client.DEFAULT_TIMEOUT,
        )

    def test_properties(self, stub):
        obj = utils.Bar(utils.Foo(1))
        wf = Workflow.build(obj, name="bar", description="a bar")

        assert wf.object == obj
        assert wf.type == type(obj)
        assert wf.id is None
        assert wf.created_timestamp is None
        assert wf.updated_timestamp is None
        assert wf.name == "bar"
        assert wf.description == "a bar"
        assert wf.channel == _channel.__channel__

        wf._message.id = "1234"
        wf._message.created_timestamp = 100
        wf._message.updated_timestamp = 200

        assert wf.id == "1234"
        assert wf.created_timestamp == pb_milliseconds_to_datetime(100)
        assert wf.updated_timestamp == pb_milliseconds_to_datetime(200)

    def test_incompatible_channel(self, stub):
        obj = utils.Foo(1)
        wf = Workflow.build(obj, name="foo", description="a foo")
        wf._message.channel = "foobar"

        with pytest.raises(ValueError, match="only defined for channel 'foobar'"):
            wf.object
