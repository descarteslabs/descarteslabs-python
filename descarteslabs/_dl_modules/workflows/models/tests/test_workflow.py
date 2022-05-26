import json
import mock
import pytest

from ....common.proto.workflow import workflow_pb2

from ...client import Client
from .. import Workflow, wmts_url
from ..utils import pb_milliseconds_to_datetime
from . import utils
from ...types import Int, Function


@mock.patch(
    "descarteslabs.workflows.models.workflow.get_global_grpc_client",
    new=lambda: utils.MockedClient(),
)
@mock.patch("descarteslabs.common.proto.workflow.workflow_pb2_grpc.WorkflowAPIStub")
class TestWorkflow(object):
    def test_build(self, stub):
        wf = Workflow(
            id="bob@gmail.com:test",
            title="test",
            description="a test",
            labels={"foo": "bar"},
            tags=["foo", "bar"],
        )
        msg = wf._message

        assert msg.id == "bob@gmail.com:test"
        assert msg.title == "test"
        assert msg.description == "a test"
        assert msg.labels == {"foo": "bar"}
        assert msg.tags == ["foo", "bar"]

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
                metadata=mock.ANY,
            )

    def test_delete_id(self, stub):
        workflow_id = "foo"
        Workflow.delete_id(workflow_id)
        stub.return_value.DeleteWorkflow.assert_called_once_with(
            workflow_pb2.DeleteWorkflowRequest(id=workflow_id),
            timeout=Client.DEFAULT_TIMEOUT,
            metadata=mock.ANY,
        )

    def test_delete(self, stub):
        workflow_id = "bob@gmail.com:test"
        wf = Workflow(id=workflow_id)
        wf.delete()
        stub.return_value.DeleteWorkflow.assert_called_once_with(
            workflow_pb2.DeleteWorkflowRequest(id=workflow_id),
            timeout=Client.DEFAULT_TIMEOUT,
            metadata=mock.ANY,
        )

    def test_save(self, stub):
        new_message = "fake message"
        stub.return_value.UpsertWorkflow.return_value = new_message

        wf = Workflow(
            id="bob@gmail.com:test",
            title="test",
            description="a test",
            labels={"foo": "bar"},
            tags=["foo", "bar"],
        )
        old_message = wf._message

        wf.save()
        assert wf._message is new_message
        stub.return_value.UpsertWorkflow.assert_called_once_with(
            workflow_pb2.UpsertWorkflowRequest(
                id=old_message.id,
                title=old_message.title,
                description=old_message.description,
                versioned_grafts=old_message.versioned_grafts,
                labels=old_message.labels,
                tags=old_message.tags,
            ),
            timeout=Client.DEFAULT_TIMEOUT,
            metadata=mock.ANY,
        )

    def test_set_version(self, stub):
        version = "0.0.1"
        obj = Int(1)
        docstring = "the integer 1"
        labels = {"foo": "bar"}
        wf = Workflow(id="bob@gmail.com:test")
        assert len(wf._message.versioned_grafts) == 0
        new_vg = wf.set_version(version, obj, docstring=docstring, labels=labels)
        assert new_vg.version == version
        assert new_vg.docstring == docstring
        assert new_vg.labels == labels
        assert len(wf._message.versioned_grafts) == 1
        new_vg_proto = wf._message.versioned_grafts[0]
        assert type(new_vg.object) == type(obj)
        assert new_vg_proto.version == version
        assert new_vg_proto.docstring == docstring
        assert new_vg_proto.labels == labels
        assert new_vg_proto.serialized_graft == json.dumps(obj.graft)

    def test_set_version_deco(self, stub):
        version = "0.0.1"
        labels = {"foo": "bar"}
        wf = Workflow(id="bob@gmail.com:test")
        assert len(wf._message.versioned_grafts) == 0

        @wf.set_version(version, labels=labels)
        def func(x: Int, y: Int):
            "add stuff"
            return x + y

        assert isinstance(func, Function)

        new_vg = wf[version]
        assert new_vg.version == version
        assert new_vg.docstring == "add stuff"
        assert new_vg.labels == labels
        assert len(wf._message.versioned_grafts) == 1
        assert type(new_vg.object) == type(func)
        new_vg_proto = wf._message.versioned_grafts[0]
        assert new_vg_proto.serialized_graft == json.dumps(func.graft)

    def test_set_version_overwrite(self, stub):
        version = "0.0.1"
        obj = Int(1)
        docstring = "the integer 1"
        labels = {"foo": "bar"}
        wf = Workflow(id="bob@gmail.com:test")
        assert len(wf._message.versioned_grafts) == 0
        wf.set_version(version, obj, docstring=docstring, labels=labels)
        assert len(wf._message.versioned_grafts) == 1
        assert wf._message.versioned_grafts[0].version == version
        new_docstring = "our super cool integer 1"
        new_labels = {"bar": "baz"}
        new_vg = wf.set_version(
            version, obj, docstring=new_docstring, labels=new_labels
        )
        assert type(new_vg.object) == type(obj)
        assert new_vg.version == version
        assert new_vg.docstring == new_docstring
        assert new_vg.labels == new_labels
        assert len(wf._message.versioned_grafts) == 1
        new_vg_proto = wf._message.versioned_grafts[0]
        assert new_vg_proto.version == version
        assert new_vg_proto.docstring == new_docstring
        assert new_vg_proto.labels == new_labels
        assert new_vg_proto.serialized_graft == json.dumps(obj.graft)

    def test_get_version(self, stub):
        version = "0.0.1"
        obj = Int(1)
        wf = Workflow(id="bob@gmail.com:test")
        assert len(wf._message.versioned_grafts) == 0
        wf.set_version(version, obj)
        assert len(wf._message.versioned_grafts) == 1
        assert wf._message.versioned_grafts[0].version == version
        vg = wf.get_version(version)
        assert vg.version == version
        assert type(vg.object) == type(obj)

    def test_get_version_raises_wrong_type(self, stub):
        version = 5
        wf = Workflow(id="bob@gmail.com:test")
        with pytest.raises(TypeError):
            wf.get_version(version)

    def test_get_version_raises_doesnt_exist(self, stub):
        version = "0.0.1"
        wf = Workflow(id="bob@gmail.com:test")
        with pytest.raises(KeyError):
            wf.get_version(version)

    def test_properties(self, stub):
        wf = Workflow(
            id="bob@gmail.com:test",
            title="test",
            description="a test",
            labels={"foo": "bar"},
            tags=["foo", "bar"],
        )

        assert wf.id == "bob@gmail.com:test"
        assert wf.title == "test"
        assert wf.description == "a test"
        assert wf.labels == {"foo": "bar"}
        assert wf.tags == ["foo", "bar"]
        assert wf.name is None
        assert wf.created_timestamp is None
        assert wf.updated_timestamp is None

        wf._message.name = "test"
        wf._message.created_timestamp = 100
        wf._message.updated_timestamp = 200

        assert wf.name == "test"
        assert wf.created_timestamp == pb_milliseconds_to_datetime(100)
        assert wf.updated_timestamp == pb_milliseconds_to_datetime(200)

    def test_workflow_wmts_url(self, stub):
        wf = Workflow(
            id="bob@gmail.com:test",
            title="test",
            description="a test",
            labels={"foo": "bar"},
            tags=["foo", "bar"],
        )

        wmts_url_template = "http://base.net/wmts/workflow/bob@gmail.com:test/1.0.0/WMTSCapabilities.xml"
        wf._message.wmts_url_template = wmts_url_template

        assert wf.wmts_url() == wmts_url_template
        assert (
            wf.wmts_url(tile_matrix_sets="utm")
            == wmts_url_template + "?tile_matrix_sets=utm"
        )
        assert (
            wf.wmts_url(tile_matrix_sets=["EPSG:4326", "EPSG:3857"])
            == wmts_url_template
            + "?tile_matrix_sets=EPSG%3A4326&tile_matrix_sets=EPSG%3A3857"
        )

    def test_wmts_url(self, stub):
        wmts_url_template = "http://base.net/wmts/workflow/1.0.0/WMTSCapabilities.xml"
        response = workflow_pb2.WmtsUrlTemplateResponse()
        response.wmts_url_template = wmts_url_template
        stub.return_value.GetWmtsUrlTemplate.return_value = response

        assert wmts_url() == wmts_url_template
        assert (
            wmts_url(tile_matrix_sets="utm")
            == wmts_url_template + "?tile_matrix_sets=utm"
        )
        assert (
            wmts_url(tile_matrix_sets=["EPSG:4326", "EPSG:3857"])
            == wmts_url_template
            + "?tile_matrix_sets=EPSG%3A4326&tile_matrix_sets=EPSG%3A3857"
        )
