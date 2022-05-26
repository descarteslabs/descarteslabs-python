import json
import mock
import pytest

from ....common.proto.workflow import workflow_pb2

from ....client.version import __version__
from ...types import Int
from ...client import Client
from ...cereal import serialize_typespec
from ..versionedgraft import VersionedGraft
from ..visualization import VizOption
from . import utils
from ... import _channel


@mock.patch(
    "descarteslabs.workflows.models.workflow.get_global_grpc_client",
    new=lambda: utils.MockedClient(),
)
@mock.patch("descarteslabs.common.proto.workflow.workflow_pb2_grpc.WorkflowAPIStub")
class TestVersionedGraft(object):
    def test_init(self, stub):
        obj = Int(42)
        version = "0.0.1"
        docstring = "int 42"
        labels = {
            "github_url": "http://github.com/someurl",
            "project": "some important project",
        }
        viz_options = [
            VizOption(
                id="viz1",
                bands=["red", "green", "blue"],
                scales=[[0, 0.4], [0, 0.4], [0, 0.4]],
            ),
        ]

        vg = VersionedGraft(
            version,
            obj,
            docstring=docstring,
            labels=labels,
            viz_options=viz_options,
        )

        assert vg.version == version
        assert vg.object is obj
        assert vg.docstring == docstring
        assert vg.labels == labels
        assert vg._message.serialized_graft == json.dumps(obj.graft)
        assert vg._message.typespec == serialize_typespec(type(obj))
        assert vg.channel == _channel.__channel__
        assert vg._message.client_version == __version__
        assert vg.type == type(obj)
        assert vg.viz_options == viz_options

    def test_get(self, stub):
        workflow_id = "foobar"
        version = "0.0.1"

        message = "foo"

        stub.return_value.GetVersion.return_value = message

        with mock.patch.object(VersionedGraft, "_from_proto") as _from_proto:
            VersionedGraft.get(workflow_id, version)
            _from_proto.assert_called_once()
            assert _from_proto.call_args[0][0] is message
            stub.return_value.GetVersion.assert_called_once_with(
                workflow_pb2.GetVersionRequest(id=workflow_id, version=version),
                timeout=Client.DEFAULT_TIMEOUT,
                metadata=mock.ANY,
            )

    def test_object_doc(self, stub):
        obj = Int(42)
        orig_doc = obj.__doc__
        doc = "foo"
        vg = VersionedGraft("0.0.1", obj, docstring=doc)
        vg._object = None

        new_obj = vg.object

        assert new_obj is not obj
        assert new_obj.__doc__ == doc
        assert obj.__doc__ is orig_doc

    def test_url(self, stub):
        obj = utils.Foo(1)
        version = "1.0.1"
        vg = VersionedGraft(version, obj)

        with pytest.raises(ValueError, match="has not been persisted"):
            vg.url()

        url_template = vg._message.url_template = "http://base.net"

        assert vg.url() == url_template

    def test_wmts_url(self, stub):
        obj = utils.Foo(1)
        version = "1.0.1"
        vg = VersionedGraft(version, obj)

        with pytest.raises(ValueError, match="has not been persisted"):
            vg.wmts_url()

        wmts_url_template = (
            "http://base.net/wmts/workflow/wid/1.0.1/1.0.0/WMTSCapabilities.xml"
        )
        vg._message.wmts_url_template = wmts_url_template

        assert vg.wmts_url() == wmts_url_template
        assert (
            vg.wmts_url(tile_matrix_sets="utm")
            == wmts_url_template + "?tile_matrix_sets=utm"
        )
        assert (
            vg.wmts_url(tile_matrix_sets=["EPSG:4326", "EPSG:3857"])
            == wmts_url_template
            + "?tile_matrix_sets=EPSG%3A4326&tile_matrix_sets=EPSG%3A3857"
        )
