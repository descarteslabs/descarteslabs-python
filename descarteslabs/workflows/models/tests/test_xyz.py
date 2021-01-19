import datetime
import logging

from six.moves import queue

import grpc
import mock
from google.protobuf.timestamp_pb2 import Timestamp

from descarteslabs.common.proto.xyz import xyz_pb2
from descarteslabs.common.proto.logging import logging_pb2

from descarteslabs.workflows import _channel, client

from .. import XYZ, XYZLogListener
from ..published_graft import PublishedGraft
from ..utils import (
    pb_datetime_to_milliseconds,
    pb_milliseconds_to_datetime,
    py_log_level_to_proto_log_level,
)
from ..visualization import VizOption
from . import utils


def mock_CreateXYZ(msg: xyz_pb2.CreateXYZRequest, **kwargs) -> xyz_pb2.XYZ:
    assert isinstance(msg, xyz_pb2.CreateXYZRequest)
    expires_timestamp = Timestamp()
    expires_timestamp.FromJsonString("2003-01-02T04:05:06.789+00:00")
    return xyz_pb2.XYZ(
        id="mclovin",
        name=msg.name,
        description=msg.description,
        serialized_graft=msg.serialized_graft,
        typespec=msg.typespec,
        parameters=msg.parameters,
        public=msg.public,
        viz_options=msg.viz_options,
        expires_timestamp=expires_timestamp,
        channel=msg.channel,
        client_version=msg.client_version,
    )


@mock.patch(
    "descarteslabs.workflows.models.published_graft.get_global_grpc_client",
    new=lambda: utils.MockedClient(),
)
@mock.patch(
    "descarteslabs.workflows.models.xyz.get_global_grpc_client",
    new=lambda: utils.MockedClient(),
)
@mock.patch("descarteslabs.common.proto.xyz.xyz_pb2_grpc.XYZAPIStub")
class TestXYZ(object):
    def test_init(self, stub):
        CreateXYZ = stub.return_value.CreateXYZ
        CreateXYZ.side_effect = mock_CreateXYZ

        obj = utils.Bar(utils.Foo(1))
        name = "bar"
        desc = "a bar"
        public = True
        viz_options = [
            VizOption(
                id="viz1",
                bands=["red", "green", "blue"],
                scales=[[0, 0.4], [0, 0.4], [0, 0.4]],
            ),
        ]

        # do some hackery to pull out the `self._message` produced by the superclass's `__init__`
        super_message = None
        orig_init = PublishedGraft.__init__

        def patched_init(self, *args, **kwargs):
            "pull out `self._message` at the end of `PublishedGraft.__init__` so we can use it in tests"
            orig_init(self, *args, **kwargs)
            nonlocal super_message
            super_message = self._message

        with mock.patch.object(PublishedGraft, "__init__", patched_init):
            xyz = XYZ(
                obj,
                name=name,
                description=desc,
                viz_options=viz_options,
            )

        expected_req = xyz_pb2.CreateXYZRequest(
            name=name,
            description=desc,
            serialized_graft=super_message.serialized_graft,
            typespec=super_message.typespec,
            parameters=super_message.parameters,
            public=public,
            viz_options=[vp._message for vp in viz_options],
            channel=super_message.channel,
            client_version=super_message.client_version,
        )

        CreateXYZ.assert_called_once_with(
            expected_req,
            timeout=client.Client.DEFAULT_TIMEOUT,
            metadata=mock.ANY,
        )

        assert xyz._message == mock_CreateXYZ(expected_req)

        assert xyz.name == name
        assert xyz.description == desc
        assert xyz.expires_timestamp == datetime.datetime(2003, 1, 2, 4, 5, 6, 789000)
        assert xyz.viz_options == viz_options

    def test_get(self, stub):
        message = "foo"
        stub.return_value.GetXYZ.return_value = message

        with mock.patch.object(XYZ, "_from_proto") as _from_proto:
            XYZ.get("fake_id")
            _from_proto.assert_called_once()
            assert _from_proto.call_args[0][0] is message
            stub.return_value.GetXYZ.assert_called_once_with(
                xyz_pb2.GetXYZRequest(xyz_id="fake_id"),
                timeout=client.Client.DEFAULT_TIMEOUT,
                metadata=mock.ANY,
            )

    def test_properties(self, stub):
        stub.return_value.CreateXYZ.side_effect = mock_CreateXYZ
        obj = utils.Bar(utils.Foo(1))
        xyz = XYZ(obj, name="bar", description="a bar")

        assert xyz.object is obj
        assert xyz.type is type(obj)
        assert xyz.name == "bar"
        assert xyz.description == "a bar"
        assert xyz.channel == _channel.__channel__

        xyz._message.id = "1234"
        xyz._message.created_timestamp = 100
        xyz._message.updated_timestamp = 200

        assert xyz.id == "1234"
        assert xyz.created_timestamp == pb_milliseconds_to_datetime(100)
        assert xyz.updated_timestamp == pb_milliseconds_to_datetime(200)

    def test_iter_tile_logs(self, stub):
        stub.return_value.CreateXYZ.side_effect = mock_CreateXYZ

        start_datetime = datetime.datetime.now(datetime.timezone.utc)
        log_level = logging.WARNING
        session_id = "bar"
        logs = [
            xyz_pb2.XYZLogRecord(
                record=logging_pb2.LogRecord(message="foo"), session_id=session_id
            ),
            xyz_pb2.XYZLogRecord(
                record=logging_pb2.LogRecord(message="bar"), session_id=session_id
            ),
        ]

        xyz = XYZ(utils.Foo(1), name="foo", description="a foo")

        stub.return_value.GetXYZSessionLogs.return_value = logs

        assert (
            list(
                xyz.iter_tile_logs(
                    session_id=session_id,
                    start_datetime=start_datetime,
                    level=log_level,
                )
            )
            == logs
        )

        stub.return_value.GetXYZSessionLogs.assert_called_once_with(
            xyz_pb2.GetXYZSessionLogsRequest(
                session_id=session_id,
                start_timestamp=pb_datetime_to_milliseconds(start_datetime),
                xyz_id=xyz.id,
                level=py_log_level_to_proto_log_level(log_level),
            ),
            timeout=client.Client.STREAM_TIMEOUT,
            metadata=mock.ANY,
        )

    def test_url(self, stub):
        stub.return_value.CreateXYZ.side_effect = mock_CreateXYZ
        obj = utils.Foo(1)
        xyz = XYZ(obj)
        url_template = xyz._message.url_template = "http://base.net"
        assert xyz.url() == url_template

    def test_wmts_url(self, stub):
        stub.return_value.CreateXYZ.side_effect = mock_CreateXYZ
        obj = utils.Foo(1)
        xyz = XYZ(obj)
        wmts_url_template = (
            "http://base.net/wmts/xyz/mclovin/1.0.0/WMTSCapabilities.xml"
        )
        xyz._message.wmts_url_template = wmts_url_template

        assert xyz.wmts_url() == wmts_url_template
        assert (
            xyz.wmts_url(tile_matrix_sets="utm")
            == wmts_url_template + "?tile_matrix_sets=utm"
        )
        assert (
            xyz.wmts_url(tile_matrix_sets=["EPSG:4326", "EPSG:3857"])
            == wmts_url_template
            + "?tile_matrix_sets=EPSG%3A4326&tile_matrix_sets=EPSG%3A3857"
        )


@mock.patch("descarteslabs.workflows.models.xyz._tile_log_stream")
def test_xyz_log_listener(log_stream_mock):
    class FakeRendezvous(object):
        def __init__(self, q):
            self.q = q

        def __iter__(self):
            while True:
                msg = self.q.get()
                if msg != "cancel":
                    yield msg
                    self.q.task_done()
                else:
                    self.q.task_done()
                    raise grpc.RpcError

        def cancel(self):
            self.q.put("cancel")

    q = queue.Queue()
    rendezvous = FakeRendezvous(q)
    log_stream_mock.return_value = rendezvous

    listener = XYZLogListener("foobar")

    msgs = []
    listener.add_callback(lambda msg: msgs.append(msg))
    listener.listen("foobar")

    log_stream_mock.assert_called_once()

    # simulate incoming messages
    q.put("first")
    q.put("second")
    q.join()  # avoid possible race condition in test
    assert msgs == ["first", "second"]

    stopped = listener.stop(timeout=1)
    assert stopped
    assert not listener.running()
    assert len(msgs) == 2
