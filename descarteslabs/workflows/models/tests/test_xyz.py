import datetime
import json
import logging

from six.moves import queue

import grpc
import mock
import pytest

from descarteslabs.client.version import __version__
from descarteslabs.common.proto.xyz import xyz_pb2
from descarteslabs.common.proto.logging import logging_pb2

from ... import _channel, types, cereal
from ...client import Client
from .. import XYZ, XYZLogListener
from ..utils import (
    pb_datetime_to_milliseconds,
    pb_milliseconds_to_datetime,
    py_log_level_to_proto_log_level,
)
from . import utils


@mock.patch(
    "descarteslabs.workflows.models.xyz.get_global_grpc_client",
    new=lambda: utils.MockedClient(),
)
@mock.patch("descarteslabs.common.proto.xyz.xyz_pb2_grpc.XYZAPIStub")
class TestXYZ(object):
    def test_build(self, stub):
        obj = utils.Foo(1)
        xyz = XYZ.build(obj, name="foo", description="a foo")

        assert xyz._object is obj
        assert xyz._params == ()
        assert xyz._client is not None
        message = xyz._message

        assert json.loads(message.serialized_graft) == obj.graft
        assert message.name == "foo"
        assert message.description == "a foo"
        assert message.channel == _channel.__channel__
        assert message.client_version == __version__

    def test_build_params(self, stub):
        x = types.parameter("x", types.Image)
        obj = utils.Foo(x)
        xyz = XYZ.build(obj)
        msg = xyz._message

        obj_func = xyz.object
        assert isinstance(xyz.object, types.Function[type(x), {}, type(obj)])

        assert json.loads(msg.serialized_graft) == obj_func.graft
        assert msg.typespec == cereal.serialize_typespec(type(obj_func))

        assert xyz.params is obj.params

        params = types.widget.deserialize_params(msg.parameters)
        assert len(params) == 1
        param = params[0]
        assert isinstance(param, type(x))
        assert param._name == x._name

    def test_roundtrip_from_proto(self, stub):
        obj = utils.Bar(utils.Foo(1))
        xyz = XYZ.build(obj, name="bar", description="a bar")
        message = xyz._message

        xyz_from_proto = XYZ._from_proto(message)

        new_obj = xyz_from_proto.object
        assert type(new_obj) == type(obj)
        utils.assert_graft_is_scope_isolated_equvalent(new_obj.graft, obj.graft)

    def test_get(self, stub):
        message = "foo"
        stub.return_value.GetXYZ.return_value = message

        with mock.patch.object(XYZ, "_from_proto") as _from_proto:
            XYZ.get("fake_id")
            _from_proto.assert_called_once()
            assert _from_proto.call_args[0][0] is message
            stub.return_value.GetXYZ.assert_called_once_with(
                xyz_pb2.GetXYZRequest(xyz_id="fake_id"),
                timeout=Client.DEFAULT_TIMEOUT,
                metadata=mock.ANY,
            )

    def test_save(self, stub):
        new_message = "fake message"
        stub.return_value.CreateXYZ.return_value = new_message

        obj = utils.Bar(utils.Foo(1))
        xyz = XYZ.build(obj, name="bar", description="a bar")
        old_message = xyz._message

        xyz.save()
        assert xyz._message is new_message
        stub.return_value.CreateXYZ.assert_called_once_with(
            xyz_pb2.CreateXYZRequest(
                name=old_message.name,
                description=old_message.description,
                serialized_graft=old_message.serialized_graft,
                typespec=old_message.typespec,
                channel=_channel.__channel__,
                client_version=__version__,
            ),
            timeout=Client.DEFAULT_TIMEOUT,
            metadata=mock.ANY,
        )

    def test_properties(self, stub):
        obj = utils.Bar(utils.Foo(1))
        xyz = XYZ.build(obj, name="bar", description="a bar")

        assert xyz.object == obj
        assert xyz.type == type(obj)
        assert xyz.id is None
        assert xyz.created_timestamp is None
        assert xyz.updated_timestamp is None
        assert xyz.name == "bar"
        assert xyz.description == "a bar"
        assert xyz.channel == _channel.__channel__

        xyz._message.id = "1234"
        xyz._message.created_timestamp = 100
        xyz._message.updated_timestamp = 200

        assert xyz.id == "1234"
        assert xyz.created_timestamp == pb_milliseconds_to_datetime(100)
        assert xyz.updated_timestamp == pb_milliseconds_to_datetime(200)

    def test_incompatible_channel(self, stub):
        obj = utils.Foo(1)
        xyz = XYZ.build(obj, name="foo", description="a foo")
        xyz._message.channel = "foobar"

        with pytest.raises(ValueError, match="only defined for channel 'foobar'"):
            xyz.object

    def test_iter_tile_logs(self, stub):
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

        xyz = XYZ.build(utils.Foo(1), name="foo", description="a foo")

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
            timeout=Client.STREAM_TIMEOUT,
            metadata=mock.ANY,
        )

    def test_from_proto(self, stub):
        message = XYZ.build(utils.Foo(1))._message
        message.id = "foo"
        message.serialized_graft = ""

        with pytest.raises(
            AttributeError,
            match=(
                r"^The serialized .* XYZ 'foo'\. To share objects with others, please"
                r" use a Workflow instead\.$"
            ),
        ):
            XYZ._from_proto(message)

    def test_url(self, stub):
        obj = utils.Foo(1)
        xyz = XYZ.build(obj)

        with pytest.raises(ValueError, match="has not been persisted"):
            xyz.url()

        url_template = xyz._message.url_template = "http://base.net"

        assert xyz.url() == url_template


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
