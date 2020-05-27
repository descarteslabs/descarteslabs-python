import datetime
import json
from urllib.parse import urlencode, parse_qs

from six.moves import queue

import grpc
import mock
import pytest
from descarteslabs.common.proto.xyz import xyz_pb2

from ... import _channel
from ...client import Client
from .. import XYZ, XYZErrorListener
from ..utils import pb_datetime_to_milliseconds, pb_milliseconds_to_datetime
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
        assert xyz._client is not None
        message = xyz._message

        assert json.loads(message.serialized_graft) == utils.json_normalize(obj.graft)
        assert message.name == "foo"
        assert message.description == "a foo"
        assert message.channel == _channel.__channel__

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
                metadata=(("x-wf-channel", _channel.__channel__),),
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
            ),
            timeout=Client.DEFAULT_TIMEOUT,
            metadata=(("x-wf-channel", _channel.__channel__),),
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

    def test_iter_tile_errors(self, stub):
        start_datetime = datetime.datetime.now(datetime.timezone.utc)
        session_id = "bar"
        errors = [
            xyz_pb2.XYZError(session_id=session_id),
            xyz_pb2.XYZError(session_id=session_id),
        ]

        xyz = XYZ.build(utils.Foo(1), name="foo", description="a foo")

        stub.return_value.GetXYZSessionErrors.return_value = errors

        assert (
            list(
                xyz.iter_tile_errors(
                    session_id=session_id, start_datetime=start_datetime
                )
            )
            == errors
        )

        stub.return_value.GetXYZSessionErrors.assert_called_once_with(
            xyz_pb2.GetXYZSessionErrorsRequest(
                session_id=session_id,
                start_timestamp=pb_datetime_to_milliseconds(start_datetime),
                xyz_id=xyz.id,
            ),
            timeout=Client.STREAM_TIMEOUT,
            metadata=(("x-wf-channel", _channel.__channel__),),
        )

    def test_from_proto(self, stub):
        message = XYZ.build(utils.Foo(1))._message
        message.id = "foo"
        message.serialized_graft = ""

        with pytest.raises(
            AttributeError,
            match=r"^The serialized .* XYZ 'foo'\. To share objects with others, please use a Workflow instead\.$",
        ):
            XYZ._from_proto(message)

    def test_url(self, stub):
        obj = utils.Foo(1)
        xyz = XYZ.build(obj)

        with pytest.raises(ValueError, match="has not been persisted"):
            xyz.url()

        xyz._message.id = "baz"
        xyz._message.channel = "v0-0"

        url_base = "{}/v0-0/xyz/baz/{{z}}/{{x}}/{{y}}.png".format(xyz.BASE_URL)
        url_base_q = url_base + "?"

        assert xyz.url() == url_base
        assert xyz.url(session_id="foo") == url_base_q + urlencode(
            {"session_id": "foo"}
        )
        assert xyz.url(colormap="foo") == url_base_q + urlencode({"colormap": "foo"})
        assert xyz.url(checkerboard=True) == url_base_q + urlencode(
            {"checkerboard": "true"}
        )
        assert xyz.url(checkerboard=False) == url_base
        # 1-band scales are normalized
        assert xyz.url(scales=[0, 1]) == url_base_q + urlencode(
            {"scales": "[[0.0, 1.0]]"}
        )
        # If all none scales, not included
        assert xyz.url(scales=[None, None]) == url_base

        # Primitives are inserted directly and JSON-encoded
        assert xyz.url(foo=1) == url_base_q + urlencode({"foo": "1"})
        assert xyz.url(bar=True) == url_base_q + urlencode({"bar": "true"})
        assert xyz.url(baz="quz") == url_base_q + urlencode({"baz": '"quz"'})
        # Grafts are JSON-encoded (along with embedded JSON in grafts)
        assert xyz.url(foo=obj) == url_base_q + urlencode(
            {"foo": json.dumps(obj.graft)}
        )

        # test everything gets added together correctly
        base, params = xyz.url(session_id="foo", arg="bar", foo=2.2, obj=obj).split("?")
        assert base == url_base
        query = parse_qs(params, strict_parsing=True, keep_blank_values=True)
        assert query == {
            # `parse_qs` returns all values wrapped in lists
            "session_id": ["foo"],
            "arg": ['"bar"'],
            "foo": ["2.2"],
            "obj": [json.dumps(obj.graft)],
        }

    def test_validate_scales(self, stub):
        assert XYZ._validate_scales([[0.0, 1.0], [0.0, 2.0], [-1.0, 1.0]]) == [
            [0.0, 1.0],
            [0.0, 2.0],
            [-1.0, 1.0],
        ]
        assert XYZ._validate_scales([[0.0, 1.0]]) == [[0.0, 1.0]]
        # ints -> floats
        assert XYZ._validate_scales([[0, 1]]) == [[0.0, 1.0]]
        # 1-band convenience
        assert XYZ._validate_scales([0, 1]) == [[0.0, 1.0]]
        # no scalings
        assert XYZ._validate_scales(None) == []
        assert XYZ._validate_scales([]) == []

        with pytest.raises(TypeError, match="Expected a list or tuple of scales"):
            XYZ._validate_scales(0)
        with pytest.raises(TypeError, match="Expected a list or tuple of scales"):
            XYZ._validate_scales("foo")
        with pytest.raises(
            TypeError, match="Scaling 0: expected a 2-item list or tuple"
        ):
            XYZ._validate_scales([1, 2, 3])
        with pytest.raises(
            TypeError, match="Scaling 0: items in scaling must be numbers"
        ):
            XYZ._validate_scales([1, "foo"])
        with pytest.raises(ValueError, match="expected 0, 1, or 3 scales, but got 2"):
            XYZ._validate_scales([[0.0, 1.0], [0.0, 1.0]])
        with pytest.raises(ValueError, match="expected 0, 1, or 3 scales, but got 4"):
            XYZ._validate_scales([[0.0, 1.0], [0.0, 1.0], [0.0, 1.0], [0.0, 1.0]])
        with pytest.raises(ValueError, match="but length was 3"):
            XYZ._validate_scales([[0.0, 1.0, 2.0]])
        with pytest.raises(ValueError, match="but length was 1"):
            XYZ._validate_scales([[0.0]])
        with pytest.raises(ValueError, match="one number and one None in scaling"):
            XYZ._validate_scales([[None, 1.0]])


@mock.patch("descarteslabs.workflows.models.xyz._tile_error_stream")
def test_xyz_error_listener(error_stream_mock):
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
    error_stream_mock.return_value = rendezvous

    listener = XYZErrorListener("foobar")

    msgs = []
    listener.add_callback(lambda msg: msgs.append(msg))
    listener.listen("foobar")

    error_stream_mock.assert_called_once()

    # simulate incoming messages
    q.put("first")
    q.put("second")
    q.join()  # avoid possible race condition in test
    assert msgs == ["first", "second"]

    stopped = listener.stop(timeout=1)
    assert stopped
    assert not listener.running()
    assert len(msgs) == 2
