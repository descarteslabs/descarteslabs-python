import datetime
import json

import mock
import pytest
from descarteslabs.common.proto import xyz_pb2

from ... import _channel
from ...client import Client
from .. import XYZ
from ..utils import pb_milliseconds_to_datetime, pb_datetime_to_milliseconds
from . import utils


@mock.patch("descarteslabs.common.proto.xyz_pb2_grpc.XYZAPIStub")
class TestXYZ(object):
    def test_build(self, stub):
        obj = utils.Foo(1)
        xyz = XYZ.build(obj, name="foo", description="a foo")

        assert xyz._object is obj
        assert xyz._client is not None
        message = xyz._message

        assert json.loads(message.serialized_graft) == utils.json_normalize(obj.graft)
        assert "Foo" in message.serialized_typespec
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
        assert new_obj.graft == utils.json_normalize(obj.graft)

    def test_get(self, stub):
        message = "foo"
        stub.return_value.GetXYZ.return_value = message

        with mock.patch.object(XYZ, "_from_proto") as _from_proto:
            XYZ.get("fake_id")
            _from_proto.assert_called_once()
            assert _from_proto.call_args[0][0] is message
            stub.return_value.GetXYZ.assert_called_once_with(
                xyz_pb2.GetXYZRequest(xyz_id="fake_id"), timeout=Client.DEFAULT_TIMEOUT
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
            xyz_pb2.CreateXYZRequest(xyz=old_message), timeout=Client.DEFAULT_TIMEOUT
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
        start_datetime = datetime.datetime.utcnow()
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
            )
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

        assert xyz.url() == url_base
        assert xyz.url("foo") == url_base + "?session_id=foo"
        assert xyz.url(arg="bar") == url_base + "?arg=bar"

        # ugh nondeterministic py2 dict order
        base, params = xyz.url("foo", arg="bar").split("?")
        assert base == url_base
        assert set(params.split("&")) == {"session_id=foo", "arg=bar"}
