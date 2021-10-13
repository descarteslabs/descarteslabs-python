import mock
import pytest
import json
from typing import Tuple

from descarteslabs.common.graft import client as graft_client

from descarteslabs.common.proto.typespec import typespec_pb2
from descarteslabs.common.proto.widgets import widgets_pb2

from descarteslabs.client.version import __version__
from descarteslabs.workflows.cereal import deserialize_typespec
from descarteslabs.workflows.types import Int, List, Function, parameter

from ..published_graft import PublishedGraft
from . import utils


class FakeProtoMessage:
    def __init__(
        self,
        serialized_graft: str,
        channel: str,
        client_version: str,
        typespec: typespec_pb2.Typespec,
        parameters: Tuple[widgets_pb2.Parameter],
    ):
        self.serialized_graft = serialized_graft
        self.channel = channel
        self.client_version = client_version
        self.typespec = typespec
        self.parameters = parameters


class SubPublished(PublishedGraft, message_type=FakeProtoMessage):
    pass


def test_init_subclass():
    assert SubPublished._message_type is FakeProtoMessage


@mock.patch(
    "descarteslabs.workflows.models.published_graft.get_global_grpc_client",
)
class TestInit:
    @pytest.mark.parametrize("client", [utils.MockedClient(), None])
    def test_basic(self, mock_gggc, client):
        obj = Int(42)
        channel = "foo"

        if client is None:
            mock_gggc.return_value._wf_channel = channel
        else:
            client._wf_channel = channel

        pub = SubPublished(obj, client=client)

        if client is None:
            mock_gggc.assert_called_once()
            assert pub._client is mock_gggc.return_value
        else:
            assert pub._client is client
            mock_gggc.assert_not_called()

        msg = pub._message
        assert isinstance(msg, SubPublished._message_type)
        assert msg.serialized_graft == json.dumps(obj.graft)
        assert deserialize_typespec(msg.typespec) is type(obj)

        assert pub.object is obj
        assert pub.params == ()
        assert pub.channel == channel
        assert pub.client_version == __version__

    def test_proxifies(self, mock_gggc):
        obj = [1, 2, Int(3)]
        pub = SubPublished(obj)
        assert isinstance(pub.object, List[Int])

    def test_to_func(self, mock_gggc):
        p1 = parameter("foo", Int)
        obj = p1 + 2.2

        pub = SubPublished(obj)

        assert isinstance(pub.object, Function[type(p1), {}, type(obj)])
        func_graft = pub.object.graft

        pub._object = None
        new_obj = pub.object
        assert new_obj is not obj  # got reconstructed
        assert isinstance(new_obj, Function[type(p1), {}, type(obj)])
        assert new_obj.graft == func_graft

        assert pub.params is obj.params

        pub._params = None
        params = pub.params

        assert params is not obj.params  # got reconstructed
        assert len(params) == len(obj.params)
        for orig_p, new_p in zip(params, obj.params):
            assert type(new_p) is type(orig_p)
            assert new_p._name == orig_p._name

    @pytest.mark.parametrize(
        "func, names",
        [
            (
                Function[dict(x=Int, y=List[Int]), Int](lambda x, y: x + y[0]),
                ["x", "y"],
            ),
            (Function[Int, List[Int], {}, Int](lambda x, y: x + y[0]), ["x", "y"]),
            (Function[Int, List[Int], {}, Int]("foo"), ["implicit0", "implicit1"]),
        ],
    )
    def test_func_params_generated(self, mock_gggc, func, names):
        pub = SubPublished(func)

        assert pub.object is func
        assert [p._name for p in pub.params] == names
        assert tuple(type(p) for p in pub.params) == func.all_arg_types

        # test proto set correctly by forcing reconstruction
        pub._object = None
        pub._params = None

        with graft_client.consistent_guid():
            isolated = graft_client.isolate_keys(func.graft)
        with graft_client.consistent_guid():
            pub.object

        assert pub.object.graft == isolated
        assert pub.type is type(func)

        assert [p._name for p in pub.params] == names
        assert tuple(type(p) for p in pub.params) == func.all_arg_types


@mock.patch(
    "descarteslabs.workflows.models.published_graft.get_global_grpc_client",
)
@pytest.mark.parametrize("client", [utils.MockedClient(), None])
def test_from_proto(mock_gggc, client):
    class SubPublished(PublishedGraft, message_type=FakeProtoMessage):
        def __init__(self):
            assert False, "init should not be called"

    fake_message = object()
    from_proto = SubPublished._from_proto(fake_message, client=client)

    assert from_proto._message is fake_message
    assert from_proto._object is None
    assert from_proto._params is None

    if client is None:
        mock_gggc.assert_called_once()
        assert from_proto._client is mock_gggc.return_value
    else:
        assert from_proto._client is client
        mock_gggc.assert_not_called()


def test_object_roundtrip_from_proto():
    obj = Int(42)
    pub_orig = SubPublished(obj, client=mock.Mock())
    pub_new = SubPublished._from_proto(pub_orig._message, client=pub_orig._client)

    assert pub_new._object is None
    new_obj = pub_new.object
    assert pub_new._object is new_obj
    assert pub_new._object is new_obj  # doesn't change the second time

    assert type(new_obj) is type(obj)
    utils.assert_graft_is_scope_isolated_equvalent(new_obj.graft, obj.graft)
    assert new_obj.params == ()


def test_object_bad_channel():
    client = utils.MockedClient()
    client._wf_channel = "foo"
    pub = SubPublished(Int(42), client=client)

    pub._message.channel = "bar"

    with pytest.raises(
        ValueError,
        match="compatible with channel 'foo', but the SubPublished is only defined for channel 'bar'",
    ):
        pub.object
