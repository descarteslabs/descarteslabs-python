import pytest
import mock

from .....common.proto.widgets import widgets_pb2

from .... import cereal
from ...identifier import parameter
from ...primitives import Str, Int
from ...containers import List
from .. import widget


@pytest.fixture
def StrWidget():
    "Fixture providing a fake Widget subclass for Str"
    with mock.patch.dict(widget.MSG_TO_WIDGET, clear=True):
        # Depending on the test environment, the actual `StringInput` widget from `interactive.widgets`
        # might already be imported and registerd to `widgets_pb2.StringInput`, so creating this subclass
        # would fail. So we clear out the widgets registry while this test is running.

        class StrWidget(widget.Widget, Str):
            _proto_type = widgets_pb2.StringInput
            __qualname__ = "StrWidget"

        yield StrWidget


class TestBaseClass:
    def test_subclass(self, StrWidget):

        assert widget.MSG_TO_WIDGET[StrWidget._proto_type] is StrWidget

    def test_bad_bases(self):
        with pytest.raises(AssertionError, match="must at least inherit from"):
            with mock.patch.dict(widget.MSG_TO_WIDGET, clear=True):

                class NotEnoughBases(widget.Widget):
                    _proto_type = widgets_pb2.StringInput

        with pytest.raises(
            AssertionError, match=r"Second-to-last base class of .+ must be Widget"
        ):
            with mock.patch.dict(widget.MSG_TO_WIDGET, clear=True):

                class WrongOrder(Str, widget.Widget):
                    _proto_type = widgets_pb2.StringInput

        with pytest.raises(
            AssertionError, match=r"Last base class of .+ must be a Proxytype"
        ):
            with mock.patch.dict(widget.MSG_TO_WIDGET, clear=True):

                class NonProxy(widget.Widget, dict):
                    _proto_type = widgets_pb2.StringInput

    def test_allow_non_final_inheritence(self):
        class NonFinalSubWidget(widget.Widget):
            def _to_proto_set_widget_msg(self, widget_msg):
                return "lolz"

        assert (
            NonFinalSubWidget._to_proto_set_widget_msg
            is not widget.Widget._to_proto_set_widget_msg
        )


class TestProxytypeBehavior:
    def test_init(self):
        name = "foo"
        default = "bar"
        instance = widget.Widget(name, default)

        # check it's a keyref graft to the given name
        assert instance.graft["returns"] == name
        # check it follows the rules of a proxytype parameter object
        assert instance.params == (instance,)
        assert instance._name == name

        assert instance._default == default
        assert instance._label == ""

    def test_promote_and_from_apply_gives_proxytype(self, StrWidget):

        promoted = StrWidget._promote("foo")
        assert type(promoted) is Str
        assert not isinstance(promoted, widget.Widget)

        applied = StrWidget._from_apply("some_func", 1, 2)
        assert type(applied) is Str
        assert not isinstance(applied, widget.Widget)

    def test_acts_like_proxytype(self, StrWidget):
        instance = StrWidget("name", "default")

        # not-at-all-exhaustive test---just making sure a few random methods are supported
        result = instance + "bar"
        assert type(result) is Str
        assert result.params == instance.params == (instance,)

        result = instance.join(["ab", "pq"])
        assert type(result) is Str
        assert result.params == instance.params == (instance,)

        result = instance.split("/")
        assert type(result) is List[Str]
        assert result.params == instance.params == (instance,)

    def test_repr(self, StrWidget):
        name = "foo"
        default = "bar"
        label = "baz"
        instance = StrWidget(name, default, label)

        assert (
            repr(instance)
            == f"StrWidget(name={name!r}, label={label!r}, default={default!r})"
        )


class TestSerialization:
    def test_param_to_proto_widget(self, StrWidget):
        name = "foo"
        default = "bar"
        label = "baz"
        instance = StrWidget(name, default, label)

        proto = widget.param_to_proto(instance)
        assert isinstance(proto, widgets_pb2.Parameter)
        assert proto.name == name
        assert proto.label == label
        assert proto.typespec == cereal.serialize_typespec(Str)

        which_oneof = proto.WhichOneof("widget")
        assert (
            which_oneof == widget.WIDGET_MSG_TYPE_TO_ONEOF_NAME[StrWidget._proto_type]
        )
        assert getattr(proto, which_oneof).default == default

    def test_param_to_proto_non_widget(self):
        name = "foo"
        param = parameter(name, List[Int])
        proto = widget.param_to_proto(param)

        assert isinstance(proto, widgets_pb2.Parameter)
        assert proto.name == name
        assert proto.label == ""
        assert proto.typespec == cereal.serialize_typespec(type(param))

        assert proto.WhichOneof("widget") is None

    def test_proto_to_param(self, StrWidget):
        name = "foo"
        default = "bar"
        label = "baz"

        proto = widgets_pb2.Parameter(
            name=name,
            label=label,
            typespec=cereal.serialize_typespec(Str),
        )
        sub_msg = getattr(
            proto, widget.WIDGET_MSG_TYPE_TO_ONEOF_NAME[StrWidget._proto_type]
        )
        sub_msg.default = default

        param = widget.proto_to_param(proto)
        assert type(param) is StrWidget
        assert param._name == name
        assert param._label == label
        assert param._default == default

        assert param.graft["returns"] == name
        assert param.params == (param,)

    def test_proto_to_param_non_widget(self):
        name = "foo"
        type_ = List[Int]

        proto = widgets_pb2.Parameter(
            name=name,
            typespec=cereal.serialize_typespec(type_),
        )
        param = widget.proto_to_param(proto)

        assert type(param) is type_
        assert param._name == name

        assert param.params == (param,)
        assert param.graft["returns"] == name

    def test_roundtrip_widget(self, StrWidget):
        name = "foo"
        default = "bar"
        label = "baz"
        instance = StrWidget(name, default, label)

        rehydrated = widget.proto_to_param(widget.param_to_proto(instance))

        assert type(rehydrated) is type(instance)
        assert rehydrated._name == instance._name
        assert rehydrated._label == instance._label
        assert rehydrated._default == instance._default

        assert rehydrated.graft == instance.graft
        assert rehydrated.params == (rehydrated,)

    def test_roundtrip_non_widget(self):
        instance = parameter("foo", List[Int])
        rehydrated = widget.proto_to_param(widget.param_to_proto(instance))

        assert type(rehydrated) is type(instance)
        assert rehydrated._name == instance._name

        assert rehydrated.graft == instance.graft
        assert rehydrated.params == (rehydrated,)
