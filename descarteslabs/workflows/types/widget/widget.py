import sys
from typing import List, Iterable, ClassVar, Type, TYPE_CHECKING

from google.protobuf import message

from descarteslabs.common.proto.widgets import widgets_pb2
from descarteslabs.common.graft import client

from ...cereal import serialize_typespec, deserialize_typespec
from ..core import Proxytype
from ..identifier import parameter

if TYPE_CHECKING:
    import ipywidgets


MSG_TO_WIDGET = {}

# OMG WTF proto. how do you figure out which oneof name corresponds to the type you want?
msg = widgets_pb2.Parameter()
WIDGET_MSG_TYPE_TO_ONEOF_NAME = {
    type(getattr(msg, oneof.name)): oneof.name
    for oneof in msg.DESCRIPTOR.oneofs_by_name["widget"].fields
}
del msg


class Widget:
    # Mixin class to create a widget parameter: class that acts like a Proxytype, but also like a widget
    # Subclasses must inherit like::
    #     class SubWidget(Widget, ImageCollection):
    #         _proto_type = widgets_pb2.SubWidgetMessage
    # Subclasses may want to override `_to_proto_set_widget_msg` and `_from_proto_init_from_widget_msg`.

    # NOTE: we don't set an actual docstring here, so subclasses will inherit whatever docstring
    # their base Proxytype has.

    _proto_type: ClassVar[Type[message.Message]]
    widget: "ipywidgets.Widget"
    "The ipywidgets Widget instance used to display this widget"

    def __init_subclass__(cls, **kwargs):
        "If the subclass has the _proto_type variable set, validate its inheritance and register it for that proto type"
        super().__init_subclass__(**kwargs)

        proto_type = getattr(cls, "_proto_type", None)
        if proto_type is not None:
            bases = cls.__bases__
            assert (
                len(bases) >= 2
            ), f"Widget subclass {cls} must at least inherit from (Widget, Proxytype), not {bases}"
            assert issubclass(
                bases[-2], Widget
            ), f"Second-to-last base class of {cls} must be Widget, not {bases[0]}"
            assert issubclass(
                bases[-1], Proxytype
            ), f"Last base class of {cls} must be a Proxytype, not {bases[1]}"

            prev = MSG_TO_WIDGET.setdefault(proto_type, cls)
            if prev is not cls:
                raise ValueError(
                    f"{cls}: message type {proto_type} is already registered to Widget type {prev}"
                )

    def __init__(self, name, default, label=""):
        self.graft = client.keyref_graft(name)
        self.params = (self,)
        self._name = name

        self._default = default
        self._label = label

    @property
    def value(self):
        "The current value of the widget. Shorthand for ``self.widget.value``."
        return self.widget.value

    @value.setter
    def value(self, v):
        self.widget.value = v

    @property
    def observe(self):
        """
        Register a handler function to be called when the widget's value changes. Shorthand for ``self.widget.observe``.

        See the `traitlets docs <https://traitlets.readthedocs.io/en/stable/api.html#traitlets.HasTraits.observe>`_
        for more information.
        """
        return self.widget.observe

    def _to_proto(self) -> widgets_pb2.Parameter:
        typespec = serialize_typespec(type(self).__bases__[-1])
        msg = widgets_pb2.Parameter(
            name=self._name, label=self._label, typespec=typespec
        )

        widget_msg = getattr(msg, WIDGET_MSG_TYPE_TO_ONEOF_NAME[self._proto_type])
        widget_msg.SetInParent()
        # ^ ensure `widget_msg` is registered as WhichOneof, even if it's empty.
        # https://stackoverflow.com/a/60022948/10519953
        self._to_proto_set_widget_msg(widget_msg)

        return msg

    @property
    def _proxytype(self):
        return type(self).__bases__[-1]

    def _to_proto_set_widget_msg(self, widget_msg: message.Message):
        # Override in subclasses as necessary
        for field in self._proto_type.DESCRIPTOR.fields_by_name:
            val = getattr(self, "_" + field)
            setattr(widget_msg, field, val)

    @classmethod
    def _from_proto(cls, msg: widgets_pb2.Parameter):
        # TODO should we check that the typespec isn't a mismatch?

        oneof_name = msg.WhichOneof("widget")
        expected_oneof_name = WIDGET_MSG_TYPE_TO_ONEOF_NAME[cls._proto_type]
        assert (
            expected_oneof_name == oneof_name
        ), f"Expected Parameter message with {expected_oneof_name!r} set for widget {cls!r}, not {oneof_name!r}"

        widget_msg = getattr(msg, oneof_name)
        return cls._from_proto_init_from_widget_msg(msg.name, msg.label, widget_msg)

    @classmethod
    def _from_proto_init_from_widget_msg(cls, name: str, label: str, widget_msg):
        # Override in subclasses as necessary
        values = {
            field: getattr(widget_msg, field)
            for field in cls._proto_type.DESCRIPTOR.fields_by_name
        }
        return cls(name=name, label=label, **values)

    @classmethod
    def _from_apply(cls, function, *args, **kwargs):
        # don't propagate the Widget type
        return cls.__bases__[-1]._from_apply(function, *args, **kwargs)

    @classmethod
    def _promote(cls, obj):
        # don't propagate the Widget type
        return cls.__bases__[-1]._promote(obj)

    def _ipython_display_(self):
        try:
            self.widget._ipython_display_()
        except AttributeError:
            pass

    def __repr__(self):
        args = [f"name={self._name!r}", f"label={self._label!r}"] + [
            f"{field}={getattr(self, '_' + field)!r}"
            for field in self._proto_type.DESCRIPTOR.fields_by_name
        ]

        type_ = type(self)
        return f"{type_.__qualname__}({', '.join(args)})"


# Only add a docstring for sphinx; you'd rather have shift-tab help give you the docstring
# for the Proxytype it's inheriting from.
if "sphinx" in sys.modules:
    Widget.__doc__ = """
Base class for all Workflows widgets.

Workflows widgets act just like normal Workflows objects, such as `.Int`, `.Str`, or `~.geospatial.ImageCollection`.

They just have a few extra fields you can use to interact with the current value of the widget.
"""


def param_to_proto(param) -> widgets_pb2.Parameter:
    try:
        return param._to_proto()
    except AttributeError:
        return widgets_pb2.Parameter(
            name=param._name,
            label=getattr(param, "_label", ""),
            typespec=serialize_typespec(type(param)),
        )


def proto_to_param(msg: widgets_pb2.Parameter):
    widget_field = msg.WhichOneof("widget")
    if widget_field is not None:
        try:
            type_ = MSG_TO_WIDGET[type(getattr(msg, widget_field))]
        except KeyError:
            pass
        else:
            return type_._from_proto(msg)

    return parameter(msg.name, deserialize_typespec(msg.typespec))


def serialize_params(params: Iterable[Proxytype]) -> List[widgets_pb2.Parameter]:
    return [param_to_proto(p) for p in params]


def deserialize_params(msgs: List[widgets_pb2.Parameter]):
    return tuple(proto_to_param(m) for m in msgs)
