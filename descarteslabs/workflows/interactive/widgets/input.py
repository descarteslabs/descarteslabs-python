from typing import TypeVar, Generic, T, ClassVar

from ._ipywidgets import ipywidgets

from descarteslabs.common.proto.widgets import widgets_pb2

from ...types import Int, Float, Str
from ...types.widget import Widget


class Input(Widget, Generic[T]):
    _widget_type: ClassVar[ipywidgets.DOMWidget]

    def __init__(self, name: str, default: T, label=""):
        super().__init__(name, default, label)
        self.widget = self._widget_type(value=default, continuous_update=False)
        self.widget._label = label


class StringInput(Input[str], Str):
    _proto_type = widgets_pb2.StringInput
    _widget_type = ipywidgets.Text


class IntInput(Input[int], Int):
    _proto_type = widgets_pb2.IntInput
    _widget_type = ipywidgets.IntText


class FloatInput(Input[float], Float):
    _proto_type = widgets_pb2.FloatInput
    _widget_type = ipywidgets.FloatText


TYPES = {str: StringInput, int: IntInput, float: FloatInput}
PrimitiveType = TypeVar("Primitive", *TYPES)


def input(
    name: str,
    default: PrimitiveType,
    label: str = "",
):
    """
    A box to type `.Str`, `.Int`, or `.Float` values into.

    Depending on the type of value you pass for ``default``, the widget will represent
    a `.Str`, an `.Int`, or a `.Float` parameter.

    Note: updates happen when you press Enter or click out of the box, not on every keystroke.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.widgets.input("param_name", default="hello", label="A string parameter")  # doctest: +SKIP
    >>> s2 = wf.ImageCollection.from_id("sentinel-2:L1C", "2018-01-01", "2018-04-01").pick_bands("red green blue")
    >>> img = s2[wf.widgets.input("index", default=0)]
    >>> img.visualize("One S2 image", scales=[[0, 0.4], [0, 0.4], [0, 0.4]])  # doctest: +SKIP
    >>> # ^ when you call .visualize, the `input` widget will automatically show up below

    Parameters
    ----------
    name: str
        The name of the parameter.
    default: str, int, float
        The default value for the widget.
    label: str, default ""
        The longform label to display next to the widget.
        If not given, the widget will display as ``name``.

    Returns
    -------
    widget: Union[StringInput, IntInput, FloatInput]
        A Widget object that acts just like a Workflows `.Str`, `.Int`, or `.Float` and displays as an input box.

    Notes
    -----
    Typing a different number into the input box above will cause the map to update
    and display the `~.geospatial.Image` at that index in the `~.geospatial.ImageCollection`.
    (If you haven't already, run ``wf.map`` in another notebook cell to see your layer.)
    """
    try:
        widget_cls = TYPES[type(default)]
    except KeyError:
        raise TypeError(
            f"Only primitive Python values {tuple(TYPES)} can be used as a default for `input`, "
            f"not {default!r}."
        ) from None

    return widget_cls(name, default, label=label)
