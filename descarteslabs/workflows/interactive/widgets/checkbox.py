from ._ipywidgets import ipywidgets

from descarteslabs.common.proto.widgets import widgets_pb2

from ...types import Bool
from ...types.widget import Widget


class Checkbox(Widget, Bool):
    _proto_type = widgets_pb2.Checkbox

    def __init__(self, name: str, default: bool = False, label=""):
        super().__init__(name, default, label)
        self.widget = ipywidgets.Checkbox(value=default)
        self.widget._label = label


def checkbox(
    name: str,
    default: bool = False,
    label: str = "",
):
    """
    A checkbox widget, which acts as a `.Bool` parameter.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.widgets.checkbox("param_name", default=True, label="A string parameter")  # doctest: +SKIP

    >>> s2 = wf.ImageCollection.from_id(
    ...     "sentinel-2:L1C", "2018-01-01", "2018-04-01",
    ...     processing_level=wf.ifelse(
    ...         wf.widgets.checkbox("surface", default=True, label="Use surface reflectance"),
    ...         wf.Str("surface"),
    ...         wf.Str("toa"),
    ...     )
    ... ).pick_bands("red green blue")
    >>> s2.visualize("Sentinel-2", scales=[[0, 0.4], [0, 0.4], [0, 0.4]])  # doctest: +SKIP
    >>> # ^ when you call .visualize, the `checkbox` widget will automatically show up below

    Clicking the checkbox above will toggle atmospheric correction on and off.
    (If you haven't already, run ``wf.map`` in another notebook cell to see your layer.)

    Parameters
    ----------
    name: str
        The name of the parameter.
    default: bool, default False
        The default value for the widget.
    label: str, default ""
        The longform label to display next to the widget.
        If not given, the widget will display as ``name``.

    Returns
    -------
    widget: Checkbox
        A Widget object that acts just like a Workflows `.Bool`, and displays as a checkbox.
    """

    return Checkbox(name, default, label)
