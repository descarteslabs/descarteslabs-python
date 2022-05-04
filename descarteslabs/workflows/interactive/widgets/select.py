from typing import TypeVar, Sequence, Optional, Generic, T

from ._ipywidgets import ipywidgets

from ....common.proto.widgets import widgets_pb2

from ...types import Int, Float, Str
from ...types.widget import Widget


FORMAT_TO_WIDGET = {
    "dropdown": ipywidgets.Dropdown,
    "radio": ipywidgets.RadioButtons,
    "slider": ipywidgets.SelectionSlider,
}


class Select(Widget, Generic[T]):
    def __init__(
        self, name: str, options: Sequence[T], format: str, default: T, label=""
    ):
        super().__init__(name, default, label)

        if len(options) == 0:
            raise ValueError("Must give at least one option")

        format = format.lower()
        self._options = options
        self._format = format

        try:
            widget_type = FORMAT_TO_WIDGET[format]
        except KeyError:
            raise ValueError(
                f"Unsupported format {format!r}. Must be one of: {tuple(FORMAT_TO_WIDGET)}."
            ) from None

        self.widget = widget_type(options=options, value=default)
        self.widget._label = label

    def _to_proto_set_widget_msg(self, widget_msg: widgets_pb2.StringSelect):
        widget_msg.default = self._default
        widget_msg.format = widgets_pb2.SelectFormat.Value(self._format.upper())
        widget_msg.options[:] = self._options

    @classmethod
    def _from_proto_init_from_widget_msg(
        cls, name: str, label: str, widget_msg: widgets_pb2.StringSelect
    ):
        return cls(
            name=name,
            options=list(widget_msg.options),
            format=widgets_pb2.SelectFormat.Name(widget_msg.format).lower(),
            default=widget_msg.default,
            label=label,
        )


class StringSelect(Select[str], Str):
    _proto_type = widgets_pb2.StringSelect


class IntSelect(Select[int], Int):
    _proto_type = widgets_pb2.IntSelect


class FloatSelect(Select[float], Float):
    _proto_type = widgets_pb2.FloatSelect


TYPES = {str: StringSelect, int: IntSelect, float: FloatSelect}
PrimitiveType = TypeVar("Primitive", *TYPES)


def select(
    name: str,
    options: Sequence[PrimitiveType],
    format: str = "dropdown",
    default: Optional[PrimitiveType] = None,
    label: str = "",
):
    """
    A widget (dropdown, radio buttons, etc.) to select a value from a list of options.

    Depending on the types of the values you pass for ``options``, the widget will act as
    a `.Str`, an `.Int`, or a `.Float` parameter.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.widgets.select(
    ...     "param_name",
    ...     ["one", "two", "three"],
    ...     format="radio",
    ...     default="two",
    ...     label="A select parameter",
    ... )  # doctest: +SKIP

    >>> s2 = wf.ImageCollection.from_id("sentinel-2:L1C", "2018-01-01", "2018-04-01")
    >>> s2_bands = s2.pick_bands(
    ...     wf.widgets.select(
    ...         "band_combo",
    ...         ["red green blue", "nir red green", "swir1 nir blue"],
    ...         format="radio",
    ...         label="Band combination"
    ...     )
    ... )
    >>> s2_bands.visualize("Sentinel-2", scales=[[0, 0.4], [0, 0.4], [0, 0.4]])  # doctest: +SKIP
    >>> # ^ when you call .visualize, the `select` widget will automatically show up below

    Selecting a band combination from the radio buttons will update the map.
    (If you haven't already, run ``wf.map`` in another notebook cell to see your layer.)

    You may want to display user-friendly terms for your options, rather than the actual values.
    A typical way to do this is to make a `.Dict` mapping the user-friendly terms to their values,
    then use the dict's keys as ``options``:

    >>> band_combos = {
    ...     "Natural color": "red green blue",
    ...     "Vegetation": "nir red green",
    ...     "Agriculture": "swir1 nir blue"
    ... }
    >>> band_combo_name = wf.widgets.select(
    ...     "band_combo",
    ...     options=band_combos.keys(),
    ...     format="radio",
    ...     label="Band combination",
    ... )
    >>> band_combos_wf = wf.Dict[wf.Str, wf.Str](band_combos)
    >>> s2_bands = s2.pick_bands(band_combos_wf[band_combo_name])
    >>> s2_bands.visualize("Sentinel-2", scales=[[0, 0.4], [0, 0.4], [0, 0.4]])  # doctest: +SKIP

    Parameters
    ----------
    name: str
        The name of the parameter.
    options: Sequence[str, int, or float]
        The available options to display. All values in the sequence must be the same type.
    format: str, default "dropdown"
        Which sort of widget to display. Options are:

        * "dropdown" (default): a dropdown selector
        * "radio": radio buttons
        * "slider": a slider to pick between the ``options`` in the order they're given.
          Displays the currently-selected option on the right side.
    default: str, int, float, optional, default None
        The default value to have selected. If None (default), picks the first value in ``options``.
        If not None, the value must be in the ``options`` list.
    label: str, default ""
        The longform label to display next to the widget.
        If not given, the widget will display as ``name``.

    Returns
    -------
    widget: Union[StringSelect, IntSelect, FloatSelect]
        A Widget object that acts just like a Workflows `.Str`, `.Int`, or `.Float`, and displays as a selector widget.
    """

    first, *rest = options
    if not isinstance(first, tuple(TYPES)):
        raise TypeError(
            f"Only primitive Python values ({tuple(TYPES)}) can be used as options for `select`, "
            f"not {first!r}."
        )

    pytype = type(first)
    for x in options:
        if not isinstance(x, pytype):
            raise TypeError(
                f"All options must be the same type. Expected all {pytype}, but got {x!r}"
            )

    if default is None:
        default = first
    elif default not in options:
        raise ValueError(f"The default {default!r} is not in the options list.")

    return TYPES[pytype](
        name=name, options=options, format=format, default=default, label=label
    )
