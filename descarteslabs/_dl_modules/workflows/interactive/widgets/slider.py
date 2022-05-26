from typing import Union, Optional, Generic, T, ClassVar

from ._ipywidgets import ipywidgets

from ....common.proto.widgets import widgets_pb2

from ...types import Int, Float
from ...types.widget import Widget


class Slider(Widget, Generic[T]):
    _widget_type: ClassVar[ipywidgets.DOMWidget]

    def __init__(self, name: str, default: T, min: T, max: T, step: T, label: str = ""):
        super().__init__(name, default, label)
        self._min = min
        self._max = max
        self._step = step
        self.widget = self._widget_type(value=default, min=min, max=max, step=step)
        self.widget._label = label


class IntSlider(Slider[int], Int):
    _proto_type = widgets_pb2.IntSlider
    _widget_type = ipywidgets.IntSlider


class FloatSlider(Slider[float], Float):
    _proto_type = widgets_pb2.FloatSlider
    _widget_type = ipywidgets.FloatSlider


def slider(
    name: str,
    min: Union[int, float],
    max: Union[int, float],
    step: Optional[Union[int, float]] = None,
    default: Optional[Union[int, float]] = None,
    label: str = "",
):
    """
    A slider, which acts as an `.Int` or `.Float` parameter.

    If any of ``min``, ``max``, or ``step`` are a float, then the slider will represent a `.Float` value,
    otherise an `.Int`.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.widgets.slider("param_name", 0, 10, step=0.5, default=3, label="A float parameter")  # doctest: +SKIP

    >>> s2 = wf.ImageCollection.from_id("sentinel-2:L1C", "2018-01-01", "2018-04-01")
    >>> nir, red = s2.unpack_bands("nir red")
    >>> ndvi = (nir - red) / (nir + red)
    >>> high_ndvi = ndvi > wf.widgets.slider("thresh", 0, 0.8, default=0.24, label="Minimum NDVI threshold")
    >>> high_ndvi.visualize("NDVI", scales=[0, 1], colormap="Greens")  # doctest: +SKIP
    >>> # ^ when you call .visualize, the `slider` widget will automatically show up below

    The map will show areas where NDVI is greater than the amount on the slider,
    and update when you drag the slider.
    (If you haven't already, run ``wf.map`` in another notebook cell to see your layer.)

    Parameters
    ----------
    name: str
        The name of the parameter.
    min: int, float
        The minimum value for the slider. Must be less than max.
    max: int, float
        The maximum value for the slider. Must be greater than min.
    step: int, float, optional, default None
        The interval between "ticks" on the slider.
        Must be less than ``max - min``.

        If None (default) and one of ``min`` or ``max`` are floats,
        defaults to ``(min - max) / 10``, meaning having 10 steps
        on the slider.

        If None and both are ints, and ``5 <= (min - max) <= 10``,
        ``step`` becomes 1, meaning 5-10 steps on the slider.
        Otherwise, ``step`` becomes ``(min - max) / 10``, rounded to
        an int if that int would evenly divide the slider,
        otherwise a float.
    default: int, float, optional, default None
        The default value for the slider. If None (default), defaults
        to ``min``. Must be between ``min`` and ``max``.
    label: str, default ""
        The longform label to display next to the widget.
        If not given, the widget will display as ``name``

    Returns
    -------
    widget: Union[IntSlider, FloatSlider]
        A Widget object that acts just like a Workflows `.Str`, `.Int`, or `.Float` and displays as a slider.
    """

    if default is None:
        default = min

    assert max > min, f"max ({max}) must be greater than min ({min})"
    assert (
        min <= default <= max
    ), f"default ({default}) must be between min and max: {min} !<= {default} !<= {max}"
    any_float = any(isinstance(x, float) for x in (min, max, step, default))
    if step is None:
        step = (max - min) / 10
        if not any_float:
            if step < 0.5:
                # Moving to a stepsize of 1 would yield an int slider with less than 5 steps,
                # so we use a float slider instead.
                any_float = True
            elif step < 1:
                step = 1
            elif step % 1 != 0:
                rounded_step = int(round(step))
                if (max - min) % rounded_step == 0:
                    # The `rounded_step` stepsize evenly divides the slider, so we can use it.
                    step = rounded_step
                else:
                    # Otherwise, use a float slider instead.
                    any_float = True
            else:
                # step is a multiple of 1, but truediv always produces floats
                step = int(step)

    assert step < (
        max - min
    ), f"Step ({step}) must be less than max - min ({max - min})."

    widget_type = FloatSlider if any_float else IntSlider
    return widget_type(name, default, min, max, step, label=label)
