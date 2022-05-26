# NOTE: this filename is underscore-prefixed just so it doesn't show up
# in tab-completion for `wf.widgets.`
import typing

from ....common.proto.widgets import widgets_pb2
from ...types import Image, ImageCollection
from ...types.widget import Widget

if typing.TYPE_CHECKING:
    from .. import WorkflowsLayer, Map, MapApp


def image(
    name: str,
    default_layer: "WorkflowsLayer" = None,
    label: str = "",
    map: typing.Optional[typing.Union["Map", "MapApp"]] = None,
):
    """
    A widget, which acts as an `~.geospatial.Image` parameter, to select one of the current layers on the map.

    Using this widget, you can create layers which use other layers as input, and update themselves
    when those input layers change.

    Its value is equivalent to the selected layer, *with all of its current parameter values applied*.
    If you've selected a layer showing an `~.geospatial.ImageCollection`, that layer's current
    `~.WorkflowsLayer.reduction` is also applied.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.widgets.image("param_name", default_layer=some_layer, label="The input Image", map=wf.map)  # doctest: +SKIP

    Use `image` widgets to show areas that have changed between any two layers on the map:

    >>> # add some layers to the map, one for each year
    >>> layers = []
    >>> for year in [2017, 2018, 2019, 2020]:
    ...     ic = (
    ...         wf.ImageCollection.from_id("sentinel-2:L1C", f"{year}-01-01", f"{year}-04-01")
    ...         .pick_bands("red green blue")
    ...     )
    ...     lyr = ic.visualize(
    ...         f"Winter {year}", scales=[[0, 0.4], [0, 0.4], [0, 0.4]], reduction="median"
    ...     ) # doctest: +SKIP
    ...     layers.append(lyr)  # doctest: +SKIP

    >>> before = wf.widgets.image("before", default_layer=layers[0])  # doctest: +SKIP
    >>> after = wf.widgets.image("after", default_layer=layers[-1])  # doctest: +SKIP
    >>> delta = after.mean(axis="bands") - before.mean(axis="bands")  # doctest: +SKIP
    >>> change = abs(delta) > 0.15  # doctest: +SKIP
    >>> changed = change.mask(~change)  # doctest: +SKIP
    >>> changed.visualize("Changed", scales=[0, 2], colormap="Reds")  # doctest: +SKIP
    >>> # ^ when you call .visualize, the two `image` widgets will automatically show up below

    As you select different layers for the ``before`` and ``after`` dropdowns, the map will update to
    show which pixels are different between them.
    Notice that changes to the input layers, such as switching the redution operation, will also cause
    the ``changed`` layer to update.
    (If you haven't already, run ``wf.map`` in another notebook cell to see your layers on the map.)

    Parameters
    ----------
    name: str
        The name of the parameter.
    default_layer: Optional[WorkflowsLayer], default None
        The layer selected by default. (A `.WorkflowsLayer` is the object returned by `~.Image.visualize`).
        If None, no layer will be selected, and nothing will display until you pick a layer from the dropdown.

        Unlike with other widget types, when you `~.toplevel.publish` an object that depend on an `image` widget,
        the ``default_layer`` isn't stored. Anyone who accesses your published object won't see the same default
        layer; for them, it will be None.
    label: str, default ""
        The longform label to display next to the widget.
        If not given, the widget will display as ``name``.
    map: Optional[Union[Map, MapApp]], default None
        The `~.Map` or `~.MapApp` object to list layers from.
        If None (default), lists the layers on `wf.map <.interactive.map>`.

    Returns
    -------
    widget: ~.geospatial.Image
        A Widget object that acts just like a Workflows `~.geospatial.Image`, and displays as a dropdown
        listing the layer names currently on the map.
    """

    return ImagePicker(name, default_layer=default_layer, label=label, map=map)


def imagecollection(
    name: str,
    default_layer: "WorkflowsLayer" = None,
    label: str = "",
    map: typing.Union["Map", "MapApp"] = None,
):
    """
    A widget, which acts as an `~.geospatial.ImageCollection` parameter, to select one of the current layers on the map.

    Using this widget, you can create layers which use other layers as input, and update themselves
    when those input layers change.

    Its value is equivalent to the selected layer, *with all of its current parameter values applied*.

    Unlike `.image`, this widget will only list layers that display ImageCollections, not Images.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.widgets.imagecollection(
    ...     "param_name",
    ...     default_layer=some_layer,
    ...     label="The input ImageCollection",
    ...     map=wf.map,
    ... )  # doctest: +SKIP

    Use an `imagecollection` widget to filter out cloudy scenes from any layer on the map:

    >>> # add some layers to the map, one for each product (Sentinel-2 and Landsat-8)
    >>> layers = []
    >>> for product in ["sentinel-2:L1C", "landsat:LC08:01:T1:TOAR"]:
    ...     ic = (
    ...         wf.ImageCollection.from_id(product, "2019-01-01", "2019-03-01")
    ...         .pick_bands("red green blue")
    ...     )
    ...     lyr = ic.visualize(
    ...         product, scales=[[0, 0.4], [0, 0.4], [0, 0.4]], reduction="mean"
    ...     )  # doctest: +SKIP
    ...     layers.append(lyr)  # doctest: +SKIP

    >>> ic = wf.widgets.imagecollection("ic", default_layer=layers[0])  # doctest: +SKIP
    >>> low_cloud = ic.filter(lambda img: img.properties["cloud_fraction"] < 0.3)  # doctest: +SKIP
    >>> low_cloud.visualize("Low-cloud", scales=[[0, 0.4], [0, 0.4], [0, 0.4]], reduction="mean")  # doctest: +SKIP
    >>> # ^ when you call .visualize, the the `imagecollection` widget will automatically show up below

    Selecting a different layer from the dropdown will show a basic cloud-filtered composite of that layer.
    (If you haven't already, run ``wf.map`` in another notebook cell to see your layers on the map.)

    Parameters
    ----------
    name: str
        The name of the parameter.
    default_layer: Optional[WorkflowsLayer], default None
        The layer selected by default. (A `.WorkflowsLayer` is the object returned by `~.Image.visualize`).
        If None, no layer will be selected, and nothing will display until you pick a layer from the dropdown.

        Unlike with other widget types, when you `~.toplevel.publish` an object that depend on an `imagecollection`
        widget, the ``default_layer`` isn't stored. Anyone who accesses your published object won't see the same default
        layer; for them, it will be None.
    label: str, default ""
        The longform label to display next to the widget.
        If not given, the widget will display as ``name``.
    map: Optional[Union[Map, MapApp]], default None
        The `~.Map` or `~.MapApp` object to list layers from.
        If None (default), lists the layers on `wf.map <.interactive.map>`.

    Returns
    -------
    widget: ImageCollection
        A Widget object that acts just like a Workflows `.ImageCollection`, and displays as a dropdown
        listing the layer names currently on the map.
    """
    return ImageCollectionPicker(
        name, default_layer=default_layer, label=label, map=map
    )


class Picker(Widget):
    _widget_type: str
    """
    The *name* of the widget type. Imported at runtime from ``..layer_picker`` to avoid
    circular imports and ipywidgets dependencies.
    """

    def __init__(self, name: str, default_layer=None, label: str = "", map=None):
        super().__init__(name, default_layer, label)
        from ... import interactive

        if interactive._successful_import:
            widget_type = getattr(interactive, self._widget_type)
        else:
            from unittest import mock

            widget_type = mock.MagicMock()

        self.widget = widget_type(
            map=map, default_layer=default_layer, hide_deps_of=self
        )
        self.widget._label = label


class ImagePicker(Picker, Image):
    _proto_type = widgets_pb2.ImagePicker
    _widget_type = "ImagePickerWidget"


class ImageCollectionPicker(Picker, ImageCollection):
    _proto_type = widgets_pb2.ImageCollectionPicker
    _widget_type = "ImageCollectionPickerWidget"
