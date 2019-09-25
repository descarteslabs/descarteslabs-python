# -*- coding: utf-8 -*-

import ipywidgets as widgets
import traitlets
import numpy as np
import threading

from ..models import JobComputeError

from .map_ import Map
from .layer import WorkflowsLayer


initial_width = widgets.Layout(width="initial")
scale_width = widgets.Layout(min_width="1.3em", max_width="4em", width="initial")
button_layout = widgets.Layout(width="initial", overflow="visible")

colormaps = [
    "viridis",
    "plasma",
    "inferno",
    "magma",
    "cividis",
    "Greys",
    "Purples",
    "Blues",
    "Greens",
    "Oranges",
    "Reds",
    "YlOrBr",
    "YlOrRd",
    "OrRd",
    "PuRd",
    "RdPu",
    "BuPu",
    "GnBu",
    "PuBu",
    "YlGnBu",
    "PuBuGn",
    "BuGn",
    "YlGn",
    "binary",
    "gist_yarg",
    "gist_gray",
    "gray",
    "bone",
    "pink",
    "spring",
    "summer",
    "autumn",
    "winter",
    "cool",
    "Wistia",
    "hot",
    "afmhot",
    "gist_heat",
    "copper",
    "PiYG",
    "PRGn",
    "BrBG",
    "PuOr",
    "RdGy",
    "RdBu",
    "RdYlBu",
    "RdYlGn",
    "Spectral",
    "coolwarm",
    "bwr",
    "seismic",
    "twilight",
    "twilight_shifted",
    "hsv",
    "Pastel1",
    "Pastel2",
    "Paired",
    "Accent",
    "Dark2",
    "Set1",
    "Set2",
    "Set3",
    "tab10",
    "tab20",
    "tab20b",
    "tab20c",
    "flag",
    "prism",
    "ocean",
    "gist_earth",
    "terrain",
    "gist_stern",
    "gnuplot",
    "gnuplot2",
    "CMRmap",
    "cubehelix",
    "brg",
    "gist_rainbow",
    "rainbow",
    "jet",
    "nipy_spectral",
    "gist_ncar",
]


class CText(widgets.Text):
    value = traitlets.CUnicode(help="String value", allow_none=True).tag(sync=True)


class LayerControllerRow(widgets.Box):
    """
    Widget for the controls of a single `WorkflowsLayer` on a single `Map`.

    Provides controls for visbility, opacity, scales, colormaps, autoscaling,
    and order/deletion on the map.

    Attributes
    ----------
    map: Map
        The map on which `layer` is displayed.
    layer: WorkflowsLayer
        The layer this widget is controlling.
    scaleable: Bool, default True
        Whether to show controls for scaling
    autoscaleable: Bool, default True
        Whether to show the button for autoscaling
    colormappable: Bool, default True
        Whether to show controls for selecting the colormap
    checkerboardable: Bool, default True
        Whether to show controls for toggling checkerboards for transparent/missing data
    """

    map = traitlets.Instance(Map)
    layer = traitlets.Instance(WorkflowsLayer)

    scaleable = traitlets.Bool(True)
    autoscaleable = traitlets.Bool(True)
    colormappable = traitlets.Bool(True)
    checkerboardable = traitlets.Bool(True)
    # TODO(gabe): would be sweet to be able to compute if the image was 1 vs 3 bands on __init__,
    # so we could decide to show the colormap box vs scales control automatically

    def __init__(self, layer, map):
        if layer.error_output is None:
            layer.error_output = map.error_log

        self.layer = layer
        self.map = map
        self._widgets = {}

        visible = widgets.Checkbox(
            value=layer.visible, layout=initial_width, indent=False
        )
        widgets.jslink((visible, "value"), (layer, "visible"))
        self._widgets["visible"] = visible

        opacity = widgets.FloatSlider(
            value=layer.opacity,
            min=0,
            max=1,
            step=0.01,
            continuous_update=True,
            readout=False,
            layout=widgets.Layout(max_width="50px", min_width="20px"),
        )
        widgets.jslink((opacity, "value"), (layer, "opacity"))
        self._widgets["opacity"] = opacity

        name = widgets.Text(
            value=layer.name,
            placeholder="Layer name",
            layout=widgets.Layout(min_width="4em", max_width="12em"),
        )
        widgets.jslink((name, "value"), (layer, "name"))
        self._widgets["name"] = name

        r_min = CText(placeholder="r min", value=layer.r_min, layout=scale_width)
        widgets.link((r_min, "value"), (layer, "r_min"))
        r_max = CText(placeholder="r max", value=layer.r_max, layout=scale_width)
        widgets.link((r_max, "value"), (layer, "r_max"))
        g_min = CText(placeholder="g min", value=layer.g_min, layout=scale_width)
        widgets.link((g_min, "value"), (layer, "g_min"))
        g_max = CText(placeholder="g max", value=layer.g_max, layout=scale_width)
        widgets.link((g_max, "value"), (layer, "g_max"))
        b_min = CText(placeholder="b min", value=layer.b_min, layout=scale_width)
        widgets.link((b_min, "value"), (layer, "b_min"))
        b_max = CText(placeholder="b max", value=layer.b_max, layout=scale_width)
        widgets.link((b_max, "value"), (layer, "b_max"))

        self._widgets["scales"] = [r_min, r_max, g_min, g_max, b_min, b_max]

        colormap = widgets.Dropdown(
            options=[None] + colormaps,
            value=layer.colormap,
            layout=widgets.Layout(width="initial", max_width="7vh"),
        )
        widgets.link((colormap, "value"), (layer, "colormap"))
        colormap.observe(self._observe_supported_controls, names="value")
        self._widgets["colormap"] = colormap

        cmap_min = CText(placeholder="min", value=layer.cmap_min, layout=scale_width)
        widgets.link((cmap_min, "value"), (layer, "cmap_min"))
        cmap_max = CText(placeholder="max", value=layer.cmap_max, layout=scale_width)
        widgets.link((cmap_max, "value"), (layer, "cmap_max"))

        self._widgets["cmap_scales"] = [cmap_min, cmap_max]

        checkerboard = widgets.ToggleButton(
            value=layer.checkerboard,
            description="",
            tooltip="Checkerboard missing data",
            icon="th",
            layout=button_layout,
        )
        widgets.link((checkerboard, "value"), (layer, "checkerboard"))
        self._widgets["checkerboard"] = checkerboard

        autoscale = widgets.Button(
            description="", tooltip="Enhance!", icon="magic", layout=button_layout
        )
        autoscale.on_click(self.autoscale)
        self._widgets["autoscale"] = autoscale

        move_up = widgets.Button(
            description=u"↑", tooltip="Move layer up", layout=button_layout
        )
        move_up.on_click(self.move_up)
        self._widgets["move_up"] = move_up

        move_down = widgets.Button(
            description=u"↓", tooltip="Move layer down", layout=button_layout
        )
        move_down.on_click(self.move_down)
        self._widgets["move_down"] = move_down

        remove = widgets.Button(
            description=u"✖︎", tooltip="Remove layer", layout=button_layout
        )
        remove.on_click(self.remove)
        self._widgets["remove"] = remove

        super(LayerControllerRow, self).__init__(self._make_children())

    def _scale_observer(self, scale):
        def _observer(change):
            try:
                setattr(self.layer, scale, float(change["new"]))
            except ValueError:
                pass

        return _observer

    def _make_children(self):
        widgets = self._widgets
        children = [widgets["visible"], widgets["opacity"], widgets["name"]]

        if self.scaleable:
            if self.layer.colormap is None:
                children.extend(widgets["scales"])
            else:
                children.extend(widgets["cmap_scales"])
        if self.colormappable:
            children.append(widgets["colormap"])
            widgets["colormap"].layout.width = (
                "2em" if self.layer.colormap is None else ""
            )
        if self.checkerboardable:
            children.append(widgets["checkerboard"])
        if self.autoscaleable:
            children.append(widgets["autoscale"])

        children += [widgets["move_up"], widgets["move_down"], widgets["remove"]]

        return children

    @traitlets.observe(
        "autoscaleable", "colormappable", "scaleable", "checkerboardable"
    )
    def _observe_supported_controls(self, change):
        self.children = self._make_children()

    def _autoscale(self, widget):
        old_icon = widget.icon
        widget.icon = "spinner"
        widget.disabled = True

        ctx = self.map.geocontext()

        try:
            result = self.layer.image.compute(ctx, progress_bar=self.map.output_log)
        except JobComputeError:
            pass
        else:
            arr = result.ndarray

            scales_attrs = (
                [("r_min", "r_max"), ("g_min", "g_max"), ("b_min", "b_max")]
                if self.layer.colormap is None
                else [("cmap_min", "cmap_max")]
            )

            with self.layer.hold_trait_notifications():
                for band, (scale_min, scale_max) in zip(arr, scales_attrs):
                    if isinstance(band, np.ma.MaskedArray):
                        data = band.compressed()  # drop masked data
                    min, max = np.percentile(data, [2, 98])

                    setattr(self.layer, scale_min, min)
                    setattr(self.layer, scale_max, max)
        finally:
            widget.icon = old_icon
            widget.disabled = False

    def autoscale(self, widget):
        "``on_click`` handler to perform autoscaling."
        thread = threading.Thread(target=self._autoscale, args=(widget,), daemon=True)
        thread.start()

    def move_up(self, _):
        "``on_click`` handler to move ``self.layer`` up on ``self.map``"
        self.map.move_layer_up(self.layer)

    def move_down(self, _):
        "``on_click`` handler to move ``self.layer`` down on ``self.map``"
        self.map.move_layer_down(self.layer)

    def remove(self, _):
        "``on_click`` handler to remove ``self.layer`` from ``self.map``"
        if self.layer.error_output is self.map.error_log:
            # stops the error listener
            self.layer.error_output = None
        self.map.remove_layer(self.layer)
