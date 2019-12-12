import textwrap

import ipyleaflet
import IPython
import ipywidgets as widgets
import traitlets

from ..types import GeoContext
from .clearable import ClearableOutput
from .layer import WorkflowsLayer
from .lonlat import PositionController
from .utils import tuple_move

app_layout = widgets.Layout(height="100%", padding="0 0 8px 0")


class MapApp(widgets.VBox):
    """
    Widget displaying a map, layers, and output logs in a nicer layout.

    Forwards attributes and methods to ``self.map``.
    """

    _forward_attrs_to_map = {
        "center",
        "zoom_start",
        "zoom",
        "max_zoom",
        "min_zoom",
        "interpolation",
        "crs",
        # Specification of the basemap
        "basemap",
        "modisdate",
        # Interaction options
        "dragging",
        "touch_zoom",
        "scroll_wheel_zoom",
        "double_click_zoom",
        "box_zoom",
        "tap",
        "tap_tolerance",
        "world_copy_jump",
        "close_popup_on_click",
        "bounce_at_zoom_limits",
        "keyboard",
        "keyboard_pan_offset",
        "keyboard_zoom_offset",
        "inertia",
        "inertia_deceleration",
        "inertia_max_speed",
        "zoom_animation_threshold",
        "fullscreen",
        "zoom_control",
        "attribution_control",
        "south",
        "north",
        "east",
        "west",
        "layers",
        "bounds",
        "bounds_polygon",
        # from subclass
        "output_log",
        "error_log",
        # methods
        "move_layer",
        "move_layer_up",
        "move_layer_down",
        "add_layer",
        "add_control",
        "remove_control",
        "clear_controls",
        "on_interaction",
        "geocontext",
    }

    def __init__(self, map=None, layer_controller_list=None, position_controller=None):
        if map is None:
            map = Map()
            map.add_control(ipyleaflet.FullScreenControl())

        if layer_controller_list is None:
            from .layer_controller import LayerControllerList

            layer_controller_list = LayerControllerList(map)
        if position_controller is None:
            position_controller = PositionController(map)

        self.map = map
        self.controller_list = layer_controller_list
        self.position_controller = position_controller

        def on_clear():
            for layer in self.map.layers:
                try:
                    layer.forget_errors()
                except AttributeError:
                    pass

        self.errors = ClearableOutput(
            map.error_log,
            on_clear=on_clear,
            layout=widgets.Layout(max_height="20rem", flex="1 0 auto"),
        )

        self.autoscale_outputs = widgets.VBox(
            [
                x.autoscale_progress
                for x in reversed(self.map.layers)
                if isinstance(x, WorkflowsLayer)
            ],
            layout=widgets.Layout(flex="1 0 auto", max_height="16rem"),
        )

        super(MapApp, self).__init__(
            [
                map,
                self.errors,
                map.output_log,
                self.autoscale_outputs,
                position_controller,
                layer_controller_list,
            ],
            layout=app_layout,
        )

        map.observe(self._update_autoscale_progress, names=["layers"])

    def remove_layer(self, layer_name):
        "Remove a named layer from the map"
        for lyr in self.map.layers:
            if lyr.name == layer_name:
                self.map.remove_layer(lyr)
                break
        else:
            raise ValueError("Layer {} does not exist on the map".format(layer_name))

    def clear_layers(self):
        "Remove all layers from the map (besides the base layer)"
        self.map.layers = tuple(lyr for lyr in self.map.layers if lyr.base)

    def _update_autoscale_progress(self, change):
        self.autoscale_outputs.children = [
            x.autoscale_progress
            for x in reversed(self.map.layers)
            if isinstance(x, WorkflowsLayer)
        ]

    def __getattr__(self, attr):
        if attr in self._forward_attrs_to_map:
            return getattr(self.__dict__["map"], attr)
        raise AttributeError(attr)

    def __setattr__(self, attr, x):
        if attr in self._forward_attrs_to_map:
            return setattr(self.__dict__["map"], attr, x)
        else:
            return super(MapApp, self).__setattr__(attr, x)

    def __dir__(self):
        return super(MapApp, self).__dir__() + list(self._forward_attrs_to_map)

    def __repr__(self):
        msg = """
        `ipyleaflet` and/or `ipywidgets` Jupyter extensions are not installed! (or you're not in a Jupyter notebook.)
        To install for JupyterLab, run this in a cell:
            !jupyter labextension install jupyter-leaflet @jupyter-widgets/jupyterlab-manager
        To install for plain Jupyter Notebook, run this in a cell:
            !jupyter nbextension enable --py --sys-prefix ipyleaflet
        Then, restart Jupyter and re-run this notebook.
        """
        return textwrap.dedent(msg)

    def _ipython_display_(self, **kwargs):
        """
        Called when `IPython.display.display` is called on the widget.

        Copied verbatim from
        https://github.com/jupyter-widgets/ipywidgets/blob/master/ipywidgets/widgets/widget.py#L709-L729,
        but with truncation 110-character repr truncation removed, so we can display a helpful message when necessary
        extensions aren't installed.
        """

        plaintext = repr(self)
        # removed 110-character truncation here
        data = {"text/plain": plaintext}
        if self._view_name is not None:
            # The 'application/vnd.jupyter.widget-view+json' mimetype has not been registered yet.
            # See the registration process and naming convention at
            # http://tools.ietf.org/html/rfc6838
            # and the currently registered mimetypes at
            # http://www.iana.org/assignments/media-types/media-types.xhtml.
            data["application/vnd.jupyter.widget-view+json"] = {
                "version_major": 2,
                "version_minor": 0,
                "model_id": self._model_id,
            }
        IPython.display.display(data, raw=True)

        if self._view_name is not None:
            self._handle_displayed(**kwargs)


class Map(ipyleaflet.Map):
    """
    Subclass of ``ipyleaflet.Map`` with Workflows defaults and extra helper methods.

    Attributes
    ----------
    output_log: ipywidgets.Output
        Widget where functions doing operations on this map (especially compute operations,
        like autoscaling or timeseries) can log their output.
    """

    center = traitlets.List(
        [35.6870, -105.93780], help="Initial geographic center of the map"
    ).tag(sync=True, o=True)
    zoom_start = traitlets.Int(8, help="Initial map zoom level").tag(sync=True, o=True)
    min_zoom = traitlets.Int(5, help="Minimum allowable zoom level of the map").tag(
        sync=True, o=True
    )
    scroll_wheel_zoom = traitlets.Bool(
        True, help="Whether the map can be zoomed by using the mouse wheel"
    ).tag(sync=True, o=True)

    error_log = traitlets.Instance(
        widgets.Output,
        args=(),
        help="Widget where tiles layers can write their error messages.",
    )

    output_log = traitlets.Instance(
        widgets.Output,
        args=(),
        help="""
        Widget where functions doing operations on this map
        (especially compute operations) can log their output.
        """,
    )

    autoscale_outputs = traitlets.Instance(
        widgets.VBox,
        args=(),
        help="Widget containing all layers' autoscale output widgets",
    )

    def move_layer(self, layer, new_index):
        """
        Move a layer to a new index.

        Parameters
        ----------
        layer: ipyleaflet.Layer
        new_index: int

        Raises
        ------
        ValueError:
            If ``layer`` is a base layer, or does not already exist on the map.
        """
        if layer.base:
            raise ValueError("Cannot reorder base layer {}".format(layer))

        try:
            old_i = self.layers.index(layer)
        except ValueError:
            raise ValueError("Layer {} does not exist on the map".format(layer))

        self.layers = tuple_move(self.layers, old_i, new_index)

    def move_layer_up(self, layer):
        """
        Move a layer up one, if not already at the top.

        Parameters
        ----------
        layer: ipyleaflet.Layer

        Raises
        ------
        ValueError:
            If ``layer`` is a base layer, or does not already exist on the map.
        """
        if layer.base:
            raise ValueError("Cannot reorder base layer {}".format(layer))

        try:
            old_i = self.layers.index(layer)
        except ValueError:
            raise ValueError("Layer {} does not exist on the map".format(layer))

        if old_i < len(self.layers) - 1:
            self.layers = tuple_move(self.layers, old_i, old_i + 1)

    def move_layer_down(self, layer):
        """
        Move a layer down one, if not already at the bottom.

        Parameters
        ----------
        layer: ipyleaflet.Layer

        Raises
        ------
        ValueError:
            If ``layer`` is a base layer, or does not already exist on the map.
        """
        if layer.base:
            raise ValueError("Cannot reorder base layer {}".format(layer))

        try:
            old_i = self.layers.index(layer)
        except ValueError:
            raise ValueError("Layer {} does not exist on the map".format(layer))

        if old_i > 0 and not self.layers[old_i - 1].base:
            self.layers = tuple_move(self.layers, old_i, old_i - 1)

    def geocontext(self):
        """
        A Workflows `~.geospatial.GeoContext` representing the current view area and resolution of the map.

        Returns
        -------
        geoctx: ~.geospatial.GeoContext
        """
        bounds = [self.west, self.south, self.east, self.north]
        if bounds == [0, 0, 0, 0]:
            raise RuntimeError(
                "Undefined bounds, please ensure that the interactive map has "
                "finished rendering and run this cell again. If you have not "
                "displayed the map yet, run `wf.map` in its own cell first."
            )

        resolution = 156543.00 / 2 ** self.zoom
        # TODO this resolution calculation is not quite right; assumes at equator.
        # TODO more importantly: can we make the request using the component XYZ tiles,
        # so that we get a cache hit when we repeat the same request after setting scaling client-side?

        return GeoContext(
            bounds=bounds,
            crs="EPSG:3857",
            bounds_crs="EPSG:4326",
            resolution=resolution,
            align_pixels=False,
        )
