import ipywidgets as widgets
import traitlets


class LonLatInput(widgets.Text):
    """
    Input for entering lon, lat as comma-separated string

    Link to ``model``, not ``value``! ``model`` is the 2-list of floats,
    ``value`` is the displayed string value.

    Use ``model_is_latlon`` to reverse the order between the ``model`` and the ``value``.
    """

    model = traitlets.List(
        traitlets.CFloat(), default_value=(0.0, 0.0), minlen=2, maxlen=2
    )
    model_is_latlon = traitlets.Bool(False)
    description = traitlets.Unicode("Lat, lon (WGS84):").tag(sync=True)
    continuous_update = traitlets.Bool(False).tag(sync=True)

    @traitlets.observe("value")
    def _sync_view_to_model(self, change):
        new = change["new"]
        values = [part.strip() for part in new.split(",")]
        if self.model_is_latlon:
            values.reverse()
        self.model = values

    @traitlets.observe("model")
    def _sync_model_to_view(self, change):
        new = change["new"]
        string = "{:.4f}, {:.4f}".format(
            # https://xkcd.com/2170/
            *(reversed(new) if self.model_is_latlon else new)
        )
        self.value = string


class PositionController(widgets.HBox):
    "Widget for controlling the center and zoom of a `Map`."

    def __init__(self, map):
        lonlat = LonLatInput(
            model=map.center,
            layout=widgets.Layout(width="initial"),
            style={"description_width": "initial"},
        )
        widgets.link((map, "center"), (lonlat, "model"))

        zoom_label = widgets.Label(
            value="Zoom:", layout=widgets.Layout(width="initial")
        )
        zoom = widgets.BoundedIntText(
            value=map.zoom,
            layout=widgets.Layout(width="3em"),
            min=map.min_zoom,
            max=map.max_zoom,
            step=1,
        )
        widgets.link((map, "zoom"), (zoom, "value"))
        widgets.link((map, "min_zoom"), (zoom, "min"))
        widgets.link((map, "max_zoom"), (zoom, "max"))

        super(PositionController, self).__init__(children=(lonlat, zoom_label, zoom))
