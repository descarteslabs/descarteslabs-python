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
