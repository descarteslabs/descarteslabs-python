import datetime
import logging
import uuid
import threading

import ipyleaflet
import ipywidgets as widgets
import traitlets

from descarteslabs.common.proto.logging import logging_pb2

from ..models import XYZ
from ..types import Image, ImageCollection

from . import parameters
from .clearable import ClearableOutput


class ScaleFloat(traitlets.CFloat):
    "Casting Float traitlet that also considers the empty string as None"

    def validate(self, obj, value):
        if value == "" and self.allow_none:
            return None
        return super(ScaleFloat, self).validate(obj, value)


class WorkflowsLayer(ipyleaflet.TileLayer):
    """
    Subclass of ``ipyleaflet.TileLayer`` for displaying a Workflows
    `~.geospatial.Image` or `~.geospatial.ImageCollection`.

    Attributes
    ----------
    imagery: ~.geospatial.Image or ~.geospatial.ImageCollection
        The `~.geospatial.Image` or `~.geospatial.ImageCollection` to use
    parameters: ParameterSet
        Parameters to use while computing; modify attributes under ``.parameters``
        (like ``layer.parameters.foo = "bar"``) to cause the layer to recompute
        and update under those new parameters.
    xyz_obj: ~.models.XYZ
        Read-only: The `XYZ` object this layer is displaying.
    session_id: str
        Read-only: Unique ID that logs will be stored under, generated automatically.
    checkerboard: bool, default True
        Whether to display a checkerboarded background for missing or masked data.
    colormap: str, optional, default None
        Name of the colormap to use.
        If set, `imagery` must have 1 band.
    reduction: {"min", "max", "mean", "median", "mosaic", "sum", "std", "count"}
        If displaying an `~.geospatial.ImageCollection`, this method is used to reduce it
        into an `~.geospatial.Image`. Reduction is performed before applying a colormap or scaling.
    r_min: float, optional, default None
        Min value for scaling the red band. Along with r_max,
        controls scaling when a colormap is enabled.
    r_max: float, optional, default None
        Max value for scaling the red band. Along with r_min, controls scaling
        when a colormap is enabled.
    g_min: float, optional, default None
        Min value for scaling the green band.
    g_max: float, optional, default None
        Max value for scaling the green band.
    b_min: float, optional, default None
        Min value for scaling the blue band.
    b_max: float, optional, default None
        Max value for scaling the blue band.
    log_output: ipywidgets.Output, optional, default None
        If set, write unique log records from tiles computation to this output area
        from a background thread. Setting to None stops the listener thread.
    log_level: int, default logging.DEBUG
        Only listen for log records at or above this log level during tile computation.
        See https://docs.python.org/3/library/logging.html#logging-levels for valid
        log levels.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.map # doctest: +SKIP
    >>> # ^ display interactive map
    >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1").pick_bands("red")
    >>> masked_img = img.mask(img > wf.parameter("threshold", wf.Float))
    >>> layer = masked_img.visualize("sample", colormap="viridis", threshold=0.07) # doctest: +SKIP
    >>> layer.colormap = "plasma" # doctest: +SKIP
    >>> # ^ change colormap (this will update the layer on the map)
    >>> layer.parameters.threshold = 0.13 # doctest: +SKIP
    >>> # ^ adjust parameters (this also updates the layer)
    >>> layer.set_scales((0.01, 0.3)) # doctest: +SKIP
    >>> # ^ adjust scaling (this also updates the layer)
    """

    attribution = traitlets.Unicode("Descartes Labs").tag(sync=True, o=True)
    min_zoom = traitlets.Int(5).tag(sync=True, o=True)
    url = traitlets.Unicode(read_only=True).tag(sync=True)

    imagery = traitlets.Union(
        [traitlets.Instance(Image), traitlets.Instance(ImageCollection)]
    )

    parameters = traitlets.Instance(parameters.ParameterSet, allow_none=True)
    xyz_obj = traitlets.Instance(XYZ, read_only=True)
    session_id = traitlets.Unicode(read_only=True)
    log_level = traitlets.Int(logging.DEBUG)

    checkerboard = traitlets.Bool(True)
    reduction = traitlets.Unicode("mosaic")
    colormap = traitlets.Unicode(None, allow_none=True)

    r_min = ScaleFloat(None, allow_none=True)
    r_max = ScaleFloat(None, allow_none=True)
    g_min = ScaleFloat(None, allow_none=True)
    g_max = ScaleFloat(None, allow_none=True)
    b_min = ScaleFloat(None, allow_none=True)
    b_max = ScaleFloat(None, allow_none=True)

    log_output = traitlets.Instance(widgets.Output, allow_none=True)
    autoscale_progress = traitlets.Instance(ClearableOutput)

    def __init__(self, imagery, *args, **kwargs):
        params = kwargs.pop("parameters", {})
        log_level = kwargs.pop("log_level", None)
        if log_level is None:
            log_level = logging.DEBUG
        super(WorkflowsLayer, self).__init__(*args, **kwargs)

        with self.hold_trait_notifications():
            self.imagery = imagery
            self.log_level = log_level
            self.set_trait("session_id", uuid.uuid4().hex)
            self.set_trait(
                "autoscale_progress",
                ClearableOutput(
                    widgets.Output(),
                    layout=widgets.Layout(max_height="10rem", flex="1 0 auto"),
                ),
            )
            self.set_parameters(**params)

        self._log_listener = None
        self._known_logs = set()
        self._known_logs_lock = threading.Lock()

    def make_url(self):
        """
        Generate the URL for this layer.

        This is called automatically as the attributes (`imagery`, `colormap`, scales, etc.) are changed.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> img = img.pick_bands("red blue green") # doctest: +SKIP
        >>> layer = img.visualize("sample") # doctest: +SKIP
        >>> layer.make_url() # doctest: +SKIP
        'https://workflows.descarteslabs.com/master/xyz/9ec70d0e99db7f50c856c774809ae454ffd8475816e05c5c/{z}/{x}/{y}.png?session_id=xxx&checkerboard=true'
        """
        if not self.visible:
            # workaround for the fact that Leaflet still loads tiles from inactive layers,
            # which is expensive computation users don't want
            return ""

        if self.colormap is not None:
            scales = [[self.r_min, self.r_max]]
        else:
            scales = [
                [self.r_min, self.r_max],
                [self.g_min, self.g_max],
                [self.b_min, self.b_max],
            ]

        scales = [scale for scale in scales if scale != [None, None]]

        parameters = self.parameters.to_dict()

        return self.xyz_obj.url(
            session_id=self.session_id,
            colormap=self.colormap,
            scales=scales,
            reduction=self.reduction,
            checkerboard=self.checkerboard,
            **parameters,
        )

    @traitlets.observe("imagery")
    def _update_xyz(self, change):
        old, new = change["old"], change["new"]
        if old is new:
            # traitlets does an == check between the old and new value to decide if it's changed,
            # which for an Image, returns another Image, which it considers changed.
            return

        xyz = XYZ.build(new, name=self.name)
        xyz.save()
        self.set_trait("xyz_obj", xyz)

    @traitlets.observe(
        "visible",
        "checkerboard",
        "colormap",
        "reduction",
        "r_min",
        "r_max",
        "g_min",
        "g_max",
        "b_min",
        "b_max",
        "xyz_obj",
        "session_id",
        "parameters",
    )
    @traitlets.observe("parameters", type="delete")
    def _update_url(self, change):
        try:
            self.set_trait("url", self.make_url())
        except ValueError as e:
            if "Invalid scales passed" not in str(e):
                raise e

    @traitlets.observe("parameters", type="delete")
    def _update_url_on_param_delete(self, change):
        # traitlets is dumb and decorator stacking doesn't work so we have to repeat this
        try:
            self.set_trait("url", self.make_url())
        except ValueError as e:
            if "Invalid scales passed" not in str(e):
                raise e

    @traitlets.observe("xyz_obj", "session_id", "log_level")
    def _update_logger(self, change):
        if self.log_output is None:
            return

        # Remove old log records for the layer
        self.forget_logs()
        new_logs = []
        for record in self.log_output.outputs:
            if not record["text"].startswith(self.name + ": "):
                new_logs.append(record)
        self.log_output.outputs = tuple(new_logs)

        if self._log_listener is not None:
            self._log_listener.stop(timeout=1)

        listener = self.xyz_obj.log_listener()
        listener.add_callback(self._logs_callback)
        listener.listen(
            self.session_id,
            datetime.datetime.now(datetime.timezone.utc),
            level=self.log_level,
        )

        self._log_listener = listener

    def _stop_logger(self):
        if self._log_listener is not None:
            self._log_listener.stop(timeout=1)
            self._log_listener = None

    @traitlets.observe("log_output")
    def _toggle_log_listener_if_output(self, change):
        if change["new"] is None:
            self._stop_logger()
        else:
            if self._log_listener is None:
                self._update_logger({})

    def _logs_callback(self, record):
        message = record.record.message

        with self._known_logs_lock:
            if message in self._known_logs:
                return
            else:
                self._known_logs.add(message)

        log_level = logging_pb2.LogRecord.Level.Name(record.record.level)
        msg = "{}: {} - {}\n".format(self.name, log_level, message)
        self.log_output.append_stdout(msg)

    def __del__(self):
        self._stop_logger()
        super(WorkflowsLayer, self).__del__()

    def forget_logs(self):
        """
        Clear the set of known log records, so they are re-displayed if they occur again

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> wf.map # doctest: +SKIP
        >>> layer = img.visualize("sample visualization") # doctest: +SKIP
        >>> # ^ will show an error for attempting to visualize more than 3 bands
        >>> layer.forget_logs() # doctest: +SKIP
        >>> wf.map.zoom = 10 # doctest: +SKIP
        >>> # ^ attempting to load more tiles from img will cause the same error to appear
        """
        with self._known_logs_lock:
            self._known_logs.clear()

    def set_scales(self, scales, new_colormap=False):
        """
        Update the scales for this layer by giving a list of scales

        Parameters
        ----------
        scales: list of lists, default None
            The scaling to apply to each band in the `Image` or `ImageCollection`.
            If displaying an `ImageCollection`, it is reduced into an `Image`
            before applying scaling.

            If `Image` or `ImageCollection` contains 3 bands,
            ``scales`` must be a list like ``[(0, 1), (0, 1), (-1, 1)]``.

            If `Image` or `ImageCollection` contains 1 band, ``scales`` must be a list like ``[(0, 1)]``,
            or just ``(0, 1)`` for convenience

            If None, each 256x256 tile will be scaled independently
            based on the min and max values of its data.
        new_colormap: str, None, or False, optional, default False
            A new colormap to set at the same time, or False to use the current colormap.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> img = img.pick_bands("red") # doctest: +SKIP
        >>> layer = img.visualize("sample visualization", colormap="viridis") # doctest: +SKIP
        >>> layer.set_scales((0.08, 0.3), new_colormap="plasma") # doctest: +SKIP
        >>> # ^ optionally set new colormap
        """
        colormap = self.colormap if new_colormap is False else new_colormap

        if scales is not None:
            scales = XYZ._validate_scales(scales)

            scales_len = 1 if colormap is not None else 3
            if len(scales) != scales_len:
                msg = "Expected {} scales, but got {}.".format(scales_len, len(scales))
                if len(scales) in (1, 2):
                    msg += " If displaying a 1-band Image, use a colormap."
                elif colormap:
                    msg += " Colormaps cannot be used with multi-band images."

                raise ValueError(msg)

            with self.hold_trait_notifications():
                if colormap is None:
                    self.r_min = scales[0][0]
                    self.r_max = scales[0][1]
                    self.g_min = scales[1][0]
                    self.g_max = scales[1][1]
                    self.b_min = scales[2][0]
                    self.b_max = scales[2][1]
                else:
                    self.r_min = scales[0][0]
                    self.r_max = scales[0][1]
                if new_colormap is not False:
                    self.colormap = new_colormap
        else:
            # scales is None
            with self.hold_trait_notifications():
                if colormap is None:
                    self.r_min = None
                    self.r_max = None
                    self.g_min = None
                    self.g_max = None
                    self.b_min = None
                    self.b_max = None
                else:
                    self.r_min = None
                    self.r_max = None
                if new_colormap is not False:
                    self.colormap = new_colormap

    def set_parameters(self, **params):
        """
        Set new parameters for this `WorkflowsLayer`.

        In typical cases, you update parameters by assigning to `parameters`
        (like ``layer.parameters.threshold = 6.6``).

        Instead, use this function when you need to change the *names or types*
        of parameters available on the `WorkflowsLayer`. (Users shouldn't need to
        do this, as `~.Image.visualize` handles it for you, but custom widget developers
        may need to use this method when they change the `imagery` field on a `WorkflowsLayer`.)

        If a value is an ipywidgets Widget, it will be linked to that parameter
        (via its ``"value"`` attribute). If a parameter was previously set with
        a widget, and a different widget instance (or non-widget) is passed
        for its new value, the old widget is automatically unlinked.
        If the same widget instance is passed as is already linked, no change occurs.

        Parameters
        ----------
        params: JSON-serializable value, Proxytype, or ipywidgets.Widget
            Paramter names to new values. Values can be Python types,
            `Proxytype` instances, or ``ipywidgets.Widget`` instances.

        Example
        -------

        >>> import descarteslabs.workflows as wf
        >>> from ipywidgets import FloatSlider
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> img = img.pick_bands("red") # doctest: +SKIP
        >>> masked_img = img.mask(img > wf.parameter("threshold", wf.Float)) # doctest: +SKIP
        >>> layer = masked_img.tile_layer("sample", colormap="plasma", threshold=0.07) # doctest: +SKIP
        >>> scaled_img = img * wf.parameter("scale", wf.Float) + wf.parameter("offset", wf.Float) # doctest: +SKIP
        >>> with layer.hold_trait_notifications(): # doctest: +SKIP
        ...     layer.imagery = scaled_img # doctest: +SKIP
        ...     layer.set_parameters(scale=FloatSlider(min=0, max=10, value=2), offset=2.5) # doctest: +SKIP
        >>> # ^ re-use the same layer instance for a new Image with different parameters
        """
        param_set = self.parameters
        if param_set is None:
            param_set = self.parameters = parameters.ParameterSet(self, "parameters")

        with self.hold_trait_notifications():
            param_set.update(**params)

    def _ipython_display_(self):
        param_set = self.parameters
        if param_set:
            widget = param_set.widget
            if widget and len(widget.children) > 0:
                widget._ipython_display_()
