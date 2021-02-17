from typing import Union
import datetime
import logging
import uuid
import threading
import warnings
import contextlib

import ipyleaflet
import ipywidgets as widgets
import traitlets

from descarteslabs.common.proto.logging import logging_pb2

from descarteslabs.common.graft import client as graft_client
from ..models import XYZ
from ..models.tile_url import validate_scales
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
        Read-only: the `~.geospatial.Image` or `~.geospatial.ImageCollection` to use.
        Change it with `set_imagery`.
    value: ~.geospatial.Image or ~.geospatial.ImageCollection
        Read-only: a parametrized version of `imagery`, with all the values of `parameters`
        embedded in it.
    image_value: ~.geospatial.Image
        Read-only: a parametrized version of `imagery` as an `~.geospatial.Image`,
        with any `reduction` applied and all the values of `parameters` embedded in it
    parameters: ParameterSet
        Parameters to use while computing; modify attributes under ``.parameters``
        (like ``layer.parameters.foo = "bar"``) to cause the layer to recompute
        and update under those new parameters. This trait is read-only in that you
        can't do ``layer.parameters = a_new_parameter_set``, but you can change the attributes
        *within* ``layer.parameters``.
    clear_on_update: bool, default True
        Whether to clear all tiles from the map as soon as the layer changes, or leave out-of-date
        tiles visible until new ones have loaded. True (default) makes it easier to tell whether
        the layer is done loading and up-to-date or not. False prevents fast-loading layers from
        appearing to "flicker" as you interact with them.
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
    clear_on_update = traitlets.Bool(default_value=True)

    imagery = traitlets.Union(
        [
            traitlets.Instance(Image, read_only=True),
            traitlets.Instance(ImageCollection, read_only=True),
        ]
    )
    value = traitlets.Union(
        [
            traitlets.Instance(Image, read_only=True, allow_none=True),
            traitlets.Instance(ImageCollection, read_only=True, allow_none=True),
        ]
    )
    image_value = traitlets.Instance(Image, read_only=True, allow_none=True)

    parameters = traitlets.Instance(parameters.ParameterSet, read_only=True)
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

    def __init__(
        self,
        imagery,
        scales=None,
        colormap=None,
        checkerboard=None,
        reduction=None,
        log_level=logging.DEBUG,
        parameter_overrides=None,
        **kwargs,
    ):
        if parameter_overrides is None:
            parameter_overrides = {}

        self._url_updates_blocked = False
        super().__init__(**kwargs)

        with self.hold_url_updates():
            self.set_trait("parameters", parameters.ParameterSet(self, "parameters"))
            self.set_scales(scales, new_colormap=colormap)
            if reduction is not None:
                self.reduction = reduction
            self.checkerboard = checkerboard
            self.log_level = log_level
            self.set_imagery(imagery, **parameter_overrides)

            self.set_trait("session_id", uuid.uuid4().hex)
            self.set_trait(
                "autoscale_progress",
                ClearableOutput(
                    widgets.Output(),
                    layout=widgets.Layout(max_height="10rem", flex="1 0 auto"),
                ),
            )

        self._log_listener = None
        self._known_logs = set()
        self._known_logs_lock = threading.Lock()

    def set_imagery(
        self, imagery: Union[Image, ImageCollection], **parameter_overrides
    ):
        """
        Set a new `~.geospatial.Image` or `~.geospatial.ImageCollection` object for this layer to use.
        You can set/override the values of any parameters the imagery depends on
        by passing them as kwargs.

        If the imagery depends on parameters that don't have default values (created with
        ``wf.parameter("name", wf.Int)`` for example, versus ``wf.widget.input("name", default=1)``),
        then you *must* pass values for those parameters.

        Parameters
        ----------
        **parameter_overrides: JSON-serializable value, Proxytype, or ipywidgets.Widget
            Paramter names to values. Values can be Python types,
            `Proxytype` instances, or ``ipywidgets.Widget`` instances.
            Names must correspond to parameters that ``imagery`` depends on.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> wf.map # doctest: +SKIP
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> red = img.pick_bands("red") # doctest: +SKIP
        >>> blue = img.pick_bands("blue") # doctest: +SKIP
        >>> layer = red.visualize(name="sample visualization") # doctest: +SKIP
        >>> layer.set_imagery(blue) # doctest: +SKIP
        """
        if not isinstance(imagery, (Image, ImageCollection)):
            raise TypeError(
                f"imagery must be an Image or ImageCollection, not {imagery!r}."
            )

        # Combine the parameter dependencies from `imagery` with any overrides
        # and raise an error for any missing or unexpected parameter overrides.
        # We don't do any typechecking of parameter values here; that'll be dealt with
        # later (within `ParameterSet` when trying to actually assign the trait values).
        merged_params = {}
        for param in imagery.params:
            name = param._name
            try:
                merged_params[name] = parameter_overrides.pop(name)
                # TODO when you override the value of a widget-based parameter, you'd like to keep
                # the same type of widget (but a new instance in case it's linked to other stuff)
            except KeyError:
                try:
                    merged_params[name] = param.widget
                except AttributeError:
                    raise ValueError(
                        f"Missing required parameter {name!r} ({type(param).__name__}) "
                        f"for layer {self.name!r}"
                    ) from None
        if parameter_overrides:
            raise ValueError(
                f"Unexpected parameters {tuple(parameter_overrides)}. This layer only "
                f"accepts the parameters {tuple(p._name for p in imagery.params)}."
            )

        xyz_warnings = []
        with self.hold_url_updates():
            if not self.trait_has_value("imagery") or imagery is not self.imagery:
                # `trait_has_value` is False when the layer is first constructed;
                # accessing `self.imagery` would cause a validation error in that case.
                with warnings.catch_warnings(record=True) as xyz_warnings:
                    xyz = XYZ(
                        imagery,
                        name=self.name,
                        public=False,
                    )
                self.set_trait("imagery", imagery)
                self.set_trait("xyz_obj", xyz)

            self.parameters.update(**merged_params)

        # NOTE: we log after the `hold_url_updates` block so our messages don't get immediately cleared
        for w in xyz_warnings:
            self._log(w.message)

    def trait_has_value(self, name):
        # Backport for traitlets < 5.0, to maintain py3.6 support.
        # Remove after support for py3.6 is dropped.
        # Copied from
        # https://github.com/ipython/traitlets/blob/2bb2597224ca5ae485761781b11c06141770f110/traitlets/traitlets.py#L1496-L1516

        return name in self._trait_values

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

        # assume a None parameter value means the value is missing
        # and we can't render the layer.
        # primarily for the `LayerPicker` widget, which can have no layer selected.
        if any(v is None for v in parameters.values()):
            return ""

        return self.xyz_obj.url(
            session_id=self.session_id,
            colormap=self.colormap or "",
            scales=scales,
            reduction=self.reduction,
            checkerboard=self.checkerboard,
            **parameters,
        )

    @contextlib.contextmanager
    def hold_url_updates(self):
        """
        Context manager to prevent the layer URL from being updated multiple times.

        When leaving the context manager, the URL is always updated exactly once.

        Also applies ``hold_trait_notifications``.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> img = img.pick_bands("red") # doctest: +SKIP
        >>> layer = img.visualize("sample visualization", colormap="viridis") # doctest: +SKIP

        >>> with layer.hold_url_updates(): # doctest: +SKIP
        ...     layer.checkerboard = False # doctest: +SKIP
        ...     layer.set_scales([0, 1], new_colormap="magma") # doctest: +SKIP
        ...     layer.set_scales([0, 1], new_colormap="magma") # doctest: +SKIP
        >>> # ^ the layer will now update only once, instead of 3 times.
        """
        if self._url_updates_blocked:
            yield
        else:
            try:
                self._url_updates_blocked = True
                with self.hold_trait_notifications():
                    yield
            finally:
                self._url_updates_blocked = False
            self._update_url({})

    @traitlets.observe("xyz_obj", "parameters", type=traitlets.All)
    def _update_value(self, change):
        if not self.trait_has_value("xyz_obj"):
            # avoids crazy tracebacks in __init__ when given bad arguments,
            # and `hold_trait_notifications` tries to fire notifiers _before_
            # reraising the exception: `xyz_obj` might not be set yet,
            # and accessing it would cause its own spew of confusing traitlets errors.
            return

        if len(self.xyz_obj.params) > 0:
            try:
                # attept to promote parameters as the Function's arguments
                args, kwargs = self.xyz_obj.object._promote_arguments(
                    **self.parameters.to_dict()
                )
            except Exception:
                # when arguments are invalid (currently, only if a LayerPicker has no layer selected),
                # `value` is None
                self.set_trait("value", None)
                return
        else:
            args, kwargs = (), {}

        graft = graft_client.apply_graft("XYZ.use", self.xyz_obj.id, *args, **kwargs)

        self.set_trait(
            "value",
            self.imagery._from_graft(graft),
        )

    @traitlets.observe("value", "reduction")
    def _update_image_value(self, change):
        # TODO do we actually want this thing that's going to inspect/compute
        # to be an XYZ reference instead of full graft? possibly not.
        # although weirdly, if the firestore doc is cached on the tiles server,
        # sending the xyz ID might actually be a tiny tiny bit faster than the whole graft
        # with network latency... idk.
        value = self.value
        if isinstance(value, ImageCollection):
            value = value.reduction(self.reduction, axis="images")

        self.set_trait("image_value", value)

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
    def _update_url(self, change):
        if self._url_updates_blocked:
            return
        try:
            self.set_trait("url", self.make_url())
        except ValueError as e:
            if "Invalid scales passed" not in str(e):
                raise e
        self.clear_logs()
        if self.clear_on_update:
            self.redraw()

    @traitlets.observe("parameters", type="delete")
    # traitlets is dumb and decorator stacking doesn't work so we have to repeat this
    def _update_url_on_param_delete(self, change):
        if self._url_updates_blocked:
            return
        try:
            self.set_trait("url", self.make_url())
        except ValueError as e:
            if "Invalid scales passed" not in str(e):
                raise e
        self.clear_logs()
        if self.clear_on_update:
            self.redraw()

    def _log(self, message: str, level: int = logging_pb2.LogRecord.Level.WARNING):
        "Log a message to the error output (if there is one), without duplicates"
        if self.log_output is None:
            return

        with self._known_logs_lock:
            if message in self._known_logs:
                return
            else:
                self._known_logs.add(message)

        log_level = logging_pb2.LogRecord.Level.Name(level)
        msg = "{}: {} - {}\n".format(self.name, log_level, message)
        self.log_output.append_stdout(msg)

    @traitlets.observe("xyz_obj", "session_id", "log_level")
    def _update_logger(self, change):
        if self.log_output is None:
            return

        self.clear_logs()

        if self._log_listener is not None:
            self._log_listener.stop(timeout=1)

        listener = self.xyz_obj.log_listener()
        listener.add_callback(
            lambda record: self._log(record.record.message, level=record.record.level)
        )
        with warnings.catch_warnings(record=True) as ws:
            listener.listen(
                self.session_id,
                datetime.datetime.now(datetime.timezone.utc),
                level=self.log_level,
            )

        self._log_listener = listener
        for w in ws:
            self._log(w.message)

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

    def __del__(self):
        self._stop_logger()
        self.clear_logs()
        super(WorkflowsLayer, self).__del__()

    def forget_logs(self):
        """
        Clear the set of known log records, so they are re-displayed if they occur again

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> wf.map # doctest: +SKIP
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> layer = img.visualize("sample visualization") # doctest: +SKIP
        >>> # ^ will show an error for attempting to visualize more than 3 bands
        >>> layer.forget_logs() # doctest: +SKIP
        >>> wf.map.zoom = 10 # doctest: +SKIP
        >>> # ^ attempting to load more tiles from img will cause the same error to appear
        """
        with self._known_logs_lock:
            self._known_logs.clear()

    def clear_logs(self):
        """
        Clear any logs currently displayed for this layer

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> wf.map # doctest: +SKIP
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> layer = img.visualize("sample visualization") # doctest: +SKIP
        >>> # ^ will show an error for attempting to visualize more than 3 bands
        >>> layer.clear_logs() # doctest: +SKIP
        >>> # ^ the errors will disappear
        >>> wf.map.zoom = 10 # doctest: +SKIP
        >>> # ^ attempting to load more tiles from img will cause the same error to appear
        """
        if self.log_output is None:
            return

        self.forget_logs()
        new_logs = []
        for error in self.log_output.outputs:
            if not error["text"].startswith(self.name + ": "):
                new_logs.append(error)
        self.log_output.outputs = tuple(new_logs)

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
        >>> wf.map # doctest: +SKIP
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1") # doctest: +SKIP
        >>> img = img.pick_bands("red") # doctest: +SKIP
        >>> layer = img.visualize("sample visualization", colormap="viridis") # doctest: +SKIP
        >>> layer.set_scales((0.08, 0.3), new_colormap="plasma") # doctest: +SKIP
        >>> # ^ optionally set new colormap
        """
        colormap = self.colormap if new_colormap is False else new_colormap

        if scales is not None:
            scales = validate_scales(scales)

            scales_len = 1 if colormap is not None else 3
            if len(scales) != scales_len:
                msg = "Expected {} scales, but got {}.".format(scales_len, len(scales))
                if len(scales) in (1, 2):
                    msg += " If displaying a 1-band Image, use a colormap."
                elif colormap:
                    msg += " Colormaps cannot be used with multi-band images."

                raise ValueError(msg)

            with self.hold_url_updates():
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
            with self.hold_url_updates():
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

    def get_scales(self):
        """
        Get scales for a layer.

        Returns
        -------
        scales: List[List[int]] or None
            A list containing a list of scales for each band in the layer or None if the layer has no scales set.


        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80330352016022_v1")
        >>> img = img.pick_bands("red") # doctest: +SKIP
        >>> layer = img.visualize("sample visualization") # doctest: +SKIP
        >>> layer.set_scales((0.08, 0.3), 'viridis') # doctest: +SKIP
        >>> layer.get_scales() # doctest: +SKIP
        [[0.08, 0.3]]
        """
        if self.r_min is None:
            return None
        if self.colormap:
            return [[self.r_min, self.r_max]]
        else:
            return [
                [self.r_min, self.r_max],
                [self.g_min, self.g_max],
                [self.b_min, self.b_max],
            ]

    def _ipython_display_(self):
        param_set = self.parameters
        if param_set:
            widget = param_set.widget
            if widget and len(widget.children) > 0:
                widget._ipython_display_()
