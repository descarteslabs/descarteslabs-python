import json
import logging
import threading
import urllib

import grpc
from descarteslabs.client.version import __version__
from descarteslabs.common.graft import client as graft_client
from descarteslabs.common.proto.xyz import xyz_pb2

from ..cereal import deserialize_typespec, serialize_typespec
from ..client import get_global_grpc_client
from .utils import (
    pb_datetime_to_milliseconds,
    pb_milliseconds_to_datetime,
    py_log_level_to_proto_log_level,
)
from ..execution import arguments_to_grafts, promote_arguments
from ..types import Function
from ..types.widget import serialize_params, deserialize_params


class XYZ(object):
    """
    Stores proxy objects to be rendered by an XYZ tile server.

    Similar to a `Workflow`, but meant for storing proxy objects
    so the XYZ tile service can display them, rather than for persisting
    and sharing workflows between users.

    Use `.url` to generate an XYZ URL template, and `.iter_tile_logs`
    or `.log_listener` to retrieve log messages that happen while
    computing them.

    Examples
    --------
    >>> from descarteslabs.workflows import Image, XYZ
    >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
    >>> rgb = img.pick_bands("red green blue")
    >>> xyz = XYZ.build(rgb, name="My RGB") # doctest: +SKIP
    >>> xyz.save() # doctest: +SKIP
    >>> xyz # doctest: +SKIP
    <descarteslabs.workflows.models.xyz.XYZ object at 0x...>
    >>> xyz.id # doctest: +SKIP
    '24d0e79c5c1e1f10a0b1177ef3974d7edefd5988291cf2c6'
    >>> same_xyz = XYZ.get('24d0e79c5c1e1f10a0b1177ef3974d7edefd5988291cf2c6') # doctest: +SKIP
    >>> same_xyz.url() # doctest: +SKIP
    'https://workflows.descarteslabs.com/master/xyz/24d0e79c5c1e1f10a0b1177ef3974d7edefd5988291cf2c6/{z}/{x}/{y}.png'
    >>> same_xyz.object # doctest: +SKIP
    <descarteslabs.workflows.types.geospatial.image.Image object at 0x...>

    >>> from descarteslabs.workflows import ImageCollection, XYZ
    >>> col = ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
    ...        start_datetime="2017-01-01",
    ...        end_datetime="2017-12-31")
    >>> rgb = col.pick_bands("red green blue")
    >>> xyz = XYZ.build(rgb, name="RGB")
    >>> xyz.save() # doctest: +SKIP
    >>> xyz.url() # doctest: +SKIP
    'https://workflows.descarteslabs.com/tiles-ic/xyz/bdbeb4706f3025f4ee4eeff4dec46f6f2554c583d830e5e9/{z}/{x}/{y}.png'
    """

    def __init__(self, proxy_object, params, proto_message, client=None):
        """
        Construct a XYZ object from a proxy object and Protobuf message.

        Do not use this method directly; use the `XYZ.build` and `XYZ.get`
        classmethods instead.

        Parameters
        ----------
        proxy_object: Proxytype
            The proxy object to store in this XYZ
        params: Tuple[Proxytype]
            Parameter metadata about the arguments to `proxy_object`, if it's a Function
        proto_message: xyz_pb2.XYZ message
            Protobuf message for the XYZ
        client : Compute, optional
            Allows you to use a specific client instance with non-default
            auth and parameters
        """
        if client is None:
            client = get_global_grpc_client()
        self._object = proxy_object
        self._params = params
        self._message = proto_message
        self._client = client

    @classmethod
    def _from_proto(cls, message, client=None):
        typespec = message.typespec
        proxytype = deserialize_typespec(typespec)
        params = deserialize_params(message.parameters)

        if message.serialized_graft:
            graft = json.loads(message.serialized_graft)
            isolated = graft_client.isolate_keys(graft)
            obj = proxytype._from_graft(
                isolated,
                params=() if issubclass(proxytype, Function) else params
                # ^ TODO can this be an accurate assumption? should maybe instead be whether it's a function graft?
            )
        else:
            raise AttributeError(
                (
                    "The serialized graft attribute does not exist or "
                    "acces is not authorized for XYZ '{}'. To share "
                    "objects with others, please use a Workflow instead."
                ).format(message.id)
            )

        return cls(obj, params, message, client=client)

    @classmethod
    def build(cls, proxy_object, name="", description="", client=None):
        """
        Construct a new XYZ from a proxy object.

        If the proxy object depends on any parameters (``proxy_object.params`` is not empty),
        it's first internally converted to a `.Function` that takes those parameters
        (using `.Function.from_object`).

        Note that this does not persist the `XYZ`,
        call `save` on the returned `XYZ` to do that.

        Parameters
        ----------
        proxy_object: Proxytype
            The proxy object to store in this XYZ.
            If it depends on parameters, ``proxy_object`` is first converted
            to a `.Function` that takes those parameters.
        name: str, default ""
            Name for the new XYZ
        description: str, default ""
            Long-form description of this XYZ. Markdown is supported.
        client : Compute, optional
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        XYZ

        Example
        -------
        >>> from descarteslabs.workflows import Image, XYZ
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> xyz = XYZ.build(rgb, name="My RGB") # doctest: +SKIP
        >>> xyz # doctest: +SKIP
        <descarteslabs.workflows.models.xyz.XYZ object at 0x...>
        """
        if client is None:
            client = get_global_grpc_client()

        # TODO what if you're using an actual Function, not a parametrized object?
        # Probably will need named positional arguments to handle that (turn args into params)
        proto_params = serialize_params(proxy_object)
        params = proxy_object.params
        if len(params) > 0:
            # turn objects that depend on parameters into Functions
            proxy_object = Function.from_object(proxy_object)

        typespec = serialize_typespec(type(proxy_object))
        graft = proxy_object.graft

        message = xyz_pb2.XYZ(
            name=name,
            description=description,
            serialized_graft=json.dumps(graft),
            typespec=typespec,
            parameters=proto_params,
            channel=client._wf_channel,
            client_version=__version__,
        )
        return cls(proxy_object, params, message, client=client)

    @classmethod
    def get(cls, xyz_id, client=None):
        """
        Get an existing XYZ by id.

        Parameters
        ----------
        id : string
            The unique id of a `XZY` object
        client : Compute, optional
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        XYZ

        Example
        -------
        >>> from descarteslabs.workflows import XYZ
        >>> xyz = XYZ.get('24d0e79c5c1e1f10a0b1177ef3974d7edefd5988291cf2c6') # doctest: +SKIP
        >>> xyz # doctest: +SKIP
        <descarteslabs.workflows.models.xyz.XYZ object at 0x...>
        """
        if client is None:
            client = get_global_grpc_client()

        message = client.api["GetXYZ"](
            xyz_pb2.GetXYZRequest(xyz_id=xyz_id), timeout=client.DEFAULT_TIMEOUT
        )
        return cls._from_proto(message, client)

    def save(self):
        """
        Persist this XYZ object.

        After saving, ``self.id`` will contain the new ID of the XYZ object.

        Example
        -------
        >>> from descarteslabs.workflows import Image, XYZ
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> xyz = XYZ.build(rgb, name="My RGB") # doctest: +SKIP
        >>> xyz.save() # doctest: +SKIP
        >>> xyz.id # doctest: +SKIP
        '24d0e79c5c1e1f10a0b1177ef3974d7edefd5988291cf2c6'
        """
        message = self._client.api["CreateXYZ"](
            xyz_pb2.CreateXYZRequest(
                name=self._message.name,
                description=self._message.description,
                serialized_graft=self._message.serialized_graft,
                typespec=self._message.typespec,
                parameters=self._message.parameters,
                channel=self._message.channel,
                client_version=self._message.client_version,
            ),
            timeout=self._client.DEFAULT_TIMEOUT,
        )
        self._message = message

    def url(
        self,
        session_id=None,
        colormap=None,
        bands=None,
        scales=None,
        reduction="mosaic",
        checkerboard=False,
        **arguments,
    ):
        """
        XYZ tile URL format-string, like ``https://workflows.descarteslabs.com/v0-5/xyz/1234567/{z}/{x}/{y}.png``

        Parameters
        ----------
        session_id: str, optional, default None
            Unique, client-generated ID that logs will be stored under.
            Since multiple users may access tiles from the same `XYZ` object,
            each user should set their own ``session_id`` to get individual logs.
        colormap: str, optional, default None
            Name of the colormap to use. If set, the displayed `~.geospatial.Image`
            or `~.geospatial.ImageCollection` must have 1 band.
        bands: list of str, optional, default None
            The band names to select from the imagery. If None (default),
            the imagery should already have 1-3 bands selected.
        scales: list of lists, optional, default None
            The scaling to apply to each band in the `~.geospatial.Image`.
            If displaying an `~.geospatial.ImageCollection`, it is reduced into
            an `~.geospatial.Image` before scaling.

            If the `~.geospatial.Image` or `~.geospatial.ImageCollection` contains 3 bands,
            ``scales`` must be a list like ``[(0, 1), (0, 1), (-1, 1)]``.

            If the `~.geospatial.Image` or `~.geospatial.ImageCollection` contains 1 band,
            ``scales`` must be a list like ``[(0, 1)]``, or just ``(0, 1)`` for convenience

            If None, each 256x256 tile will be scaled independently.
        reduction: str, optional, default "mosaic"
            One of "mosaic", "min", "max", "mean", "median", "sum", "std", or "count".
            If displaying an `~.geospatial.ImageCollection`, this method is used to reduce it into
            an `~.geospatial.Image`. The reduction is performed before applying a colormap or scaling.
            If displaying an `~.geospatial.Image`, reduction is ignored.
        checkerboard: bool, default False
            Whether to display a checkerboarded background for missing or masked data.
        **arguments: Any
            Values for all parameters that the `object` depends on
            (or arguments that `object` takes, if it's a `.Function`).
            Can be given as Proxytypes, or as Python objects like numbers,
            lists, and dicts that can be promoted to them.
            These arguments cannot depend on any parameters.

        Returns
        -------
        url: str
            Tile URL containing ``{z}``, ``{x}``, and ``{y}`` as Python format string parameters,
            and query arguments URL-quoted.

        Raises
        ------
        ValueError
            If the `XYZ` object has no `id` and `.save` has not been called yet.

        TypeError
            If the ``scales`` are of invalid type.

            If the ``arguments`` names or types don't match the `params`
            that the `object` depends on.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> red = img.pick_bands("red")
        >>> viz = red ** wf.parameter("exponent", wf.Float)
        >>> xyz = wf.XYZ.build(viz, name="Red band raised to exponent")
        >>> xyz.save() # doctest: +SKIP
        >>> xyz.url("some_session", colormap="magma", scales=[(0.2, 0.8)], exponent=2.5) # doctest: +SKIP
        'https://workflows.descarteslabs.com/master/xyz/0d21037edb4bdd16b735f24bb3bff6d4202a71c20404b101/
         {z}/{x}/{y}.png?session_id=some_session&colormap=magma&scales=[[0.2, 0.8]]&exponent=2.5'

        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> red = col.pick_bands("red")
        >>> viz = red ** wf.parameter("exponent", wf.Float)
        >>> xyz = wf.XYZ.build(viz, name="Red band ImageCollection raised to exponent")
        >>> xyz.save() # doctest: +SKIP
        >>> xyz.url("some_session", reduction="min", exponent=2.5) # doctest: +SKIP
        'https://workflows.descarteslabs.com/tiles-ic/xyz/bdbeb4706f3025f4ee4eeff4dec46f6f2554c583d830e5e9/
         {z}/{x}/{y}.png?session_id=some_session&reduction=min&exponent=2.5'
        """
        url = self._message.url_template
        if not url:
            raise ValueError(
                "This XYZ object has not been persisted yet; call .save() to do so."
            )

        query_args = {}
        if session_id is not None:
            query_args["session_id"] = session_id
        if colormap is not None:
            query_args["colormap"] = colormap
        if reduction != "mosaic":
            # We don't include a reduction query arg if it's the default value
            query_args["reduction"] = reduction
        if checkerboard:
            query_args["checkerboard"] = "true"

        if bands is not None:
            try:
                nbands = len(bands)
            except Exception:
                raise TypeError(f"bands must be a sequence; got {bands!r}")

            if nbands > 3:
                raise ValueError(
                    f"Up to 3 bands may be specified, not {nbands}: {bands!r}"
                )

            query_args["band"] = bands

        if scales is not None:
            scales = self._validate_scales(scales)

            if any(scale != [None, None] for scale in scales):
                query_args["scales"] = json.dumps(scales)

        promoted_arguments = promote_arguments(arguments, self.params)
        if promoted_arguments:
            query_args.update(
                {
                    param: json.dumps(graft)
                    for param, graft in arguments_to_grafts(
                        **promoted_arguments
                    ).items()
                }
            )

        if query_args:
            url = url + "?" + urllib.parse.urlencode(query_args, doseq=True)
        return url

    @staticmethod
    def _validate_scales(scales):
        """
        Validate and normalize a list of scales for an XYZ layer.

        A _scaling_ is a 2-tuple (or 2-list) like ``[min, max]``,
        meaning the range of values in your data you want to stretch
        to the 0..255 output range.

        If ``min`` and ``max`` are ``None``, the min/max values in the
        data will be used automatically. Since each tile is computed
        separately and may have different min/max values, this can
        create a "patchwork" effect when viewed on a map.

        Scales can be given as:

        * Three scalings, for 3-band images, like ``[[0, 1], [0, 0.5], [None, None]]``
        * Two scalings, for 2-band images, like ``[[0, 1], [None, None]]``
        * 1-list/tuple of 1 scaling, for 1-band images, like ``[[0, 1]]``
        * 1 scaling (for convenience), which is equivalent to the above: ``[0, 1]``
        * None, or an empty list or tuple for no scalings

        Parameters
        ----------
        scales: list, tuple, or None
            The scales to validate, in the format shown above

        Returns
        -------
        scales: list
            0- to 3-length list of scalings, where each item is a float.
            (``[0, 1]`` would become ``[[0.0, 1.0]]``, for example.)
            If no scalings are given, an empty list is returned.

        Raises
        ------
        TypeError, ValueError
            If the scales do not match the correct format
        """
        if scales is not None:
            if not isinstance(scales, (list, tuple)):
                raise TypeError(
                    "Expected a list or tuple of scales, but got {}".format(scales)
                )

            if (
                len(scales) == 2
                and not isinstance(scales[0], (list, tuple))
                and not isinstance(scales[1], (list, tuple))
            ):
                # allow a single 2-tuple for convenience with colormaps/1-band images
                scales = (scales,)

            if len(scales) > 3:
                raise (
                    ValueError(
                        "Too many scales passed: expected up to 3 scales, but got {}".format(
                            len(scales)
                        )
                    )
                )

            for i, scaling in enumerate(scales):
                if not isinstance(scaling, (list, tuple)):
                    raise TypeError(
                        "Scaling {}: expected a 2-item list or tuple for the scaling, "
                        "but got {}".format(i, scaling)
                    )
                if len(scaling) != 2:
                    raise ValueError(
                        "Scaling {}: expected a 2-item list or tuple for the scaling, "
                        "but length was {}".format(i, len(scaling))
                    )
                if not all(isinstance(x, (int, float, type(None))) for x in scaling):
                    raise TypeError(
                        "Scaling {}: items in scaling must be numbers or None; "
                        "got {}".format(i, scaling)
                    )
                # At this point we know they are all int, float, or None
                # So we check to see if we have an int/float and a None
                if any(isinstance(x, (int, float)) for x in scaling) and any(
                    x is None for x in scaling
                ):
                    raise ValueError(
                        "Invalid scales passed: one number and one None in scaling {}".format(
                            i, scaling
                        )
                    )

            return [
                [float(x) if isinstance(x, int) else x for x in scaling]
                for scaling in scales
            ]
            # be less strict about floats than traitlets is
        else:
            return []

    def iter_tile_logs(self, session_id, start_datetime=None, level=None):
        """
        Iterator over log messages generated while computing tiles

        Parameters
        ----------
        session_id: str
            Unique, client-generated that logs are stored under.
        start_datetime: datetime.datetime
            Only return log records occuring after this datetime
        level: int, default logging.DEBUG
            Only return log records at or above this log level.
            See https://docs.python.org/3/library/logging.html#logging-levels for valid
            log levels.

        Yields
        ------
        log_record: descarteslabs.common.proto.xyz_pb2.XYZLogRecord
            Logs in protobuf message objects,
            with fields ``session_id`` and ``record``, with the ``record`` field
            containing ``level``, ``message``, and ``timestamp``.

        Example
        -------
        >>> from descarteslabs.workflows import Image, XYZ
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> xyz = XYZ.build(rgb, name="My RGB") # doctest: +SKIP
        >>> url = xyz.url(session_id="some_session") # doctest: +SKIP
        >>> for record in xyz.iter_tile_logs("some_session", start_datetime=datetime.datetime.now()): # doctest: +SKIP
        ...     print(record.level, record.message)
        >>>     # any logs that occur loading tiles from the generated URL will be printed here
        """
        return _tile_log_stream(
            self.id,
            session_id,
            start_datetime=start_datetime,
            level=level,
            client=self._client,
        )

    def log_listener(self):
        """An `XYZLogListener` to trigger callbacks when logs occur computing tiles.

        Example
        -------
        >>> from descarteslabs.workflows import Image, XYZ
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> xyz = XYZ.build(rgb, name="My RGB") # doctest: +SKIP
        >>> url = xyz.url(session_id="some_session") # doctest: +SKIP
        >>> listener = xyz.log_listener() # doctest: +SKIP
        >>> log = []
        >>> listener.add_callback(lambda record: log.append(record.message)) # doctest: +SKIP
        >>> listener.listen("some_session") # doctest: +SKIP
        >>> # any logs that occur loading tiles from the generated URL will be appended
        >>> # to `log` in the background
        """
        return XYZLogListener(self.id, client=self._client)

    @property
    def object(self):
        """
        Proxytype: The proxy object of this XYZ.

        Note that if the XYZ was originally constructed from an object that depended on parameters,
        then this `object` property won't hold the same object, but rather a `.Function` which takes
        those parameters and returns that object.

        Raises ValueError if the XYZ is not compatible with the current channel.
        """
        if self.channel != self._client._wf_channel:
            raise ValueError(
                "This client is compatible with channel '{}', "
                "but the XYZ '{}' is only defined for channel '{}'.".format(
                    self._client._wf_channel, self.id, self.channel
                )
            )
        return self._object

    @property
    def params(self):
        """
        tuple: Parameter objects corresponding to the arguments to `object`, if it's a `.Function`.

        These represent any parameters (including widget objects from ``wf.widgets``) that
        `object` depended on before it became a `.Function`.
        """
        return self._params

    @property
    def type(self):
        "type: The type of the proxy object."
        return type(self._object)

    @property
    def id(self):
        """
        str or None: The globally unique identifier for the XYZ,
        or None if it hasn't been saved yet.
        """
        return None if self._message.id == "" else self._message.id

    @property
    def created_timestamp(self):
        """
        datetime.datetime or None: The UTC date this XYZ was created,
        or None if it hasn't been saved yet. Cannot be modified.
        """
        return pb_milliseconds_to_datetime(self._message.created_timestamp)

    @property
    def updated_timestamp(self):
        """
        datetime.datetime or None: The UTC date this XYZ was most recently modified,
        or None if it hasn't been saved yet. Updated automatically.
        """
        return pb_milliseconds_to_datetime(self._message.updated_timestamp)

    @property
    def name(self):
        "str: The name of this XYZ."
        return self._message.name

    @property
    def description(self):
        "str: A long-form description of this xyz. Markdown is supported."
        return self._message.description

    @property
    def channel(self):
        "str: The channel name this XYZ is compatible with."
        return self._message.channel

    @property
    def user(self):
        "str: The user which created this XYZ."
        return self._message.user

    @property
    def org(self):
        "str: The org of the user which created this XYZ."
        return self._message.org


class XYZLogListener(object):
    """
    Calls callback functions in a background thread when XYZ log records occur.

    Note: the thread is automatically cleaned up on garbage collection.

    Example
    -------
    >>> from descarteslabs.workflows import Image, XYZ
    >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
    >>> xyz = XYZ.build(img)
    >>> xyz.save()  # doctest: +SKIP
    >>> listener = xyz.log_listener()
    >>> def callback(msg):
    ...     print(msg.level, msg.message)
    >>> listener.add_callback(callback)
    >>> listener.listen("session_id", start_datetime=datetime.datetime.now())  # doctest: +SKIP
    >>> # later
    >>> listener.stop()  # doctest: +SKIP
    """

    def __init__(self, xyz_id, client=None):
        self.xyz_id = xyz_id
        self.callbacks = []
        self._rendezvous = None
        self._thread = None
        self._client = client if client is not None else get_global_grpc_client()

    def add_callback(self, callback):
        """
        Function will be called with ``descarteslabs.common.proto.xyz_pb2.XYZLogRecord``
        on each log record.

        Parameters
        ----------
        callback: callable
            Function that takes one argument, a
            ``descarteslabs.common.proto.xyz_pb2.XYZLogRecord`` protobuf message object.
            This message contains the fields ``code``, ``message``,
            ``timestamp``, ``session_id``.
            This message contains the fields ``session_id`` and ``record``, with the
            ``record`` field containing ``level``, ``message``, and ``timestamp``.

            The function will be called within a separate thread,
            therefore it must behave thread-safely. Any errors raised by the function will
            terminate the listener.

        Example
        -------
        >>> from descarteslabs.workflows import XYZLogListener
        >>> listener = XYZLogListener("xyz_id") # doctest: +SKIP
        >>> def callback(msg):
        ...     print(msg.level, msg.message)
        >>> listener.add_callback(callback) # doctest: +SKIP
        """
        self.callbacks.append(callback)

    def listen(self, session_id, start_datetime=None, level=None):
        """
        Start listening for logs.

        Parameters
        ----------
        session_id: str
            Unique, client-generated ID that logs are stored under.
            See `XYZ.url` for more information.
        start_datetime: datetime.datetime
            Only listen for log records occuring after this datetime. Must be tz-aware.
        level: int, default logging.DEBUG
            Only listen for log records at or above this log level.
            See https://docs.python.org/3/library/logging.html#logging-levels for valid
            log levels.

        Example
        -------
        >>> from descarteslabs.workflows import XYZLogListener
        >>> listener = XYZLogListener("xyz_id") # doctest: +SKIP
        >>> listener.listen(
        ...     "session-id",
        ...     start_datetime=datetime.datetime.now(datetime.timezone.utc),
        ...     level=logging.WARNING,
        ... ) #doctest: +SKIP
        """
        self._rendezvous = _tile_log_stream(
            self.xyz_id,
            session_id,
            start_datetime=start_datetime,
            level=level,
            client=self._client,
        )
        self._thread = threading.Thread(target=self._listener)
        self._thread.daemon = True
        self._thread.start()

    def running(self):
        """bool: whether this is an active listener

        Example
        -------
        >>> from descarteslabs.workflows import XYZLogListener
        >>> listener = XYZLogListener("xyz_id") # doctest: +SKIP
        >>> listener.listen("session-id", start_datetime=datetime.datetime.now()) # doctest: +SKIP
        >>> listener.running() # doctest: +SKIP
        True
        """
        return self._thread and self._thread.is_alive()

    def stop(self, timeout=None):
        """
        Cancel and clean up the listener. Blocks up to ``timeout`` seconds, or forever if None.

        Returns True if the background thread stopped successfully.

        Example
        -------
        >>> from descarteslabs.workflows import XYZLogListener
        >>> listener = XYZLogListener("xyz_id") # doctest: +SKIP
        >>> listener.listen("session-id", start_datetime=datetime.datetime.now()) # doctest: +SKIP
        >>> listener.stop() # doctest: +SKIP
        >>> listener.running() # doctest: +SKIP
        False
        """
        self._rendezvous.cancel()
        self._thread.join(timeout)
        return not self._thread.is_alive()

    def _listener(self):
        try:
            for msg in self._rendezvous:
                for callback in self.callbacks:
                    callback(msg)
        except grpc.RpcError:
            return

    def __del__(self):
        if self.running():
            self.stop(0)


def _tile_log_stream(xyz_id, session_id, start_datetime=None, level=None, client=None):
    if client is None:
        client = get_global_grpc_client()

    if start_datetime is None:
        start_timestamp = 0
    else:
        start_timestamp = pb_datetime_to_milliseconds(start_datetime)

    if level is None:
        level = logging.DEBUG
    else:
        level = py_log_level_to_proto_log_level(level)

    msg = xyz_pb2.GetXYZSessionLogsRequest(
        session_id=session_id,
        xyz_id=xyz_id,
        start_timestamp=start_timestamp,
        level=level,
    )

    return client.api["GetXYZSessionLogs"](msg, timeout=client.STREAM_TIMEOUT)
