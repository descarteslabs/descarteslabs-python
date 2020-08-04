import os
import json
import threading
import urllib

import six

import grpc
from descarteslabs.common.graft import client as graft_client
from descarteslabs.common.proto.xyz import xyz_pb2

from .. import _channel
from ..cereal import deserialize_typespec, serialize_typespec
from ..client import get_global_grpc_client
from .utils import pb_datetime_to_milliseconds, pb_milliseconds_to_datetime
from .parameters import parameters_to_grafts


class XYZ(object):
    """
    Stores proxy objects to be rendered by an XYZ tile server.

    Similar to a `Workflow`, but meant for storing proxy objects
    so the XYZ tile service can display them, rather than for persisting
    and sharing workflows between users.

    Use `.url` to generate an XYZ URL template, and `.iter_tile_errors`
    or `.error_listener` to retrieve error messages that happen while
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
    """

    BASE_URL = "https://" + os.environ.get(
        "DESCARTESLABS_WORKFLOWS_TILES_HOST", "workflows.descarteslabs.com"
    )

    def __init__(self, proxy_object, proto_message, client=None):
        """
        Construct a XYZ object from a proxy object and Protobuf message.

        Do not use this method directly; use the `XYZ.build` and `XYZ.get`
        classmethods instead.

        Parameters
        ----------
        proxy_object: Proxytype
            The proxy object to store in this XYZ
        proto_message: xyz_pb2.XYZ message
            Protobuf message for the XYZ
        client : Compute, optional
            Allows you to use a specific client instance with non-default
            auth and parameters
        """
        if client is None:
            client = get_global_grpc_client()
        self._object = proxy_object
        self._message = proto_message
        self._client = client

    @classmethod
    def _from_proto(cls, message, client=None):
        typespec = message.typespec
        proxytype = deserialize_typespec(typespec)

        if message.serialized_graft:
            graft = json.loads(message.serialized_graft)
            isolated = graft_client.isolate_keys(graft)
            obj = proxytype._from_graft(isolated)
        else:
            raise AttributeError(
                (
                    "The serialized graft attribute does not exist or "
                    "acces is not authorized for XYZ '{}'. To share "
                    "objects with others, please use a Workflow instead."
                ).format(message.id)
            )

        return cls(obj, message, client=client)

    @classmethod
    def build(cls, proxy_object, name="", description="", client=None):
        """
        Construct a new XYZ from a proxy object.

        Note that this does not persist the `XYZ`,
        call `save()` on the returned `XYZ` to do that.

        Parameters
        ----------
        proxy_object: Proxytype
            The proxy object to store in this XYZ
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
        typespec = serialize_typespec(type(proxy_object))
        graft = proxy_object.graft

        message = xyz_pb2.XYZ(
            name=name,
            description=description,
            serialized_graft=json.dumps(graft),
            typespec=typespec,
            channel=_channel.__channel__,
        )
        return cls(proxy_object, message, client=client)

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
        Persist this XYZ layer.

        After saving, ``self.id`` will contain the new ID of the XYZ layer.

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
                channel=self._message.channel,
            ),
            timeout=self._client.DEFAULT_TIMEOUT,
        )
        self._message = message

    def url(
        self,
        session_id=None,
        colormap=None,
        scales=None,
        checkerboard=False,
        **parameters
    ):
        """
        XYZ tile URL format-string, like ``https://workflows.descarteslabs.com/v0-5/xyz/1234567/{z}/{x}/{y}.png``

        Parameters
        ----------
        session_id: str, optional, default None
            Unique, client-generated ID that error logs will be stored under.
            Since multiple users may access tiles from the same `XYZ` object,
            each user should set their own ``session_id`` to get individual error logs.
        colormap: str, optional, default None
            Name of the colormap to use. If set, the displayed `~.geospatial.Image` must have 1 band.
        scales: list of lists, optional, default None
            The scaling to apply to each band in the `~.geospatial.Image` this `XYZ` object will display.

            If the `~.geospatial.Image` contains 3 bands, ``scales`` must be a list like ``[(0, 1), (0, 1), (-1, 1)]``.

            If the `~.geospatial.Image` contains 1 band, ``scales`` must be a list like ``[(0, 1)]``,
            or just ``(0, 1)`` for convenience

            If None, each 256x256 tile will be scaled independently.
        checkerboard: bool, default False
            Whether to display a checkerboarded background for missing or masked data.
        parameters: dict[str, Union[Proxytype, json_serializable_value]]
            Parameters to use while computing.

            Each argument must be the name of a parameter created with `~.identifier.parameter`.
            Each value must be a JSON-serializable type (``bool``, ``int``, ``float``,
            ``str``, ``list``, ``dict``, etc.), a `Proxytype` (like `~.geospatial.Image` or `.Timedelta`),
            or a value that `proxify` can handle (like a ``datetime.datetime``).

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
            If the ``scales`` or ``parameters`` are of invalid type.

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
        """
        if self.id is None:
            raise ValueError(
                "This XYZ object has not been persisted yet; call .save() to do so."
            )
        url = "{base}/{channel}/xyz/{id}/{{z}}/{{x}}/{{y}}.png".format(
            base=self.BASE_URL, channel=self.channel, id=self.id
        )

        query_args = {}
        if session_id is not None:
            query_args["session_id"] = session_id
        if colormap is not None:
            query_args["colormap"] = colormap
        if checkerboard:
            query_args["checkerboard"] = "true"

        if scales is not None:
            scales = self._validate_scales(scales)

            if any(scale != [None, None] for scale in scales):
                query_args["scales"] = json.dumps(scales)

        if parameters is not None:
            query_args.update(
                {
                    param: json.dumps(graft)
                    for param, graft in six.iteritems(
                        parameters_to_grafts(**parameters)
                    )
                }
            )

        if query_args:
            url = url + "?" + urllib.parse.urlencode(query_args)
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
            0, 1- or 3-length list of scalings, where each item is a float.
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

            if len(scales) not in (0, 1, 3):
                raise (
                    ValueError(
                        "Invalid scales passed: expected 0, 1, or 3 scales, but got {}"
                        .format(len(scales))
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
                        "Invalid scales passed: one number and one None in scaling {}"
                        .format(i, scaling)
                    )

            return [
                [float(x) if isinstance(x, int) else x for x in scaling]
                for scaling in scales
            ]
            # be less strict about floats than traitlets is
        else:
            return []

    def iter_tile_errors(self, session_id, start_datetime=None):
        """
        Iterator over errors generated while computing tiles

        Parameters
        ----------
        session_id: str
            Unique, client-generated that error logs are stored under.
        start_datetime: datetime.datetime
            Only return errors occuring after this datetime

        Yields
        ------
        error: descarteslabs.common.proto.xyz_pb2.XYZError
            Errors in protobuf message objects,
            with fields ``code``, ``message``, ``timestamp``, ``session_id``.

        Example
        -------
        >>> from descarteslabs.workflows import Image, XYZ
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> xyz = XYZ.build(rgb, name="My RGB") # doctest: +SKIP
        >>> url = xyz.url(session_id="some_session") # doctest: +SKIP
        >>> for error in xyz.iter_tile_errors("some_session", start_datetime=datetime.datetime.now()): # doctest: +SKIP
        ...     print(error.code, error.message)
        >>>     # any errors that occur loading tiles from the generated URL will be printed here
        """
        return _tile_error_stream(
            self.id, session_id, start_datetime, client=self._client
        )

    def error_listener(self):
        """An `XYZErrorListener` to trigger callbacks when errors occur computing tiles.

        Example
        -------
        >>> from descarteslabs.workflows import Image, XYZ
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> xyz = XYZ.build(rgb, name="My RGB") # doctest: +SKIP
        >>> url = xyz.url(session_id="some_session") # doctest: +SKIP
        >>> listener = xyz.error_listener() # doctest: +SKIP
        >>> errors_log = []
        >>> listener.add_callback(lambda error: errors_log.append(error.message)) # doctest: +SKIP
        >>> listener.listen("some_session") # doctest: +SKIP
        >>> # any errors that occur loading tiles from the generated URL will be appended
        >>> # to `errors_log` in the background
        """
        return XYZErrorListener(self.id, client=self._client)

    @property
    def object(self):
        """
        Proxytype: The proxy object of this XYZ.

        Raises ValueError if the XYZ is not compatible with the current channel.
        """
        if self.channel != _channel.__channel__:
            raise ValueError(
                "This client is compatible with channel '{}', "
                "but the XYZ '{}' is only defined for channel '{}'.".format(
                    _channel.__channel__, self.id, self.channel
                )
            )
        return self._object

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


class XYZErrorListener(object):
    """
    Calls callback functions in a background thread when XYZ errors occur.

    Note: the thread is automatically cleaned up on garbage collection.

    Example
    -------
    >>> from descarteslabs.workflows import Image, XYZ
    >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
    >>> xyz = XYZ.build(img)
    >>> xyz.save()  # doctest: +SKIP
    >>> listener = xyz.error_listener()
    >>> def callback(msg):
    ...     print(msg.code, msg.message)
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
        Function will be called with ``descarteslabs.common.proto.xyz_pb2.XYZError`` on each error.

        Parameters
        ----------
        callback: callable
            Function that takes one argument, a ``descarteslabs.common.proto.xyz_pb2.XYZError``
            protobuf message object. This message contains the fields ``code``, ``message``,
            ``timestamp``, ``session_id``.

            The function will be called within a separate thread,
            therefore it must behave thread-safely. Any errors raised by the function will
            terminate the listener.

        Example
        -------
        >>> from descarteslabs.workflows import XYZErrorListener
        >>> listener = XYZErrorListener("xyz_id") # doctest: +SKIP
        >>> def callback(msg):
        ...     print(msg.code, msg.message)
        >>> listener.add_callback(callback) # doctest: +SKIP
        """
        self.callbacks.append(callback)

    def listen(self, session_id, start_datetime=None):
        """
        Start listening for errors.

        Parameters
        ----------
        session_id: str
            Unique, client-generated ID that error logs are stored under.
            See `XYZ.url` for more information.
        start_datetime: datetime.datetime
            Only listen for errors occuring after this datetime. Must be tz-aware.

        Example
        -------
        >>> from descarteslabs.workflows import XYZErrorListener
        >>> listener = XYZErrorListener("xyz_id") # doctest: +SKIP
        >>> listener.listen("session-id", start_datetime=datetime.datetime.now(datetime.timezone.utc)) #doctest: +SKIP
        """
        self._rendezvous = _tile_error_stream(
            self.xyz_id, session_id, start_datetime=start_datetime, client=self._client
        )
        self._thread = threading.Thread(target=self._listener)
        self._thread.daemon = True
        self._thread.start()

    def running(self):
        """bool: whether this is an active listener

        Example
        -------
        >>> from descarteslabs.workflows import XYZErrorListener
        >>> listener = XYZErrorListener("xyz_id") # doctest: +SKIP
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
        >>> from descarteslabs.workflows import XYZErrorListener
        >>> listener = XYZErrorListener("xyz_id") # doctest: +SKIP
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


def _tile_error_stream(xyz_id, session_id, start_datetime=None, client=None):
    if client is None:
        client = get_global_grpc_client()

    if start_datetime is None:
        start_timestamp = 0
    else:
        start_timestamp = pb_datetime_to_milliseconds(start_datetime)

    msg = xyz_pb2.GetXYZSessionErrorsRequest(
        session_id=session_id, xyz_id=xyz_id, start_timestamp=start_timestamp
    )

    return client.api["GetXYZSessionErrors"](msg, timeout=client.STREAM_TIMEOUT)
