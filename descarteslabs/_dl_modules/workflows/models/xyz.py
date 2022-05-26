from typing import Any, Callable, Iterator, Optional, Sequence
import datetime
import logging
import threading
from urllib.parse import urlencode
import warnings

import grpc
from ...common.proto.xyz import xyz_pb2

from ..client import get_global_grpc_client, Client
from ..types import Proxytype

from .published_graft import PublishedGraft
from .utils import (
    pb_datetime_to_milliseconds,
    pb_milliseconds_to_datetime,
    pb_timestamp_to_datetime,
    py_log_level_to_proto_log_level,
)
from .tile_url import tile_url
from .visualization import VizOption


class XYZ(PublishedGraft, message_type=xyz_pb2.XYZ):
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
    >>> xyz = XYZ(rgb, name="My RGB") # doctest: +SKIP
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
    >>> xyz = XYZ(rgb, name="RGB") # doctest: +SKIP
    >>> xyz.url() # doctest: +SKIP
    'https://workflows.descarteslabs.com/tiles-ic/xyz/bdbeb4706f3025f4ee4eeff4dec46f6f2554c583d830e5e9/{z}/{x}/{y}.png'
    """

    def __init__(
        self,
        proxy_object: Proxytype,
        name: str = "",
        description: str = "",
        public: bool = True,
        viz_options: Optional[Sequence[VizOption]] = None,
        days_to_expiration: int = None,
        client: Optional[Client] = None,
    ):
        """
        Construct a new XYZ from a proxy object.

        If the proxy object depends on any parameters (``proxy_object.params`` is not empty),
        it's first internally converted to a `.Function` that takes those parameters
        (using `.Function.from_object`).

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
        public: bool, default `True`
            If ``True`` then this object is shared and accessible to all,
            otherwise it is private.
        viz_options: list, default None
            List of `~.models.VizOption` visualization parameter sets.
        days_to_expiration: int, default None
            Days until this XYZ object will expire.
            If None defaults to 10 days.
        client: `.workflows.client.Client`, optional, default None
            Allows you to use a specific client instance with non-default
            auth and parameters

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> xyz = wf.XYZ(rgb, name="My RGB") # doctest: +SKIP
        >>> xyz.id # doctest: +SKIP
        '24d0e79c5c1e1f10a0b1177ef3974d7edefd5988291cf2c6'
        """
        if public and viz_options is None:
            warnings.warn(
                "viz_options should be provided for public XYZ instances. "
                "Provide an empty list if you want to override this."
            )

        super().__init__(
            proxy_object,
            viz_options=viz_options,
            client=client,
        )

        response_message = self._client.api["CreateXYZ"](
            xyz_pb2.CreateXYZRequest(
                name=name,
                description=description,
                serialized_graft=self._message.serialized_graft,
                typespec=self._message.typespec,
                parameters=self._message.parameters,
                public=public,
                viz_options=self._message.viz_options,
                days_to_expiration=int(days_to_expiration)
                if days_to_expiration is not None
                else None,
                channel=self._message.channel,
                client_version=self._message.client_version,
            ),
            timeout=self._client.DEFAULT_TIMEOUT,
        )
        self._message = response_message

    @classmethod
    def get(cls, xyz_id: str, client: Optional[Client] = None) -> "XYZ":
        """
        Get an existing XYZ by id.

        Parameters
        ----------
        id: str
            The unique id of a `XZY` object
        client: Optional[Client], default None
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
        return cls._from_proto(message, client=client)

    @classmethod
    def list(
        cls, public: bool = False, client: Optional[Client] = None
    ) -> Iterator["XYZ"]:
        """
        Get all XYZ objects created by this user.

        Parameters
        ----------
        public: bool, default False
            If ``True`` then return public (shared) XYZ objects created by this
            user. If ``False`` then return non-public XYZ objects created through
            the use of workflow map widgets.
        client: Optional[Client], default None
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        Iterator[XYZ]

        Example
        -------
        >>> from descarteslabs.workflows import XYZ
        >>> for xyz in XYZ.list(): # doctest: +SKIP
        ...     print(xyz.id) # doctest: +SKIP
        24d0e79c5c1e1f10a0b1177ef3974d7edefd5988291cf2c6
        """
        if client is None:
            client = get_global_grpc_client()

        iter = client.api["ListXYZ"](
            xyz_pb2.ListXYZRequest(public=public), timeout=client.STREAM_TIMEOUT
        )
        return map(lambda xyz: cls._from_proto(xyz, client=client), iter)

    @staticmethod
    def delete_id(id, client=None):
        """
        Delete the `XYZ` that has the provided ID. Only the user
        that created the `XYZ` can delete it.

        **Warning:** this cannot be undone!

        Parameters
        ----------
        id: str
            The ID of the `XYZ` that we wish to delete.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Example
        -------
        >>> from descarteslabs.workflows import XYZ
        >>> xyz_id = '24d0e79c5c1e1f10a0b1177ef3974d7edefd5988291cf2c6'
        >>> xyz = XYZ.get(xyz_id) # doctest: +SKIP
        >>> xyz # doctest: +SKIP
        <descarteslabs.workflows.models.xyz.XYZ object at 0x...>
        >>> XYZ.delete_id(xyz_id) # doctest: +SKIP
        >>> XYZ.get(xyz_id) # doctest: +SKIP
        ...
        NotFound: 404 XYZ '24d0e79c5c1e1f10a0b1177ef3974d7edefd5988291cf2c6' does not exist.
        """
        if client is None:
            client = get_global_grpc_client()

        client.api["DeleteXYZ"](
            xyz_pb2.DeleteXYZRequest(xyz_id=id),
            timeout=client.DEFAULT_TIMEOUT,
        )

    def delete(self, client: Client = None) -> None:
        """
        Delete this XYZ object. Only the user
        that created the `XYZ` can delete it.

        Parameters
        ----------
        client: Client, default None
            Allows you to use a specific client instance with non-default auth and parameters

        Example
        -------
        Returns
        -------
        None

        Example
        -------
        >>> from descarteslabs.workflows import XYZ
        >>> xyz = XYZ.get('24d0e79c5c1e1f10a0b1177ef3974d7edefd5988291cf2c6') # doctest: +SKIP
        >>> xyz.delete() # doctest: +SKIP
        """
        if client is None:
            client = self._client

        self.delete_id(self.id, client=client)
        self._message.Clear()

    def url(
        self,
        session_id=None,
        colormap=None,
        bands=None,
        scales=None,
        reduction=None,
        checkerboard=None,
        **arguments,
    ):
        """
        URL template for displaying this `XYZ` object on a web map,
        like ``https://workflows.descarteslabs.com/v0-5/xyz/1234567/{z}/{x}/{y}.png``

        The returned URL uses the `XYZ/OpenStreetMap tiling scheme
        <https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames>`_.

        `object` must be an `~.geospatial.Image`, an `~.geospatial.ImageCollection`, or a `.Function`
        that returns an `~.geospatial.Image` or `~.geospatial.ImageCollection` for the URL to work.

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
        reduction: str, optional, default None
            One of "mosaic", "min", "max", "mean", "median", "sum", "std", or "count".
            If displaying an `~.geospatial.ImageCollection`, this method is used to reduce it into
            an `~.geospatial.Image`. The reduction is performed before applying a colormap or scaling.
            If displaying an `~.geospatial.Image`, reduction is ignored.
        checkerboard: bool, optional, default None
            Whether to display a checkerboarded background for missing or masked data.
        **arguments: Any
            Values for all the arguments that `object` takes, if it's a `.Function`.
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
        TypeError
            If the ``scales`` are of invalid type.

            If the ``arguments`` names or types don't match the arguments that
            the `object` takes. (Those required arguments are equivalent to `params`.)

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> red = img.pick_bands("red")
        >>> viz = red ** wf.parameter("exponent", wf.Float)
        >>> xyz = wf.XYZ(viz, name="Red band raised to exponent") # doctest: +SKIP
        >>> xyz.url("some_session", colormap="magma", scales=[(0.2, 0.8)], exponent=2.5) # doctest: +SKIP
        'https://workflows.descarteslabs.com/v0-0/xyz/0d21037edb4bdd16b735f24bb3bff6d4202a71c20404b101/
         {z}/{x}/{y}.png?session_id=some_session&colormap=magma&scales=[[0.2, 0.8]]&exponent=2.5'

        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> red = col.pick_bands("red")
        >>> viz = red ** wf.parameter("exponent", wf.Float)
        >>> xyz = wf.XYZ(viz, name="Red band ImageCollection raised to exponent") # doctest: +SKIP
        >>> xyz.url("some_session", reduction="min", exponent=2.5) # doctest: +SKIP
        'https://workflows.descarteslabs.com/v0-0/xyz/bdbeb4706f3025f4ee4eeff4dec46f6f2554c583d830e5e9/
         {z}/{x}/{y}.png?session_id=some_session&reduction=min&exponent=2.5'
        """
        url_template = self._message.url_template

        return tile_url(
            url_template,
            self.object,
            session_id=session_id,
            colormap=colormap,
            bands=bands,
            scales=scales,
            reduction=reduction,
            checkerboard=checkerboard,
            **arguments,
        )

    def wmts_url(self, tile_matrix_sets=None, dimensions=None) -> str:
        """
        Get the WMTS endpoint which gives access to this XYZ object.

        Parameters
        ----------
        tile_matrix_sets: str or list, optional
            Desired tile matrix sets. Defaults to EPSG:3857.
        dimensions: bool, optional
            If True, then provide dimensions definitions to WMTS. If
            False, then generate a layer for each possible dimensions
            attribute combination. If not specified, the WMTS service itself
            will determine how to handle dimensions (currently it does
            not provide dimensions definitions).

        Returns
        -------
        wmts_url: str
            The URL for the WMTS service endpoint corresponding to this XYZ object.

        Example
        -------
        >>> from descarteslabs.workflows import Image, XYZ
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> xyz = XYZ(rgb, name="My RGB") # doctest: +SKIP
        >>> xyz.wmts_url() # doctest: +SKIP
        'https://workflows.prod.descarteslabs.com/master/wmts/xyz/...'
        """
        url_params = {}

        if tile_matrix_sets:
            url_params["tile_matrix_sets"] = tile_matrix_sets
        if dimensions is not None:
            url_params["dimensions"] = "true" if dimensions else "false"

        wmts_url = self._message.wmts_url_template
        if url_params:
            wmts_url = f"{wmts_url}?{urlencode(url_params, doseq=True)}"

        return wmts_url

    def iter_tile_logs(
        self,
        session_id: str,
        start_datetime: datetime.datetime = None,
        level: int = None,
    ) -> Iterator[xyz_pb2.XYZLogRecord]:
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
        >>> xyz = XYZ(rgb, name="My RGB") # doctest: +SKIP
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

    def log_listener(self) -> "XYZLogListener":
        """An `XYZLogListener` to trigger callbacks when logs occur computing tiles.

        Example
        -------
        >>> from descarteslabs.workflows import Image, XYZ
        >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> xyz = XYZ(rgb, name="My RGB") # doctest: +SKIP
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
    def id(self) -> str:
        """str: The globally unique identifier for the `XYZ`"""
        return self._message.id

    @property
    def created_timestamp(self) -> datetime.datetime:
        """
        datetime.datetime or None: The UTC date this `XYZ` was created,
        or None if it hasn't been saved yet. Cannot be modified.
        """
        return pb_milliseconds_to_datetime(self._message.created_timestamp)

    @property
    def updated_timestamp(self) -> datetime.datetime:
        """
        datetime.datetime or None: The UTC date this `XYZ` was most recently modified,
        or None if it hasn't been saved yet. Updated automatically.
        """
        return pb_milliseconds_to_datetime(self._message.updated_timestamp)

    @property
    def expires_timestamp(self) -> datetime.datetime:
        """
        datetime.datetime: The UTC date this `XYZ` will be expired.
        """
        return pb_timestamp_to_datetime(self._message.expires_timestamp)

    @property
    def name(self) -> str:
        """str: The name of this XYZ."""
        return self._message.name

    @property
    def description(self) -> str:
        """str: A long-form description of this `XYZ`. Markdown is supported."""
        return self._message.description

    @property
    def user(self) -> str:
        """str: The user ID which created this `XYZ`."""
        return self._message.user

    @property
    def org(self) -> str:
        """str: The org of the user which created this `XYZ`."""
        return self._message.org

    @property
    def public(self) -> bool:
        """bool: True if this `XYZ` is shared."""
        return self._message.public


class XYZLogListener(object):
    """
    Calls callback functions in a background thread when XYZ log records occur.

    Note: the thread is automatically cleaned up on garbage collection.

    Example
    -------
    >>> from descarteslabs.workflows import Image, XYZ
    >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
    >>> xyz = XYZ(img) # doctest: +SKIP
    >>> listener = xyz.log_listener() # doctest: +SKIP
    >>> def callback(msg):
    ...     print(msg.level, msg.message)
    >>> listener.add_callback(callback) # doctest: +SKIP
    >>> listener.listen("session_id", start_datetime=datetime.datetime.now())  # doctest: +SKIP
    >>> # later
    >>> listener.stop()  # doctest: +SKIP
    """

    def __init__(self, xyz_id: str, client: Optional[Client] = None):
        self.xyz_id = xyz_id
        self.callbacks = []
        self._rendezvous = None
        self._thread = None
        self._client = client if client is not None else get_global_grpc_client()

    def add_callback(self, callback: Callable[[xyz_pb2.XYZLogRecord], Any]):
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

    def listen(
        self,
        session_id: str,
        start_datetime: datetime.datetime = None,
        level: int = None,
    ):
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

    def running(self) -> bool:
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

    def stop(self, timeout: Optional[int] = None) -> bool:
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


def _tile_log_stream(
    xyz_id: str,
    session_id: str,
    start_datetime: datetime.datetime = None,
    level: int = None,
    client: Optional[Client] = None,
) -> Iterator[xyz_pb2.XYZLogRecord]:
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
