import json
import textwrap

from descarteslabs.client.version import __version__
from descarteslabs.common.graft import client as graft_client
from descarteslabs.common.proto.workflow import workflow_pb2
from descarteslabs.workflows.cereal import deserialize_typespec, serialize_typespec
from descarteslabs.workflows.client import get_global_grpc_client

from descarteslabs.workflows.types import Function, proxify
from descarteslabs.workflows.types.widget import serialize_params, deserialize_params

from .tile_url import tile_url


class VersionedGraft:
    """
    A specific version of a Workflow.

    Except in advanced cases, you shouldn't need to interact with this object much—you'll primarily
    use the `Workflow` object and `wf.use <.models.use>`.
    """

    def __init__(self, version, proxy_object, docstring="", labels=None, client=None):
        # TODO mention function conversion in docstring
        """
        Construct a VersionedGraft object from a proxy object.

        You shouldn't construct a `VersionedGraft` directly; use `Workflow.set_version`
        or `wf.publish <.models.publish>` instead.

        If the proxy object depends on any parameters (``proxy_object.params`` is not empty),
        it's first internally converted to a `.Function` that takes those parameters
        (using `.Function.from_object`).

        Parameters
        ----------
        version: str
            Version of the graft. This should adhere to the semantic versioning schema (https://semver.org).
        proxy_object: Proxytype
            The proxy object source of the graft.
            If it depends on parameters, ``proxy_obj`` is first converted
            to a `.Function` that takes those parameters.
        docstring: str, default ""
            Docstring for the VersionedGraft.
        labels: dict, optional
            Key-value pair labels to add to the VersionedGraft.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        VersionedGraft
        """
        if client is None:
            client = get_global_grpc_client()

        proxy_object = proxify(proxy_object)

        proto_params = serialize_params(proxy_object)
        params = proxy_object.params
        if len(params) > 0:
            # turn objects that depend on parameters into Functions
            proxy_object = Function.from_object(proxy_object)

        if (
            isinstance(proxy_object, Function)
            and issubclass(proxy_object.return_type, Function)
            and len(proxy_object.return_type._type_params) > 2
        ):
            raise NotImplementedError(
                f"Cannot currently publish Functions that return Functions "
                f"which take >0 arguments ({type(proxy_object).__name__}). "
                "Please let us know why you are trying to do this, so we can support it in the future!"
            )
            # Why? Because we can't tell what the parameter names are of the returned inner function,
            # so `parameters` will be empty, even though the typespec defines a Function that takes >0 arguments.
            # That breaks our invariant of the `parameters` field on the proto message.
            # The actual practical implication of this is that we can't generate a graft with the
            # appropriate argument names to call `Workflow.use`. Other users would need to receive that generated graft
            # in order for the Workflow run under the correct version.

            # We'll solve this with named positional arguments to the Function typespec. Though ideally we'd
            # actually make widgets into types, so we can also encode any recursive widget information
            # about the returned inner function(s).

        typespec = serialize_typespec(type(proxy_object))
        graft = proxy_object.graft

        message = workflow_pb2.VersionedGraft(
            version=version,
            serialized_graft=json.dumps(graft),
            channel=client._wf_channel,
            client_version=__version__,
            typespec=typespec,
            docstring=textwrap.dedent(docstring),
            parameters=proto_params,
            labels=labels,
        )
        self._client = client
        self._object = proxy_object
        self._params = params
        self._message = message

    @classmethod
    def get(cls, workflow_id, version, client=None):
        """
        Get a specific `VersionedGraft` of a `Workflow`.

        Parameters
        ----------
        workflow_id: str
            The ID of the `Workflow`.
        version: str
            The version of the `Workflow` that you wish to fetch.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Returns
        -------
        VersionedGraft
        """
        if client is None:
            client = get_global_grpc_client()

        req = workflow_pb2.GetVersionRequest(id=workflow_id, version=version)

        versioned_graft_message = client.api["GetVersion"](
            req, timeout=client.DEFAULT_TIMEOUT
        )

        return cls._from_proto(versioned_graft_message)

    @classmethod
    def _from_proto(cls, message, client=None):
        """
        Low-level constructor for a `VersionedGraft` object from a Protobuf message.

        Do not use this method directly; use `VersionedGraft.__init__`
        or `VersionedGraft.get` instead.

        Parameters
        ----------
        proto_message: workflow_pb2.VersionedGraft message
            Protobuf message for the VersionedGraft
        client : `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        VersionedGraft
        """
        obj = cls.__new__(cls)  # bypass __init__

        if client is None:
            client = get_global_grpc_client()

        obj._client = client
        obj._message = message
        obj._object = None
        obj._params = None

        return obj

    @property
    def type(self):
        """
        type: The type of the proxy object.

        Raises ValueError if the `VersionedGraft` is not compatible with the current client version.
        """
        return type(self.object)

    @property
    def version(self):
        """str: The version of this `VersionedGraft`."""
        return self._message.version

    @property
    def labels(self):
        """dict: The labels attached to this `VersionedGraft`."""
        return self._message.labels

    @property
    def channel(self):
        """str: The channel under which this `VersionedGraft` was created."""
        return self._message.channel

    @property
    def docstring(self):
        """str: The docstring for this `VersionedGraft`."""
        return self._message.docstring

    @property
    def object(self):
        """
        Proxytype: The proxy object of this `VersionedGraft`.

        Note that if the `VersionedGraft` was originally constructed from an object that depended on parameters,
        then this `object` property won't hold the same object, but rather a `.Function` which takes
        those parameters and returns that object.

        Raises ValueError if the `VersionedGraft` is not compatible with the current client version.
        """
        if self.channel != self._client._wf_channel:
            raise ValueError(
                "This client is compatible with channel '{}', "
                "but the VersionedGraft is only defined for channel '{}'.".format(
                    self._client._wf_channel, self.channel
                )
            )
        if self._object is None:
            proxy_type = deserialize_typespec(self._message.typespec)
            graft = json.loads(self._message.serialized_graft)
            isolated = graft_client.isolate_keys(graft)
            proxy_obj = proxy_type._from_graft(
                isolated, params=() if issubclass(proxy_type, Function) else self.params
            )
            proxy_obj.__doc__ = self.docstring
            self._object = proxy_obj

        return self._object

    @property
    def params(self):
        """
        tuple: Parameter objects corresponding to the arguments to `object`, if it's a `.Function`.

        These represent any parameters (including widget objects from ``wf.widgets``) that
        `object` depended on before it became a `.Function`.
        """
        if self._params is None:
            self._params = deserialize_params(self._message.parameters)
        return self._params

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._message == other._message

    def __repr__(self):
        try:
            type_line = "type: {}".format(self.type.__name__)
        except ValueError:
            type_line = "channel: {} (incompatible with current version)".format(
                self.channel
            )

        return """\
VersionedGraft: {self.version}
    - {type_line}
    - labels: {self.labels}
    - channel: {self.channel}
    {docstring}
""".format(
            self=self,
            type_line=type_line,
            docstring=textwrap.indent(self.docstring, "    "),
        )

    def url(
        self,
        colormap=None,
        bands=None,
        scales=None,
        reduction="mosaic",
        checkerboard=False,
        **arguments,
    ):
        """
        URL template for displaying this `VersionedGraft` on a web map,
        like ``https://workflows.descarteslabs.com/v0-5/workflow/test@example.com:foo/1.0.1/{z}/{x}/{y}.png``

        The returned URL uses the `XYZ/OpenStreetMap tiling scheme
        <https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames>`_.

        `object` must be an `~.geospatial.Image`, an `~.geospatial.ImageCollection`, or a `.Function`
        that returns an `~.geospatial.Image` or `~.geospatial.ImageCollection` for the URL to work.

        Parameters
        ----------
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
        ValueError
            If `Workflow` containing this `VersionedGraft` object has not been saved yet.

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
        >>> flow = viz.publish("you@example.com:s2_red", "1.0.0") # doctest: +SKIP
        >>> vg = flow["1.0.0"] # doctest: +SKIP
        >>> vg.url(colormap="magma", scales=[(0.2, 0.8)], exponent=2.5) # doctest: +SKIP
        'https://workflows.descarteslabs.com/v0-0/workflow/you@example.com:s2_red/1.0.0/
        {z}/{x}/{y}.png?colormap=magma&scales=%5B%5B0.2%2C+0.8%5D%5D&exponent=2.5'

        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...        start_datetime="2017-01-01",
        ...        end_datetime="2017-12-31")
        >>> red = col.pick_bands("red")
        >>> viz = red ** wf.parameter("exponent", wf.Float)
        >>> flow = viz.publish("you@example.com:l8_red", "1.0.0") # doctest: +SKIP
        >>> vg = flow["1.0.0"] # doctest: +SKIP
        >>> vg.url(reduction="min", exponent=2.5) # doctest: +SKIP
        'https://workflows.descarteslabs.com/v0-0/workflow/you@example.com:l8_red/1.0.0/
        {z}/{x}/{y}.png?colormap=magma&scales=%5B%5B0.2%2C+0.8%5D%5D&reduction=min&exponent=2.5'
        """
        url_template = self._message.url_template
        if not url_template:
            raise ValueError(
                "This VersionedGraft object has not been persisted yet. "
                "Call .save() on the Workflow that contains it, then call "
                f".get_version({self.version!r}) on that parent Workflow to get an updated "
                "copy of this VersionedGraft."
            )

        return tile_url(
            url_template,
            self.params,
            colormap=colormap,
            bands=bands,
            scales=scales,
            reduction=reduction,
            checkerboard=checkerboard,
            **arguments,
        )
