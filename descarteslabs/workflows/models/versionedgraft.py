from typing import Dict, Optional, Sequence
import textwrap
from urllib.parse import urlencode

from descarteslabs.common.proto.workflow import workflow_pb2
from descarteslabs.workflows.client import get_global_grpc_client, Client
from descarteslabs.workflows.types import Proxytype

from .published_graft import PublishedGraft

from .tile_url import tile_url
from .visualization import VizOption


class VersionedGraft(PublishedGraft, message_type=workflow_pb2.VersionedGraft):
    """
    A specific version of a Workflow.

    Except in advanced cases, you shouldn't need to interact with this object much—you'll primarily
    use the `Workflow` object and `wf.use <.models.use>`.
    """

    def __init__(
        self,
        version: str,
        proxy_object: Proxytype,
        docstring: str = "",
        labels: Optional[Dict[str, str]] = None,
        viz_options: Optional[Sequence[VizOption]] = None,
        client: Optional[Client] = None,
    ):
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
        viz_options: list, default None
            List of `~.models.VizOption` visualization parameter sets.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        VersionedGraft
        """
        super().__init__(
            proxy_object,
            viz_options=viz_options,
            client=client,
        )
        message = self._message

        message.version = version
        message.docstring = textwrap.dedent(docstring)
        if labels:
            message.labels.update(labels)
            # ^ NOTE: we `.update` because you can't assign directly to a proto map field.

    @classmethod
    def get(cls, workflow_id: str, version: str, client: Optional[Client] = None):
        """
        Get a specific `VersionedGraft` of a `Workflow`.

        Parameters
        ----------
        workflow_id: str
            The ID of the `Workflow`.
        version: str
            The version of the `Workflow` that you wish to fetch.
        client: `.workflows.client.Client`, optional, default None
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

        return cls._from_proto(versioned_graft_message, client=client)

    @property
    def object(self) -> Proxytype:
        """
        Proxytype: The stored proxy object.

        Note that if this was originally constructed from a Workflows object that depended on parameters,
        then this `object` property won't hold the same object, but rather a `.Function` which takes
        those parameters and returns that object.

        Raises ValueError if not compatible with the current channel.
        """
        obj_is_none = self._object is None
        obj = super().object
        if obj_is_none and self.docstring:
            obj.__doc__ = self.docstring
        return obj

    @property
    def version(self) -> str:
        """str: The version of this `VersionedGraft`."""
        return self._message.version

    @property
    def labels(self) -> str:
        """dict: The labels attached to this `VersionedGraft`."""
        return self._message.labels

    @property
    def docstring(self) -> str:
        """str: The docstring for this `VersionedGraft`."""
        return self._message.docstring

    def __repr__(self) -> str:
        try:
            type_line = "type: {}".format(self.type.__name__)
        except ValueError:
            type_line = "channel: {} (incompatible with current version)".format(
                self.channel
            )

        return """\
VersionedGraft: {self.version}
    - {type_line}
    - channel: {self.channel}
    - labels: {self.labels}
    - viz_options: {viz_options}
    {docstring}
""".format(
            self=self,
            type_line=type_line,
            viz_options=(
                ", ".join(repr(viz_option.id) for viz_option in self.viz_options)
                if self.viz_options
                else "[]"
            ),
            docstring=textwrap.indent(self.docstring, "    "),
        )

    def url(
        self,
        colormap=None,
        bands=None,
        scales=None,
        reduction=None,
        checkerboard=None,
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
        reduction: str, optional, default None
            One of "mosaic", "min", "max", "mean", "median", "sum", "std", or "count".
            If displaying an `~.geospatial.ImageCollection`, this method is used to reduce it into
            an `~.geospatial.Image`. The reduction is performed before applying a colormap or scaling.
            If displaying an `~.geospatial.Image`, reduction is ignored.
        checkerboard: bool, default None
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
            self.object,
            colormap=colormap,
            bands=bands,
            scales=scales,
            reduction=reduction,
            checkerboard=checkerboard,
            **arguments,
        )

    def wmts_url(self, tile_matrix_sets=None, dimensions=None) -> str:
        """
        Get the WMTS endpoint which gives access to this versioned graft.

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
            The URL for the WMTS service endpoint corresponding to this versioned graft.
        """
        wmts_url = self._message.wmts_url_template
        if not wmts_url:
            raise ValueError(
                "This VersionedGraft object has not been persisted yet. "
                "Call .save() on the Workflow that contains it, then call "
                f".get_version({self.version!r}) on that parent Workflow to get an updated "
                "copy of this VersionedGraft."
            )

        url_params = {}

        if tile_matrix_sets:
            url_params["tile_matrix_sets"] = tile_matrix_sets
        if dimensions is not None:
            url_params["dimensions"] = "true" if dimensions else "false"

        wmts_url = self._message.wmts_url_template
        if url_params:
            wmts_url = f"{wmts_url}?{urlencode(url_params, doseq=True)}"

        return wmts_url
