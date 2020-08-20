import json
import datetime
import re

import ipywidgets
import ipyleaflet
import traitlets
import markdown2

from ..client import get_global_grpc_client
from .. import types
from ..models import Workflow, VersionedGraft

from .map_ import MapApp
from .parameters import py_type_to_widget


markdowner = markdown2.Markdown(
    extras=[
        "fenced-code-blocks",
        "footnotes",
        "target-blank-links",
        "tables",
        "strike",
    ],
    safe_mode="escape",
)

# because jupyter/ipywidgets only styles <b>, not <strong> apparently
_strong_to_b = re.compile(r"<(/?)strong>")


class VersionGraftWidget(ipywidgets.HTML):
    "Widget for displaying a `~.VersionedGraft` object."
    vg = traitlets.Instance(klass=VersionedGraft, allow_none=True)

    @traitlets.observe("vg")
    def render(self, change):
        vg = self.vg
        if vg is None:
            self.value = ""
            return

        self.value = f"""
        <h2>Version {vg.version}</h2>
        <p><b>Type:</b> <code>{vg.type.__name__}</code></p>
        <p><b>Labels:</b> <code>{vg.labels!r}</code></p>
        <hr>
        <pre>
        {vg.docstring}
        </pre>
        """


class WorkflowWidget(ipywidgets.VBox):
    """
    Widget for displaying a single `Workflow` object, with a table of its versions.

    `version` holds the currently-selected version name as a string, and can be modified.
    `current_vg` is a read-only trait holding the currently-selected `~.VersionedGraft`.
    """
    flow = traitlets.Instance(klass=Workflow, allow_none=True)
    version = traitlets.CUnicode(allow_none=True)

    current_vg = traitlets.Instance(
        klass=VersionedGraft, read_only=True, allow_none=True
    )

    def __init__(self, **kwargs):
        self._version_selector = ipywidgets.Select(
            layout=ipywidgets.Layout(width="initial")
        )
        super().__init__(**kwargs)
        ipywidgets.link((self._version_selector, "value"), (self, "version"))

    @traitlets.observe("flow")
    def _update_flow(self, change):
        flow = self.flow
        if flow is None:
            self.children = []
            self.version = None
            return

        info = ipywidgets.HTML(
            f"""
            <h2>{flow.title}</h2>
            <p><code>{flow.id}</p></code>
            <p>{"<i class='fa fa-unlock'></i> Public" if flow.public else "<i class='fa fa-lock'></i> Private"}</p>
            <p><b>Labels:</b> <code>{flow.labels}</code></p>
            <p><b>Tags:</b> <code>{flow.tags}</code></p>
            """
        )

        latest = flow.versions[-1] if flow.versions else None

        self._version_selector.options = flow.version_names[::-1]
        self._version_selector.value = latest.version if latest else None
        self._version_selector.rows = min(5, len(flow.versions))
        self.set_trait("current_vg", latest)  # in case version name was the same

        desc = ipywidgets.HTML(
            value=_strong_to_b.sub(r"<\1b>", markdowner.convert(flow.description))
        )

        self.children = [info, self._version_selector, ipywidgets.HTML("<hr>"), desc]

    @traitlets.observe("version")
    def _update_version(self, change):
        vg = (
            self.flow[self.version]
            if self.flow is not None and self.version is not None
            else None
        )
        self.set_trait("current_vg", vg)


class WorkflowsListerWidget(ipywidgets.GridspecLayout):
    """
    Widget for displaying a table of `Workflow` objects, and selecting one ID and version.

    `selected_id` and `selected_version` hold the current ID and version name, as strings,
    and can be modified to change the selection with code.

    `current_flow` and `current_vg` are read-only traits holding the currently-selected `~.Workflow`
    and `~.VersionedGraft` objects.
    """

    flows = traitlets.List(trait=traitlets.Instance(klass=Workflow))
    selected_id = traitlets.Unicode(allow_none=True)
    selected_version = traitlets.Unicode(allow_none=True)

    current_flow = traitlets.Instance(klass=Workflow, read_only=True, allow_none=True)
    current_vg = traitlets.Instance(
        klass=VersionedGraft, read_only=True, allow_none=True
    )

    def __init__(self, flows, **kwargs):
        super().__init__(1, 3, grid_gap="10px", **kwargs)

        self._selector = ipywidgets.Select(
            layout=ipywidgets.Layout(height="100%", width="initial")
        )
        self._flow_widget = WorkflowWidget(layout=ipywidgets.Layout(flex="1 0 auto"))
        self._vg_widget = VersionGraftWidget(layout=ipywidgets.Layout(flex="1 0 auto"))

        self.flows = flows

        ipywidgets.link((self._selector, "value"), (self, "selected_id"))
        ipywidgets.link((self._flow_widget, "version"), (self, "selected_version"))
        traitlets.dlink(
            (self._selector, "value"),
            (self._flow_widget, "flow"),
            transform=lambda flow_id: self._flows_lookup.get(flow_id, None),
        )
        traitlets.dlink((self._flow_widget, "current_vg"), (self._vg_widget, "vg"))

        self._flow_widget.observe(
            lambda change: self.set_trait("current_flow", change["new"]), names="flow"
        )
        self._flow_widget.observe(
            lambda change: self.set_trait("current_vg", change["new"]),
            names="current_vg",
        )

        self[0, 0] = ipywidgets.VBox(
            children=[ipywidgets.HTML("<h2>Workflows</h2>"), self._selector],
            layout=ipywidgets.Layout(flex="1 0 auto"),
        )

        self[0, 1] = self._flow_widget
        self[0, 2] = self._vg_widget

    @traitlets.observe("flows")
    def _update_flows(self, change):
        self._flows_lookup = {f.id: f for f in self.flows}

        self._selector.options = [
            (f.title if f.title else id, id) for id, f in self._flows_lookup.items()
        ]
        self.selected_id = self.flows[0].id if self.flows else None


proxytype_to_py_type = {
    types.Datetime: datetime.datetime,
    types.Str: str,
    types.Int: int,
    types.Float: float,
    types.Bool: bool,
}
proxytype_to_widget = {
    proxytype: py_type_to_widget[py_type]
    for proxytype, py_type in proxytype_to_py_type.items()
}


class WorkflowsBrowser(ipywidgets.VBox):
    """
    Widget displaying a UI for browsing and searching `~.Workflow` objects, and displaying them on the map.

    Attributes
    ----------
    name: str
        The name prefix used in the search
    mine: bool
        Whether to seach for only the current user's Workflows. If True, `email` is ignored.
    email: str
        The email used in the search
    selected_id: str
        The currently-selected `~.Workflow` ID (like ``you@example.com:workflow_name``).
    selected_version: str
        The currently-selected version name (like ``1.2.0``).
    map: ipyleaflet.Map, MapApp
        Map instance used by the "Add to map" button and `add_current_to_map`. Defaults to ``wf.map``.
    flows: List[Workflow]
        List of `~.Workflow` objects currently being displayed that matched the search. Read-only.
    current_flow: Optional[Workflow]
        The currently-selected `~.Workflow` object, or None.
    current_vg: Optional[VersionedGraft]
        The currently-selected `~.VersionedGraft` object, or None.
    """

    name = traitlets.Unicode()
    mine = traitlets.Bool()
    email = traitlets.Unicode()

    selected_id = traitlets.Unicode(allow_none=True)
    selected_version = traitlets.Unicode(allow_none=True)

    flows = traitlets.List(trait=traitlets.Instance(klass=Workflow), read_only=True)
    current_flow = traitlets.Instance(klass=Workflow, read_only=True, allow_none=True)
    current_vg = traitlets.Instance(
        klass=VersionedGraft, read_only=True, allow_none=True
    )

    def __init__(self, map=None, **kwargs):
        super().__init__(**kwargs)

        if map is None:
            from . import map

        self.map = map

        refresh = ipywidgets.Button(
            icon="repeat",
            description="",
            tooltip="Reload",
            layout=ipywidgets.Layout(width="initial", overflow="visible"),
        )
        refresh.on_click(self._observe_search)
        name = ipywidgets.Text(description="Name prefix:")
        mine = ipywidgets.Checkbox(description="Only mine")
        self._email = ipywidgets.Text(description="Email:")
        self._error = ipywidgets.HTML()

        ipywidgets.link((name, "value"), (self, "name"))
        ipywidgets.link((mine, "value"), (self, "mine"))
        ipywidgets.link((self._email, "value"), (self, "email"))

        self._lister = WorkflowsListerWidget(flows=[])

        self._lister.observe(
            lambda change: self.set_trait("current_flow", change["new"]),
            names="current_flow",
        )
        self._lister.observe(
            lambda change: self.set_trait("current_vg", change["new"]),
            names="current_vg",
        )
        self._lister.observe(
            lambda change: self.set_trait("flows", change["new"]), names="flows"
        )
        ipywidgets.link((self._lister, "selected_id"), (self, "selected_id"))
        ipywidgets.link((self._lister, "selected_version"), (self, "selected_version"))

        self._use = ipywidgets.HTML()
        self._add_to_map = ipywidgets.Button(
            description="Add to map", button_style="info"
        )
        self._add_to_map.on_click(lambda btn: self.add_current_to_map())

        self.children = [
            ipywidgets.HBox(children=[refresh, name, mine, self._email, self._error]),
            ipywidgets.HTML("<hr>"),
            self._lister,
            ipywidgets.HTML("<hr>"),
            ipywidgets.HBox(
                children=[self._use, self._add_to_map],
                layout=ipywidgets.Layout(justify_content="space-between"),
            ),
        ]

        # Track that we haven't done the initial search or gotten `own_email`
        # (which may require a token fetch) yet.
        # We should do this before displaying to users, but we'll
        # defer until `_ipython_display_` so constructing the singleton
        # `wf.flows` (and therefore import) doesn't make a network call.
        self._initialized = False
        self._own_email = None

    @traitlets.observe("name", "mine", "email")
    def _observe_search(self, change):
        self._email.disabled = self.mine

        try:
            self._lister.flows = list(
                Workflow.search(
                    name=self.name, email=self._own_email if self.mine else self.email
                )
            )
            self._initialized = True
        except Exception as e:
            # TODO more specific (catch gRPC errors, which need to be wrapped better anyway)
            self._lister.flows = []
            self._error.value = f'<span style="color: red">{e.details()}</span>'
        else:
            self._error.value = ""

    @traitlets.observe("current_flow", "current_vg")
    def _observe_use(self, change):
        try:
            id = self.current_flow.id
            version = self.current_vg.version
        except AttributeError:
            self._use.value = ""
            self._add_to_map.disabled = True
        else:
            self._use.value = f'<code>wf.use("{id}", "{version}")</code>'

            obj = self.current_vg.object
            if isinstance(obj, types.Function):
                if any(t not in proxytype_to_py_type for t in obj.arg_types):
                    obj = None  # dumb way to ensure the `isinstance` below will fail
                else:
                    obj = obj.return_type._from_apply("")  # just to get an instance

            self._add_to_map.disabled = not isinstance(
                obj, (types.Image, types.ImageCollection)
            )

    def add_current_to_map(self):
        """
        Add the currently-selected `~.Workflow` to ``wf.map`` or ``self.map``, if possible.

        The `~.Workflow` must be one of these types:

        * `~.geospatial.Image`
        * `~.ImageCollection`, in which case it's mosaicked
        * `~.Function`, in which case it must return an `~.geospatial.Image` or `~.ImageCollection`,
          and only take `~.Int`, `~.Float`, `~.Str`, `~.Bool`, or `~.Datetime` as parameters.
          A widget will be added to the map to control the function arguments interactively.

        These `~.VersionedGraft.labels` set on the version will customize the map:

        * ``default_bands``: space-separated string, like ``"red green blue"``.
          These bands will be picked from the `~.geospatial.Image` before displaying.
        * ``colormap``: str, like ``"viridis"``.
          This colormap will be applied to the layer.
        * ``scales``: JSON-encoded list, like ``"[[0, 0.5], [0, 0.5], [0, 0.5]]"``.
          This will be JSON-decoded, and used as the ``scales=`` argument to `~.Image.visualize`.

        If ``self.map`` is None, the layer will be added to ``wf.map``, otherwise to ``self.map``.

        Returns
        -------
        lyr: WorkflowsLayer
            The layer added to the map

        Raises
        ------
        ValueError
            If a Workflow and version are not currently selected.
        TypeError
            If the selected version is not one of the supported types.
        """
        flow = self.current_flow
        vg = self.current_vg
        if not (flow and vg):
            raise ValueError("No Workflow and version currently selected")

        obj = vg.object
        parameters = {}

        if isinstance(obj, types.Function):
            param_names = [f"arg_{i}" for i in range(len(obj.arg_types))]
            graft_params = [
                types.parameter(name, type_)
                for name, type_ in zip(param_names, obj.arg_types)
            ]

            try:
                parameters = {
                    name: proxytype_to_widget[type_]()
                    for name, type_ in zip(param_names, obj.arg_types)
                }
            except KeyError as e:
                raise TypeError(
                    f"Cannot create interactive control for type {e}"
                )

            obj = obj(*graft_params)

        if isinstance(obj, types.ImageCollection):
            obj = obj.mosaic()

        if isinstance(obj, types.Image):
            # NOTE(gabe): we LBYL for all these because accessing a missing key on the
            # fake proto dict object inserts it (like `setdefault`, not `get`), mutating `vg.labels` :(
            if "default_bands" in vg.labels:
                obj = obj.pick_bands(vg.labels["default_bands"])

            colormap = vg.labels["colormap"] if "colormap" in vg.labels else None
            scales = json.loads(vg.labels["scales"]) if "scales" in vg.labels else None

            map_ = self.map
            if isinstance(map_, MapApp):
                map_ = map_.map

            lyr = obj.visualize(
                flow.name + ":" + vg.version,
                colormap=colormap,
                scales=scales,
                map=map_,
                **parameters,
            )

            if len(parameters) > 0:
                # hack to display layer params on map
                ctrl = ipyleaflet.WidgetControl(
                    widget=lyr.parameters.widget, position="bottomleft"
                )
                map_.add_control(ctrl)

                def remove_ctrl_obs(layers_change):
                    if lyr not in layers_change["new"]:
                        map_.remove_control(ctrl)
                        map_.unobserve(remove_ctrl_obs, names="layers")

                map_.observe(remove_ctrl_obs, names="layers")

            return lyr

        raise TypeError(
            f"Can only visualize Image, ImageCollection, and Functions that return those, not {vg.type.__name__}"
        )

    def _ipython_display_(self):
        # the first time we're displayed, do the search so there's something to show.
        if not self._initialized:
            self._own_email = get_global_grpc_client().auth.payload["email"]
            self._observe_search({})
            self._initialized = True
        super()._ipython_display_()
