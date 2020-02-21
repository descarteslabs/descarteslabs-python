from .types import (
    Proxytype,
    ProxyTypeError,
    proxify,
    Function,
    Primitive,
    Number,
    Int,
    Float,
    Bool,
    Str,
    NoneType,
    Any,
    Tuple,
    List,
    Dict,
    Struct,
    zip,
    Datetime,
    Timedelta,
    Feature,
    FeatureCollection,
    load_geojson,
    load_geojson_file,
    Geometry,
    GeometryCollection,
    GeoContext,
    Image,
    ImageCollection,
    Kernel,
    ImageCollectionGroupby,
    #    PCA,
    log,
    log2,
    log10,
    log1p,
    sqrt,
    cos,
    arccos,
    sin,
    arcsin,
    tan,
    arctan,
    arctan2,
    exp,
    square,
    normalized_difference,
    concat,
    where,
    conv2d,
    e,
    inf,
    nan,
    pi,
    parameter,
)

from .models import (
    JobComputeError,
    TimeoutError,
    Workflow,
    Job,
    XYZ,
    XYZErrorListener,
    compute,
    retrieve,
    use,
    publish as _publish,
)

from .interactive import map, Map, WorkflowsLayer, LayerController, LayerControllerList

from . import env
from ._channel import __channel__, _set_channel
from .client import Client, exceptions


# NOTE(gabe): we define this top-level `publish` implementation, which tries to proxify
# any plain Python objects before publishing them. Since this brings together two
# otherwise-separate submodules (`models` and `types`), it's cleaner to write it here
# in __init__ than have circular dependencies between those submodules.
def publish(obj, name="", description="", client=None):
    obj = proxify(obj)
    return _publish(obj, name, description, client)


__all__ = [
    # .types
    "Proxytype",
    "ProxyTypeError",
    "parameter",
    "proxify",
    "Function",
    "Primitive",
    "Number",
    "Int",
    "Float",
    "Bool",
    "Str",
    "NoneType",
    "Any",
    "Tuple",
    "List",
    "Dict",
    "Struct",
    "zip",
    "Datetime",
    "Timedelta",
    "Feature",
    "FeatureCollection",
    "load_geojson",
    "load_geojson_file",
    "Geometry",
    "GeometryCollection",
    "GeoContext",
    "Image",
    "ImageCollection",
    "Kernel",
    "ImageCollectionGroupby",
    #    "PCA",
    "log",
    "log2",
    "log10",
    "log1p",
    "sqrt",
    "cos",
    "arccos",
    "sin",
    "arcsin",
    "tan",
    "arctan",
    "arctan2",
    "exp",
    "square",
    "normalized_difference",
    "concat",
    "where",
    "conv2d",
    "e",
    "inf",
    "nan",
    "pi",
    # .models
    "JobComputeError",
    "TimeoutError",
    "Workflow",
    "Job",
    "XYZ",
    "compute",
    "XYZErrorListener",
    "retrieve",
    "use",
    # .env
    "env",
    # ._channel
    "__channel__",
    "_set_channel",
    # .client
    "Client",
    "exceptions",
    # __init__
    "publish",
    # .interactive
    "map",
    "Map",
    "WorkflowsLayer",
    "LayerController",
    "LayerControllerList",
]

# NOTE(gabe): we monkey-patch these `.compute` and `.persist` methods onto the base
# Proxytype class here, rather than adding them to Proxytype directly in ``types/core/core.py``,
# because (as described above) they create a circular dependency between `models` and `types`.
# We feel this is the most reasonable approach because:
# * `models` and `types` are otherwise logically separate modules.
# * They are purely user-facing convenience methods; none of the types know,
#   care about, or rely on having a `compute` method.
# * __init__ is the place that brings these two submodules together,
#   thus it's also the right place for the helper methods that unify these modules.
# * The circular dependencies get ridiculous otherwise (the abstract base Proxytype class
#   depends on the `GeoContext` concrete subtype!)


def _compute_mixin(
    self,
    geoctx=None,
    timeout=None,
    block=True,
    progress_bar=None,
    channel=None,
    client=None,
    **params
):
    """
    Compute this proxy object and wait for its result.

    Parameters
    ----------
    geoctx: `.scenes.geocontext.GeoContext`, `~.workflows.types.geospatial.GeoContext`, or None
        The GeoContext parameter under which to run the computation.
        Almost all computations will require a `~.workflows.types.geospatial.GeoContext`,
        but for operations that only involve non-geospatial types,
        this parameter is optional.
    timeout: int, optional
        The number of seconds to wait for the result, if ``block`` is True.
        Raises `~descarteslabs.workflows.models.TimeoutError` if the timeout passes.
    block: bool, default True
        If True (default), block until the job is completed,
        or ``timeout`` has passed.
        If False, immediately returns a `.Job` (which has already had `~.Job.execute` called).
    progress_bar: bool, default None
        Whether to draw the progress bar. If ``None`` (default),
        will display a progress bar in Jupyter Notebooks, but not elsewhere.
        Ignored if ``block==False``.
    channel: str or None, optional
        Channel name to submit the `.Job` to.
        If None, uses the default channel for this client
        (``descarteslabs.workflows.__channel__``).

        Channels are different versions of the backend,
        to allow for feature changes without breaking existing code.
        Not all clients are compatible with all channels.
        This client is only guaranteed to work with its default channel,
        whose name can be found under ``descarteslabs.workflows.__channel__``.
    client : `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters
    **params: Proxytype
        Parameters under which to run the computation.

    Returns
    -------
    result
        Appropriate Python object representing the result,
        either as a plain Python type, or object from
        `descarteslabs.workflows.results`.
    """
    if geoctx is not None:
        params["geoctx"] = GeoContext._promote(geoctx)

    return compute(
        self,
        timeout=timeout,
        block=block,
        progress_bar=progress_bar,
        channel=channel,
        client=client,
        **params
    )


def _publish_mixin(self, name="", description="", client=None):
    """
    Publish this proxy object as a `.Workflow`.

    Parameters
    ----------
    name: str, default ""
        Name for the new `.Workflow`
    description: str, default ""
        Long-form description of this `.Workflow`. Markdown is supported.
    client : `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters

    Returns
    -------
    workflow: `.Workflow`
        The saved `.Workflow` object. ``workflow.id`` contains the ID of the new Workflow.
    """
    return publish(self, name, description, client)


_compute_mixin.__name__ = "compute"
_publish_mixin.__name__ = "publish"

Proxytype.compute = _compute_mixin
Proxytype.publish = _publish_mixin
