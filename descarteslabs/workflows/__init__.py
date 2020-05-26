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
    Array,
    MaskedArray,
    Tuple,
    List,
    Dict,
    Struct,
    range,
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
    numpy,
)

from .models import (
    JobComputeError,
    JobTimeoutError,
    Workflow,
    Job,
    XYZ,
    XYZErrorListener,
    compute as _compute,
    retrieve,
    use,
    publish as _publish,
)

from .inspect import (
    InspectClient,
    get_global_inspect_client as _get_global_inspect_client,
)

from .interactive import map, Map, WorkflowsLayer, LayerController, LayerControllerList

from . import env
from ._channel import __channel__, _set_channel
from .client import Client, exceptions


# NOTE(gabe): we define these top-level `compute` and `publish` implementations, which try to proxify
# any plain Python objects and promote the geoctx before computing/publishing them. Since this brings together two
# otherwise-separate submodules (`models` and `types`), it's cleaner to write it here
# in __init__ than have circular dependencies between those submodules.
def compute(
    obj,
    geoctx=None,
    format="pyarrow",
    timeout=None,
    block=True,
    progress_bar=None,
    client=None,
    **params
):
    """
    Compute a proxy object and wait for its result.

    Parameters
    ----------
    obj: Proxytype, list, tuple
        A proxy object to compute. Can also provide a Python list/tuple of proxy objects.
    geoctx: `.scenes.geocontext.GeoContext`, `~.workflows.types.geospatial.GeoContext`, or None
        The GeoContext parameter under which to run the computation.
        Almost all computations will require a `~.workflows.types.geospatial.GeoContext`,
        but for operations that only involve non-geospatial types,
        this parameter is optional.
    format: str or dict, default "pyarrow"
        The serialization format for the result.
        See the `formats
        <https://docs.descarteslabs.com/descarteslabs/workflows/docs/formats.html#output-formats>`_
        documentation for more information.
    timeout: int, optional
        The number of seconds to wait for the result, if ``block`` is True.
        Raises ``JobTimeoutError`` if the timeout passes.
    block: bool, default True
        If True (default), block until the job is completed,
        or ``timeout`` has passed.
        If False, immediately returns a `Job` (which has already had `~Job.execute` called).
    progress_bar: bool, default None
        Whether to draw the progress bar. If ``None`` (default),
        will display a progress bar in Jupyter Notebooks, but not elsewhere.
        Ignored if ``block==False``.
    client : `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters
    **params: Proxytype
        Parameters under which to run the computation, such as ``geoctx``.

    Returns
    -------
    result
        Appropriate Python object representing the result,
        either as a plain Python type, or object from
        `descarteslabs.workflows.results`.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> num = wf.Int(1) + 1
    >>> wf.compute(num) # doctest: +SKIP
    2
    >>> # same computation but do not block
    >>> job = wf.compute(block=False) # doctest: +SKIP
    >>> job # doctest: +SKIP
    <descarteslabs.workflows.models.job.Job object at 0x...>
    >>> job.result() # doctest: +SKIP
    2
    >>> # pass multiple proxy objects to compute at once
    >>> wf.compute((num, num, num)) # doctest: +SKIP
    (2, 2, 2)

    >>> # specifying a format
    >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1").pick_bands("red")
    >>> wf.compute(img, geoctx=ctx, format="pyarrow") # default # doctest: +SKIP
    ImageResult:
    ...
    >>> # same computation but with json format
    >>> wf.compute(img, geoctx=ctx, format="json") # doctest: +SKIP
    {'ndarray': [[[0.39380000000000004,
        0.3982,
        0.3864,
    ...
    >>> # same computation but with geotiff format (and some format options)
    >>> bytes_ = wf.compute(img, geoctx=ctx, format={"type": "geotiff", "tiled": False}) # doctest: +SKIP
    >>> with open("/home/example.tiff", "wb") as out: # doctest: +SKIP
    >>>     out.write(bytes_) # doctest: +SKIP
    """
    if isinstance(obj, (tuple, list)):
        obj = proxify(obj)

    if geoctx is not None:
        geoctx = GeoContext._promote(geoctx)

    return _compute(
        obj,
        geoctx=geoctx,
        format=format,
        timeout=timeout,
        block=block,
        progress_bar=progress_bar,
        client=client,
        **params
    )


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
    "Array",
    "MaskedArray",
    "Tuple",
    "List",
    "Dict",
    "Struct",
    "range",
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
    "JobTimeoutError",
    "Workflow",
    "Job",
    "XYZ",
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
    "compute",
    "publish",
    # .inspect
    "InspectClient",
    # .interactive
    "map",
    "Map",
    "WorkflowsLayer",
    "LayerController",
    "LayerControllerList",
    # .numpy
    "numpy",
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
        Raises `~descarteslabs.workflows.models.JobTimeoutError` if the timeout passes.
    block: bool, default True
        If True (default), block until the job is completed,
        or ``timeout`` has passed.
        If False, immediately returns a `.Job` (which has already had `~.Job.execute` called).
    progress_bar: bool, default None
        Whether to draw the progress bar. If ``None`` (default),
        will display a progress bar in Jupyter Notebooks, but not elsewhere.
        Ignored if ``block==False``.
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


def _inspect_mixin(self, geoctx=None, timeout=30, client=None, **params):
    """
    Quickly compute this proxy object using a low-latency, lower-reliability backend.

    Inspect is meant for getting simple computations out of Workflows, primarily for interactive use.
    It's quicker but less resilient, won't be retried if it fails, and has no progress updates.

    If you have a larger computation (longer than ~30sec), or you want to be sure the computation will succeed,
    use `~.compute` instead. `~.compute` creates a `.Job`, which runs asynchronously, will be retried if it fails,
    and stores its results for later retrieval.

    Parameters
    ----------
    geoctx: `.scenes.geocontext.GeoContext`, `~.workflows.types.geospatial.GeoContext`, or None
        The GeoContext parameter under which to run the computation.
        Almost all computations will require a `~.workflows.types.geospatial.GeoContext`,
        but for operations that only involve non-geospatial types,
        this parameter is optional.
    timeout: int, optional, default 30
        The number of seconds to wait for the result.
        Raises `~descarteslabs.workflows.models.JobTimeoutError` if the timeout passes.
    client: `.workflows.inspect.InspectClient`, optional
        Allows you to use a specific InspectClient instance with non-default
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

    if client is None:
        client = _get_global_inspect_client()

    return client.inspect(self, timeout=timeout, **params)


_compute_mixin.__name__ = "compute"
_publish_mixin.__name__ = "publish"
_inspect_mixin.__name__ = "inspect"

Proxytype.compute = _compute_mixin
Proxytype.publish = _publish_mixin
Proxytype.inspect = _inspect_mixin
