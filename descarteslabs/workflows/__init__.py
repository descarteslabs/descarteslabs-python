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
    ifelse,
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
    VersionedGraft,
    Job,
    XYZ,
    XYZErrorListener,
    compute as _compute,
    use,
    publish as _publish,
)

from .inspect import (
    InspectClient,
    get_global_inspect_client as _get_global_inspect_client,
)

from .interactive import map, flows, Map, WorkflowsLayer, LayerController, LayerControllerList

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
    destination="download",
    file=None,
    timeout=None,
    block=True,
    progress_bar=None,
    client=None,
    cache=True,
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
        If "pyarrow" (the default), returns an appropriate Python object, otherwise returns raw bytes.
    destination: str or dict, default "download"
        The destination for the result.
        See the `destinations
        <https://docs.descarteslabs.com/descarteslabs/workflows/docs/destinations.html#output-destinations>`_
        documentation for more information.
    file: path or file-like object, optional
        If specified, writes results to the path or file instead of returning them.
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
    client: `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters
    cache: bool, default True
        Whether to use the cache for this job.
    **params: Proxytype
        Parameters under which to run the computation, such as ``geoctx``.

    Returns
    -------
    result: Python object, bytes, or None
        When ``format="pyarrow"`` (the default), returns an appropriate Python object representing
        the result, either as a plain Python type, or object from `descarteslabs.workflows.result_types`.
        For other formats, returns raw bytes. Consider using `file` in that case to save the results to a file.
        If the destination doesn't support retrieving results (like "email"), returns None

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> num = wf.Int(1) + 1
    >>> wf.compute(num) # doctest: +SKIP
    2
    >>> # same computation but do not block
    >>> job = wf.compute(num, block=False) # doctest: +SKIP
    >>> job # doctest: +SKIP
    <descarteslabs.workflows.models.job.Job object at 0x...>
    >>> job.result() # doctest: +SKIP
    2
    >>> # pass multiple proxy objects to `wf.compute` to compute all at once
    >>> wf.compute((num, num, num)) # doctest: +SKIP
    (2, 2, 2)

    >>> # specifying a format
    >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1").pick_bands("red")
    >>> wf.compute(img, geoctx=ctx, format="pyarrow") # default # doctest: +SKIP
    ImageResult:
    ...
    >>> # same computation but with json format
    >>> wf.compute(img, geoctx=ctx, format="json") # doctest: +SKIP
    b'{"ndarray":[[[0.39380000000000004,0.3982,0.3864,...
    >>> # same computation but with geotiff format (and some format options)
    >>> bytes_ = wf.compute(img, geoctx=ctx, format={"type": "geotiff", "tiled": False}) # doctest: +SKIP
    >>> # you probably want to save the geotiff to a file:
    >>> wf.compute(img, geoctx=ctx, file="my_geotiff.tif", format={"type": "geotiff", "tiled": False}) # doctest: +SKIP

    >>> # specifying a destination
    >>> num = wf.Int(1) + 1
    >>> wf.compute(num, destination="download") # default # doctest: +SKIP
    2
    >>> # same computation but with email destination
    >>> wf.compute(num, destination="email") # doctest: +SKIP
    >>> # now with some destination options
    >>> wf.compute(
    ...     num,
    ...     destination={
    ...         "type": "email",
    ...         "subject": "My Computation is Done"
    ...     },
    ...     format="json",
    ... ) # doctest: +SKIP
    """
    if isinstance(obj, (tuple, list)):
        obj = proxify(obj)

    if geoctx is not None:
        geoctx = GeoContext._promote(geoctx)

    return _compute(
        obj,
        geoctx=geoctx,
        format=format,
        destination=destination,
        file=file,
        timeout=timeout,
        block=block,
        progress_bar=progress_bar,
        client=client,
        cache=cache,
        **params
    )


def publish(
    id,
    version,
    obj=None,
    title="",
    description="",
    public=False,
    labels=None,
    tags=None,
    docstring="",
    version_labels=None,
    client=None,
):
    """
    Publish a proxy object as a `Workflow` with the given version. Can also be used as a decorator.

    Parameters
    ----------
    id: str
        ID for the new `Workflow`. This should be of the form ``"email:workflow_name"``
        and should be globally unique. If this ID is not of the proper format, you will
        not be able to save the `Workflow`.
    version: str
        The version to be set, tied to the given `proxy_object`. This should adhere
        to the semantic versioning schema (https://semver.org).
    obj: Proxytype
        The object to store as this version. If not provided, it's assumed
        that `publish` is being used as a decorator on a function.
    title: str, default ""
        User-friendly title for the `Workflow`.
    description: str, default ""
        Long-form description of this `Workflow`. Markdown is supported.
    public: bool, default `False`
        Whether this `Workflow` will be publicly accessible.
    labels: dict, optional
        Key-value pair labels to add to the `Workflow`.
    tags: list, optional
        A list of strings to add as tags to the `Workflow`.
    docstring: str, default ""
        The docstring for this version.
    version_labels: dict, optional
        Key-value pair labels to add to the version.
    client: `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters

    Returns
    -------
    workflow: Workflow or Function
        The saved `Workflow` object. ``workflow.id`` contains the ID of the new Workflow.
        If used as a decorator, returns the `~.Function` instead.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> @wf.publish("bob@gmail.com:ndvi", "0.0.1") # doctest: +SKIP
    ... def ndvi(img: wf.Image) -> wf.Image:
    ...     "Compute the NDVI of an Image"
    ...     nir, red = img.unpack_bands("nir red")
    ...     return (nir - red) / (nir + red)

    >>> two = wf.Int(1) + 1
    >>> workflow = wf.publish("bob@gmail.com:two", "1.0.0", two) # doctest: +SKIP
    >>> workflow # doctest: +SKIP
    <descarteslabs.workflows.models.workflow.Workflow object at 0x...>
    >>> workflow.version_names # doctest: +SKIP
    ["1.0.0"]
    """
    if obj is not None:
        obj = proxify(obj)
    return _publish(
        id,
        version,
        obj,
        title=title,
        description=description,
        public=public,
        labels=labels,
        tags=tags,
        docstring=docstring,
        version_labels=version_labels,
        client=client,
    )


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
    "ifelse",
    "e",
    "inf",
    "nan",
    "pi",
    # .models
    "JobComputeError",
    "JobTimeoutError",
    "Workflow",
    "VersionedGraft",
    "Job",
    "XYZ",
    "XYZErrorListener",
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
    "flows",
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
    format="pyarrow",
    destination="download",
    file=None,
    timeout=None,
    block=True,
    progress_bar=None,
    client=None,
    cache=True,
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
    format: str or dict, default "pyarrow"
        The serialization format for the result.
        See the `formats
        <https://docs.descarteslabs.com/descarteslabs/workflows/docs/formats.html#output-formats>`_
        documentation for more information.
        If "pyarrow" (the default), returns an appropriate Python object, otherwise returns raw bytes.
    destination: str or dict, default "download"
        The destination for the result.
        See the `destinations
        <https://docs.descarteslabs.com/descarteslabs/workflows/docs/destinations.html#output-destinations>`_
        documentation for more information.
    file: path or file-like object, optional
        If specified, writes results to the path or file instead of returning them.
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
    client: `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters
    cache: bool, default True
        Whether to use the cache for this job.
    **params: Proxytype
        Parameters under which to run the computation.

    Returns
    -------
    result: Python object, bytes, or None
        When ``format="pyarrow"`` (the default), returns an appropriate Python object representing
        the result, either as a plain Python type, or object from `descarteslabs.workflows.result_types`.
        For other formats, returns raw bytes. Consider using `file` in that case to save the results to a file.
        If the destination doesn't support retrieving results (like "email"), returns None
    """
    if geoctx is not None:
        params["geoctx"] = GeoContext._promote(geoctx)

    return compute(
        self,
        format=format,
        destination=destination,
        file=file,
        timeout=timeout,
        block=block,
        progress_bar=progress_bar,
        client=client,
        cache=cache,
        **params
    )


def _publish_mixin(
    self,
    id,
    version,
    title="",
    description="",
    public=False,
    labels=None,
    tags=None,
    docstring="",
    version_labels=None,
    client=None,
):
    """
    Publish this proxy object as a `.Workflow` version.

    Parameters
    ----------
    id: str
        ID for the new `~.Workflow`. This should be of the form ``"email:workflow_name"``
        and should be globally unique. If this ID is not of the proper format, you will
        not be able to save the `~.Workflow`.
    version: str
        The version to be set, tied to the given `proxy_object`. This should adhere
        to the semantic versioning schema.
    title: str, default ""
        User-friendly title for the `~.Workflow`.
    description: str, default ""
        Long-form description of this `~.Workflow`. Markdown is supported.
    public: bool, default `False`
        Whether this `~.Workflow` will be publicly accessible.
    labels: dict, optional
        Key-value pair labels to add to the `~.Workflow`.
    tags: list, optional
        A list of strings to add as tags to the `~.Workflow`.
    docstring: str, default ""
        The docstring for this version.
    version_labels: dict, optional
        Key-value pair labels to add to the version.
    client: `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters

    Returns
    -------
    workflow: `.Workflow`
        The saved `.Workflow` object. ``workflow.id`` contains the ID of the new Workflow.
    """
    return publish(
        id,
        version,
        obj=self,
        title=title,
        description=description,
        public=public,
        labels=labels,
        tags=tags,
        docstring=docstring,
        version_labels=version_labels,
        client=client,
    )


def _inspect_mixin(
    self, geoctx=None, format="pyarrow", file=None, timeout=30, client=None, **params
):
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
    format: str or dict, default "pyarrow"
        The serialization format for the result.
        See the `formats
        <https://docs.descarteslabs.com/descarteslabs/workflows/docs/formats.html#output-formats>`_
        documentation for more information.
        If "pyarrow" (the default), returns an appropriate Python object, otherwise returns raw bytes.
    file: path or file-like object, optional
        If specified, writes results to the path or file instead of returning them.
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
    result: Python object or bytes
        When ``format="pyarrow"`` (the default), returns an appropriate Python object representing
        the result, either as a plain Python type, or object from `descarteslabs.workflows.result_types`.
        For other formats, returns raw bytes. Consider using `file` in that case to save the results to a file.
    """
    if geoctx is not None:
        params["geoctx"] = GeoContext._promote(geoctx)

    if client is None:
        client = _get_global_inspect_client()

    return client.inspect(self, format=format, file=file, timeout=timeout, **params)


_compute_mixin.__name__ = "compute"
_publish_mixin.__name__ = "publish"
_inspect_mixin.__name__ = "inspect"

Proxytype.compute = _compute_mixin
Proxytype.publish = _publish_mixin
Proxytype.inspect = _inspect_mixin
