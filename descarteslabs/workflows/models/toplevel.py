from .workflow import Workflow
from .versionedgraft import VersionedGraft
from .job import Job


def compute(
    obj,
    geoctx=None,
    format="pyarrow",
    destination="download",
    file=None,
    timeout=None,
    block=True,
    progress_bar=None,
    cache=True,
    _ruster=None,
    _trace=False,
    client=None,
    num_retries=None,
    **arguments,
):
    """
    Compute a proxy object and wait for its result.

    If the caller has too many outstanding compute jobs, this will raise a ``ResourceExhausted`` exception.

    Parameters
    ----------
    obj: Proxytype
        Proxy object to compute, or list/tuple of proxy objects. If it depends on parameters, ``obj``
        is first converted to a `.Function` that takes those parameters.
    geoctx: `~.workflows.types.geospatial.GeoContext`, or None
        The GeoContext parameter under which to run the computation. Almost all computations will
        require a `~.workflows.types.geospatial.GeoContext`, but for operations that only involve
        non-geospatial types, this parameter is optional.
    format: Str or Dict, default "pyarrow"
        The serialization format for the result.
        See the `formats
        <https://docs.descarteslabs.com/descarteslabs/workflows/docs/formats.html#output-formats>`_
        documentation for more information.
        If "pyarrow" (the default), returns an appropriate Python object, otherwise returns raw bytes or None.
    destination: str or dict, default "download"
        The destination for the result.
        See the `destinations
        <https://docs.descarteslabs.com/descarteslabs/workflows/docs/destinations.html#output-destinations>`_
        documentation for more information.
    file: path or file-like object, optional
        If specified, writes results to the path or file instead of returning them.
    timeout: Int, optional
        The number of seconds to wait for the result, if ``block`` is True. Raises ``JobTimeoutError``
        if the timeout passes.
    block: Bool, default True
        If True (default), block until the job is completed, or ``timeout`` has passed.
        If False, immediately returns a `Job` (which has already had `~Job.execute` called).
    progress_bar: Bool, default None
        Whether to draw the progress bar. If ``None`` (default), will display a progress bar in
        Jupyter Notebooks, but not elsewhere. Ignored if ``block==False``.
    client: `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default auth and parameters
    num_retries: Int, optional
        The number of retries to make in the event of a request failure. If you are making numerous long-running
        asynchronous requests, you can use this parameter as a way to indicate that you are comfortable waiting
        and retrying in response to RESOURCE EXHAUSTED errors. By default, most failures will trigger a small number
        of retries, but if you have reached your outstanding job limit, by default, the client will not retry. This
        parameter is unnecessary when making synchronous `compute` requests (ie. block=True, the default).
        See the `compute section of the Workflows Guide </guides/workflows/compute.html>` for more information.
    **arguments: Any
        Values for all parameters that ``obj`` depends on (or arguments that ``obj`` takes,
        if it's a `.Function`). Can be given as Proxytypes, or as Python objects like numbers, lists,
        and dicts that can be promoted to them. These arguments cannot depend on any parameters.

    Returns
    -------
    result: Python object, bytes, or None
        When ``format="pyarrow"`` (the default), returns an appropriate Python object representing
        the result, either as a plain Python type, or object from `descarteslabs.workflows.result_types`.
        For other formats, returns raw bytes. Consider using `file` in that case to save the results to a file.
        If the destination doesn't support retrieving results (like "email"), returns None.

    Raises
    ------
    ~descarteslabs.common.retry.RetryError
        Raised if there are too many failed retries. Inspect
        `RetryError.exceptions <descarteslabs.common.retry.RetryError.exceptions>` to determine the ultimate cause
        of the error. If you reach your maximum number of outstanding compute jobs, there will be
        one or more `~descarteslabs.client.grpc.exceptions.ResourceExhausted` exceptions.

    Examples
    --------
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
    job = Job(
        obj,
        geoctx=geoctx,
        format=format,
        destination=destination,
        cache=cache,
        _ruster=_ruster,
        _trace=_trace,
        client=client,
        num_retries=num_retries,
        **arguments,
    )
    if block:
        if file is not None:
            return job.result_to_file(file, timeout=timeout, progress_bar=progress_bar)
        else:
            try:
                return job.result(timeout=timeout, progress_bar=progress_bar)
            except NotImplementedError:
                # suppress error if destination isn't supported; just wait and return nothing
                job.wait(timeout=timeout, progress_bar=progress_bar)
    else:
        return job


def publish(
    id,
    version,
    obj=None,
    title="",
    description="",
    labels=None,
    tags=None,
    docstring="",
    version_labels=None,
    viz_options=None,
    client=None,
):
    """
    Publish a proxy object as a `Workflow` with the given version. Can also be used as a decorator.

    If the proxy object depends on any parameters (``obj.params`` is not empty),
    it's first internally converted to a `.Function` that takes those parameters
    (using `.Function.from_object`).

    Parameters
    ----------
    id: Str
        ID for the new Workflow object. This should be of the form `email:workflow_name` and
        should be globally unique. If this ID is not of the proper format, you will not be able to save the Workflow.
    version: Str
        The version to be set, tied to the given `obj`. This should adhere
        to the semantic versioning schema.
    obj: Proxytype, optional
        The object to store as this version.
        If it depends on parameters, ``obj`` is first converted
        to a `.Function` that takes those parameters.

        If not provided, it's assumed that `set_version` is being
        used as a decorator on a function.
    title: Str, default ""
        User-friendly title for the `Workflow`.
    description: str, default ""
        Long-form description of this `Workflow`. Markdown is supported.
    labels: Dict, optional
        Key-value pair labels to add to the `Workflow`.
    tags: list, optional
        A list of strings to add as tags to the `Workflow`.
    docstring: Str, default ""
        The docstring for this version.
    version_labels: Dict, optional
        Key-value pair labels to add to the version.
    client: `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters

    Returns
    -------
    workflow: Workflow or Function
        The saved `Workflow` object. ``workflow.id`` contains the ID of the new Workflow.
        If used as a decorator, returns the `~.Function` instead.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> @wf.publish("bob@gmail.com:ndvi", "0.0.1") # doctest: +SKIP
    ... def ndvi(img: wf.Image) -> wf.Image:
    ...     "Compute the NDVI of an Image"
    ...     nir, red = img.unpack_bands("nir red")
    ...     return (nir - red) / (nir + red)
    >>> # `ndvi` becomes a Function proxy object
    >>> ndvi  # doctest: +SKIP
    <descarteslabs.workflows.types.Function[Image, {}, Image] object at 0x...>

    >>> two = wf.Int(1) + 1
    >>> workflow = wf.publish("bob@gmail.com:two", "1.0.0", two) # doctest: +SKIP
    >>> workflow # doctest: +SKIP
    <descarteslabs.workflows.models.workflow.Workflow object at 0x...>
    >>> workflow.version_names # doctest: +SKIP
    ["1.0.0"]
    >>> workflow["1.0.0"].object # doctest: +SKIP
    <descarteslabs.workflows.types.Int object at 0x...>

    If you publish an object that depends on parameters,
    it gets turned into a Function that takes those parameters:

    >>> something_plus_one = wf.parameter("x", wf.Int) + 1
    >>> workflow = wf.publish("bob@gmail.com:plus_one", "1.0.0", something_plus_one) # doctest: +SKIP
    >>> # `something_plus_one` depended on an Int parameter,
    >>> # so the stored object turned into a Function that takes an Int
    >>> add_one_func = workflow["1.0.0"].object # doctest: +SKIP
    >>> add_one_func # doctest: +SKIP
    <descarteslabs.workflows.types.Function[Int, {}, Int] object at 0x...>
    >>> add_one_func(2).inspect() # doctest: +SKIP
    3
    """
    workflow = Workflow(
        id,
        title=title,
        description=description,
        labels=labels,
        tags=tags,
        client=client,
    )
    vg_or_deco = workflow.set_version(
        version,
        obj=obj,
        docstring=docstring,
        labels=version_labels,
        viz_options=viz_options,
    )

    if callable(vg_or_deco):
        # decorator format
        def publish_decorator(func):
            wf_func = vg_or_deco(func)
            workflow.save()
            return wf_func

        return publish_decorator

    workflow.save()
    return workflow


def use(workflow_id, version, client=None):
    """
    Use like ``import``: load the proxy object of a published `Workflow` version.

    Parameters
    ----------
    workflow_id: Str
        ID of the `Workflow` to retrieve
    version: Str
        Version of the workflow to retrive
    client: `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters

    Returns
    -------
    obj: Proxytype
        Proxy object of the `Workflow` version.

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> @wf.publish("bob@gmail.com:ndvi", "0.0.1") # doctest: +SKIP
    ... def ndvi(img: wf.Image) -> wf.Image:
    ...     nir, red = img.unpack_bands("nir red")
    ...     return (nir - red) / (nir + red)

    >>> same_function = wf.use("bob@gmail.com:ndvi", "0.0.1") # doctest: +SKIP
    >>> same_function # doctest: +SKIP
    <descarteslabs.workflows.types.function.function.Function[Image, {}, Image] object at 0x...>
    >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
    >>> same_function(img).compute(geoctx) # geoctx is an arbitrary geocontext for 'img' # doctest: +SKIP
    ImageResult:
    ...
    """
    return VersionedGraft.get(workflow_id, version, client=client).object
