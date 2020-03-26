from .workflow import Workflow
from .job import Job


def compute(
    obj, geoctx=None, timeout=None, block=True, progress_bar=None, client=None, **params
):
    """
    Compute a proxy object and wait for its result.

    Parameters
    ----------
    obj: Proxytype
        A proxy object to compute.
    geoctx: `~.workflows.types.geospatial.GeoContext`, or None
        The GeoContext parameter under which to run the computation.
        Almost all computations will require a `~.workflows.types.geospatial.GeoContext`,
        but for operations that only involve non-geospatial types,
        this parameter is optional.
    timeout: int, optional
        The number of seconds to wait for the result, if ``block`` is True.
        Raises ``TimeoutError`` if the timeout passes.
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
    >>> from descarteslabs.workflows import Int
    >>> num = Int(1) + 1
    >>> num.compute() # doctest: +SKIP
    2
    >>> # same computation but do not block
    >>> job = num.compute(block=False) # doctest: +SKIP
    >>> job # doctest: +SKIP
    <descarteslabs.workflows.models.job.Job object at 0x...>
    >>> job.result() # doctest: +SKIP
    2
    """
    if geoctx is not None:
        params["geoctx"] = geoctx

    job = Job.build(obj, params, client=client)
    job.execute()
    if block:
        return job.result(timeout=timeout, progress_bar=progress_bar)
    else:
        return job


def publish(obj, name="", description="", client=None):
    """
    Publish a proxy object as a `Workflow`.

    Parameters
    ----------
    obj: Proxytype
        A proxy object to compute
    name: str, default ""
        Name for the new `Workflow`
    description: str, default ""
        Long-form description of this `Workflow`. Markdown is supported.
    client : `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters

    Returns
    -------
    workflow: `Workflow`
        The saved `Workflow` object. ``workflow.id`` contains the ID of the new Workflow.

    Example
    -------
    >>> from descarteslabs.workflows import Image, Function
    >>> def ndvi(img):
    ...     nir, red = img.unpack_bands("nir red")
    ...     return (nir - red) / (nir + red)
    >>> func = Function.from_callable(ndvi, Image)
    >>> workflow = func.publish("NDVI") # doctest: +SKIP
    >>> workflow # doctest: +SKIP
    <descarteslabs.workflows.models.workflow.Workflow object at 0x...>
    """
    workflow = Workflow.build(obj, name=name, description=description, client=client)
    workflow.save()
    return workflow


def retrieve(workflow_id, client=None):
    """
    Load a published `Workflow` object.

    Parameters
    ----------
    workflow_id: str
        ID of the `Workflow` to retrieve
    client : Compute, optional
        Allows you to use a specific client instance with non-default
        auth and parameters

    Returns
    -------
    workflow: `Workflow`
        Object representing the workflow, including both its metadata
        (like ``workflow.name``, ``workflow.description``) and proxy object
        (``workflow.object``).

    Example
    -------
    >>> from descarteslabs.workflows import Image, Function, retrieve
    >>> def ndvi(img):
    ...     nir, red = img.unpack_bands("nir red")
    ...     return (nir - red) / (nir + red)
    >>> func = Function.from_callable(ndvi, Image) # create a function that can be called on an Image
    >>> workflow = func.publish("NDVI") # doctest: +SKIP
    >>> workflow.id # doctest: +SKIP
    '42cea96a864811f00f0bcdb8177ba80d6dc9c7492e13e794'
    >>> same_workflow = retrieve('42cea96a864811f00f0bcdb8177ba80d6dc9c7492e13e794') # doctest: +SKIP
    >>> same_workflow # doctest: +SKIP
    <descarteslabs.workflows.models.workflow.Workflow object at 0x...>
    >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
    >>> same_workflow.object(img).compute(geoctx) # geoctx is an arbitrary geocontext for 'img' # doctest: +SKIP
    >>> # notice the bandname is comprised of the operations called to create it
    ImageResult:
      * ndarray: MaskedArray<shape=(1, 512, 512), dtype=float64>
      * properties: 'absolute_orbit', 'acquired', 'archived', 'area', ...
      * bandinfo: 'nir_sub_red_div_nir_add_red'
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
    """
    return Workflow.get(workflow_id, client=client)


def use(workflow_id, client=None):
    """
    Use like ``import``: load the proxy object of a published `Workflow`.

    Shorthand for ``retrieve(workflow_id).object``.

    Parameters
    ----------
    workflow_id: str
        ID of the `Workflow` to retrieve
    client : `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters

    Returns
    -------
    obj: Proxytype
        Proxy object of the `Workflow`.

    Example
    -------
    >>> from descarteslabs.workflows import Image, Function, use
    >>> def ndvi(img):
    ...     nir, red = img.unpack_bands("nir red")
    ...     return (nir - red) / (nir + red)
    >>> func = Function.from_callable(ndvi, Image) # create a function that can be called on an Image
    >>> workflow = func.publish("NDVI") # doctest: +SKIP
    >>> workflow.id # doctest: +SKIP
    '42cea96a864811f00f0bcdb8177ba80d6dc9c7492e13e794'
    >>> same_function = use('42cea96a864811f00f0bcdb8177ba80d6dc9c7492e13e794') # doctest: +SKIP
    >>> same_function # doctest: +SKIP
    <descarteslabs.workflows.types.function.function.Function[Image, {}, Image] object at 0x...>
    >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
    >>> same_function(img).compute(geoctx) # geoctx is an arbitrary geocontext for 'img' # doctest: +SKIP
    ImageResult:
    ...
    """
    return retrieve(workflow_id, client=client).object
