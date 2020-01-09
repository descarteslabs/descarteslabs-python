from .workflow import Workflow
from .job import Job


def compute(
    obj,
    timeout=None,
    block=True,
    progress_bar=None,
    channel=None,
    client=None,
    **params
):
    """
    Compute a proxy object and wait for its result.

    Examples
    --------
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

    Parameters
    ----------
    obj: Proxytype
        A proxy object to compute
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
    channel: str or None, optional
        Channel name to submit the `Job` to.
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
        Parameters under which to run the computation, such as ``geoctx``.

    Returns
    -------
    result
        Appropriate Python object representing the result,
        either as a plain Python type, or object from
        `descarteslabs.workflows.results`.
    """
    job = Job.build(obj, params, channel=channel, client=client)
    job.execute()
    if block:
        return job.result(timeout=timeout, progress_bar=progress_bar)
    else:
        return job


def publish(obj, name="", description="", client=None):
    """
    Publish a proxy object as a `Workflow`.

    Examples
    --------
    >>> from descarteslabs.workflows import Image, Function
    >>> def ndvi(img):
    ...     nir, red = img.unpack_bands("nir red")
    ...     return (nir - red) / (nir + red)
    >>> func = Function.from_callable(ndvi, Image)
    >>> workflow = func.publish("NDVI") # doctest: +SKIP
    >>> workflow # doctest: +SKIP
    <descarteslabs.workflows.models.workflow.Workflow object at 0x...>

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
    """
    workflow = Workflow.build(obj, name=name, description=description, client=client)
    workflow.save()
    return workflow


def retrieve(workflow_id, client=None):
    """
    Load a published `Workflow` object.

    Examples
    --------
    >>> from descarteslabs.workflows import Image, Function, retrieve
    >>> def ndvi(img):
    ...     nir, red = img.unpack_bands("nir red")
    ...     return (nir - red) / (nir + red)
    >>> func = Function.from_callable(ndvi, Image) # create a function that can be called on an Image
    >>> workflow = func.publish("NDVI") # doctest: +SKIP
    >>> workflow.id # doctest: +SKIP
    '42cea96a864811f00f0bcdb8177ba80d6dc9c7492e13e794'
    >>> same_workflow = retrieve('42cea96a864811f00f0bcdb8177ba80d6dc9c7492e13e794') # doctest: +SKIP
    >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
    >>> same_workflow.object(img).compute(geoctx) # geoctx is an arbitrary geocontext for 'img' # doctest: +SKIP
    ImageResult:
    ...

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
    """
    return Workflow.get(workflow_id, client=client)


def use(workflow_id, client=None):
    """
    Use like ``import``: load the proxy object of a published `Workflow`.

    Shorthand for ``retrieve(workflow_id).object``.

    Examples
    --------
    >>> from descarteslabs.workflows import Image, Function, use
    >>> def ndvi(img):
    ...     nir, red = img.unpack_bands("nir red")
    ...     return (nir - red) / (nir + red)
    >>> func = Function.from_callable(ndvi, Image) # create a function that can be called on an Image
    >>> workflow = func.publish("NDVI") # doctest: +SKIP
    >>> workflow.id # doctest: +SKIP
    '42cea96a864811f00f0bcdb8177ba80d6dc9c7492e13e794'
    >>> same_function = use('42cea96a864811f00f0bcdb8177ba80d6dc9c7492e13e794') # doctest: +SKIP
    >>> img = Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
    >>> same_function(img).compute(geoctx) # geoctx is an arbitrary geocontext for 'img' # doctest: +SKIP
    ImageResult:
    ...

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
    """
    return retrieve(workflow_id, client=client).object
