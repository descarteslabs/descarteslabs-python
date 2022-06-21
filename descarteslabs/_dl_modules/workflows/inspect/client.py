import os
import random
import shutil
import warnings
from http import HTTPStatus

import requests
from descarteslabs.config import get_settings
from urllib3.util.retry import Retry

from ...client.services.service.service import HttpRequestMethod, Service
from ...common.workflows.arrow_serialization import deserialize_pyarrow
from ...common.workflows.outputs import field_name_to_mimetype, user_format_to_mimetype
from .. import _channel
from ..execution import to_computable
from ..models.exceptions import JobTimeoutError
from ..result_types import unmarshal
from ..types import GeoContext, ProxyTypeError

_pyarrow_content_type = field_name_to_mimetype["pyarrow"]


class InspectClient(Service):
    RETRY_CONFIG = Retry(
        total=3,
        backoff_factor=random.uniform(1, 3),
        allowed_methods=frozenset(
            [HttpRequestMethod.HEAD, HttpRequestMethod.GET, HttpRequestMethod.POST]
        ),
        status_forcelist=[
            HTTPStatus.BAD_GATEWAY,
            HTTPStatus.SERVICE_UNAVAILABLE,
            HTTPStatus.GATEWAY_TIMEOUT,
        ],
        remove_headers_on_redirect=[],
    )

    def __init__(self, channel=None, host=None, url=None, auth=None, retries=None):
        if channel is None:
            channel = _channel.__channel__
        self._channel = channel

        if host is None:
            host = get_settings().workflows_host_http or _channel.DEFAULT_HTTP_HOST

        if url is None:
            url = f"https://{host}/{channel}"

        super().__init__(
            url,
            auth=auth,
            retries=retries if retries is not None else self.RETRY_CONFIG,
        )

    def inspect(
        self,
        obj,
        geoctx=None,
        format="pyarrow",
        file=None,
        cache=True,
        _ruster=None,
        timeout=60,
        **arguments,
    ):
        if geoctx is not None:
            try:
                geoctx = GeoContext._promote(geoctx)
            except ProxyTypeError as e:
                raise TypeError(f"Invalid GeoContext {geoctx!r}: {e}")

        obj, arguments, typespec, result_type = to_computable(obj, arguments)

        mimetype = user_format_to_mimetype(format)

        if file is not None and not hasattr(file, "read"):
            # assume it's a path
            file = open(os.path.expanduser(file), "wb")
            close_file = True
        else:
            close_file = False

        try:
            headers = {"Accept": mimetype}
            if cache is False:
                headers["Cache-Control"] = "no-cache"

            body = {"graft": obj.graft, "arguments": arguments}
            if geoctx is not None:
                body["geoctx"] = geoctx.graft
            if _ruster is False:
                body["no_ruster"] = True

            try:
                # TODO stream=True, use resp.raw and stream through pyarrow?
                resp = self.session.post(
                    "/inspect",
                    json=body,
                    timeout=timeout,
                    headers=headers,
                    stream=file is not None,
                )
                resp.raise_for_status()
            except requests.exceptions.Timeout as e:
                raise JobTimeoutError(e) from None

            wf_warnings = resp.headers.get("x-wf-warnings")
            if wf_warnings:
                # NOTE: If any warning header values contain commas, those messages will
                # be split.
                for wf_warning in wf_warnings.split(","):
                    warnings.warn(wf_warning)

            if file is None:
                if resp.headers["Content-Type"] == _pyarrow_content_type:
                    codec = resp.headers["X-Arrow-Codec"]
                    marshalled = deserialize_pyarrow(resp.content, codec)
                    return unmarshal.unmarshal(result_type, marshalled)
                else:
                    return resp.content
            else:
                shutil.copyfileobj(resp.raw, file)
        finally:
            if close_file:
                file.close()


global_inspect_client = None


def get_global_inspect_client():
    global global_inspect_client
    if global_inspect_client is None:
        global_inspect_client = InspectClient()
    return global_inspect_client


def inspect(
    obj,
    geoctx=None,
    format="pyarrow",
    file=None,
    cache=True,
    _ruster=None,
    timeout=60,
    client=None,
    **arguments,
):
    """
    Quickly compute a proxy object using a low-latency, lower-reliability backend.

    Inspect is meant for getting simple computations out of Workflows, primarily for interactive use.
    It's quicker but less resilient, won't be retried if it fails, and has no progress updates.

    If you have a larger computation (longer than ~30sec), or you want to be sure the computation will succeed,
    use `~models.compute` instead. `~models.compute` creates a `.Job`, which runs asynchronously,
    will be retried if it fails, and stores its results for later retrieval.

    Parameters
    ----------
    obj: Proxytype
        Proxy object to compute, or list/tuple of proxy objects.
        If it depends on parameters, ``obj`` is first converted
        to a `.Function` that takes those parameters.
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
    cache: bool, default True
        Whether to use the cache for this job.
    timeout: int, optional, default 60
        The number of seconds to wait for the result.
        Raises `~descarteslabs.workflows.models.JobTimeoutError` if the timeout passes.
    client: `.workflows.inspect.InspectClient`, optional
        Allows you to use a specific InspectClient instance with non-default
        auth and parameters
    **arguments: Any
        Values for all parameters that ``obj`` depends on
        (or arguments that ``obj`` takes, if it's a `.Function`).
        Can be given as Proxytypes, or as Python objects like numbers,
        lists, and dicts that can be promoted to them.
        These arguments cannot depend on any parameters.

    Returns
    -------
    result: Python object or bytes
        When ``format="pyarrow"`` (the default), returns an appropriate Python object representing
        the result, either as a plain Python type, or object from `descarteslabs.workflows.result_types`.
        For other formats, returns raw bytes. Consider using `file` in that case to save the results to a file.
    """
    if client is None:
        client = get_global_inspect_client()

    return client.inspect(
        obj,
        geoctx=geoctx,
        format=format,
        file=file,
        cache=cache,
        _ruster=_ruster,
        timeout=timeout,
        **arguments,
    )
