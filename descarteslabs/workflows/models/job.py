import datetime
import json
import logging
import os
import shutil
import sys

import grpc
import requests

from descarteslabs.client.version import __version__
from descarteslabs.common.graft import client as graft_client, syntax as graft_syntax
from descarteslabs.common.registry import registry
from descarteslabs.common.proto.job import job_pb2
from descarteslabs.common.proto.types import types_pb2
from descarteslabs.common.proto.destinations import destinations_pb2
from descarteslabs.common.proto.formats import formats_pb2
from descarteslabs.common.workflows.outputs import (
    user_format_to_proto,
    user_destination_to_proto,
)
from descarteslabs.common.workflows.proto_munging import (
    which_has,
    has_proto_to_user_dict,
)
from descarteslabs.common.workflows.arrow_serialization import deserialize_pyarrow

from descarteslabs import catalog

from ..cereal import deserialize_typespec
from ..client import get_global_grpc_client, default_grpc_retry_predicate
from ..execution import to_computable
from ..result_types import unmarshal
from ..types import GeoContext, ProxyTypeError, Any
from .exceptions import error_code_to_exception, JobTimeoutError, JobCancelled
from .utils import (
    in_notebook,
    pb_milliseconds_to_datetime,
    pb_timestamp_to_datetime,
)

logger = logging.getLogger(__name__)


def _write_to_io_or_widget(io, string):
    if io is not None:
        # try/except avoids having to import ipywidgets just for an isinstance check
        try:
            io.append_stdout(string)
        except AttributeError:
            io.write(string)
            io.flush()


class Job:
    """
    A `Job` represents the computation of a proxy object within a `~.geospatial.GeoContext`,
    with values (arguments) set for any parameters it depends on.

    If the proxy object depends on any parameters (``obj.params`` is not empty),
    it's first internally converted to a `.Function` that takes those parameters
    (using `.Function.from_object`).

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> num = wf.Int(1) + wf.parameter("x", wf.Int)
    >>> job = num.compute(block=False, x=1)  # doctest: +SKIP
    >>> job # doctest: +SKIP
    <descarteslabs.workflows.models.job.Job object at 0x...>
    >>> job.id # doctest: +SKIP
    '3754676080bbb2b857fbc04a3e48f6312732e1bc42e0bd7b'
    >>> job.result() # doctest: +SKIP
    2
    >>> same_job = wf.Job.get('3754676080bbb2b857fbc04a3e48f6312732e1bc42e0bd7b') # doctest: +SKIP
    >>> same_job.stage # doctest: +SKIP
    'STAGE_DONE'
    >>> same_job.result() # doctest: +SKIP
    2
    >>> same_job.arguments() # doctest: +SKIP
    {'x': 1}
    """

    def __init__(
        self,
        obj,
        geoctx=None,
        format="pyarrow",
        destination="download",
        cache=True,
        _ruster=None,
        _trace=False,
        client=None,
        num_retries=None,
        **arguments,
    ):
        """
        Creates a new `Job` to compute the provided proxy object with the given
        arguments.

        Parameters
        ----------
        obj: Proxytype
            Proxy object to compute, or list/tuple of proxy objects.
            If it depends on parameters, ``obj`` is first converted
            to a `.Function` that takes those parameters.
        geoctx: `~.workflows.types.geospatial.GeoContext`, or None
            The GeoContext parameter under which to run the computation.
            Almost all computations will require a `~.workflows.types.geospatial.GeoContext`,
            but for operations that only involve non-geospatial types,
            this parameter is optional.
        format: str or dict, default "pyarrow"
            The serialization format for the result.
        destination: str or dict, default "download"
            The destination for the result.
        cache: bool, default True
            Whether to use the cache for this job.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters
        num_retries: Int, optional
            The number of retries to make in the event of a request failure. If you are making numerous long-running
            asynchronous requests, you can use this parameter as a way to indicate that you are comfortable waiting
            and retrying in response to RESOURCE EXHAUSTED errors. By default, most failures will trigger a small number
            of retries, but if you have reached your outstanding job limit, by default, the client will not retry.
            See the `compute section of the Workflows Guide </guides/workflows/compute.html>` for more information.
        **arguments: Any
            Values for all parameters that ``obj`` depends on
            (or arguments that ``obj`` takes, if it's a `.Function`).
            Can be given as Proxytypes, or as Python objects like numbers,
            lists, and dicts that can be promoted to them.
            These arguments cannot depend on any parameters.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int, parameter
        >>> my_int = Int(1) + parameter("other_int", Int)
        >>> job = Job(my_int, other_int=10) # doctest: +SKIP
        >>> job.stage # doctest: +SKIP
        QUEUED
        """
        if client is None:
            client = get_global_grpc_client()

        if geoctx is not None:
            try:
                geoctx = GeoContext._promote(geoctx)
            except ProxyTypeError as e:
                raise TypeError(f"Invalid GeoContext {geoctx!r}: {e}")

        obj, argument_grafts, typespec, result_type = to_computable(obj, arguments)

        format_proto = user_format_to_proto(format)
        destination_proto = user_destination_to_proto(destination)

        message = client.api["CreateJob"](
            job_pb2.CreateJobRequest(
                serialized_graft=json.dumps(obj.graft),
                typespec=typespec,
                arguments={
                    name: json.dumps(arg) for name, arg in argument_grafts.items()
                },
                geoctx_graft=json.dumps(geoctx.graft) if geoctx is not None else None,
                no_ruster=_ruster is False,
                channel=client._wf_channel,
                client_version=__version__,
                no_cache=not cache,
                trace=_trace,
                type=types_pb2.ResultType.Value(result_type),
                format=format_proto,
                destination=destination_proto,
            ),
            timeout=client.DEFAULT_TIMEOUT,
            num_retries=num_retries,
        )

        self._message = message
        self._client = client
        self._object = obj
        self._arguments = None

    @classmethod
    def get(cls, id, client=None):
        """
        Get a currently-running `Job` by its ID.

        Parameters
        ----------
        id: string
            The ID of a running job.
        client : `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters

        Example
        -------
        >>> from descarteslabs.workflows import Job
        >>> job = Job.get('3754676080bbb2b857fbc04a3e48f6312732e1bc42e0bd7b') # doctest: +SKIP
        """
        if client is None:
            client = get_global_grpc_client()

        message = client.api["GetJob"](
            job_pb2.GetJobRequest(id=id), timeout=client.DEFAULT_TIMEOUT
        )
        return cls._from_proto(message, client)

    @classmethod
    def _from_proto(cls, proto_message, client=None):
        """
        Low-level constructor for creating a Job from a Protobuf message.

        Do not use this method directly; use `Job.__init__` or `Job.get` instead.

        Parameters
        ----------
        proto_message: job_pb2.Job message
            Job Protobuf message
        client : `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters
        """
        obj = cls.__new__(cls)
        obj._message = proto_message

        if client is None:
            client = get_global_grpc_client()

        obj._client = client
        obj._object = None
        obj._arguments = None
        return obj

    def refresh(self):
        """
        Refresh the attributes and state of the job.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int
        >>> job = Job(Int(1)) # doctest: +SKIP
        >>> job.stage # doctest: +SKIP
        QUEUED
        >>> job.refresh() # doctest: +SKIP
        >>> job.stage # doctest: +SKIP
        SUCCEEDED
        """
        message = self._client.api["GetJob"](
            job_pb2.GetJobRequest(id=self.id), timeout=self._client.DEFAULT_TIMEOUT
        )
        self._message = message

    def resubmit(self):
        """
        Resubmit this job, returning a new `Job` object.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int
        >>> job = Job(Int(1)) # doctest: +SKIP
        >>> job.id # doctest: +SKIP
        abc123
        >>> job.result() # doctest: +SKIP
        1
        >>> new_job = job.resubmit() # doctest: +SKIP
        >>> new_job.id # doctest: +SKIP
        xyz456
        >>> new_job.result() # doctest: +SKIP
        1
        """
        if self.version != __version__:
            raise NotImplementedError(
                f"Resubmitting a Job from a different version is not supported. "
                f"This Job {self.id!r} was created by client version {self.version!r}, "
                f"but you're currently running {__version__!r}."
            )

        return Job(
            self.object,
            geoctx=self.geoctx,
            format=self.format,
            destination=self.destination,
            client=self._client,
            cache=self.cache_enabled,
            _ruster=not self._message.no_ruster,
            _trace=self._message.trace,
            **self.arguments,
        )

    def cancel(self):
        """
        Cancel a running job.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int
        >>> my_int = Int(1)
        >>> job = Job(my_int) # doctest: +SKIP
        >>> job.cancel() # doctest: +SKIP
        """
        self._client.api["CancelJob"](
            job_pb2.CancelJobRequest(id=self.id), timeout=self._client.DEFAULT_TIMEOUT
        )

    def watch(self, timeout=None):
        """
        Generator that yields ``self`` each time an update to the Job occurs.

        Parameters
        ----------
        timeout: int, optional
            The number of seconds to watch for Job updates. Defaults to
            self._client.STREAM_TIMEOUT, which is also the maximum allowed.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int
        >>> job = Job(Int(1)) # doctest: +SKIP
        >>> for job in job.watch(): # doctest: +SKIP
        ...     print(job.stage)
        QUEUED
        PREPARING
        RUNNING
        RUNNING
        SAVING
        SUCCEEDED
        """
        if timeout is None:
            timeout = self._client.STREAM_TIMEOUT
        else:
            # Take the shortest of the user-specified timeout and the client default
            # stream timeout.
            timeout = min(timeout, self._client.STREAM_TIMEOUT)

        stream = self._client.api["WatchJob"](
            job_pb2.WatchJobRequest(id=self.id), timeout=timeout
        )

        for state in stream:
            self._message.state.CopyFrom(state)
            yield self

    def result(self, timeout=None, progress_bar=None):
        """
        Get the result of the job. This blocks until the job is
        complete.

        Only the "download" destination can be retrieved.
        Raises NotImplementedError for other destinations.

        Parameters
        ----------
        timeout: int, optional
            The number of seconds to wait for the result.
        progress_bar: bool, optional
            Flag to draw the progress bar. Default is to ``True`` if in
            Jupyter Notebook.

        Returns
        -------
        result: Python object or bytes
            When the Job's format is "pyarrow", returns a Python object representing
            the result, either as a plain Python type, or object from `descarteslabs.workflows.result_types`.
            For other formats, returns raw bytes. Consider using `result_to_file` in that case
            to save the results to a file.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int
        >>> job = Job(Int(1)) # doctest: +SKIP
        >>> job.result() # doctest: +SKIP
        1
        """
        handler = get_loader(self._message.destination)
        self.wait(timeout=timeout, progress_bar=progress_bar)
        return handler(self)

    def result_to_file(self, file, timeout=None, progress_bar=None):
        """
        Save the result of the job to a file. This blocks until the job is
        complete.

        Only the "download" destination can be written to a file.
        For destinations like "catalog", where the data is handed off
        to another service, you'll need to use that service to retrieve it.
        (In the "catalog" case, that's `Raster` and `Metadata`.)

        Parameters
        ----------
        file: path or file-like object
            Path or file where results will be written
        timeout: int, optional
            The number of seconds to wait for the result.
        progress_bar: bool, optional
            Flag to draw the progress bar. Default is to ``True`` if in
            Jupyter Notebook.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int
        >>> job = Job(Int(1), format="json") # doctest: +SKIP
        >>> job.result_to_file("one.json") # doctest: +SKIP

        >>> import io
        >>> from descarteslabs.workflows import Job, Int
        >>> job = Job(Int(2), format="json") # doctest: +SKIP
        >>> bytestream = io.BytesIO() # doctest: +SKIP
        >>> job.result_to_file(bytestream) # doctest: +SKIP
        >>> print(bytestream.read()) # doctest: +SKIP
        b'2'
        """
        destination_name = which_has(self._message.destination)
        if destination_name not in ("download", "email"):
            raise NotImplementedError(
                "Not possible to automatically write results to a file for "
                "output destination {}. You'll need to load the data and write it "
                "out yourself.".format(destination_name)
            )

        if hasattr(file, "read"):
            close_file = False
        else:
            # assume it's a path
            file = open(os.path.expanduser(file), "wb")
            close_file = True

        try:
            self.wait(timeout=timeout, progress_bar=progress_bar)

            response = requests.get(self.url, stream=True)
            response.raise_for_status()
            # TODO error handling; likely the result has expired

            response.raw.decode_content = True
            shutil.copyfileobj(response.raw, file)
            # https://stackoverflow.com/a/13137873/10519953

        finally:
            if close_file:
                file.close()

    def wait(
        self,
        timeout=None,
        progress_bar=False,
        cancel_on_timeout=True,
        cancel_on_interrupt=True,
    ):
        """
        Block until the Job is complete, optionally displaying a progress bar.

        Raises any error that occurs with the `Job`, or `JobTimeoutError` if
        the timeout passes before the `Job` is complete.

        Parameters
        ----------
        timeout: int, optional
            The number of seconds to wait for the result.
        progress_bar: bool, optional
            Flag to draw the progress bar. Default is to ``True`` if in
            Jupyter Notebook.
        cancel_on_timeout: bool, optional
            Whether to cancel the job on client timeout. Default is True.
        cancel_on_interrupt: bool, optional
            Whether to cancel the job on interrupt (e.g. ctrl + c). Default is True.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> job = wf.Int(1).compute(block=False) # doctest: +SKIP
        >>> job.wait() # doctest: +SKIP
        >>> # ^ blocks until `job` is done
        >>> job.result() # doctest: +SKIP
        1
        """
        if progress_bar is None:
            progress_bar = in_notebook()

        stream = self.watch(timeout)

        show_progress = progress_bar is not False
        if show_progress:
            progress_bar_io = sys.stdout if progress_bar is True else progress_bar
            if not self.done:
                _write_to_io_or_widget(
                    progress_bar_io, "\nJob ID: {}\n".format(self.id)
                )

        try:
            while not self.done:
                try:
                    next(stream)
                except grpc.RpcError as e:
                    if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                        if cancel_on_timeout:
                            if show_progress:
                                _write_to_io_or_widget(
                                    progress_bar_io,
                                    "\nCancelling job {}\n".format(self.id),
                                )
                            self.cancel()
                        raise JobTimeoutError(
                            "Timed out waiting for Job {}".format(self.id)
                        )
                    elif default_grpc_retry_predicate(e):
                        stream = self.watch(timeout)
                    else:
                        raise
                except Exception as e:
                    if isinstance(e, StopIteration):
                        stream = self.watch(timeout)
                    else:
                        raise

                if show_progress:
                    self._draw_progress_bar(output=progress_bar_io)
            else:
                if self._message.state.stage == job_pb2.Job.Stage.SUCCEEDED:
                    return
                if self._message.state.stage == job_pb2.Job.Stage.FAILED:
                    raise self.error
                if self._message.state.stage == job_pb2.Job.Stage.CANCELLED:
                    raise JobCancelled("Job {} was cancelled.".format(self.id))
        except KeyboardInterrupt:
            if cancel_on_interrupt:
                if show_progress:
                    _write_to_io_or_widget(
                        progress_bar_io, "\nCancelling job {}\n".format(self.id)
                    )
                self.cancel()
            raise

    @property
    def object(self):
        "Proxytype: The proxy object this Job computes."
        if self.version != __version__:
            raise NotImplementedError(
                f"Accessing the `object` of a Job from a different version is not supported. "
                f"This Job {self.id!r} was created by client version {self.version!r}, "
                f"but you're currently running {__version__!r}."
            )

        if self._object is None:
            self._object = _proxy_object_from_message(self._message)
        return self._object

    @property
    def arguments(self):
        "The arguments of the Job, as a dict of names to Python primitives or Workflows objects."
        if self.version != __version__:
            raise NotImplementedError(
                f"Accessing the `arguments` of a Job from a different version is not supported. "
                f"This Job {self.id!r} was created by client version {self.version!r}, "
                f"but you're currently running {__version__!r}."
            )

        if self._arguments is None:
            if len(self._message.arguments) == 0:
                arg_grafts = {}
            else:
                arg_grafts = {}
                kwarg_types = self.object.kwarg_types
                for name, json_graft in self._message.arguments.items():
                    try:
                        graft = json.loads(json_graft)
                    except json.JSONDecodeError as e:
                        raise ValueError(
                            f"Invalid JSON in graft for argument {name!r}: {e}. Value: {json_graft!r}."
                        )

                    obj = (
                        kwarg_types.get(name, Any)._from_graft(
                            graft_client.isolate_keys(graft)
                        )
                        if not (
                            graft_syntax.is_literal(graft)
                            or graft_syntax.is_quoted_json(graft)
                        )
                        else graft
                    )
                    arg_grafts[name] = obj

            self._arguments = arg_grafts

        return self._arguments

    @property
    def geoctx(self):
        """The Workflows `~.geospatial.GeoContext` the Job was run within, or None"""
        graft_json = self._message.geoctx_graft
        if not graft_json:
            return None

        return GeoContext._from_graft(graft_client.isolate_keys(json.loads(graft_json)))

    @property
    def type(self):
        """type: The type of the proxy object."""
        return type(self.object)

    @property
    def result_type(self):
        "str: Name of the type of object that will be used to hold the result"
        return types_pb2.ResultType.Name(self._message.type)

    @property
    def id(self):
        """
        str or None: The globally unique identifier for the Job,
        or None if it hasn't been executed yet.
        """
        return self._message.id or None

    @property
    def channel(self):
        """str: The channel name where this Job will execute."""
        return self._message.channel

    @property
    def version(self):
        """str: The ``descarteslabs`` client version that constructed this Job."""
        return self._message.client_version

    @property
    def stage(self):
        """
        The current stage of the Job (queued, preparing, running, saving, succeeded,
        failed).
        """
        return job_pb2.Job.Stage.Name(self._message.state.stage)

    @property
    def done(self):
        """bool: Whether the Job has completed or not."""
        return _is_job_done(self._message.state.stage)

    @property
    def cancelled(self):
        """Whether the job has been cancelled."""
        return self._message.state.stage == job_pb2.Job.Stage.CANCELLED

    @property
    def cache_enabled(self):
        """Whether caching is enabled for this job."""
        return not self._message.no_cache

    @property
    def created_datetime(self):
        """datetime: The time the Job was created."""
        return pb_milliseconds_to_datetime(self._message.timestamp)

    @property
    def updated_datetime(self):
        """datetime: The time of the most recent Job update."""
        return pb_milliseconds_to_datetime(self._message.state.timestamp)

    @property
    def expires_datetime(self) -> datetime.datetime:
        """
        datetime.datetime: The UTC date this Job will be expired.
        """
        return pb_timestamp_to_datetime(self._message.expires_timestamp)

    @property
    def runtime(self):
        """datetime: The total time it took the Job to run."""
        if self.updated_datetime is None or self.created_datetime is None:
            return None
        else:
            return self.updated_datetime - self.created_datetime

    @property
    def format(self):
        """The serialization format of the Job, as a dictionary."""
        return has_proto_to_user_dict(self._message.format)

    @property
    def destination(self):
        """The destination for the Job results, as a dictionary."""
        return has_proto_to_user_dict(self._message.destination)

    @property
    def url(self):
        """
        The download URL for this Job's results.

        If `format` is not "download" or "email", `url` will be None.
        """
        destination = which_has(self._message.destination)
        if destination not in ("download", "email"):
            return None

        return getattr(self._message.destination, destination).result_url

    @property
    def error(self):
        """The error of the Job, or None if it finished successfully."""
        error_code = self._message.state.error.code
        exc = error_code_to_exception(error_code)
        return exc(self) if exc is not None else None

    def _draw_progress_bar(self, output=None):
        """
        Draw the progress bar of a running job.

        Parameters
        ----------
        output: ipywidgets Output, file-like object
            The output widget/stream to write a job's progress bar to.
        """
        _draw_progress_bar(
            finished=self._message.state.tasks_progress.finished.value,
            total=sum(
                (
                    self._message.state.tasks_progress.waiting.value,
                    self._message.state.tasks_progress.ready.value,
                    self._message.state.tasks_progress.running.value,
                    self._message.state.tasks_progress.finished.value,
                )
            ),
            stage=self.stage,
            output=output,
        )


def _draw_progress_bar(finished, total, stage, output, width=6):
    if total == 0:
        percent = 0
    else:
        percent = finished / total

    if _is_job_done(stage):
        bar = "#" * int(width)
    else:
        bar = "#" * int(width * percent)

    progress_output = (
        "\r[{bar:<{width}}] | Steps: {finished}/{total} | Stage: {stage}".format(
            bar=bar, width=width, finished=finished, total=total, stage=stage
        )
    )

    _write_to_io_or_widget(output, "{:<79}".format(progress_output))


def _proxy_object_from_message(message):
    typespec = message.typespec
    proxytype = deserialize_typespec(typespec)
    graft = json.loads(message.serialized_graft)
    isolated = graft_client.isolate_keys(graft)
    # TODO what about params? Job doesn't store them right now.
    # Anything that had params would have become a Function anyway.
    return proxytype._from_graft(isolated)


def _is_job_done(stage):
    return stage in (
        job_pb2.Job.Stage.SUCCEEDED,
        job_pb2.Job.Stage.FAILED,
        job_pb2.Job.Stage.CANCELLED,
    )


LOADERS, register = registry()


def get_loader(output_destination: destinations_pb2.Destination):
    specific_destination = getattr(output_destination, which_has(output_destination))
    try:
        return LOADERS[type(specific_destination)]
    except KeyError:
        raise NotImplementedError(
            "Not possible to load results for output destination {}".format(
                type(specific_destination).__name__
            )
        )


@register(destinations_pb2.Download)
# NOTE(gabe): Disabling email as a downloadable destination for now, because it's confusing
# when `.compute(destination="email")` returns the data in your notebook.
# Especially if that data is 30mb of binary GeoTIFF dumped into your terminal.
# @register(destinations_pb2.Email)
def download(job: Job):
    response = requests.get(job.url)
    response.raise_for_status()
    # TODO error handling; likely the result has expired
    data = response.content

    message = job._message
    specific_format = getattr(message.format, which_has(message.format))

    if isinstance(specific_format, formats_pb2.Pyarrow):
        codec = response.headers["x-goog-meta-X-Arrow-Codec"]
        result_type = job.result_type
        marshalled = deserialize_pyarrow(data, codec)
        return unmarshal.unmarshal(result_type, marshalled)

    return data


@register(destinations_pb2.Catalog)
def catalog_image(job: Job):
    destination = job.destination
    return catalog.Image.get(destination["product_id"] + ":" + destination["name"])
