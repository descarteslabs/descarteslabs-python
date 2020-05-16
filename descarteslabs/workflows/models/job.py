from __future__ import division

import json
import logging
import sys
import time

import six

import pyarrow as pa
import requests
from descarteslabs.common.graft import client as graft_client
from descarteslabs.common.proto.job import job_pb2
from descarteslabs.common.proto.types import types_pb2
from descarteslabs.common.workflows.arrow_serialization import serialization_context

from .. import _channel
from ..cereal import deserialize_typespec, serialize_typespec, typespec_to_unmarshal_str
from ..client import get_global_grpc_client, default_grpc_retry_predicate
from .exceptions import error_code_to_exception, JobTimeoutError, JobCancelled
from .utils import in_notebook, pb_milliseconds_to_datetime
from .parameters import parameters_to_grafts

from descarteslabs.common.workflows import unmarshal

from descarteslabs.workflows import results  # noqa: F401 isort:skip

# ^ we must import to register its unmarshallers

logger = logging.getLogger(__name__)


def _write_to_io_or_widget(io, string):
    if io is not None:
        # try/except avoids having to import ipywidgets just for an isinstance check
        try:
            io.append_stdout(string)
        except AttributeError:
            io.write(string)
            io.flush()


class Job(object):
    """
    A `Job` represents the computation of a proxy object's graft
    within a specific environment of parameters.

    Example
    -------
    >>> from descarteslabs.workflows import Int, Job
    >>> num = Int(1) + 1
    >>> job = num.compute(block=False)  # doctest: +SKIP
    >>> job # doctest: +SKIP
    <descarteslabs.workflows.models.job.Job object at 0x...>
    >>> job.id # doctest: +SKIP
    '3754676080bbb2b857fbc04a3e48f6312732e1bc42e0bd7b'
    >>> job.result() # doctest: +SKIP
    2
    >>> same_job = Job.get('3754676080bbb2b857fbc04a3e48f6312732e1bc42e0bd7b') # doctest: +SKIP
    >>> same_job.stage # doctest: +SKIP
    'STAGE_DONE'
    >>> same_job.result # doctest: +SKIP
    2
    """

    BUCKET_PREFIX = "https://storage.googleapis.com/dl-compute-dev-results/{}"
    WAIT_INTERVAL = 0.1

    def __init__(self, proxy_object, parameters, client=None, cache=True):
        """
        Creates a new `Job` to compute the provided proxy object with the given
        parameters.

        Parameters
        ----------
        proxy_object: Proxytype
            Proxy object to compute
        parameters: dict[str, Proxytype]
            Python dictionary of parameter names and values
        client : `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters
        cache : bool, default True
            Whether to use the cache for this job.

        Returns
        -------
        Job
            The job that's executing.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int, parameter
        >>> my_int = Int(1) + parameter("other_int", Int)
        >>> job = Job(my_int, {"other_int": 10}) # doctest: +SKIP
        >>> job.stage # doctest: +SKIP
        QUEUED
        """
        if client is None:
            client = get_global_grpc_client()

        typespec = serialize_typespec(type(proxy_object))
        result_type = typespec_to_unmarshal_str(typespec)
        # ^ this also preemptively checks whether the result type is something we'll know how to unmarshal
        parameters = parameters_to_grafts(**parameters)

        message = client.api["CreateJob"](
            job_pb2.CreateJobRequest(
                parameters=json.dumps(parameters),
                serialized_graft=json.dumps(proxy_object.graft),
                typespec=typespec,
                type=types_pb2.ResultType.Value(result_type),
                no_cache=not cache,
                channel=_channel.__channel__,
            ),
            timeout=client.DEFAULT_TIMEOUT,
        )

        self._message = message
        self._client = client
        self._object = proxy_object

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
        return obj

    def refresh(self):
        """
        Refresh the attributes and state of the job.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int
        >>> job = Job(Int(1), {}) # doctest: +SKIP
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

    # Not implemented on the backend yet
    # def cancel(self):
    #     """
    #     Cancel a running job.

    #     Example
    #     -------
    #     >>> from descarteslabs.workflows import Job, Int, parameter
    #     >>> my_int = Int(1) + parameter("other_int", Int)
    #     >>> job = Job(my_int, {"other_int": 10}) # doctest: +SKIP
    #     >>> job.cancel() # doctest: +SKIP
    #     """
    #     message = self._client.api["CancelJob"](
    #         job_pb2.CancelJobRequest(id=self.id), timeout=self._client.DEFAULT_TIMEOUT
    #     )
    #     self._message.state.CopyFrom(message)

    def watch(self):
        """
        Generator that yields ``self`` each time an update to the Job occurs.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int
        >>> job = Job(Int(1), {}) # doctest: +SKIP
        >>> for job in job.watch(): # doctest: +SKIP
        ...     print(job.stage)
        QUEUED
        PREPARING
        RUNNING
        RUNNING
        SAVING
        SUCCEEDED
        """
        # Note(Winston): If we need to support long-running connections,
        # this is where we would infinitely loop on `grpc.StatusCode.DEADLINE_EXCEEDED` exceptions.
        # Currently, this will timeout as specified with `client.STREAM_TIMEOUT`, (as of writing, 24 hours).

        stream = self._client.api["WatchJob"](
            job_pb2.WatchJobRequest(id=self.id), timeout=self._client.STREAM_TIMEOUT
        )

        for state in stream:
            self._message.state.CopyFrom(state)
            yield self

    def result(self, timeout=None, progress_bar=None):
        """
        Get the result of the job. This blocks until the job is
        complete.

        Parameters
        ----------
        timeout: int, optional
            The number of seconds to wait for the result.
        progress_bar: bool, optional
            Flag to draw the progress bar. Default is to ``True`` if in
            Jupyter Notebook.

        Returns
        -------
        result
            Appropriate Python object representing the result,
            either as a plain Python type, or object from
            ``descarteslabs.common.workflows.results``.

        Example
        -------
        >>> from descarteslabs.workflows import Job, Int
        >>> job = Job(Int(1), {}) # doctest: +SKIP
        >>> job.result() # doctest: +SKIP
        1
        """

        if progress_bar is None:
            progress_bar = in_notebook()

        return self._wait_for_result(timeout=timeout, progress_bar=progress_bar)

    @property
    def object(self):
        "Proxytype: The proxy object this Job computes."
        if self._object is None:
            self._object = _proxy_object_from_message(self._message)
        return self._object

    @property
    def type(self):
        "type: The type of the proxy object."
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
        "str: The channel name where this Job will execute."
        return self._message.channel

    @property
    def stage(self):
        """
        The current stage of the Job (queued, preparing, running, saving, succeeded,
        failed).
        """
        return job_pb2.Job.Stage.Name(self._message.state.stage)

    @property
    def done(self):
        "bool: Whether the Job has completed or not."
        return _is_job_done(self._message.state.stage)

    # @property
    # def cancelled(self):
    #     """Whether the job has been cancelled."""
    #     return self._message.state.stage == job_pb2.Job.Stage.CANCELLED

    @property
    def cache_enabled(self):
        """Whether caching is enabled for this job."""
        return not self._message.no_cache

    @property
    def created_datetime(self):
        "datetime: The time the Job was created."
        return pb_milliseconds_to_datetime(self._message.timestamp)

    @property
    def updated_datetime(self):
        "datetime: The time of the most recent Job update."
        return pb_milliseconds_to_datetime(self._message.state.timestamp)

    @property
    def runtime(self):
        "datetime: The total time it took the Job to run."
        if self.updated_datetime is None or self.created_datetime is None:
            return None
        else:
            return self.updated_datetime - self.created_datetime

    @property
    def parameters(self):
        "The parameters of the Job, as a graft."
        # TODO(gabe): this isn't very useful without reconstructing them into Proxytypes
        return json.loads(self._message.parameters)

    @property
    def error(self):
        "The error of the Job, or None if it finished successfully."
        error_code = self._message.state.error.code
        exc = error_code_to_exception(error_code)
        return exc(self) if exc is not None else None

    def _load_result(self):
        if self._message.state.stage == job_pb2.Job.Stage.SUCCEEDED:
            return self._unmarshal(self._download_result())
        elif self._message.state.stage == job_pb2.Job.Stage.FAILED:
            raise self.error
        elif self._message.state.stage == job_pb2.Job.Stage.CANCELLED:
            raise JobCancelled("Job {} was cancelled.".format(self.id))
        else:
            raise AttributeError("job {} {}".format(self.id, self.stage))

    def _download_result(self):
        response = requests.get(self.BUCKET_PREFIX.format(self.id))
        response.raise_for_status()

        buffer = pa.decompress(
            response.content,
            codec=response.headers["x-goog-meta-codec"],
            decompressed_size=int(response.headers["x-goog-meta-decompressed_size"]),
        )
        return pa.deserialize(buffer, context=serialization_context)

    def _unmarshal(self, marshalled):
        return unmarshal.unmarshal(self.result_type, marshalled)

    def _wait_for_result(self, timeout=None, progress_bar=False):
        if timeout is None:
            exceeded_timeout = lambda: False  # noqa
        else:
            stop_at = time.time() + timeout
            exceeded_timeout = lambda: time.time() > stop_at  # noqa

        stream = self.watch()

        show_progress = progress_bar is not False
        if show_progress:
            progress_bar_io = sys.stdout if progress_bar is True else progress_bar
            _write_to_io_or_widget(progress_bar_io, "\nJob ID: {}\n".format(self.id))

        while not self.done and not exceeded_timeout():
            try:
                next(stream)

            except Exception as e:
                if isinstance(e, StopIteration) or default_grpc_retry_predicate(e):
                    stream = self.watch()
                else:
                    six.reraise(*sys.exc_info())

            if show_progress:
                self._draw_progress_bar(output=progress_bar_io)
        else:
            if self.done:
                return self._load_result()
            else:
                raise JobTimeoutError(
                    "timeout while waiting on result for Job('{}')".format(self.id)
                )

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

    progress_output = "\r[{bar:<{width}}] | Steps: {finished}/{total} | Stage: {stage}".format(
        bar=bar, width=width, finished=finished, total=total, stage=stage,
    )

    _write_to_io_or_widget(output, "{:<79}".format(progress_output))


def _proxy_object_from_message(message):
    typespec = message.typespec
    proxytype = deserialize_typespec(typespec)
    graft = json.loads(message.serialized_graft)
    isolated = graft_client.isolate_keys(graft)
    return proxytype._from_graft(isolated)


def _is_job_done(stage):
    return stage in (
        job_pb2.Job.Stage.SUCCEEDED,
        job_pb2.Job.Stage.FAILED,
        job_pb2.Job.Stage.CANCELLED,
    )
