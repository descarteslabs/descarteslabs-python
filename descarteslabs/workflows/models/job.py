from __future__ import division

import json
import logging
import sys
import time

import six

import grpc
import pyarrow as pa
import requests
from descarteslabs.common.graft import client as graft_client
from descarteslabs.common.proto import job_pb2, types_pb2
from descarteslabs.common.workflows.arrow_serialization import serialization_context

from .. import _channel
from ..cereal import deserialize_typespec, serialize_typespec
from ..client import Client
from .exceptions import ERRORS, TimeoutError
from .utils import in_notebook, pb_milliseconds_to_datetime

from descarteslabs.common.workflows import unmarshal

from descarteslabs.workflows import (  # noqa: F401 isort:skip
    containers,
)  # we must import to register its unmarshallers


logger = logging.getLogger(__name__)


def _typespec_to_unmarshal_str(typespec):
    if isinstance(typespec, six.string_types):
        marshal_type = typespec
    else:
        marshal_type = typespec["type"]
    if marshal_type not in unmarshal.registry:
        raise TypeError(
            "{!r} is not a computable type. Note that if this is a function-like type, "
            "you should call it and compute the result, "
            "not the function itself.".format(marshal_type)
        )
    return marshal_type


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
    >>> import descarteslabs.workflows as wf
    >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
    >>> job = img.compute(geoctx, block=False)  # doctest: +SKIP
    >>> result = job.result()  # doctest: +SKIP
    """

    BUCKET_PREFIX = "https://storage.googleapis.com/dl-compute-dev-results/{}"
    WATCH_TIMEOUT = 30
    WAIT_INTERVAL = 0.1

    def __init__(self, message, client=None):
        self._message = message
        if client is None:
            client = Client()
        self._client = client
        self._object = None

    @classmethod
    def build(cls, proxy_object, parameters, channel=None, client=None):
        """
        Build a new `Job` for computing a proxy object under certain parameters.
        Does not actually trigger computation; call `Job.execute` on the result to do so.

        Parameters
        ----------
        proxy_object: Proxytype
            Proxy object to compute
        parameters: dict[str, Proxytype]
            Python dictionary of parameter names and values
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

        Returns
        -------
        Job
            The job waiting to be executed.
        """
        if channel is None:
            # NOTE(gabe): we look up the variable from the `_channel` package here,
            # rather than importing it directly at the top,
            # so it can easily be changed during an interactive session.
            channel = _channel.__channel__
        if client is None:
            client = Client()

        typespec = serialize_typespec(type(proxy_object))
        result_type = _typespec_to_unmarshal_str(typespec)
        # ^ this also preemptively checks whether the result type is something we'll know how to unmarshal
        parameters = {
            key: graft_client.value_graft(value)
            for key, value in six.iteritems(parameters)
        }

        message = job_pb2.Job(
            parameters=json.dumps(parameters),
            serialized_graft=json.dumps(proxy_object.graft),
            serialized_typespec=json.dumps(typespec),
            type=types_pb2.ResultType.Value(result_type),
            channel=channel,
        )

        instance = cls(message, client)
        instance._object = proxy_object
        return instance

    @classmethod
    def get(cls, id, client=None):
        "Get a currently-running `Job` by its ID."
        if client is None:
            client = Client()

        message = client.api["GetJob"](
            job_pb2.GetJobRequest(id=id), timeout=client.DEFAULT_TIMEOUT
        )
        return cls(message, client)

    def execute(self):
        """
        Asynchronously submit a job for execution.

        After submission, ``self.id`` will be the ID of the running job.

        This method is idempotent: calling it multiple times on the same `Job` object
        will only trigger execution once.
        """
        if self.id is not None:
            return

        message = self._client.api["CreateJob"](
            job_pb2.CreateJobRequest(
                parameters=self._message.parameters,
                serialized_graft=self._message.serialized_graft,
                serialized_typespec=self._message.serialized_typespec,
                type=self._message.type,
                channel=self._message.channel,
            ),
            timeout=self._client.DEFAULT_TIMEOUT,
        )
        self._message = message

    def refresh(self):
        """
        Refresh the attributes and status of the job.
        """
        message = self._client.api["GetJob"](
            job_pb2.GetJobRequest(id=self.id), timeout=self._client.DEFAULT_TIMEOUT
        )
        self._message = message

    def cancel(self):
        """
        Cancel a running job.
        """
        message = self._client.api["CancelJob"](
            job_pb2.CancelJobRequest(id=self.id), timeout=self._client.DEFAULT_TIMEOUT
        )
        self._message = message

    def watch(self):
        while True:
            try:
                for message in self._client.api["WatchJob"](
                    job_pb2.WatchJobRequest(id=self.id), timeout=self.WATCH_TIMEOUT
                ):
                    self._message = message
                    yield self
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    # use the same channel to open another rpc stream
                    continue
                else:
                    six.reraise(*sys.exc_info())
            finally:
                if self.done:
                    return

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
            ``descarteslabs.common.workflows.containers``.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> job = wf.Int(1).compute()  # doctest: +SKIP
        >>> job.result(timeout=10)  # doctest: +SKIP
        1
        """

        if progress_bar is None:
            progress_bar = in_notebook()

        return self._wait_for_result(timeout=timeout, progress_bar=progress_bar)

    @property
    def object(self):
        "Proxytype: The proxy object this Job computes."
        if self._object is None:
            typespec = json.loads(self._message.serialized_typespec)
            proxytype = deserialize_typespec(typespec)
            graft = json.loads(self._message.serialized_graft)
            isolated = graft_client.isolate_keys(graft)
            self._object = proxytype._from_graft(isolated)
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
    def status(self):
        return job_pb2.JobStatus.Name(self._message.status)

    @property
    def stage(self):
        return job_pb2.JobStage.Name(self._message.stage)

    @property
    def done(self):
        return self._message.status in [job_pb2.STATUS_FAILURE, job_pb2.STATUS_SUCCESS]

    @property
    def created_datetime(self):
        return pb_milliseconds_to_datetime(self._message.created_timestamp)

    @property
    def updated_datetime(self):
        return pb_milliseconds_to_datetime(self._message.updated_timestamp)

    @property
    def runtime(self):
        if self.updated_datetime is None or self.created_datetime is None:
            return None
        else:
            return self.updated_datetime - self.created_datetime

    @property
    def parameters(self):
        # TODO(gabe): this isn't very useful without reconstructing them into Proxytypes
        return json.loads(self._message.parameters)

    @property
    def error(self):
        error_code = self._message.error.code
        # If no errors on the message, then the error code will be 0.
        return ERRORS[error_code](self) if error_code != 0 else None

    def _load_result(self):
        if self._message.status == job_pb2.STATUS_SUCCESS:
            return self._unmarshal(self._download_result())
        elif self._message.status == job_pb2.STATUS_FAILURE:
            raise self.error
        else:
            raise AttributeError("job {} {}".format(self.id, self.status))

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

        # we refresh after starting the watch to avoid race condition
        self.refresh()

        show_progress = progress_bar is not False
        if show_progress:
            progress_bar_io = sys.stdout if progress_bar is True else progress_bar
            _write_to_io_or_widget(progress_bar_io, "\nJob ID: {}\n".format(self.id))

        while not self.done and not exceeded_timeout():
            try:
                next(stream)

            except StopIteration:
                # TODO(justin) stopiteration will likely be caused by connectivity issues
                stream = self.watch()

            if show_progress:
                self._draw_progress_bar(output=progress_bar_io)
        else:
            if self.done:
                return self._load_result()
            else:
                raise TimeoutError(
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
            finished=self._message.progress.finished,
            total=sum(
                (
                    self._message.progress.waiting,
                    self._message.progress.ready,
                    self._message.progress.running,
                    self._message.progress.finished,
                )
            ),
            stage=self.stage,
            status=self.status,
            output=output,
        )


def _draw_progress_bar(finished, total, stage, status, output, width=6):
    if total == 0:
        percent = 0
    else:
        percent = finished / total

    if job_pb2.JobStage.Value(stage) == job_pb2.STAGE_DONE:
        bar = "#" * int(width)
    else:
        bar = "#" * int(width * percent)

    progress_output = "\r[{bar:<{width}}] | Steps: {finished}/{total} | Stage: {stage} | Status: {status}".format(
        bar=bar,
        width=width,
        finished=finished,
        total=total,
        stage=stage.replace("STAGE_", ""),
        status=status.replace("STATUS_", ""),
    )

    _write_to_io_or_widget(output, "{:<79}".format(progress_output))
