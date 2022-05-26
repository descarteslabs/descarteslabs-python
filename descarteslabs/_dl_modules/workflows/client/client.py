import functools
import random
import time
import warnings

import grpc

from descarteslabs.config import get_settings
from ...common.retry import (
    Retry,
    truncated_delay_generator,
    _name_of_func,
    _wraps,
)
from ...common.proto.job import job_pb2_grpc
from ...common.proto.xyz import xyz_pb2_grpc
from ...common.proto.workflow import workflow_pb2_grpc
from ...common.proto.discover import discover_pb2_grpc

from ...client.grpc import (  # noqa: F401
    GrpcClient,
    default_grpc_retry_predicate,
)

from .. import _channel


# For lack of a better location for these:
ALL_AUTHENTICATED_USERS = "allAuthenticatedUsers"
ROLE_WORKFLOWS_VIEWER = "workflows/role/viewer"


def _do_retry(exception, retry_on_429s):
    try:
        code = exception.code()
    except AttributeError:
        return False
    else:
        return code in _CreateJobRetry._RETRYABLE_STATUS_CODES or (
            code == grpc.StatusCode.RESOURCE_EXHAUSTED and retry_on_429s
        )


class _CreateJobRetry(Retry):
    """
    A custom Retry class that provides retry logic specific to the CreateJob RPC.

    By default, this will not retry on a 429 response. However, if the user passes a `num_retries` parameter to the
    wrapped call, then 429s as well as the other retryable status codes will be retried `num_retries` times. If the
    server retruns a 429 response that includes a pushback duration, that duration will be incorporated into the delay.
    """

    _RETRYABLE_STATUS_CODES = {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    }

    def _retry(self, func, delay_generator, num_retries=None):
        deadline = self._deadline_datetime(self._deadline)
        retries = num_retries if num_retries is not None else self._retries
        previous_exceptions = []

        retry_on_429s = num_retries is not None and num_retries > 0

        for delay in delay_generator:
            try:
                return func()
            except Exception as e:
                self._handle_exception(e, previous_exceptions, retry_on_429s)

                if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED and retry_on_429s:
                    trailing_metadata = dict(e.trailing_metadata())
                    pushback_delay = trailing_metadata.get("grpc-retry-pushback-ms")
                    if pushback_delay is not None:
                        pushback_delay = int(pushback_delay)
                        delay = (pushback_delay / 1000) + random.uniform(*self._jitter)

            # will raise RetryError if deadline or retries exceeded
            retries = self._check_retries(
                retries, _name_of_func(func), deadline, previous_exceptions
            )

            time.sleep(delay)
        else:
            raise ValueError("Bad delay generator")

    def __call__(self, func):
        @_wraps(func)
        def wrapper(*args, **kwargs):
            num_retries = kwargs.pop("num_retries", None)
            if num_retries is not None and not isinstance(num_retries, int):
                raise TypeError("`num_retries` must be an integer")

            target = functools.partial(func, *args, **kwargs)
            delay_generator = truncated_delay_generator(
                initial=self._initial,
                maximum=self._maximum,
                jitter=self._jitter,
                multiplier=self._multiplier,
            )

            return self._retry(target, delay_generator, num_retries)

        return wrapper

    def _handle_exception(self, exception, previous_exceptions, retry_on_429s):
        if not _do_retry(exception, retry_on_429s):
            raise

        previous_exceptions.append(exception)


class Client(GrpcClient):
    """Low-level gRPC client for interacting with the Workflows backend. Not intended for users to use directly.

    Examples
    --------
    >>> from descarteslabs.workflows import Client, Int
    >>> my_client = Client(auth=non_default_auth) # doctest: +SKIP
    >>> Int(1).compute(client=my_client) # doctest: +SKIP
    1
    >>> Int(1).publish("One", client=my_client) # doctest: +SKIP
    <descarteslabs.workflows.models.workflow.Workflow object at 0x...>
    """

    def __init__(self, host=None, auth=None, certificate=None, port=None, channel=None):
        if host is None:
            host = get_settings().workflows_host or _channel.DEFAULT_GRPC_HOST

        if port is None:
            port = get_settings().workflows_port

        if channel is None:
            channel = _channel.__channel__

        super().__init__(
            host=host,
            auth=auth,
            certificate=certificate,
            port=port,
            default_metadata=(("x-wf-channel", channel),),
        )

        self._wf_channel = channel
        self._register_interceptor(_LogWarningsInterceptor())

    def _populate_api(self):
        self._add_stub("Workflow", workflow_pb2_grpc.WorkflowAPIStub)
        self._add_stub("Job", job_pb2_grpc.JobAPIStub)
        self._add_stub("XYZ", xyz_pb2_grpc.XYZAPIStub)

        self._add_api("Workflow", "GetWorkflow")
        self._add_api("Workflow", "UpsertWorkflow")
        self._add_api("Workflow", "GetVersion")
        self._add_api("Workflow", "SearchWorkflows")
        self._add_api("Workflow", "DeleteWorkflow")
        self._add_api("Workflow", "GetWmtsUrlTemplate")
        self._add_api("XYZ", "CreateXYZ")
        self._add_api("XYZ", "GetXYZ")
        self._add_api("XYZ", "ListXYZ")
        self._add_api("XYZ", "DeleteXYZ")
        self._add_api("XYZ", "GetXYZSessionLogs")

        self._add_api("Job", "CreateJob", default_retry=_CreateJobRetry())
        self._add_api("Job", "WatchJob")
        self._add_api("Job", "GetJob")
        self._add_api("Job", "ListJobs")
        self._add_api("Job", "CancelJob")

        # for the time being, the AccessGrantAPI will be implemented by the API service
        # but this might be refactored later
        self._add_stub("AccessGrant", discover_pb2_grpc.AccessGrantApiStub)
        self._add_api("AccessGrant", "CreateAccessGrant")
        # self._add_api("AccessGrant", "GetAccessGrant")
        self._add_api("AccessGrant", "DeleteAccessGrant")
        # self._add_api("AccessGrant", "ListAccessGrants")
        self._add_api("AccessGrant", "ListAccessGrantsStream")
        # self._add_api("AccessGrant", "ReplaceAccessGrant")


class _LogWarningsInterceptor(grpc.UnaryUnaryClientInterceptor):
    """
    A gRPC client-side interceptor that logs warnings contained within the
    x-wf-warnings response header.
    """

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return _log_warnings_postprocess(continuation(client_call_details, request))


def _log_warnings_postprocess(response):
    """Logs warnings contained within the x-wf-warnings response header."""
    # NOTE: .initial_metadata() will block.
    for key, value in response.initial_metadata():
        if key == "x-wf-warnings":
            warnings.warn(value)

    return response


global_grpc_client = None


def get_global_grpc_client():
    global global_grpc_client
    if global_grpc_client is None:
        global_grpc_client = Client()
    return global_grpc_client
