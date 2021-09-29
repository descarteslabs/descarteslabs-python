import os
import random
import time
import warnings

import grpc

from descarteslabs.common.retry import Retry, _name_of_func
from descarteslabs.common.proto.job import job_pb2_grpc
from descarteslabs.common.proto.xyz import xyz_pb2_grpc
from descarteslabs.common.proto.workflow import workflow_pb2_grpc
from descarteslabs.common.proto.discover import discover_pb2_grpc

from descarteslabs.client.grpc import (  # noqa: F401
    GrpcClient,
    default_grpc_retry_predicate,
)

from descarteslabs.workflows import _channel


# For lack of a better location for these:
ALL_AUTHENTICATED_USERS = "allAuthenticatedUsers"
ROLE_WORKFLOWS_VIEWER = "workflows/role/viewer"
TYPE_USER_EMAIL = "user-email"


class PushbackHonoringRetry(Retry):
    """
    In most cases, we want to use a single delay generator (eg. truncated exponential backoff w/ jitter). But sometimes
    we want to combine our standard retry strategy with special handling for certain exceptions (eg. a 429 with a
    specified pushback / retry-after). A `PushbackHonoringRetry` accommodates exactly this situation.
    """
    def _retry(self, func, delay_generator):
        deadline = self._deadline_datetime(self._deadline)
        retries = self._retries
        previous_exceptions = []

        for delay in delay_generator:
            try:
                return func()
            except Exception as e:
                self._handle_exception(e, previous_exceptions)

                if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
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
            host = os.environ.get(
                "DESCARTESLABS_WORKFLOWS_HOST", _channel.DEFAULT_GRPC_HOST
            )

        if port is None:
            port = os.environ.get("DESCARTESLABS_WORKFLOWS_PORT", 443)

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

        createJobRetryConfig = PushbackHonoringRetry(predicate=default_grpc_retry_predicate, retries=5)
        self._add_api("Job", "CreateJob", default_retry=createJobRetryConfig)
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
