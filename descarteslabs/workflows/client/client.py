import os

import certifi
import grpc
from descarteslabs.client.auth import Auth
from descarteslabs.common.proto import (
    health_pb2,
    health_pb2_grpc,
    job_pb2_grpc,
    xyz_pb2_grpc,
    workflow_pb2_grpc,
)
from descarteslabs.common.retry import Retry
from descarteslabs.common.retry.retry import _wraps

_RETRYABLE_STATUS_CODES = (
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.INTERNAL,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.UNKNOWN,
    grpc.StatusCode.DEADLINE_EXCEEDED,
)


def wrap_stub(func, default_retry):
    @_wraps(func)
    def wrapper(*args, **kwargs):
        retry = kwargs.pop("retry", None)

        if retry is None:
            retry = default_retry

        return retry(func)(*args, **kwargs)

    return wrapper


def default_grpc_retry_predicate(e):
    try:
        code = e.code()
    except AttributeError:
        return False
    else:
        return code in _RETRYABLE_STATUS_CODES


class Client:
    DEFAULT_TIMEOUT = 5
    STREAM_TIMEOUT = 60 * 60 * 24

    def __init__(self, host=None, auth=None, certificate=None, port=443):
        if auth is None:
            auth = Auth()

        if host is None:
            host = os.environ.get(
                "DESCARTESLABS_WORKFLOWS_HOST", "workflows.descarteslabs.com"
            )

        self.auth = auth
        self.host = host
        self.port = port

        self._channel = None
        self._certificate = certificate
        self._stubs = None
        self._api = None
        self._default_retry = Retry(predicate=default_grpc_retry_predicate, retries=5)

    @property
    def token(self):
        return self.auth.token

    @property
    def channel(self):
        if self._channel is None:
            self._channel = self._open_channel()
        return self._channel

    @property
    def certificate(self):
        if self._certificate is None:
            cert_file = os.getenv("SSL_CERT_FILE", certifi.where())
            with open(cert_file, "rb") as f:
                self._certificate = f.read()

        return self._certificate

    @property
    def api(self):
        if self._api is None:
            self._initialize()
        return self._api

    def _initialize(self):
        self._stubs = {
            "Health": health_pb2_grpc.HealthStub(self.channel),
            "Workflow": workflow_pb2_grpc.WorkflowAPIStub(self.channel),
            "Job": job_pb2_grpc.JobAPIStub(self.channel),
            "XYZ": xyz_pb2_grpc.XYZAPIStub(self.channel),
        }

        # TODO wrap these functions in retry
        self._api = {
            "Check": self._stubs["Health"].Check,
            "CreateWorkflow": wrap_stub(
                self._stubs["Workflow"].CreateWorkflow,
                default_retry=self._default_retry,
            ),
            "GetWorkflow": wrap_stub(
                self._stubs["Workflow"].GetWorkflow, default_retry=self._default_retry
            ),
            "ListWorkflows": wrap_stub(
                self._stubs["Workflow"].ListWorkflows, default_retry=self._default_retry
            ),
            "UpdateWorkflow": wrap_stub(
                self._stubs["Workflow"].UpdateWorkflow,
                default_retry=self._default_retry,
            ),
            "CreateXYZ": wrap_stub(
                self._stubs["XYZ"].CreateXYZ, default_retry=self._default_retry
            ),
            "GetXYZ": wrap_stub(
                self._stubs["XYZ"].GetXYZ, default_retry=self._default_retry
            ),
            "GetXYZSessionErrors": wrap_stub(
                self._stubs["XYZ"].GetXYZSessionErrors,
                default_retry=self._default_retry,
            ),
            "CreateJob": wrap_stub(
                self._stubs["Job"].CreateJob, default_retry=self._default_retry
            ),
            "WatchJob": wrap_stub(
                self._stubs["Job"].WatchJob, default_retry=self._default_retry
            ),
            "GetJob": wrap_stub(
                self._stubs["Job"].GetJob, default_retry=self._default_retry
            ),
            "ListJobs": wrap_stub(
                self._stubs["Job"].ListJobs, default_retry=self._default_retry
            ),
            "CancelJob": wrap_stub(
                self._stubs["Job"].CancelJob, default_retry=self._default_retry
            ),
        }

    def _get_credentials(self):
        token_call_credentials = grpc.access_token_call_credentials(self.auth.token)
        ssl_channel_credentials = grpc.ssl_channel_credentials(self.certificate)

        composite_credentials = grpc.composite_channel_credentials(
            ssl_channel_credentials, token_call_credentials
        )

        return composite_credentials

    def _open_channel(self):
        return grpc.secure_channel(
            "{}:{}".format(self.host, self.port), self._get_credentials()
        )

    def health(self, timeout=None):
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT

        return self.api["Check"](
            health_pb2.HealthCheckRequest(), timeout=self.DEFAULT_TIMEOUT
        )

    def close(self):
        # NOTE: this may be a blocking operation
        if self._channel:
            self._channel.close()
            self._channel = None

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def __del__(self):
        self.close()
