import os
from collections import OrderedDict

import certifi
import grpc
from descarteslabs.client.auth import Auth
from descarteslabs.common.proto.health import health_pb2, health_pb2_grpc
from descarteslabs.common.proto.job import job_pb2_grpc
from descarteslabs.common.proto.xyz import xyz_pb2_grpc
from descarteslabs.common.proto.workflow import workflow_pb2_grpc
from descarteslabs.common.retry import Retry, RetryError
from descarteslabs.common.retry.retry import _wraps

from descarteslabs.workflows import _channel

from .exceptions import from_grpc_error

_RETRYABLE_STATUS_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.INTERNAL,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.UNKNOWN,
    grpc.StatusCode.DEADLINE_EXCEEDED,
}


def wrap_stub(func, default_retry):
    @_wraps(func)
    def wrapper(*args, **kwargs):
        retry = kwargs.pop("retry", default_retry)

        # If retry is none, use identity function.
        if retry is None:
            retry = lambda f: f  # noqa: E73

        # set channel as header
        default_metadata = (("x-wf-channel", _channel.__channel__),)

        # Merge and set default request headers
        # example: https://github.com/grpc/grpc/blob/master/examples/python/metadata/metadata_client.py
        # NOTE(Clark): We use an OrderedDict to ensure a stable ordering for Python 3.5
        # and 3.6.
        # TODO(Clark): Revert back to dict once Python 3.5 is dropped.
        merged_metadata = OrderedDict(
            default_metadata + kwargs.get("metadata", tuple())
        )

        kwargs["metadata"] = tuple(merged_metadata.items())

        try:
            return retry(func)(*args, **kwargs)
        except grpc.RpcError as e:
            raise from_grpc_error(e) from None
        except RetryError as e:
            e._exceptions = [
                from_grpc_error(exc) if isinstance(exc, grpc.RpcError) else exc
                for exc in e._exceptions
            ]
            raise e from e._exceptions[-1]

    return wrapper


def default_grpc_retry_predicate(e):
    try:
        code = e.code()
    except AttributeError:
        return False
    else:
        return code in _RETRYABLE_STATUS_CODES


class Client:
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

    DEFAULT_TIMEOUT = 5
    STREAM_TIMEOUT = 60 * 60 * 24

    def __init__(self, host=None, auth=None, certificate=None, port=443):
        if auth is None:
            auth = Auth()

        if host is None:
            host = os.environ.get(
                "DESCARTESLABS_WORKFLOWS_HOST", "workflows-api.descarteslabs.com"
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
        "The Client token."
        return self.auth.token

    @property
    def channel(self):
        "The GRPC channel of the Client."
        if self._channel is None:
            self._channel = self._open_channel()
        return self._channel

    @property
    def certificate(self):
        "The Client SSL certificate."
        if self._certificate is None:
            cert_file = os.getenv("SSL_CERT_FILE", certifi.where())
            with open(cert_file, "rb") as f:
                self._certificate = f.read()

        return self._certificate

    @property
    def api(self):
        "The available Client operations, as a dict."
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

        self._api = {
            "Check": wrap_stub(self._stubs["Health"].Check, default_retry=None),
            "CreateWorkflow": wrap_stub(
                self._stubs["Workflow"].CreateWorkflow,
                default_retry=self._default_retry,
            ),
            "GetWorkflow": wrap_stub(
                self._stubs["Workflow"].GetWorkflow, default_retry=self._default_retry,
            ),
            "ListWorkflows": wrap_stub(
                self._stubs["Workflow"].ListWorkflows,
                default_retry=self._default_retry,
            ),
            "UpdateWorkflow": wrap_stub(
                self._stubs["Workflow"].UpdateWorkflow,
                default_retry=self._default_retry,
            ),
            "CreateXYZ": wrap_stub(
                self._stubs["XYZ"].CreateXYZ, default_retry=self._default_retry,
            ),
            "GetXYZ": wrap_stub(
                self._stubs["XYZ"].GetXYZ, default_retry=self._default_retry,
            ),
            "GetXYZSessionErrors": wrap_stub(
                self._stubs["XYZ"].GetXYZSessionErrors,
                default_retry=self._default_retry,
            ),
            "CreateJob": wrap_stub(
                self._stubs["Job"].CreateJob, default_retry=self._default_retry,
            ),
            "WatchJob": wrap_stub(
                self._stubs["Job"].WatchJob, default_retry=self._default_retry,
            ),
            "GetJob": wrap_stub(
                self._stubs["Job"].GetJob, default_retry=self._default_retry,
            ),
            "ListJobs": wrap_stub(
                self._stubs["Job"].ListJobs, default_retry=self._default_retry,
            ),
            "CancelJob": wrap_stub(
                self._stubs["Job"].CancelJob, default_retry=self._default_retry,
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
        """Check the health of the GRPC server (SERVING, NOT_SERVING, UNKNOWN).

        Example
        -------
        >>> from descarteslabs.workflows import Client
        >>> Client().health() # doctest: +SKIP
        SERVING
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT

        return self.api["Check"](
            health_pb2.HealthCheckRequest(), timeout=self.DEFAULT_TIMEOUT,
        )

    def close(self):
        "Close the GRPC channel associated with the Client."
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
