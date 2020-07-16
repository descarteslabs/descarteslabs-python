import os

from descarteslabs.common.proto.job import job_pb2_grpc
from descarteslabs.common.proto.xyz import xyz_pb2_grpc
from descarteslabs.common.proto.workflow import workflow_pb2_grpc

from descarteslabs.client.grpc import (  # noqa: F401
    GrpcClient,
    default_grpc_retry_predicate,
)

from descarteslabs.workflows import _channel


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

    def __init__(self, host=None, auth=None, certificate=None, port=443):
        if host is None:
            host = os.environ.get(
                "DESCARTESLABS_WORKFLOWS_HOST", "workflows-api.descarteslabs.com"
            )

        super().__init__(
            host=host,
            auth=auth,
            certificate=certificate,
            port=port,
            default_metadata=(("x-wf-channel", _channel.__channel__),),
        )

    def _populate_api(self):
        self._add_stub("Workflow", workflow_pb2_grpc.WorkflowAPIStub)
        self._add_stub("Job", job_pb2_grpc.JobAPIStub)
        self._add_stub("XYZ", xyz_pb2_grpc.XYZAPIStub)

        self._add_api("Workflow", "GetWorkflow", self._default_retry)
        self._add_api("Workflow", "UpsertWorkflow", self._default_retry)
        self._add_api("Workflow", "GetVersion", self._default_retry)
        self._add_api("Workflow", "SearchWorkflows", self._default_retry)
        self._add_api("Workflow", "DeleteWorkflow", self._default_retry)
        self._add_api("XYZ", "CreateXYZ", self._default_retry)
        self._add_api("XYZ", "GetXYZ", self._default_retry)
        self._add_api("XYZ", "GetXYZSessionErrors", self._default_retry)
        self._add_api("Job", "CreateJob", self._default_retry)
        self._add_api("Job", "WatchJob", self._default_retry)
        self._add_api("Job", "GetJob", self._default_retry)
        self._add_api("Job", "ListJobs", self._default_retry)
        self._add_api("Job", "CancelJob", self._default_retry)


global_grpc_client = None


def get_global_grpc_client():
    global global_grpc_client
    if global_grpc_client is None:
        global_grpc_client = Client()
    return global_grpc_client
