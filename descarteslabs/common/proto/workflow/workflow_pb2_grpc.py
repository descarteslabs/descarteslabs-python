# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from descarteslabs.common.proto.workflow import workflow_pb2 as descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2


class WorkflowAPIStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.UpsertWorkflow = channel.unary_unary(
                '/descarteslabs.workflows.WorkflowAPI/UpsertWorkflow',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.UpsertWorkflowRequest.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Workflow.FromString,
                )
        self.GetWorkflow = channel.unary_unary(
                '/descarteslabs.workflows.WorkflowAPI/GetWorkflow',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.GetWorkflowRequest.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Workflow.FromString,
                )
        self.GetVersion = channel.unary_unary(
                '/descarteslabs.workflows.WorkflowAPI/GetVersion',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.GetVersionRequest.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.VersionedGraft.FromString,
                )
        self.SearchWorkflows = channel.unary_stream(
                '/descarteslabs.workflows.WorkflowAPI/SearchWorkflows',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.SearchWorkflowsRequest.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Workflow.FromString,
                )
        self.DeleteWorkflow = channel.unary_unary(
                '/descarteslabs.workflows.WorkflowAPI/DeleteWorkflow',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.DeleteWorkflowRequest.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Empty.FromString,
                )
        self.GetWmtsUrlTemplate = channel.unary_unary(
                '/descarteslabs.workflows.WorkflowAPI/GetWmtsUrlTemplate',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Empty.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.WmtsUrlTemplateResponse.FromString,
                )


class WorkflowAPIServicer(object):
    """Missing associated documentation comment in .proto file."""

    def UpsertWorkflow(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetWorkflow(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetVersion(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SearchWorkflows(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def DeleteWorkflow(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetWmtsUrlTemplate(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_WorkflowAPIServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'UpsertWorkflow': grpc.unary_unary_rpc_method_handler(
                    servicer.UpsertWorkflow,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.UpsertWorkflowRequest.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Workflow.SerializeToString,
            ),
            'GetWorkflow': grpc.unary_unary_rpc_method_handler(
                    servicer.GetWorkflow,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.GetWorkflowRequest.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Workflow.SerializeToString,
            ),
            'GetVersion': grpc.unary_unary_rpc_method_handler(
                    servicer.GetVersion,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.GetVersionRequest.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.VersionedGraft.SerializeToString,
            ),
            'SearchWorkflows': grpc.unary_stream_rpc_method_handler(
                    servicer.SearchWorkflows,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.SearchWorkflowsRequest.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Workflow.SerializeToString,
            ),
            'DeleteWorkflow': grpc.unary_unary_rpc_method_handler(
                    servicer.DeleteWorkflow,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.DeleteWorkflowRequest.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Empty.SerializeToString,
            ),
            'GetWmtsUrlTemplate': grpc.unary_unary_rpc_method_handler(
                    servicer.GetWmtsUrlTemplate,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Empty.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.WmtsUrlTemplateResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'descarteslabs.workflows.WorkflowAPI', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class WorkflowAPI(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def UpsertWorkflow(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/descarteslabs.workflows.WorkflowAPI/UpsertWorkflow',
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.UpsertWorkflowRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Workflow.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetWorkflow(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/descarteslabs.workflows.WorkflowAPI/GetWorkflow',
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.GetWorkflowRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Workflow.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetVersion(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/descarteslabs.workflows.WorkflowAPI/GetVersion',
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.GetVersionRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.VersionedGraft.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def SearchWorkflows(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/descarteslabs.workflows.WorkflowAPI/SearchWorkflows',
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.SearchWorkflowsRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Workflow.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def DeleteWorkflow(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/descarteslabs.workflows.WorkflowAPI/DeleteWorkflow',
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.DeleteWorkflowRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Empty.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetWmtsUrlTemplate(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/descarteslabs.workflows.WorkflowAPI/GetWmtsUrlTemplate',
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.Empty.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_workflow_dot_workflow__pb2.WmtsUrlTemplateResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
