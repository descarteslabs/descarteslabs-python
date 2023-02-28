# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from ..job import job_pb2 as descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2


class JobAPIStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.CreateJob = channel.unary_unary(
            "/descarteslabs.workflows.JobAPI/CreateJob",
            request_serializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.CreateJobRequest.SerializeToString,
            response_deserializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.FromString,
        )
        self.ListJobs = channel.unary_stream(
            "/descarteslabs.workflows.JobAPI/ListJobs",
            request_serializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.ListJobsRequest.SerializeToString,
            response_deserializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.FromString,
        )
        self.GetJob = channel.unary_unary(
            "/descarteslabs.workflows.JobAPI/GetJob",
            request_serializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.GetJobRequest.SerializeToString,
            response_deserializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.FromString,
        )
        self.CancelJob = channel.unary_unary(
            "/descarteslabs.workflows.JobAPI/CancelJob",
            request_serializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.CancelJobRequest.SerializeToString,
            response_deserializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.CancelJobResponse.FromString,
        )
        self.WatchJob = channel.unary_stream(
            "/descarteslabs.workflows.JobAPI/WatchJob",
            request_serializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.WatchJobRequest.SerializeToString,
            response_deserializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.State.FromString,
        )


class JobAPIServicer(object):
    """Missing associated documentation comment in .proto file."""

    def CreateJob(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def ListJobs(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def GetJob(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def CancelJob(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def WatchJob(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_JobAPIServicer_to_server(servicer, server):
    rpc_method_handlers = {
        "CreateJob": grpc.unary_unary_rpc_method_handler(
            servicer.CreateJob,
            request_deserializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.CreateJobRequest.FromString,
            response_serializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.SerializeToString,
        ),
        "ListJobs": grpc.unary_stream_rpc_method_handler(
            servicer.ListJobs,
            request_deserializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.ListJobsRequest.FromString,
            response_serializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.SerializeToString,
        ),
        "GetJob": grpc.unary_unary_rpc_method_handler(
            servicer.GetJob,
            request_deserializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.GetJobRequest.FromString,
            response_serializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.SerializeToString,
        ),
        "CancelJob": grpc.unary_unary_rpc_method_handler(
            servicer.CancelJob,
            request_deserializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.CancelJobRequest.FromString,
            response_serializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.CancelJobResponse.SerializeToString,
        ),
        "WatchJob": grpc.unary_stream_rpc_method_handler(
            servicer.WatchJob,
            request_deserializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.WatchJobRequest.FromString,
            response_serializer=descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.State.SerializeToString,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "descarteslabs.workflows.JobAPI", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


# This class is part of an EXPERIMENTAL API.
class JobAPI(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def CreateJob(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/descarteslabs.workflows.JobAPI/CreateJob",
            descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.CreateJobRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def ListJobs(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            "/descarteslabs.workflows.JobAPI/ListJobs",
            descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.ListJobsRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def GetJob(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/descarteslabs.workflows.JobAPI/GetJob",
            descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.GetJobRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def CancelJob(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_unary(
            request,
            target,
            "/descarteslabs.workflows.JobAPI/CancelJob",
            descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.CancelJobRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.CancelJobResponse.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

    @staticmethod
    def WatchJob(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            "/descarteslabs.workflows.JobAPI/WatchJob",
            descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.WatchJobRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_job_dot_job__pb2.Job.State.FromString,
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )
