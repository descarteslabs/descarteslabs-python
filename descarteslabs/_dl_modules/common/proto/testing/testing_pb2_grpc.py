# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from descarteslabs._dl_modules.common.proto.testing import testing_pb2 as descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2


class TestServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.StreamStream = channel.stream_stream(
                '/testing.v1.TestService/StreamStream',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.FromString,
                )
        self.StreamUnary = channel.stream_unary(
                '/testing.v1.TestService/StreamUnary',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.FromString,
                )
        self.UnaryStream = channel.unary_stream(
                '/testing.v1.TestService/UnaryStream',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.FromString,
                )
        self.UnaryUnary = channel.unary_unary(
                '/testing.v1.TestService/UnaryUnary',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.FromString,
                )


class TestServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def StreamStream(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def StreamUnary(self, request_iterator, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UnaryStream(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def UnaryUnary(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_TestServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'StreamStream': grpc.stream_stream_rpc_method_handler(
                    servicer.StreamStream,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.SerializeToString,
            ),
            'StreamUnary': grpc.stream_unary_rpc_method_handler(
                    servicer.StreamUnary,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.SerializeToString,
            ),
            'UnaryStream': grpc.unary_stream_rpc_method_handler(
                    servicer.UnaryStream,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.SerializeToString,
            ),
            'UnaryUnary': grpc.unary_unary_rpc_method_handler(
                    servicer.UnaryUnary,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'testing.v1.TestService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class TestService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def StreamStream(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_stream(request_iterator, target, '/testing.v1.TestService/StreamStream',
            descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def StreamUnary(request_iterator,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.stream_unary(request_iterator, target, '/testing.v1.TestService/StreamUnary',
            descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def UnaryStream(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_stream(request, target, '/testing.v1.TestService/UnaryStream',
            descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def UnaryUnary(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/testing.v1.TestService/UnaryUnary',
            descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Request.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_testing_dot_testing__pb2.Response.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)