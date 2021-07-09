# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from descarteslabs.common.proto.currier import currier_pb2 as descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2


class CurrierInvokeStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Invoke = channel.unary_unary(
                '/descarteslabs.currier.v1.CurrierInvoke/Invoke',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.InvokeRequest.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.InvokeResponse.FromString,
                )
        self.GetWorkStatus = channel.unary_unary(
                '/descarteslabs.currier.v1.CurrierInvoke/GetWorkStatus',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.GetWorkStatusRequest.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.GetWorkStatusResponse.FromString,
                )


class CurrierInvokeServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Invoke(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def GetWorkStatus(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_CurrierInvokeServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Invoke': grpc.unary_unary_rpc_method_handler(
                    servicer.Invoke,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.InvokeRequest.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.InvokeResponse.SerializeToString,
            ),
            'GetWorkStatus': grpc.unary_unary_rpc_method_handler(
                    servicer.GetWorkStatus,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.GetWorkStatusRequest.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.GetWorkStatusResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'descarteslabs.currier.v1.CurrierInvoke', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class CurrierInvoke(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Invoke(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/descarteslabs.currier.v1.CurrierInvoke/Invoke',
            descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.InvokeRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.InvokeResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def GetWorkStatus(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/descarteslabs.currier.v1.CurrierInvoke/GetWorkStatus',
            descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.GetWorkStatusRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.GetWorkStatusResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)


class CurrierRegisterStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Register = channel.unary_unary(
                '/descarteslabs.currier.v1.CurrierRegister/Register',
                request_serializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.RegisterModelRequest.SerializeToString,
                response_deserializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.RegisterModelResponse.FromString,
                )


class CurrierRegisterServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Register(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_CurrierRegisterServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Register': grpc.unary_unary_rpc_method_handler(
                    servicer.Register,
                    request_deserializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.RegisterModelRequest.FromString,
                    response_serializer=descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.RegisterModelResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'descarteslabs.currier.v1.CurrierRegister', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class CurrierRegister(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Register(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/descarteslabs.currier.v1.CurrierRegister/Register',
            descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.RegisterModelRequest.SerializeToString,
            descarteslabs_dot_common_dot_proto_dot_currier_dot_currier__pb2.RegisterModelResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
