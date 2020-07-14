# Copied, with modifications, from
# https://github.com/googleapis/google-cloud-python/blob/f20a5b8/api_core/google/api_core/exceptions.py.
# Original copyright 2014 Google LLC


# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Exceptions raised by clients."""

import http.client as http_client
import grpc


# Lookup tables for mapping exceptions from HTTP and gRPC transports.
# Populated by _APICallErrorMeta
_HTTP_CODE_TO_EXCEPTION = {}
_GRPC_CODE_TO_EXCEPTION = {}


class APIError(Exception):
    """Base class for all exceptions raised by clients."""

    pass


# TODO(gabe): remove and use __init_subclass__ once py3.5 support is dropped
class _APICallErrorMeta(type):
    """Metaclass for registering APICallError subclasses."""

    def __new__(mcs, name, bases, class_dict):
        cls = type.__new__(mcs, name, bases, class_dict)
        if cls.code is not None:
            _HTTP_CODE_TO_EXCEPTION.setdefault(cls.code, cls)
        if cls.grpc_status_code is not None:
            _GRPC_CODE_TO_EXCEPTION.setdefault(cls.grpc_status_code, cls)
        return cls


class APICallError(APIError, metaclass=_APICallErrorMeta):
    """Base class for exceptions raised by calling API methods.

    Args:
        message (str): The exception message.
        errors (Sequence[Any]): An optional list of error details.
        response (Union[requests.Request, grpc.Call]): The response or
            gRPC call metadata.
    """

    code = None
    """Optional[int]: The HTTP status code associated with this error.

    This may be ``None`` if the exception does not have a direct mapping
    to an HTTP error.

    See http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html
    """

    grpc_status_code = None
    """Optional[grpc.StatusCode]: The gRPC status code associated with this
    error.

    This may be ``None`` if the exception does not match up to a gRPC error.
    """

    def __init__(self, message, errors=(), response=None):
        super(APICallError, self).__init__(message)
        self.message = message
        """str: The exception message."""
        self._errors = errors
        self._response = response

    def __str__(self):
        return "{} {}".format(self.code, self.message)

    @property
    def errors(self):
        """Detailed error information.

        Returns:
            Sequence[Any]: A list of additional error details.
        """
        return list(self._errors)

    @property
    def response(self):
        """Optional[Union[requests.Request, grpc.Call]]: The response or
        gRPC call metadata."""
        return self._response


class Redirection(APICallError):
    """Base class for for all redirection (HTTP 3xx) responses."""


class MovedPermanently(Redirection):
    """Exception mapping a ``301 Moved Permanently`` response."""

    code = http_client.MOVED_PERMANENTLY


class NotModified(Redirection):
    """Exception mapping a ``304 Not Modified`` response."""

    code = http_client.NOT_MODIFIED


class TemporaryRedirect(Redirection):
    """Exception mapping a ``307 Temporary Redirect`` response."""

    code = http_client.TEMPORARY_REDIRECT


class ResumeIncomplete(Redirection):
    """Exception mapping a ``308 Resume Incomplete`` response.

    .. note:: :attr:`http_client.PERMANENT_REDIRECT` is ``308``, but
        APIs differ in their use of this status code.
    """

    code = 308


class ClientError(APICallError):
    """Base class for all client error (HTTP 4xx) responses."""


class BadRequest(ClientError):
    """Exception mapping a ``400 Bad Request`` response."""

    code = http_client.BAD_REQUEST


class InvalidArgument(BadRequest):
    """Exception mapping a :attr:`grpc.StatusCode.INVALID_ARGUMENT` error."""

    grpc_status_code = grpc.StatusCode.INVALID_ARGUMENT


class FailedPrecondition(BadRequest):
    """Exception mapping a :attr:`grpc.StatusCode.FAILED_PRECONDITION`
    error."""

    grpc_status_code = grpc.StatusCode.FAILED_PRECONDITION


class OutOfRange(BadRequest):
    """Exception mapping a :attr:`grpc.StatusCode.OUT_OF_RANGE` error."""

    grpc_status_code = grpc.StatusCode.OUT_OF_RANGE


class Unauthorized(ClientError):
    """Exception mapping a ``401 Unauthorized`` response."""

    code = http_client.UNAUTHORIZED


class Unauthenticated(Unauthorized):
    """Exception mapping a :attr:`grpc.StatusCode.UNAUTHENTICATED` error."""

    grpc_status_code = grpc.StatusCode.UNAUTHENTICATED


class Forbidden(ClientError):
    """Exception mapping a ``403 Forbidden`` response."""

    code = http_client.FORBIDDEN


class PermissionDenied(Forbidden):
    """Exception mapping a :attr:`grpc.StatusCode.PERMISSION_DENIED` error."""

    grpc_status_code = grpc.StatusCode.PERMISSION_DENIED


class NotFound(ClientError):
    """Exception mapping a ``404 Not Found`` response or a
    :attr:`grpc.StatusCode.NOT_FOUND` error."""

    code = http_client.NOT_FOUND
    grpc_status_code = grpc.StatusCode.NOT_FOUND


class MethodNotAllowed(ClientError):
    """Exception mapping a ``405 Method Not Allowed`` response."""

    code = http_client.METHOD_NOT_ALLOWED


class Conflict(ClientError):
    """Exception mapping a ``409 Conflict`` response."""

    code = http_client.CONFLICT


class AlreadyExists(Conflict):
    """Exception mapping a :attr:`grpc.StatusCode.ALREADY_EXISTS` error."""

    grpc_status_code = grpc.StatusCode.ALREADY_EXISTS


class Aborted(Conflict):
    """Exception mapping a :attr:`grpc.StatusCode.ABORTED` error."""

    grpc_status_code = grpc.StatusCode.ABORTED


class LengthRequired(ClientError):
    """Exception mapping a ``411 Length Required`` response."""

    code = http_client.LENGTH_REQUIRED


class PreconditionFailed(ClientError):
    """Exception mapping a ``412 Precondition Failed`` response."""

    code = http_client.PRECONDITION_FAILED


class RequestRangeNotSatisfiable(ClientError):
    """Exception mapping a ``416 Request Range Not Satisfiable`` response."""

    code = http_client.REQUESTED_RANGE_NOT_SATISFIABLE


class TooManyRequests(ClientError):
    """Exception mapping a ``429 Too Many Requests`` response."""

    # http_client does not define a constant for this in Python 2.
    code = 429


class ResourceExhausted(TooManyRequests):
    """Exception mapping a :attr:`grpc.StatusCode.RESOURCE_EXHAUSTED` error."""

    grpc_status_code = grpc.StatusCode.RESOURCE_EXHAUSTED


class Cancelled(ClientError):
    """Exception mapping a :attr:`grpc.StatusCode.CANCELLED` error."""

    # This maps to HTTP status code 499. See
    # https://github.com/googleapis/googleapis/blob/master/google/rpc\
    # /code.proto
    code = 499
    grpc_status_code = grpc.StatusCode.CANCELLED


class ServerError(APICallError):
    """Base for 5xx responses."""


class InternalServerError(ServerError):
    """Exception mapping a ``500 Internal Server Error`` response. or a
    :attr:`grpc.StatusCode.INTERNAL` error."""

    code = http_client.INTERNAL_SERVER_ERROR
    grpc_status_code = grpc.StatusCode.INTERNAL


class Unknown(ServerError):
    """Exception mapping a :attr:`grpc.StatusCode.UNKNOWN` error."""

    grpc_status_code = grpc.StatusCode.UNKNOWN


class DataLoss(ServerError):
    """Exception mapping a :attr:`grpc.StatusCode.DATA_LOSS` error."""

    grpc_status_code = grpc.StatusCode.DATA_LOSS


class MethodNotImplemented(ServerError):
    """Exception mapping a ``501 Not Implemented`` response or a
    :attr:`grpc.StatusCode.UNIMPLEMENTED` error."""

    code = http_client.NOT_IMPLEMENTED
    grpc_status_code = grpc.StatusCode.UNIMPLEMENTED


class BadGateway(ServerError):
    """Exception mapping a ``502 Bad Gateway`` response."""

    code = http_client.BAD_GATEWAY


class ServiceUnavailable(ServerError):
    """Exception mapping a ``503 Service Unavailable`` response or a
    :attr:`grpc.StatusCode.UNAVAILABLE` error."""

    code = http_client.SERVICE_UNAVAILABLE
    grpc_status_code = grpc.StatusCode.UNAVAILABLE


class GatewayTimeout(ServerError):
    """Exception mapping a ``504 Gateway Timeout`` response."""

    code = http_client.GATEWAY_TIMEOUT


class DeadlineExceeded(GatewayTimeout):
    """Exception mapping a :attr:`grpc.StatusCode.DEADLINE_EXCEEDED` error."""

    grpc_status_code = grpc.StatusCode.DEADLINE_EXCEEDED


def exception_class_for_http_status(status_code):
    """Return the exception class for a specific HTTP status code.

    Args:
        status_code (int): The HTTP status code.

    Returns:
        :func:`type`: the appropriate subclass of :class:`APICallError`.
    """
    return _HTTP_CODE_TO_EXCEPTION.get(status_code, APICallError)


def from_http_status(status_code, message, **kwargs):
    """Create a :class:`APICallError` from an HTTP status code.

    Args:
        status_code (int): The HTTP status code.
        message (str): The exception message.
        kwargs: Additional arguments passed to the :class:`APICallError`
            constructor.

    Returns:
        APICallError: An instance of the appropriate subclass of
            :class:`APICallError`.
    """
    error_class = exception_class_for_http_status(status_code)
    error = error_class(message, **kwargs)

    if error.code is None:
        error.code = status_code

    return error


def from_http_response(response):
    """Create a :class:`APICallError` from a :class:`requests.Response`.

    Args:
        response (requests.Response): The HTTP response.

    Returns:
        APICallError: An instance of the appropriate subclass of
            :class:`APICallError`, with the message and errors populated
            from the response.
    """
    try:
        payload = response.json()
    except ValueError:
        payload = {"error": {"message": response.text or "unknown error"}}

    error_message = payload.get("error", {}).get("message", "unknown error")
    errors = payload.get("error", {}).get("errors", ())

    message = "{method} {url}: {error}".format(
        method=response.request.method, url=response.request.url, error=error_message
    )

    exception = from_http_status(
        response.status_code, message, errors=errors, response=response
    )
    return exception


def exception_class_for_grpc_status(status_code):
    """Return the exception class for a specific :class:`grpc.StatusCode`.

    Args:
        status_code (grpc.StatusCode): The gRPC status code.

    Returns:
        :func:`type`: the appropriate subclass of :class:`APICallError`.
    """
    return _GRPC_CODE_TO_EXCEPTION.get(status_code, APICallError)


def from_grpc_status(status_code, message, **kwargs):
    """Create a :class:`APICallError` from a :class:`grpc.StatusCode`.

    Args:
        status_code (grpc.StatusCode): The gRPC status code.
        message (str): The exception message.
        kwargs: Additional arguments passed to the :class:`APICallError`
            constructor.

    Returns:
        APICallError: An instance of the appropriate subclass of
            :class:`APICallError`.
    """
    error_class = exception_class_for_grpc_status(status_code)
    error = error_class(message, **kwargs)

    if error.grpc_status_code is None:
        error.grpc_status_code = status_code

    return error


def from_grpc_error(rpc_exc):
    """Create a :class:`APICallError` from a :class:`grpc.RpcError`.

    Args:
        rpc_exc (grpc.RpcError): The gRPC error.

    Returns:
        APICallError: An instance of the appropriate subclass of
            :class:`APICallError`.
    """
    if isinstance(rpc_exc, grpc.Call):
        return from_grpc_status(
            rpc_exc.code(), rpc_exc.details(), errors=(rpc_exc,), response=rpc_exc
        )
    else:
        return APICallError(str(rpc_exc), errors=(rpc_exc,), response=rpc_exc)
