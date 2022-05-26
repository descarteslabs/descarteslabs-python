class VectorException(Exception):
    """Base exception for Vector operations"""

    pass


class WaitTimeoutError(VectorException):
    """The timeout period for a wait operation has been exceeded"""

    pass


class FailedJobError(VectorException):
    """Used to indicate that an asynchronous job has failed"""

    pass


class InvalidQueryException(VectorException):
    """The submitted query is invalid"""

    pass


# FailedCopyError, use the FailedJobError
FailedCopyError = FailedJobError
