from descarteslabs.common.proto import errors_pb2


class JobComputeError(Exception):
    "Generic error raised when a job computation fails."

    def __init__(self, job):
        self._id = job.id
        self._code = errors_pb2.ErrorCode.Name(job._message.error.code)
        self._message = job._message.error.message

        super(JobComputeError, self).__init__(
            'Job("{}") failed with: code={}, message="{}"'.format(
                self._id, self._code, self._message
            )
        )

    @property
    def code(self):
        return self._code

    @property
    def id(self):
        return self._id

    @property
    def message(self):
        return self._message


class JobOOM(JobComputeError):
    "Raised when a job computation runs out of memory."
    pass


class JobAuth(JobComputeError):
    "Raised when a job computation fails due to invalid authentication."
    pass


class JobInvalid(JobComputeError):
    "Raised when a job computation is invalid."
    pass


class JobInvalidTyping(JobInvalid):
    "Raised when a job computation fails due to an operation being applied to an inappropriate type."
    pass


class JobDeadlineExceeded(JobComputeError):
    "Raised when a job takes too long to compute (currently 30 mins)."
    pass


class JobTerminated(JobComputeError):
    "Raised when a job computation is terminated before finishing."
    pass


class JobInterrupt(JobComputeError):
    "Raised when a job computation is interrupted before finishing."
    pass


class TimeoutError(Exception):
    "Raised when a computation took longer to complete than a specified timeout."
    pass


ERRORS = {
    errors_pb2.ERROR_NONE: None,
    errors_pb2.ERROR_UNKNOWN: JobComputeError,
    errors_pb2.ERROR_INVALID: JobInvalid,
    errors_pb2.ERROR_DEADLINE: JobDeadlineExceeded,
    errors_pb2.ERROR_OOM: JobOOM,
    errors_pb2.ERROR_INTERRUPT: JobInterrupt,
    errors_pb2.ERROR_TERMINATED: JobTerminated,
    errors_pb2.ERROR_AUTH: JobAuth,
    errors_pb2.ERROR_TYPING: JobInvalidTyping,
}


def error_code_to_exception(error_code):
    return ERRORS.get(error_code, JobComputeError)
