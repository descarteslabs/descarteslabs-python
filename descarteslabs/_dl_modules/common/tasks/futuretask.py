# Copyright 2018-2020 Descartes Labs.
#
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

import pickle
import json
import time

from descarteslabs.exceptions import NotFoundError
from ...client.services.storage import Storage


class TransientResultError(Exception):
    """
    Raised when attempting to access results for a task that hasn't
    completed.
    """

    def __init__(self, message="Result not yet ready"):
        super(Exception, self).__init__(message)


class TimeoutError(Exception):
    """
    Raised when attempting to access results for a task that hasn't
    completed.
    """

    def __init__(self, message="Timeout exceeded"):
        super(Exception, self).__init__(message)


class ResultType(object):
    """
    Possible types of return values for a function.
    """

    JSON = "json"
    LEGACY_PICKLE = "pickle"


class FutureTask(object):
    """
    A submitted task which may or may not have completed yet. Accessing any
    attributes only available on a completed task (for example `result`)
    blocks until the task completes.
    """

    COMPLETION_POLL_INTERVAL_SECONDS = 3
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"

    def __init__(self, guid, tuid, client=None, args=None, kwargs=None):
        self.guid = guid
        self.tuid = tuid
        if client is None:
            from descarteslabs.client.services.tasks import Tasks  # circular import

            client = Tasks()

        self.client = client
        self.args = args
        self.kwargs = kwargs
        self._is_return_value_loaded = False
        self._return_value = None
        self._task_result = None
        self._is_log_loaded = False
        self._log = None
        self._json_arguments = None

    def get_result(self, wait=False, timeout=None):
        """
        Attempt to load the result for this task. After returning from this
        method without an exception raised, the return value for the task is
        available through the :attr:`result` property.

        :param bool wait: Whether to wait for the task to complete or raise
            a :exc:`~descarteslabs.common.tasks.futuretask.TransientResultError`
            if the task hasnt completed yet.
        :param int timeout: How long to wait for the task to complete, or
            :const:`None` to wait indefinitely.
        """
        if self._task_result is None:
            start = time.time()

            while timeout is None or (time.time() - start) < timeout:
                try:
                    self._task_result = self.client.get_task_result(
                        self.guid, self.tuid, include=["stacktrace"]
                    )
                except NotFoundError:
                    if not wait:
                        raise TransientResultError()
                else:
                    break

                time.sleep(self.COMPLETION_POLL_INTERVAL_SECONDS)
            else:
                raise TimeoutError()

    def _result_attribute(self, attribute_name, default=None):
        self.get_result(wait=True)

        return self._task_result.get(attribute_name, default)

    @property
    def ready(self):
        """
        Property indicating whether the task has completed

        :rtype: bool
        :return: True if the upload task has completed and status is available, otherwise False.
        """
        try:
            self.get_result(wait=False)
            return True
        except TransientResultError:
            return False

    @property
    def result(self):
        """
        Property indicating the return value of the function for this completed task.

        :rtype: json or pickled type
        :return: The return value of the function for this completed task.
        """
        if not self.is_success:
            return None

        if not self._is_return_value_loaded:
            if self._task_result.result_size_bytes > 0:
                return_value = Storage().get(
                    self._task_result.result_key, storage_type="result"
                )
                result_type = self._task_result.get(
                    "result_type", ResultType.LEGACY_PICKLE
                )
                if result_type == ResultType.JSON:
                    self._return_value = json.loads(return_value.decode("utf-8"))
                elif result_type == ResultType.LEGACY_PICKLE:
                    return_value = pickle.loads(return_value)

                    if isinstance(return_value, dict):
                        # For backwards-compatibility reasons (the old dlrun client requires it),
                        # results to be pickled have always been wrapped in a dictionary. However,
                        # all clients since version 0.10.0 ignore all the dictionary items except
                        # for 'result', and all clients have always extracted the 'result' element.
                        # In order for the service to remain compatible with older clients, we
                        # must continue to do this even though it is wasteful.
                        self._return_value = return_value["result"]
                    else:
                        # for the above reason, this code will likely never be reached.
                        self._return_value = return_value
                else:
                    raise RuntimeError(
                        "Unknown result type: %s - update your tasks client"
                    )
            else:
                self._return_value = None

            self._is_return_value_loaded = True

        return self._return_value

    @property
    def log(self):
        """
        Property indicating the log output for this completed task.

        :rtype: str
        :return: The log output
        """
        self.get_result(wait=True)

        if not self._is_log_loaded and self._task_result.get("log_size_bytes", 1) > 0:
            try:
                self._log = Storage().get(
                    self._task_result.result_key, storage_type="logs"
                )
            except NotFoundError:
                self._log = None

            self._is_log_loaded = True

        return self._log

    @property
    def peak_memory_usage(self):
        """
        Property indicating the peak memory usage for this completed task, in bytes.

        :rtype: int
        :return: The peak memory usage
        """
        return self._result_attribute("peak_memory_usage")

    @property
    def runtime(self):
        """
        Property indicating the time spent executing the function for this task,
        in seconds.

        :rtype: int
        :return: The time spent executing the function
        """
        return self._result_attribute("runtime")

    @property
    def status(self):
        """
        Property indicating the status (:const:`SUCCESS` or :const`FAILURE`) for
        this completed task.

        :rtype: str
        :return: The status for this completed task.
        """
        return self._result_attribute("status")

    @property
    def is_success(self):
        """
        Did this task succeeed?

        :rtype: bool
        :return: Whether this task succeeded.
        """
        return self.status == FutureTask.SUCCESS

    @property
    def exception_name(self):
        """
        Property indicating the name of the exception raised during the function
        execution, if any

        :rtype: str
        :return: The name of the exception or :const:`None`
        """
        return self._result_attribute("exception_name")

    exception = exception_name

    @property
    def stacktrace(self):
        """
        Property indicating the stacktrace of the exception raised during the function
        execution, if any.

        :rtype: str
        :return: The stacktrace of the exception or :const:`None`
        """
        return self._result_attribute("stacktrace")

    traceback = stacktrace

    @property
    def failure_type(self):
        """
        The type of failure if this task did not succeed.

        :rtype: str
        :return: The failure type
        """
        return self._result_attribute("failure_type")

    def __eq__(self, other):
        return self.guid == other.guid and self.tuid == other.tuid

    def __repr__(self):
        s = "Task\n"
        if self.ready:
            s += "\tStatus: {}\n".format(self._task_result.status)
            s += "\tMemory usage (MiB): {:.2f}\n".format(
                self._task_result.peak_memory_usage / (1024 * 1024.0)
            )
            s += "\tRuntime (s): {}\n".format(self._task_result.runtime)
        else:
            s += "\tStatus: Pending\n"

        return s
