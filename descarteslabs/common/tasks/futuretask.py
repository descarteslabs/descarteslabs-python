# Copyright 2018 Descartes Labs.
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

import cloudpickle
import json
import time

from descarteslabs.client.exceptions import NotFoundError
from descarteslabs.client.services.storage import Storage


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
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'

    def __init__(self, guid, tuid, client=None, args=None, kwargs=None):
        self.guid = guid
        self.tuid = tuid
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
        available through the `result` property.

        :param bool wait: Whether to wait for the task to complete or raise
            a :class:`TransientResultError` if the task hasnt completed
            yet.
        :param int timeout: How long to wait for the task to complete, or
            `None` to wait indefinitely.
        """
        if self._task_result is None:
            start = time.time()

            while timeout is None or \
                    (time.time() - start) < timeout:
                try:
                    self._task_result = self.client.get_task_result(
                        self.guid, self.tuid, include=['stacktrace'])
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
    def result(self):
        """
        :return: The return value of the function for this completed task.
        """
        if not self.is_success:
            return None

        if not self._is_return_value_loaded:
            return_value = Storage().get(self._task_result.result_key, storage_type='result')
            result_type = self._task_result.get('result_type', ResultType.LEGACY_PICKLE)

            if result_type == ResultType.LEGACY_PICKLE:
                return_value = cloudpickle.loads(return_value)

                if isinstance(return_value, dict):
                    # For backwards-compatibility reasons, for legacy pickles the result is
                    # wrapped in a dictionary.
                    self._return_value = return_value['result']
                else:
                    self._return_value = return_value
            elif result_type == ResultType.JSON:
                self._return_value = json.loads(return_value.decode('utf-8'))
            else:
                raise RuntimeError("Unknown result type: %s - update your tasks client")

            self._is_return_value_loaded = True

        return self._return_value

    @property
    def log(self):
        """
        :return: The log output for this completed task.
        """
        self.get_result(wait=True)

        if not self._is_log_loaded and self._task_result.get('log_size_bytes', 1) > 0:
            try:
                self._log = Storage().get(self._task_result.result_key, storage_type='logs')
            except NotFoundError:
                self._log = None

            self._is_log_loaded = True

        return self._log

    @property
    def peak_memory_usage(self):
        """
        :return: The peak memory usage for this completed task, in bytes.
        """
        return self._result_attribute('peak_memory_usage')

    @property
    def runtime(self):
        """
        :return: The time spent executing the function for this task, in seconds.
        """
        return self._result_attribute('runtime')

    @property
    def status(self):
        """
        :return: The status (``SUCCESS`` or ``FAILURE``) for this completed task.
        """
        return self._result_attribute('status')

    @property
    def is_success(self):
        """
        :return: Whether this task succeeded.
        """
        return self.status == FutureTask.SUCCESS

    @property
    def exception_name(self):
        """
        :return: The name of the exception raised during the function execution,
            if any.
        """
        return self._result_attribute('exception_name')

    exception = exception_name

    @property
    def stacktrace(self):
        """
        :return: The stacktrace of the exception raised during the function
            execution, if any.
        """
        return self._result_attribute('stacktrace')

    traceback = stacktrace

    @property
    def failure_type(self):
        """
        :return: The type of failure if this task did not succeed.
        """
        return self._result_attribute('failure_type')

    def __eq__(self, other):
        return self.guid == other.guid and self.tuid == other.tuid

    def __repr__(self):
        s = "Task\n"
        if self._task_result is None:
            s += "\tStatus: Pending\n"
        else:
            s += "\tStatus: {}\n".format(self.status)
            s += "\tMemory usage (MiB): {:.2f}\n".format(
                self.peak_memory_usage / (1024 * 1024.)
            )
            s += "\tRuntime (s): {}\n".format(self.runtime)

        return s
