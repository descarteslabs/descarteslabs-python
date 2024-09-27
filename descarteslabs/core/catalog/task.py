# Copyright 2018-2024 Descartes Labs.

import time
from concurrent.futures import TimeoutError

from strenum import StrEnum

from .catalog_base import _new_abstract_class
from .catalog_client import CatalogClient


class TaskState(StrEnum):
    """The state of a task.

    Attributes
    ----------
    NEVERRAN : enum
        The operation was never invoked.
    RUNNING : enum
        The operation is in progress.
    SUCCEEDED : enum
        The operation was successfully completed.
    FAILED : enum
        The operation resulted in a failure and may not have been completed.
    """

    NEVERRAN = "NONE"  # The operation was never started
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCESS"
    FAILED = "FAILURE"


class TaskStatus(object):
    """A base class for the status of asynchronous jobs."""

    _TERMINAL_STATES = [TaskState.NEVERRAN, TaskState.SUCCEEDED, TaskState.FAILED]
    _POLLING_INTERVAL = 60

    # The following 2 attributes must be set correctly in any derived class
    _task_name = "task"  # The name of the task as shown in __repr__()
    _url = "{}"  # The url for getting the status of the task with the `id` passed in

    def __new__(cls, *args, **kwargs):
        return _new_abstract_class(cls, TaskStatus)

    def __init__(
        self,
        id=None,
        status=None,
        start_datetime=None,
        duration_in_seconds=None,
        errors=None,
        _client=None,
        **kwargs
    ):
        self.id = id
        self.start_datetime = start_datetime
        self.duration_in_seconds = duration_in_seconds
        self.errors = errors
        self._client = _client or CatalogClient.get_default_client()

        try:
            self.status = TaskState(status)
        except ValueError:
            pass

    def __repr__(self):
        status = self.status.value if self.status else "UNKNOWN"
        text = ["{} {} status: {}".format(self.id, self._task_name, status)]
        if self.start_datetime:
            text.append("  - started: {}".format(self.start_datetime))

        if self.duration_in_seconds:
            text.append("  - took {:,.4f} seconds".format(self.duration_in_seconds))

        if self.errors:
            text.append("  - {} errors reported:".format(len(self.errors)))
            for e in self.errors:
                text.append("    - {}".format(e))
        return "\n".join(text)

    def reload(self):
        """Update the task information.

        Raises
        ------
        ~descarteslabs.exceptions.ClientError or ~descarteslabs.exceptions.ServerError
            :ref:`Spurious exception <network_exceptions>` that can occur during a
            network request.
        """
        r = self._client.session.get(self._url.format(self.id))
        response = r.json()
        new_values = response["data"]["attributes"]

        self.status = TaskState(new_values.pop("status"))
        for key, value in new_values.items():
            setattr(self, key, value)

    def wait_for_completion(self, timeout=None):
        """Wait for the task to complete.

        Parameters
        ----------
        timeout : int, optional
            If specified, will wait up to specified number of seconds and will raise
            a :py:exc:`concurrent.futures.TimeoutError` if the task has not completed.

        Raises
        ------
        :py:exc:`concurrent.futures.TimeoutError`
            If the specified timeout elapses and the task has not completed
        """
        if self.status in self._TERMINAL_STATES:
            return

        if timeout:
            timeout = time.time() + timeout
        while True:
            self.reload()
            if self.status in self._TERMINAL_STATES:
                return
            if timeout:
                t = timeout - time.time()
                if t <= 0:
                    raise TimeoutError()
                t = min(t, self._POLLING_INTERVAL)
            else:
                t = self._POLLING_INTERVAL
            time.sleep(t)
