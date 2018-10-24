from descarteslabs.client.exceptions import NotFoundError
from descarteslabs.common.tasks import FutureTask, TransientResultError, TimeoutError

import time


class UploadTask(FutureTask):
    """
    An upload task which may or may not have completed yet. Accessing any
    attributes before the task is completed (for example ``status``)
    will block until the task completes.

    If you want to check whether the attributes are available, use
    `get_result()` which will raise a `TransientResultException` if the
    attributes are not available yet.

    Certain attributes, like ``output_rows`` will only become available
    once ``status`` is ``SUCCESS`` and the ``load_state`` is ``DONE``.

    Upload tasks don't have a ``result`` or ``log`` attribute.

    Do not create an `UploadTask` yourself; it is returned by
    `FeatureCollection.upload()` and `FeatureCollection.list_uploads()`.
    """
    PENDING = 'PENDING'
    RUNNING = 'RUNNING'

    _DONE = 'DONE'

    def __init__(self, guid, tuid=None, client=None, upload_id=None,
                 result_attrs=None):
        FutureTask.__init__(self, guid, tuid, client)

        self._upload_id = upload_id
        self._task_result = result_attrs

    def get_result(self, wait=False, timeout=None):
        """
        Attempt to load the result for this upload task. After returning
        from this method without an exception raised, the information for
        the task is available through the various properties.

        Note that depending on the ``status`` and the ``load_state`` not
        all attributes may be available.

        Parameters
        ----------
        wait : bool
            Whether to wait for the task to complete or raise
            a `TransientResultException` if the task hasn't completed
            yet.
        timeout : int
            How long to wait in seconds for the task to complete, or
            ``None`` to wait indefinitely.

        Raises
        ------
        `TransientResultException`
            When the result is not ready yet (and not waiting).

        RuntimeError
            When the timeout has been reached (if waiting and set).
        """
        if self._task_result is None:
            if self._upload_id is None:
                raise ValueError("Cannot retrieve upload task without upload id")

            start = time.time()

            while timeout is None or (time.time() - start) < timeout:
                try:
                    result = self.client.get_upload_result(
                        self.guid, self._upload_id)
                    self.tuid = result.data.id
                    self._task_result = result.data.attributes
                except NotFoundError:
                    if not wait:
                        raise TransientResultError()
                else:
                    break

                time.sleep(self.COMPLETION_POLL_INTERVAL_SECONDS)
            else:
                raise TimeoutError()

    @property
    def result(self):
        """
        Raises
        ------
        AttributeError
            Upload tasks don't have a result."""
        raise AttributeError("Upload tasks don't have a result")

    @property
    def log(self):
        """
        Raises
        ------
        AttributeError
            Upload tasks don't have a log."""
        raise AttributeError("Upload tasks don't have a log")

    @property
    def upload_id(self):
        """
        :return: (*str*) The id of the upload that resulted in this task.
        """
        if self._upload_id is None:
            labels = self._result_attribute('labels')

            if labels is not None:
                self._upload_id = labels[2]

        return self._upload_id

    def _load_attribute(self, name):
        """
        Get an attribute value from the returned values.

        Returns
        -------
        object
            The named attribute within the ``load`` attribute.
        """
        load = self._result_attribute('load')

        # If the load operation is running; we better get the latest state
        if load is None or load.get('state') != UploadTask._DONE:
            result = self.client.get_upload_result(self.guid, self.upload_id)
            self._task_result = result.data.attributes
            load = self._result_attribute('load')

        if load is None:
            return None
        else:
            return load.get(name)

    @property
    def error_rows(self):
        """
        :return: (*int*) The number of rows that could not be loaded.
        """
        return self._load_attribute('errors')

    @property
    def output_rows(self):
        """
        :return: (*int*) The number of rows that were added.
        """
        return self._load_attribute('output_rows')

    @property
    def status(self):
        """
        :return: (*str*) The state of the ``load`` operation.

            - ``PENDING`` -- The upload task has been scheduled.
            - ``RUNNING`` -- The upload task is currently running.
            - ``SUCCESS`` -- The upload task has completed.  At least some
                rows were loaded.  You can find the number of rows added in
                ``output_rows`` and the number of lines in the JSON file
                that were skipped in ``error_rows``.
            - ``FAILURE`` -- The upload task failed and no rows were loaded.
                    You can find additional information in ``exception_name``
                    and ``stacktrace``.
        """
        status = self._result_attribute('status')

        if status == FutureTask.SUCCESS:
            state = self._load_attribute('state')

            if state != UploadTask._DONE:
                return state

        return status
