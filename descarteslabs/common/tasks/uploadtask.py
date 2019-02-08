from descarteslabs.client.exceptions import NotFoundError
from descarteslabs.common.tasks import FutureTask, TransientResultError, TimeoutError

import time


class UploadTask(FutureTask):
    """
    An upload task which may or may not have completed yet. Accessing any
    attributes before the task is completed (for example ``status``)
    will block until the task completes.

    The upload process is two-phased. In the first phase, the input file is
    processed to extract features from the file. Errors parsing (invalid NDJSon
    or invalid GeoJson) will be captured in this phase. The features are then
    transformed into BigQuery inserts including sharding. In the second phase,
    the inserts are uploaded to BigQuery for processing. Once both phases
    have completed, the upload is considered done.

    If you want to check whether the attributes are available, use
    ``get_result()`` which will raise a ``TransientResultError`` if the
    attributes are not available yet.

    Upload tasks don't have a ``result`` or ``log`` attribute.

    Do not create an ``UploadTask`` yourself; it is returned by
    ``FeatureCollection.upload()`` and ``FeatureCollection.list_uploads()``.
    """

    _SKIPPED = 'SKIPPED'
    _PENDING = 'PENDING'
    _RUNNING = 'RUNNING'
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

        Parameters
        ----------
        wait : bool
            Whether to wait for the task to complete or raise
            a ``TransientResultError`` if the task hasn't completed
            yet.
        timeout : int
            How long to wait in seconds for the task to complete, or
            ``None`` to wait indefinitely.

        Raises
        ------
        ``TransientResultError``
            When the result is not ready yet (and not waiting).

        ``TimeoutError``
            When the timeout has been reached (if waiting and set).
        """

        # Things are complicated compared to FutureTask, because the upload task
        # can have completed, but the BigQuery upload process may not yet have terminated.
        # We wait for termination of the BigQuery upload.
        if self._task_result is None or (self._task_result.status == self.SUCCESS and
                                         ('load' not in self._task_result or
                                          self._task_result.load.state in (self._PENDING, self._RUNNING))):
            if self._upload_id:
                id = self._upload_id
            elif self.tuid:
                id = self.tuid
            else:
                raise ValueError("Cannot retrieve upload task without task id or upload id")

            start = time.time()

            while timeout is None or (time.time() - start) < timeout:
                try:
                    result = self.client.get_upload_result(self.guid, id)
                    self.tuid = result.data.id
                    self._task_result = result.data.attributes
                except NotFoundError:
                    if not wait:
                        raise TransientResultError()
                else:
                    if (self._task_result.status == self.SUCCESS and
                        self._task_result.load.state not in (self._PENDING, self._RUNNING)):  # noqa
                        break
                    if not wait:
                        raise TransientResultError()

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
        Get the upload_id for the task.

        :return: (*str*) The id of the upload that resulted in this task.
        """
        if self._upload_id is None:
            labels = self._result_attribute('labels')

            if labels is not None:
                self._upload_id = labels[2]

        return self._upload_id

    @property
    def error_rows(self):
        """
        :return: (*int*) The number of rows that could not be loaded. This is the sum
        of the number of invalid features plus the number of valid features which otherwise
        failed to load.
        """
        return (len(self._result_attribute('result', {}).get('errors', [])) +
                len(self._result_attribute('load', {}).get('errors') or []))

    @property
    def errors(self):
        """
        :return: (*list* or None) Error records from upload. Errors may come from parsing of the
        input file or from attempting to add the features to the collection.
        """
        result = (self._result_attribute('result', {}).get('errors', []) +
                  (self._result_attribute('load', {}).get('errors') or []))
        if len(result) == 0:
            return None
        return result

    @property
    def input_features(self):
        """
        :return: (*int*) The number of features to insert. This may be different
        from the number of lines in the input file due to errors while parsing
        the input file.
        """
        return self._result_attribute('result', {}).get('input_features', 0)

    @property
    def input_rows(self):
        """
        :return: (*int*) The number of rows to insert. This may be different from
        the number reported by ``input_features`` due to sharding.
        """
        return self._result_attribute('result', {}).get('input_rows', 0)

    @property
    def output_rows(self):
        """
        :return: (*int*) The number of rows that were added. This may be different than
        the number reported by ``input_rows`` if errors occurred.
        """
        return self._result_attribute('load', {}).get('output_rows', 0)

    @property
    def status(self):
        """
        :return: The status (``SUCCESS`` or ``FAILURE``) for this completed task.

        Some errors may have occurred even when the status is ``SUCCESS``, if the
        upload was initiated with a non-zero ``max_errors`` parameter. The
        ``error_rows`` or ``errors`` property should be consulted.

        Conversely, some rows may have been inserted even with the status is ``FAILURE``.
        """
        status = self._result_attribute('status', None)

        if status == self.SUCCESS:
            status = self._result_attribute('load', {}).get('state', self._SKIPPED)
            if status == self._SKIPPED:
                if self.error_rows > 0:
                    status = self.FAILURE
                else:
                    status = self.SUCCESS
            elif status == self._DONE:
                status = self.SUCCESS

        return status
