from . import FutureTask, TransientResultError, TimeoutError

import time


class UploadTask(FutureTask):
    """
    An upload task which may or may not have completed yet. Accessing any
    attributes before the task is completed (for example :attr:`status`)
    will block until the task completes.

    The upload process is two-phased. In the first phase, the input file is
    processed to extract features from the file. Errors parsing (invalid NDJSon
    or invalid GeoJson) will be captured in this phase. The features are then
    transformed into BigQuery inserts including sharding. In the second phase,
    the inserts are uploaded to BigQuery for processing. Once both phases
    have completed, the upload is considered done.

    If you want to check whether the attributes are available, use
    :meth:`get_result()` which will raise a
    :exc:`~descarteslabs.common.tasks.futuretask.TransientResultError` if the
    attributes are not available yet.

    Upload tasks don't have a :attr:`result` or :attr:`log` attribute.

    Do not create an :class:`UploadTask` yourself; it is returned by
    :meth:`FeatureCollection.upload <descarteslabs.vectors.featurecollection.FeatureCollection.upload>`
    and :meth:`FeatureCollection.list_uploads <descarteslabs.vectors.featurecollection.FeatureCollection.list_uploads>`.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    _SKIPPED = "SKIPPED"
    _DONE = "DONE"

    def __init__(self, guid, tuid=None, client=None, upload_id=None, result_attrs=None):
        if client is None:
            from ...client.services.vector import Vector  # circular import

            client = Vector()

        super(UploadTask, self).__init__(guid, tuid, client=client)

        self._upload_id = upload_id
        self._task_result = result_attrs

    def get_result(self, wait=False, timeout=None):
        """
        Attempt to load the result for this upload task. After returning
        from this method without an exception raised, the information for
        the task is available through the various properties.

        :param bool wait: Whether to wait for the task to complete or raise
            a :exc:`~descarteslabs.common.tasks.futuretask.TransientResultError`
            if the task hasn't completed yet.

        :param int timeout: How long to wait in seconds for the task to complete, or
            :const:`None` to wait indefinitely.

        :raises NotFoundError: When the upload id or task id does not exist.

        :raises TransientResultError: When the result is not ready yet (and not waiting).

        :raises ~descarteslabs.common.tasks.TimeoutError: When the timeout has been reached (if waiting and set).
        """

        # Things are complicated compared to FutureTask, because the upload task
        # can have completed, but the BigQuery upload process may not yet have terminated.
        # We wait for termination of the BigQuery upload.
        if (
            self._task_result is None
            or self._task_result.status == self.PENDING
            or (
                self._task_result.status == self.SUCCESS
                and (
                    "load" not in self._task_result
                    or self._task_result.load.state in (self.PENDING, self.RUNNING)
                )
            )
        ):  # noqa
            if self._upload_id:
                id = self._upload_id
            elif self.tuid:
                id = self.tuid
            else:
                raise ValueError(
                    "Cannot retrieve upload task without task id or upload id"
                )

            start = time.time()

            while timeout is None or (time.time() - start) < timeout:
                result = self.client.get_upload_result(self.guid, id, pending=True)
                if result.data.attributes.status != self.PENDING:
                    # we have actual task results
                    id = self.tuid = result.data.id
                self._task_result = result.data.attributes
                if self._task_result.status == self.FAILURE or (
                    self._task_result.status == self.SUCCESS
                    and "load" in self._task_result
                    and self._task_result.load.state not in (self.PENDING, self.RUNNING)
                ):  # noqa
                    break
                if not wait:
                    raise TransientResultError()

                time.sleep(self.COMPLETION_POLL_INTERVAL_SECONDS)
            else:
                raise TimeoutError()

    @property
    def result(self):
        """Upload tasks don't have a result.

        :raises AttributeError: No result available
        """
        raise AttributeError("Upload tasks don't have a result")

    @property
    def log(self):
        """Upload tasks don't have a log.

        :raises AttributeError: No log available
        """
        raise AttributeError("Upload tasks don't have a log")

    @property
    def upload_id(self):
        """
        Property indicating the upload_id for the task.

        :rtype: str
        :return: The id of the upload that resulted in this task.
        """
        if self._upload_id is None:
            labels = self._result_attribute("labels")
            if labels is not None:
                self._upload_id = labels[2]

        return self._upload_id

    @property
    def error_rows(self):
        """
        Property indicating the number of rows that could not be loaded. This is the sum
        of the number of invalid features plus the number of valid features
        which otherwise failed to load.

        :rtype: int
        :return: The number of rows that could not be loaded.
        """
        return len(self._result_attribute("result", {}).get("errors", [])) + len(
            self._result_attribute("load", {}).get("errors") or []
        )

    @property
    def errors(self):
        """
        Property indicating the list of error records from upload. Errors may come
        from parsing of the input file or from attempting to add the features to
        the collection.

        :rtype: list or None
        :return: Error records from upload.
        """
        result = self._result_attribute("result", {}).get("errors", []) + (
            self._result_attribute("load", {}).get("errors") or []
        )
        if len(result) == 0:
            return None
        return result

    @property
    def input_features(self):
        """
        Property indicating the number of features to insert. This may be different
        from the number of lines in the input file due to errors while parsing
        the input file.

        :rtype: int
        :return: The number of features to insert.
        """
        return self._result_attribute("result", {}).get("input_features", 0)

    @property
    def input_rows(self):
        """
        Property indicating the number of rows to insert. This may be different from
        the number reported by :attr:`input_features` due to sharding.

        :rtype: int
        :return: The number of rows to insert.
        """
        return self._result_attribute("result", {}).get("input_rows", 0)

    @property
    def output_rows(self):
        """
        Property indicating the number of rows that were added. This may be different
        than the number reported by :attr:`input_rows` if errors occurred.

        :rtype: int
        :return: The number of rows that were added.
        """
        return self._result_attribute("load", {}).get("output_rows", 0)

    @property
    def status(self):
        """
        Property indicating the status of the task, which can be :const:`PENDING`,
        :const:`RUNNING`, :const:`SUCCESS` or :const:`FAILURE`.

        Some errors may have occurred even when the status is :const:`SUCCESS`, if the
        upload was initiated with a non-zero :attr:`max_errors` parameter. The
        :attr:`error_rows` or :attr:`errors` property should be consulted.

        Conversely, some rows may have been inserted even with the status is
        :const:`FAILURE`.

        Status values of :const:`PENDING` or :const:`RUNNING` indicate the upload
        is still in progress, and can be waited upon for completion.

        :rtype: str
        :return: The status for this task.
        """
        try:
            self.get_result(wait=False)
        except TransientResultError:
            # expected if not yet done
            pass
        status = self._task_result.status

        if status == self.SUCCESS:
            status = self._task_result.load.state
            if status == self._SKIPPED:
                if self.error_rows > 0:
                    status = self.FAILURE
                else:
                    status = self.SUCCESS
            elif status == self._DONE:
                status = self.SUCCESS

        return status

    def __repr__(self):
        s = "UploadTask\n"
        if self._task_result is None or self._task_result.status == self.PENDING:
            s += "\tStatus: Pending\n"
        else:
            s += "\tStatus: {}\n".format(self._task_result.status)
            s += "\tMemory usage (MiB): {:.2f}\n".format(
                self._task_result.peak_memory_usage / (1024 * 1024.0)
            )
            s += "\tRuntime (s): {}\n".format(self._task_result.runtime)
            if "load" in self._task_result:
                s += "\tLoad State: {}\n".format(self._task_result.load.state)

        return s
