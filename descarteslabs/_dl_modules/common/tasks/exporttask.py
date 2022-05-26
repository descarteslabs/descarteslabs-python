import time

from descarteslabs.exceptions import NotFoundError
from ...client.services.storage import Storage
from . import FutureTask, TransientResultError, TimeoutError


class ExportTask(FutureTask):
    """
    An export task. Accessing any attributes before the task is completed
    (for example :attr:`status`) will block until the task completes.

    If you want to check whether the attributes are available, use
    :attr:`is_ready` which will return :const:`True` when attributes are available.

    Do not create an :class:`ExportTask` yourself; it is returned by
    :meth:`FeatureCollection.export
    <descarteslabs.vectors.featurecollection.FeatureCollection.export>`
    and :meth:`FeatureCollection.list_exports
    <descarteslabs.vectors.featurecollection.FeatureCollection.list_exports>`.
    """

    def __init__(self, guid, tuid=None, client=None, result_attrs=None, key=None):
        if client is None:
            from ...client.services.vector import Vector  # circular import

            client = Vector()

        super(ExportTask, self).__init__(guid, tuid, client=client)
        self.export_id = tuid
        self._task_result = result_attrs
        self._set_key(key)

    def _set_key(self, key):
        if key is not None:
            self.key = key
        elif self._task_result is not None:
            labels = self._result_attribute("labels")

            if labels is not None:
                self.key = labels[3]

    def get_file(self, file_obj):
        """Download the exported Storage object to a local file.

        :param str file_obj: A file-like object or name of file to download into.

        :raises TransientResultError: If the export hasn't completed yet.
        """
        if self.key is None:
            raise TransientResultError()
        else:
            return Storage().get_file(self.key, file_obj)

    def get_result(self, wait=False, timeout=None):
        """
        Attempt to load the result for this export task. After returning
        from this method without an exception raised, the information for
        the task is available through the various properties.

        :param bool wait: Whether to wait for the task to complete or raise
            a :exc:`~descarteslabs.common.tasks.futuretask.TransientResultError`
            if the task hasn't completed yet.

        :param int timeout: How long to wait in seconds for the task to complete, or
            :const:`None` to wait indefinitely.

        :raises TransientResultError: When the result is not ready yet (and not waiting).

        :raises ~descarteslabs.common.tasks.TimeoutError: When the timeout has been reached (if waiting and set).
        """

        # We have to go through the vector service since we don't
        # own the task group
        if self._task_result is None:
            start = time.time()

            while timeout is None or (time.time() - start) < timeout:
                try:
                    result = self.client.get_export_result(self.guid, self.tuid)
                    self._task_result = result.data.attributes
                    self._set_key(None)
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
        Export tasks don't have a result.

        :raises AttributeError: No result available
        """
        raise AttributeError("Export tasks don't have a result")

    def __repr__(self):
        s = "ExportTask\n"
        if self.ready:
            s += "\tStatus: {}\n".format(self._task_result.status)
            s += "\tMemory usage (MiB): {:.2f}\n".format(
                self._task_result.peak_memory_usage / (1024 * 1024.0)
            )
            s += "\tRuntime (s): {}\n".format(self._task_result.runtime)
        else:
            s += "\tStatus: Pending\n"

        return s
