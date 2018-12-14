import os
import threading


class ThreadLocalWrapper(object):
    """
    A wrapper around a thread-local object that gets created lazily in every
    thread of every process via the given factory callable when it is
    accessed. I.e., at most one instance per thread exists.

    In contrast to standard thread-locals this is compatible with multiple
    processes.
    """

    def __init__(self, factory):
        self._factory = factory
        self._create_local(os.getpid())

    def get(self):
        self._init_local()
        if not hasattr(self._local, "wrapped"):
            self._local.wrapped = self._factory()
        return self._local.wrapped

    def _init_local(self):
        local_pid = os.getpid()
        previous_pid = getattr(self._local, "_pid", None)
        if previous_pid is None:
            self._local._pid = local_pid
        elif local_pid != previous_pid:
            self._create_local(local_pid)

    def _create_local(self, pid):
        self._local = threading.local()
        self._local._pid = pid
