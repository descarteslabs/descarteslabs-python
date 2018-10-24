import multiprocessing
import os
import unittest
import threading

from descarteslabs.common.threading.local import ThreadLocalWrapper


class ThreadLocalWrapperTest(unittest.TestCase):

    def setUp(self):
        self.wrapper = ThreadLocalWrapper(lambda: (os.getpid(), threading.current_thread().ident))

    def _store_id(self):
        self.thread_id = self.wrapper.get()

    def _send_id(self, queue):
        queue.put(self.wrapper.get())

    def test_wrapper(self):
        main_thread_id = self.wrapper.get()
        self.assertEqual(main_thread_id, self.wrapper.get())

        thread = threading.Thread(target=self._store_id)
        thread.start()
        thread.join()
        self.assertNotEqual(main_thread_id, self.thread_id)

        queue = multiprocessing.Queue()
        process = multiprocessing.Process(target=self._send_id, args=(queue,))
        process.start()
        process_id = queue.get()
        process.join()
        self.assertNotEqual(main_thread_id, process_id)
        self.assertNotEqual(self.thread_id, process_id)

    def test_wrapper_unused_in_main_process(self):
        queue = multiprocessing.Queue()
        process = multiprocessing.Process(target=self._send_id, args=(queue,))
        process.start()
        process_id = queue.get()
        process.join()
        self.assertNotEqual(process_id, self.wrapper.get())

    def test_fork_from_fork(self):
        # A gross edge case discovered by Clark: if a process is forked from a forked process
        # things will go awry if we hadn't initialized the internal threading.local's pid.
        def fork_another(queue):
            queue.put(self.wrapper.get())
            process3 = multiprocessing.Process(target=self._send_id, args=(queue,))
            process3.start()
            process3.join()
        process1_id = self.wrapper.get()
        queue = multiprocessing.Queue()
        process = multiprocessing.Process(target=fork_another, args=(queue,))
        process.start()
        process2_id = queue.get()
        process3_id = queue.get()
        process.join()
        self.assertNotEqual(process1_id, process2_id)
        self.assertNotEqual(process2_id, process3_id)
        self.assertNotEqual(process1_id, process3_id)
