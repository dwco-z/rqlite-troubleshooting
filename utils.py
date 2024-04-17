from threading import Timer

import threading
import time
import signal

from rqlite_manager import RqliteManager

def setup_signal_handler(daemon):
    def signal_handler(signum, frame):
        print("Signal received, stopping daemon.")
        daemon.stop()
        daemon.wait()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

class Daemon:
    def __init__(self, interval, function, iterations=-1, *args, **kwargs):
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.iterations = iterations

    def start(self):
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()
        setup_signal_handler(self)

    def run(self):
        c = 0
        while not self.stop_event.is_set() and (self.iterations == -1 or c < self.iterations):
            self.function(*self.args, **self.kwargs)
            time.sleep(self.interval)
            c += 1

    def stop(self):
        self.stop_event.set()

    def wait(self):
        """
        Wait for the thread to finish.
        """
        self.thread.join()

class RqliteSequentialCaller:

    def __init__(self, queries, manager : RqliteManager, interval=0):
        self.queries = queries
        self.client = manager.client
        self.interval = interval  # Time in seconds between each query execution
        self._stop_event = threading.Event()

    def _run(self):
        queries_queue = self.queries.copy()
        self.client.connect()
        total_duration = 0
        while not self._stop_event.is_set() and len(queries_queue) > 1:
            try:
                query = queries_queue.pop(0)
                start = time.time()
                result = self.client.execute_query(query)
                duration = time.time() - start
                total_duration += duration
                print(f'finished queries, runtime=[{round(duration, 4)}s]')
                time.sleep(self.interval)
            except Exception as ex:
                print(f"Error executing query: {ex}, ignoring it...")
                time.sleep(1)
                self.client.connect()
        print(f'\nQUERIES TOTAL RUNTIME=[{round(total_duration, 4)}s]\n\n')

    def start(self):
        print("starting rqlite sequential caller")
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def wait(self):
        self._thread.join()

    def stop(self):
        self._stop_event.set()
        self._thread.join()