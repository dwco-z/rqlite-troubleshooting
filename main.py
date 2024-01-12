
import shutil
from rqlite_manager import RqliteManager

import threading
import time
import os
from rqlite_client import RqliteClient

class RqliteSequentialCaller:

    def __init__(self, queries, host='localhost', port=4001, interval=0):
        self.queries = queries
        self.client = RqliteClient(host=host, port=port)
        self.interval = interval  # Time in seconds between each query execution
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        self.client.connect()
        while not self._stop_event.is_set():
            try:
                query = self.queries.pop(0)
                result = self.client.execute_query(query)
                time.sleep(self.interval)
            except Exception as ex:
                # print(f"Error executing query: {e}, ignoring it...")
                time.sleep(1)
                self.client.connect()

    def start(self):
        print("starting rqlite sequential caller")
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join()


if __name__ == '__main__':
    # clear files
    if os.path.exists('rqlited.log'):
        os.remove('rqlited.log')
    shutil.rmtree('DummyDatabase', ignore_errors=True)

    manager = RqliteManager()

    # 1. Initialize cluster using peers.json
    # peers.json cannot be set rightaway in a brand new cluster otherwise it will fail in further startups (https://github.com/rqlite/rqlite/issues/1293#issuecomment-1664140683) 
    # therefore we need to start the cluster for the first time before setting the peers and then we can proceed 
    manager.start_rqlited()
    manager.stop_rqlited()
    manager.set_peers() 
    manager.start_rqlited()

    # 2. Restore database
    restore_result = manager.client.restore('dummy.db')
    print(restore_result)

    # 3. Send queries
    queries = [ f"CREATE TABLE bar{i} ([Id] guid PRIMARY KEY NOT NULL)" for i in range(1000) ]
    caller = RqliteSequentialCaller(queries, interval=0)
    caller.start()

    # 4. Wait for rqlite failure and stop
    time.sleep(10)
    manager.stop_rqlited()

