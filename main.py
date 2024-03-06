
import shutil
from rqlite_manager import RqliteManager
from concurrent.futures import ThreadPoolExecutor

import threading
import time
import os
from rqlite_client import RqliteClient

import signal
import sys

def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    # Implement graceful shutdown logic here
    sys.exit(0)

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

def start_manager(manager, joinAddresses):
    manager.start_rqlited(joinAddresses)

def start_all_instances(manager_instances, joinAddresses=[]):
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(start_manager, manager, joinAddresses) for manager in manager_instances]
        for future in futures:
            future.result()

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    for i in range(100):
        try:
            # clear files
            if os.path.exists('node1.log'):
                os.remove('node1.log')
            if os.path.exists('node2.log'):
                os.remove('node2.log')
            if os.path.exists('node3.log'):
                os.remove('node3.log')
            shutil.rmtree('Node1', ignore_errors=True)
            shutil.rmtree('Node2', ignore_errors=True)
            shutil.rmtree('Node3', ignore_errors=True)

            node1Manager = RqliteManager(data_path="Node1", host='localhost', http_port=4001, raft_port=5001, log_file="node1.log", rqlited_path="rqlited.exe")
            node2Manager = RqliteManager(data_path="Node2", host='localhost', http_port=4002, raft_port=5002, log_file="node2.log", rqlited_path="rqlited.exe")
            node3Manager = RqliteManager(data_path="Node3", host='localhost', http_port=4003, raft_port=5003, log_file="node3.log", rqlited_path="rqlited.exe")
            
            all_instances = (node1Manager, node2Manager, node3Manager)
            joinAddresses = [f"{manager.host}:{manager.raft_port}" for manager in all_instances]

            peers_entries = [manager.get_peers_entry() for manager in all_instances]
            
            # test start-stop-configure_peers-start while monitoring readyz during start
            start_all_instances(all_instances, joinAddresses)
            [manager.stop_rqlited() for manager in all_instances]
            [manager.set_peers(peers_entries) for manager in all_instances]
            start_all_instances(all_instances)
        finally:
            [manager.stop_rqlited() for manager in all_instances]