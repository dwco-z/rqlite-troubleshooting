import subprocess
import os
import json

import subprocess
import threading
from rqlite_client import RqliteClient  # Replace with the actual module name

class RqliteManager:
    def __init__(self, data_path="DummyDatabase", host='localhost', http_port=4001, raft_port=4004, log_file="rqlited.log"):
        self.data_path = data_path
        self.log_file = log_file
        self.host = host
        self.http_port = http_port
        self.raft_port = raft_port
        self.process = None
        self.client = None
    
    def set_peers(self):
        peers = [
            {
                "id": f"{self.host}:{self.raft_port}",
                "address": f"{self.host}:{self.raft_port}",
                "non_voter": False
            }
        ]
        with open(os.path.join(self.data_path, "raft", 'peers.json'), 'w') as fp:
            json.dump(peers, fp)

    def start_rqlited(self):
        print('starting rqlited...')
        cmd = [
            "rqlited.exe",  # Path to the rqlited executable
            f"-http-addr={self.host}:{self.http_port}",
            f"-raft-addr={self.host}:{self.raft_port}",
            self.data_path  # Data directory for rqlite
        ]
        self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        # Start a thread to asynchronously log stdout to a file
        self.log_thread = threading.Thread(target=self._log_stdout, daemon=True)
        self.log_thread.start()

        # Instantiate RqliteClient and check if leader is ready
        self.client = RqliteClient(host=self.host, port=self.http_port)
        ready = self.client.is_leader_ready()
        if not ready:
            raise Exception('something is wrong, leader is not ready...')

    def stop_rqlited(self):
        print('stopping rqlited...')
        if self.process:
            self.process.terminate()
            print('stopped!')
            self.process = None

    def is_running(self):
        return self.process is not None and self.process.poll() is None
    
    def _log_stdout(self):
        with open(self.log_file, "a") as file:
            while True:
                try:
                    output = self.process.stdout.readline()
                    if output:
                        file.write(output)
                        file.flush()
                    if not self.is_running() or output == '':
                        break
                except:
                    break


