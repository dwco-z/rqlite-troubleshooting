import time
from rqlite_client import RqliteClient
import signal
import subprocess
import os
import json
import subprocess
import threading

class RqliteManager:
    def __init__(self, data_path="DummyDatabase", host='localhost', http_port=4001, raft_port=4004, log_file="rqlited.log", rqlited_path="rqlited.exe", raft_heartbeat_timeout=10, raft_election_timeout=20):
        self.data_path = data_path
        self.log_file = log_file
        self.host = host
        self.http_port = http_port
        self.raft_port = raft_port
        self.process = None
        self.client = None
        self.rqlited_path = rqlited_path
        self.raft_heartbeat_timeout = raft_heartbeat_timeout
        self.raft_election_timeout = raft_election_timeout

    def set_peers(self, peers: list):
        with open(os.path.join(self.data_path, "raft", 'peers.json'), 'w') as fp:
            json.dump(peers, fp)

    def get_peers_entry(self):
        return {
            "id": self.get_id(),
            "address": f"{self.host}:{self.raft_port}",
            "non_voter": False
        }

    def get_id(self):
        return f"{self.host}:{self.raft_port}"
    
    def start_rqlited(self, join_addresses=""):
        print(f'starting rqlited [node id={self.get_id()}]...')
        cmd = [
            self.rqlited_path,  # Path to the rqlited executable
            f"-http-addr={self.host}:{self.http_port}",
            f"-raft-addr={self.host}:{self.raft_port}",
            f"-raft-log-level=DEBUG",
            f"-raft-timeout={self.raft_heartbeat_timeout}s",
            f"-raft-election-timeout={self.raft_election_timeout}s",
            f"-cluster-connect-timeout=5s",
            self.data_path  # Data directory for rqlite
        ]
        if join_addresses:
            cmd.insert(-2, f"-bootstrap-expect={len(join_addresses)}")
            cmd.insert(-2, "-join=" + ",".join(join_addresses))
            cmd.insert(-2, f"--join-attempts=50")
        if os.name == 'nt':
            self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        else:
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
        print(f'stopping rqlited instance [node id={self.get_id()}]...')
        if self.process:
            if os.name == 'nt':
                self.process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(30)
            except:
                self.process.kill()
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
    

    def restart(self):
        self.stop_rqlited()
        self.start_rqlited()

    def get_nodes(self, display=False):
        start = time.time()
        nodes = self.client.nodes()
        duration = time.time() - start
        if display:
            print("-- Nodes status --")
            for nodes_response in nodes:
                # remove irrelevant fields for this issue
                nodes_response.pop("api_addr", None)
                nodes_response.pop("addr", None)
                nodes_response.pop("voter", None)
                print(nodes_response)
        print(f"finished /nodes [runtime={round(duration, 4)}]")
        return nodes
