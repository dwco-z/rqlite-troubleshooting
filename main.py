
import shutil
from rqlite_manager import RqliteManager
from concurrent.futures import ThreadPoolExecutor
from utils import Daemon, RqliteSequentialCaller

import time
import os
import uuid
import signal
import sys


def signal_handler(sig, frame):
    print('You pressed Ctrl+C!')
    # Implement graceful shutdown logic here
    sys.exit(0)

def start_manager(manager, joinAddresses):
    manager.start_rqlited(joinAddresses)

def start_all_instances(manager_instances, joinAddresses=[]):
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(start_manager, manager, joinAddresses) for manager in manager_instances]
        for future in futures:
            future.result()

def send_queries_test(manager: RqliteManager, interval=0):
    total_duration = 0
    for i in range(20):
        table_name = f"foo{uuid.uuid4().hex}"
        start = time.time()
        manager.client.execute_query(f"CREATE TABLE {table_name} ([Id] guid PRIMARY KEY NOT NULL)")
        count = manager.client.execute_query(f"SELECT count(*) FROM sqlite_master WHERE type = 'table' and name='{table_name}'")[0][0]
        if (count != 1):
            raise (f"Table [{table_name}] was not added")
        duration = time.time() - start
        total_duration += duration
        print(f'finished queries, runtime=[{duration}]s')
        time.sleep(interval)
    return total_duration

def clear_files():
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
    
if __name__ == '__main__':
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    nodes_monitor = None
    query_callers = []
    try:
        clear_files()

        node1Manager = RqliteManager(data_path="Node1", host='127.0.0.1', http_port=4001, raft_port=5001, log_file="node1.log", rqlited_path="rqlited_8.22.1.exe", raft_heartbeat_timeout=5, raft_election_timeout=10)
        node2Manager = RqliteManager(data_path="Node2", host='192.168.15.108', http_port=4002, raft_port=5002, log_file="node2.log", rqlited_path="rqlited_8.22.1.exe", raft_heartbeat_timeout=5, raft_election_timeout=10)
        node3Manager = RqliteManager(data_path="Node3", host='192.168.1.7', http_port=4003, raft_port=5003, log_file="node3.log", rqlited_path="rqlited_8.22.1.exe", raft_heartbeat_timeout=5, raft_election_timeout=10)

        all_instances = (node1Manager, node2Manager, node3Manager)
        joinAddresses = [f"{manager.host}:{manager.raft_port}" for manager in all_instances]
        
        print("<< INITIALIZING SYSTEM >>")
        # 1. Start cluster
        start_all_instances(all_instances, joinAddresses)
        # all_instances[0].client.restore("Global.Backup.db")

        # Populate cluster
        [all_instances[0].client.execute_query(f"CREATE TABLE foo{uuid.uuid4().hex} ([Id] guid PRIMARY KEY NOT NULL)") for _ in range(50)]

        print("<< STARTS MONITORING CLUSTER (/nodes) >>")
        # 2. Start monitoring /nodes
        nodes_monitor = Daemon(interval=1, function=node1Manager.get_nodes)
        nodes_monitor.start()

        # 3. Test the database queries
        print("<< TESTING DATABASE PERFORMANCE BEFORE SHUTDOWN >>")
        queries = [ f"SELECT count(*) FROM sqlite_master WHERE type = 'table'" for i in range(20) ]
        query_caller = RqliteSequentialCaller(queries, node1Manager, interval=1)
        query_caller.start()
        query_caller.wait()
        
        # 4. Stop one of the machines 
        print("<< SHUTTING DOWN NODE >>")
        node3Manager.stop_rqlited()
        running_instances = [i for i in all_instances if i != node3Manager]

        # 5. Restart node 
        print("<< RESTARTING REMAINING NODES >>")
        # stop calls to cluster since we are gonna loose quorum while restarting the remaining node
        nodes_monitor.stop()
        node1Manager.restart()
        nodes_monitor.start()

        # 6. Test post-restart performance
        print("<< TESTING DATABASE PERFORMANCE AFTER SHUTDOWN AND RESTARTS >>")
        # execute queries on leader and follower machines
        leader_id = node1Manager.client.get_leader_id()
        leader_manager = next((m for m in running_instances if m.get_id() == leader_id), None)
        follower_manager = next((m for m in running_instances if m.get_id() != leader_id), None)
        print(f"Leader node id={leader_manager.get_id()}")
        print(f"Follower node id={follower_manager.get_id()}")
        
        print("Starting to query leader node")
        query_caller = RqliteSequentialCaller(queries, leader_manager, interval=1)
        query_caller.start()
        query_caller.wait()

    finally:
        if nodes_monitor:
            nodes_monitor.stop()
        [caller.stop for caller in query_callers]
        [manager.stop_rqlited() for manager in all_instances]
        

