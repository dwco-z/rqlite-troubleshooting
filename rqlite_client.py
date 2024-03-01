import requests
import time
from pyrqlite.dbapi2 import connect

class RqliteClient:
    def __init__(self, host='localhost', port=4001):
        self.host = host
        self.port = port
        self.base_url = f'http://{host}:{port}'

    def connect(self):
        self.connection = connect(host=self.host, port=self.port)
        return self.connection

    def execute_query(self, query):
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def backup(self):
        backup_url = f'{self.base_url}/db/backup'
        response = requests.get(backup_url)
        if response.status_code == 200:
            with open('backup.sqlite', 'wb') as f:
                f.write(response.content)
            return 'Backup successful'
        else:
            return 'Backup failed'

    def restore(self, backup_file_path):
        print('starting restore...')
        restore_url = f'{self.base_url}/db/load'
        headers = {'Content-type': 'application/octet-stream'}
        with open(backup_file_path, 'rb') as f:
            response = requests.post(restore_url, headers=headers, data=f)
        if response.status_code == 200:
            return 'Restore successful'
        else:
            return 'Restore failed'

    def is_leader_ready(self, timeout=60):
        print('starting to check if leader is ready...')
        start_time = time.time()
        ready_url = f'{self.base_url}/readyz?sync&timeout=5s'
        while time.time() - start_time < timeout:
            try:
                response = requests.get(ready_url)
                if response.status_code == 200 and 'leader ok' in response.text and 'sync ok' in response.text:
                    print('leader is ready!')
                    return True
                else:
                    print('leader still not ready...')
                time.sleep(0.5)  # Wait for 1 second before retrying
            except requests.RequestException as ex:
                print(ex)
                time.sleep(0.5)  # Wait and retry in case of a request exception

        return False
