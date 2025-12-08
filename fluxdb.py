import socket
import json

class FluxDB:
    def __init__(self, host='127.0.0.1', port=8080):
        self.host = host
        self.port = port
        self.sock = None
        self.connect()

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect((self.host, self.port))
            print(f"✅ Connected to FluxDB at {self.host}:{self.port}")
        except ConnectionRefusedError:
            print(f"❌ Could not connect to {self.host}:{self.port}. Is the server running?")
            self.sock = None

    def _send_command(self, cmd):
        if not self.sock:
            raise Exception("Not connected to database")
        
        try:
            self.sock.sendall((cmd + "\n").encode('utf-8'))
            response = b""
            try:
                while True:
                    chunk = self.sock.recv(4096)
                    if not chunk: break
                    response += chunk
                    if len(chunk) < 4096: break 
            except socket.timeout:
                pass 
            
            return response.decode('utf-8').strip()

        except Exception as e:
            print(f"⚠️ Socket Error: {e}")
            self.connect()
            return "ERROR CONNECTION_LOST"

    def insert(self, document):

        json_str = json.dumps(document)
        cmd = f"INSERT {json_str}"
        
        resp = self._send_command(cmd)
        
        if resp.startswith("OK ID="):
            return int(resp.split("=")[1])
        
        print(f"Insert Failed: {resp}")
        return None

    def find(self, query):

        json_str = json.dumps(query)
        cmd = f"FIND {json_str}"
        
        resp = self._send_command(cmd)
        
        lines = resp.split('\n')
        header = lines[0]
        
        if header.startswith("OK COUNT="):
            count = int(header.split("=")[1])
            ids = []

            for line in lines[1:]:
                if line.startswith("ID "):
                    ids.append(int(line.split(" ")[1]))
            return ids
            
        return []

    def update(self, doc_id, document):
        json_str = json.dumps(document)
        cmd = f"UPDATE {doc_id} {json_str}"
        resp = self._send_command(cmd)
        return resp == "OK UPDATED"

    def delete(self, doc_id):
        cmd = f"DELETE {doc_id}"
        resp = self._send_command(cmd)
        return resp == "OK DELETED"

    def create_index(self, field, index_type=0):
        cmd = f"INDEX {field} {index_type}"
        resp = self._send_command(cmd)
        return "OK INDEX_CREATED" in resp
    
    def get(self, query=""):
        cmd = f"GET {query}".strip()
        resp = self._send_command(cmd)
        
        if resp.startswith("OK COUNT="):
            results = []
            lines = resp.split('\n')
            for line in lines[1:]:
                if line.startswith("ID "):
                    parts = line.split(" ", 2) 
                    if len(parts) == 3:
                        obj = json.loads(parts[2])
                        obj["_id"] = int(parts[1]) 
                        results.append(obj)
            return results
            
        elif resp.startswith("OK {"):
            return json.loads(resp[3:])
            
        return None

    def close(self):
        if self.sock:
            self.sock.close()