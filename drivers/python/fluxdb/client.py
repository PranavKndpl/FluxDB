import socket
import json
import time
from typing import Optional, List, Dict, Any, Union, Callable

class FluxDB:
    """
    Official Python Driver for FluxDB.
    Supports: CRUD, Auth, Multi-Tenancy, Pub/Sub, TTL, and Adaptive Indexing.
    """

    def __init__(self, host: str = '127.0.0.1', port: int = 8080, password: Optional[str] = None):
        self.host = host
        self.port = port
        self.password = password
        self.sock: Optional[socket.socket] = None
        self.connect()

    def connect(self):
        """Establishes a connection to the FluxDB server."""
        try:
            if self.sock:
                self.sock.close()
            
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(5.0) # 5s timeout for connection
            self.sock.connect((self.host, self.port))
            print(f"✅ Connected to FluxDB at {self.host}:{self.port}")
            
            # Auto-Authenticate if password is provided
            if self.password:
                if not self.auth(self.password):
                    print("❌ Authentication Failed! Check your password.")
                    self.sock.close()
                    self.sock = None
        except ConnectionRefusedError:
            print(f"❌ Could not connect to {self.host}:{self.port}. Is the server running?")
            self.sock = None
        except Exception as e:
            print(f"⚠️ Connection Error: {e}")
            self.sock = None

    def _send_command(self, cmd: str) -> str:
        """Sends a raw command and handles buffering for large responses."""
        if not self.sock:
            raise Exception("Not connected to database")
        
        try:
            self.sock.sendall((cmd + "\n").encode('utf-8'))
            
            # Buffering Loop: Read until server stops sending
            # (FluxDB protocol doesn't send length headers yet, so we read until timeout or empty)
            self.sock.settimeout(0.5) 
            response = b""
            try:
                while True:
                    chunk = self.sock.recv(4096)
                    if not chunk: break
                    response += chunk
                    if len(chunk) < 4096: break 
            except socket.timeout:
                pass # Expected end of message
            
            return response.decode('utf-8').strip()

        except Exception as e:
            print(f"⚠️ Socket Error: {e}")
            print("🔄 Attempting to reconnect...")
            self.connect()
            # Retry once
            if self.sock:
                 return self._send_command(cmd)
            return "ERROR CONNECTION_LOST"

    # --- 🔐 SECURITY ---

    def auth(self, password: str) -> bool:
        """Authenticates the session."""
        resp = self._send_command(f"AUTH {password}")
        return resp == "OK AUTHENTICATED"

    # --- 🏛️ MULTI-TENANCY ---

    def use(self, db_name: str) -> bool:
        """Switches to a different database instance (Lazily created)."""
        resp = self._send_command(f"USE {db_name}")
        return resp.startswith("OK SWITCHED_TO")

    def show_dbs(self) -> List[str]:
        """Returns a list of all active databases."""
        resp = self._send_command("SHOW DBS")
        if resp.startswith("OK ["):
            try:
                return json.loads(resp[3:])
            except:
                pass
        return []

    def drop_database(self, db_name: str) -> bool:
        """Permanently deletes a database and its files."""
        resp = self._send_command(f"DROP DATABASE {db_name}")
        return resp.startswith("OK DROPPED")

    # --- 📝 CRUD OPERATIONS ---

    def insert(self, document: Dict[str, Any]) -> Optional[int]:
        """Inserts a Python dict as a JSON document. Returns the new ID."""
        json_str = json.dumps(document)
        resp = self._send_command(f"INSERT {json_str}")
        
        if resp.startswith("OK ID="):
            return int(resp.split("=")[1])
        print(f"Insert Failed: {resp}")
        return None

    def get(self, query: Union[int, str] = "") -> Union[Dict, List[Dict], None]:
        """
        Get by ID, Range, or Dump all.
        Usage:
          db.get(1)       -> Single Dict
          db.get("1-10")  -> List of Dicts
          db.get()        -> List of Dicts (All data)
        """
        cmd = f"GET {query}".strip()
        resp = self._send_command(cmd)
        
        # Single Document
        if resp.startswith("OK {"):
            return json.loads(resp[3:])
        
        # List of Documents (Range or Dump)
        if resp.startswith("OK COUNT="):
            return self._parse_multi_line_response(resp)
            
        return None

    def find(self, query: Dict[str, Any]) -> List[Dict]:
        """
        Search with Smart Logic ($gt, $lt, $ne).
        Example: db.find({"age": {"$gt": 18}})
        """
        json_str = json.dumps(query)
        resp = self._send_command(f"FIND {json_str}")
        
        if resp.startswith("OK COUNT="):
            return self._parse_multi_line_response(resp)
            
        return []

    def update(self, doc_id: int, document: Dict[str, Any]) -> bool:
        """Updates a document by ID."""
        json_str = json.dumps(document)
        resp = self._send_command(f"UPDATE {doc_id} {json_str}")
        return resp == "OK UPDATED"

    def delete(self, doc_id: int) -> bool:
        """Deletes a document by ID."""
        resp = self._send_command(f"DELETE {doc_id}")
        return resp == "OK DELETED"

    # --- ⚡ REAL-TIME & UTILITIES ---

    def expire(self, doc_id: int, seconds: int) -> bool:
        """Sets a Time-To-Live (TTL) for a document."""
        resp = self._send_command(f"EXPIRE {doc_id} {seconds}")
        return resp == "OK TTL_SET"

    def checkpoint(self) -> bool:
        """Forces a save to disk."""
        resp = self._send_command("CHECKPOINT")
        return resp == "OK CHECKPOINT_COMPLETE"

    def stats(self) -> Dict[str, Any]:
        """Returns database statistics (count, fields, adaptive status)."""
        resp = self._send_command("STATS")
        if resp.startswith("OK {"):
            return json.loads(resp[3:])
        return {}

    def toggle_adaptive(self, enable: bool) -> bool:
        """Turns Adaptive Indexing ON/OFF."""
        val = 1 if enable else 0
        resp = self._send_command(f"CONFIG ADAPTIVE {val}")
        return "ADAPTIVE=ON" in resp

    def toggle_pubsub(self, enable: bool) -> bool:
        """Turns Pub/Sub Module ON/OFF."""
        val = 1 if enable else 0
        resp = self._send_command(f"CONFIG PUBSUB {val}")
        return "PUBSUB=ON" in resp

    # --- 📡 PUB/SUB ---

    def publish(self, channel: str, message: str) -> int:
        """Sends a message to a channel. Returns count of receivers."""
        resp = self._send_command(f"PUBLISH {channel} {message}")
        if resp.startswith("OK RECEIVERS="):
            return int(resp.split("=")[1])
        return 0

    def subscribe(self, channel: str, callback: Callable[[str], None]):
        """
        BLOCKING: Listens for messages on a channel indefinitely.
        Calls 'callback(message_string)' when data arrives.
        Press Ctrl+C to stop.
        """
        if not self.sock: raise Exception("Not connected")
        
        cmd = f"SUBSCRIBE {channel}\n"
        self.sock.sendall(cmd.encode('utf-8'))
        print(f"🎧 Listening to '{channel}'... (Ctrl+C to stop)")
        
        try:
            buffer = ""
            while True:
                chunk = self.sock.recv(4096).decode('utf-8', errors='ignore')
                if not chunk: break
                
                buffer += chunk
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    if line.startswith("MESSAGE"):
                        # Format: MESSAGE <channel> <content>
                        parts = line.split(" ", 2)
                        if len(parts) == 3:
                            callback(parts[2])
                    elif line.startswith("OK SUBSCRIBED"):
                        continue
        except KeyboardInterrupt:
            print("\n🛑 Stopped Listening.")
        except Exception as e:
            print(f"⚠️ Subscription Error: {e}")

    def close(self):
        """Closes the connection."""
        if self.sock:
            self.sock.close()
            self.sock = None

    # --- INTERNAL HELPERS ---

    def _parse_multi_line_response(self, resp: str) -> List[Dict]:
        """Parses the 'ID <id> <json>' format into a list of dicts."""
        results = []
        lines = resp.split('\n')
        # Skip the first line (OK COUNT=N)
        for line in lines[1:]:
            if line.startswith("ID "):
                try:
                    # Line format: "ID 123 {"name":...}"
                    # Split into max 3 parts: ["ID", "123", "json_string"]
                    parts = line.split(" ", 2)
                    if len(parts) == 3:
                        doc = json.loads(parts[2])
                        doc["_id"] = int(parts[1]) # Inject ID for convenience
                        results.append(doc)
                except:
                    continue
        return results