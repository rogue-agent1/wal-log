#!/usr/bin/env python3
"""Write-Ahead Log (WAL) for crash recovery. Zero dependencies."""
import json, os, sys, time, struct

class WAL:
    def __init__(self, path="wal.log"):
        self.path = path
        self.entries = []
        if os.path.exists(path):
            self._recover()

    def append(self, operation, key, value=None):
        entry = {"seq": len(self.entries), "ts": time.time(),
                 "op": operation, "key": key, "value": value}
        self.entries.append(entry)
        with open(self.path, "a") as f:
            f.write(json.dumps(entry) + "\n")
        return entry["seq"]

    def _recover(self):
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        self.entries.append(json.loads(line))
                    except json.JSONDecodeError:
                        break  # corrupted entry, stop

    def replay(self, store=None):
        if store is None: store = {}
        for entry in self.entries:
            if entry["op"] == "set":
                store[entry["key"]] = entry["value"]
            elif entry["op"] == "delete":
                store.pop(entry["key"], None)
        return store

    def checkpoint(self, state):
        cp_path = self.path + ".checkpoint"
        with open(cp_path, "w") as f:
            json.dump({"state": state, "seq": len(self.entries)}, f)
        # Truncate WAL
        with open(self.path, "w") as f:
            pass
        self.entries.clear()

    def __len__(self):
        return len(self.entries)

class WALStore:
    def __init__(self, path="wal_store"):
        os.makedirs(path, exist_ok=True)
        self.wal = WAL(os.path.join(path, "wal.log"))
        self.cp_path = os.path.join(path, "wal.log.checkpoint")
        self.data = {}
        if os.path.exists(self.cp_path):
            with open(self.cp_path) as f:
                self.data = json.load(f).get("state", {})
        self.data = self.wal.replay(self.data)

    def get(self, key, default=None):
        return self.data.get(key, default)

    def set(self, key, value):
        self.wal.append("set", key, value)
        self.data[key] = value

    def delete(self, key):
        self.wal.append("delete", key)
        self.data.pop(key, None)

    def checkpoint(self):
        self.wal.checkpoint(self.data)

if __name__ == "__main__":
    import tempfile
    path = tempfile.mkdtemp()
    store = WALStore(path)
    store.set("name", "Alice")
    store.set("age", "30")
    print(f"name: {store.get('name')}")
    store.checkpoint()
    store.set("city", "NYC")
    # Simulate recovery
    store2 = WALStore(path)
    print(f"Recovered name: {store2.get('name')}")
    print(f"Recovered city: {store2.get('city')}")
