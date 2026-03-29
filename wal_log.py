#!/usr/bin/env python3
"""wal_log - Write-ahead log for crash recovery."""
import sys, json, os, tempfile

class WAL:
    def __init__(self, path):
        self.path = path
        self.lsn = 0
    def append(self, op, data):
        self.lsn += 1
        entry = {"lsn": self.lsn, "op": op, "data": data}
        with open(self.path, "a") as f:
            f.write(json.dumps(entry) + "\n")
        return self.lsn
    def read(self, from_lsn=0):
        entries = []
        if not os.path.exists(self.path):
            return entries
        with open(self.path) as f:
            for line in f:
                line = line.strip()
                if line:
                    entry = json.loads(line)
                    if entry["lsn"] > from_lsn:
                        entries.append(entry)
        return entries
    def replay(self, state=None):
        if state is None:
            state = {}
        for entry in self.read():
            op = entry["op"]
            data = entry["data"]
            if op == "set":
                state[data["key"]] = data["value"]
            elif op == "delete":
                state.pop(data["key"], None)
        return state
    def truncate(self, up_to_lsn):
        entries = [e for e in self.read() if e["lsn"] > up_to_lsn]
        with open(self.path, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")
    def checkpoint(self, state, checkpoint_path):
        with open(checkpoint_path, "w") as f:
            json.dump({"lsn": self.lsn, "state": state}, f)

def test():
    with tempfile.TemporaryDirectory() as td:
        wal = WAL(os.path.join(td, "wal.log"))
        wal.append("set", {"key": "a", "value": 1})
        wal.append("set", {"key": "b", "value": 2})
        wal.append("delete", {"key": "a"})
        wal.append("set", {"key": "c", "value": 3})
        state = wal.replay()
        assert state == {"b": 2, "c": 3}
        entries = wal.read()
        assert len(entries) == 4
        entries_from_2 = wal.read(from_lsn=2)
        assert len(entries_from_2) == 2
        wal.truncate(2)
        assert len(wal.read()) == 2
        cp = os.path.join(td, "checkpoint.json")
        wal.checkpoint(state, cp)
        with open(cp) as f:
            data = json.load(f)
        assert data["state"] == {"b": 2, "c": 3}
        print("OK: wal_log")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        test()
    else:
        print("Usage: wal_log.py test")
