#!/usr/bin/env python3
"""Write-Ahead Log for crash recovery."""
import json, os, tempfile, time

class WAL:
    def __init__(self, path: str):
        self.path = path
        self._f = open(path, "a")
        self._seq = 0

    def append(self, op: str, key: str, value=None) -> int:
        self._seq += 1
        entry = {"seq": self._seq, "op": op, "key": key, "ts": time.time()}
        if value is not None:
            entry["value"] = value
        self._f.write(json.dumps(entry) + "\n")
        self._f.flush()
        os.fsync(self._f.fileno())
        return self._seq

    def replay(self) -> list:
        entries = []
        with open(self.path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    entries.append(json.loads(line))
        return entries

    def truncate(self, after_seq: int = 0):
        entries = [e for e in self.replay() if e["seq"] > after_seq]
        self._f.close()
        with open(self.path, "w") as f:
            for e in entries:
                f.write(json.dumps(e) + "\n")
        self._f = open(self.path, "a")

    def close(self):
        self._f.close()

    @staticmethod
    def recover(path: str) -> dict:
        state = {}
        if not os.path.exists(path):
            return state
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                entry = json.loads(line)
                if entry["op"] == "put":
                    state[entry["key"]] = entry.get("value")
                elif entry["op"] == "delete":
                    state.pop(entry["key"], None)
        return state

def test():
    with tempfile.NamedTemporaryFile(suffix=".wal", delete=False) as tmp:
        path = tmp.name
    try:
        wal = WAL(path)
        wal.append("put", "a", 1)
        wal.append("put", "b", 2)
        wal.append("delete", "a")
        wal.append("put", "c", 3)
        wal.close()
        state = WAL.recover(path)
        assert state == {"b": 2, "c": 3}
        # Replay
        wal2 = WAL(path)
        entries = wal2.replay()
        assert len(entries) == 4
        assert entries[0]["op"] == "put"
        # Truncate
        wal2.truncate(after_seq=2)
        entries2 = wal2.replay()
        assert len(entries2) == 2
        wal2.close()
    finally:
        os.unlink(path)
    print("  wal_log: ALL TESTS PASSED")

if __name__ == "__main__":
    test()
