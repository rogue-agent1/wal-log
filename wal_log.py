#!/usr/bin/env python3
"""Write-Ahead Log (WAL) implementation for crash recovery."""
import sys, json, os, struct, hashlib, time

class WALEntry:
    def __init__(self, txn_id, op, key, value=None, ts=None):
        self.txn_id, self.op, self.key, self.value = txn_id, op, key, value
        self.ts = ts or time.time()
    def serialize(self):
        d = json.dumps({"t": self.txn_id, "o": self.op, "k": self.key, "v": self.value, "ts": self.ts})
        b = d.encode(); ck = hashlib.md5(b).hexdigest()[:8]
        return f"{ck}|{d}\n"
    @staticmethod
    def deserialize(line):
        ck, d = line.strip().split("|", 1)
        if hashlib.md5(d.encode()).hexdigest()[:8] != ck: return None
        o = json.loads(d); return WALEntry(o["t"], o["o"], o["k"], o["v"], o["ts"])

class WAL:
    def __init__(self):
        self.entries, self.committed, self.next_txn = [], set(), 1
    def begin(self):
        txn = self.next_txn; self.next_txn += 1; return txn
    def write(self, txn_id, key, value):
        self.entries.append(WALEntry(txn_id, "W", key, value))
    def delete(self, txn_id, key):
        self.entries.append(WALEntry(txn_id, "D", key))
    def commit(self, txn_id):
        self.entries.append(WALEntry(txn_id, "C", "")); self.committed.add(txn_id)
    def recover(self):
        store = {}
        committed = {e.txn_id for e in self.entries if e.op == "C"}
        for e in self.entries:
            if e.txn_id in committed:
                if e.op == "W": store[e.key] = e.value
                elif e.op == "D": store.pop(e.key, None)
        return store
    def checkpoint(self):
        store = self.recover(); self.entries.clear(); self.committed.clear()
        return store

def main():
    if len(sys.argv) < 2: print("Usage: wal_log.py <demo|test>"); return
    cmd = sys.argv[1]
    if cmd == "demo":
        w = WAL()
        t1 = w.begin(); w.write(t1, "x", 10); w.write(t1, "y", 20); w.commit(t1)
        t2 = w.begin(); w.write(t2, "z", 30)  # uncommitted
        print(f"Recovered (uncommitted t2 lost): {w.recover()}")
    elif cmd == "test":
        w = WAL()
        t1 = w.begin(); w.write(t1, "a", 1); w.write(t1, "b", 2); w.commit(t1)
        t2 = w.begin(); w.write(t2, "c", 3)  # no commit
        r = w.recover(); assert r == {"a": 1, "b": 2}, f"Got {r}"
        t3 = w.begin(); w.write(t3, "a", 99); w.commit(t3)
        r = w.recover(); assert r == {"a": 99, "b": 2}
        t4 = w.begin(); w.delete(t4, "b"); w.commit(t4)
        r = w.recover(); assert r == {"a": 99}
        s = w.checkpoint(); assert s == {"a": 99}; assert len(w.entries) == 0
        e = WALEntry(1, "W", "k", "v"); line = e.serialize()
        e2 = WALEntry.deserialize(line); assert e2.key == "k"
        assert WALEntry.deserialize("bad|{}") is None
        print("All tests passed!")

if __name__ == "__main__": main()
