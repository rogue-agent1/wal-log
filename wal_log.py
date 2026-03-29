#!/usr/bin/env python3
"""Write-Ahead Log (WAL) implementation."""
import struct, hashlib, time, os, tempfile

MAGIC = b"WAL1"

def _checksum(data: bytes) -> bytes:
    return hashlib.md5(data).digest()[:4]

class WAL:
    def __init__(self, path: str):
        self.path = path
        self._fd = open(path, "ab+")
        self._seq = 0

    def append(self, operation: str, key: str, value: str = "") -> int:
        self._seq += 1
        payload = f"{operation}\x00{key}\x00{value}".encode()
        record = struct.pack(">I", self._seq) + struct.pack(">I", len(payload)) + payload
        record += _checksum(record)
        self._fd.write(MAGIC + record)
        self._fd.flush()
        return self._seq

    def replay(self) -> list:
        records = []
        self._fd.seek(0)
        data = self._fd.read()
        pos = 0
        while pos < len(data):
            if data[pos:pos+4] != MAGIC:
                break
            pos += 4
            seq = struct.unpack(">I", data[pos:pos+4])[0]; pos += 4
            plen = struct.unpack(">I", data[pos:pos+4])[0]; pos += 4
            payload = data[pos:pos+plen]; pos += plen
            cksum = data[pos:pos+4]; pos += 4
            # Verify
            record = struct.pack(">I", seq) + struct.pack(">I", plen) + payload
            if _checksum(record) != cksum:
                break  # Corrupted
            parts = payload.decode().split("\x00", 2)
            records.append({"seq": seq, "op": parts[0], "key": parts[1], "value": parts[2] if len(parts) > 2 else ""})
        return records

    def truncate(self):
        self._fd.close()
        self._fd = open(self.path, "wb+")
        self._seq = 0

    def close(self):
        self._fd.close()

if __name__ == "__main__":
    import sys
    with tempfile.NamedTemporaryFile(suffix=".wal", delete=False) as f:
        path = f.name
    wal = WAL(path)
    wal.append("SET", "name", "Alice")
    wal.append("SET", "age", "30")
    wal.append("DEL", "temp")
    print(f"Replayed: {wal.replay()}")
    wal.close()
    os.unlink(path)

def test():
    import tempfile
    path = tempfile.mktemp(suffix=".wal")
    try:
        w = WAL(path)
        w.append("SET", "k1", "v1")
        w.append("SET", "k2", "v2")
        w.append("DEL", "k1")
        records = w.replay()
        assert len(records) == 3
        assert records[0]["op"] == "SET"
        assert records[0]["key"] == "k1"
        assert records[2]["op"] == "DEL"
        w.close()
        # Reopen and replay (persistence)
        w2 = WAL(path)
        records2 = w2.replay()
        assert len(records2) == 3
        w2.truncate()
        assert w2.replay() == []
        w2.close()
    finally:
        if os.path.exists(path): os.unlink(path)
    print("  wal_log: ALL TESTS PASSED")
