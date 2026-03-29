import tempfile
from wal_log import WALStore
p = tempfile.mkdtemp()
s = WALStore(p)
s.set("x", "1"); s.set("y", "2")
assert s.get("x") == "1"
s.checkpoint()
s.set("z", "3")
s2 = WALStore(p)
assert s2.get("x") == "1"
assert s2.get("z") == "3"
print("WAL tests passed")