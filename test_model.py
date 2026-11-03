"""Replay an event file through the dashboard model (no Tk needed).

    SDNB_EVENTS=/tmp/sdnbackup/test_events.jsonl python3 local_demo.py
    python3 tests/test_model.py /tmp/sdnbackup/test_events.jsonl
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboard"))
from model import Model  # noqa: E402

path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sdnbackup/test_events.jsonl"
m = Model()
logs = []
for line in open(path):
    dirty, log = m.apply(json.loads(line))
    if log:
        logs.append(log)

print("\n".join("%s %-10s %s" % l for l in logs[:25]))
print()
print(m.status_text())
c = m.counts()
print("counts:", {k: v for k, v in c.items() if v})

assert m.total > 0, "no backup_start seen"
done = c["stored"] + c["replicated"] + c["verified"]
assert done == m.total, "all chunks should end up stored/verified, got %s" % c
assert c["failed"] == 0 and c["retry"] == 0 and c["sending"] == 0 and c["pending"] == 0
assert m.failover_time is not None, "failover_done event missing"
assert m.servers["h3"]["state"] in ("up", "down")
assert any("FAILOVER" in l[2] for l in logs), "failover not logged"
print("\nMODEL TEST PASSED")
