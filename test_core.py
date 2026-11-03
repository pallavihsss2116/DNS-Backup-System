"""Core scenario tests on localhost (no Mininet): resume, checksum NACK, total outage + recovery.

    python3 tests/test_core.py
"""
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, os.path.join(ROOT, "backup"))
from common import request, sha256_hex  # noqa: E402

PY = sys.executable
SERVER = os.path.join(ROOT, "backup", "server.py")
CLIENT = os.path.join(ROOT, "backup", "client.py")
BASE = "/tmp/sdnbackup/coretest"
SPEC = "h3=127.0.0.1:19103,h2=127.0.0.1:19102"
os.environ["SDNB_EVENTS"] = "/tmp/sdnbackup/coretest_events.jsonl"
os.environ["SDNB_META"] = "/tmp/sdnbackup/coretest_meta"


def sha(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


def server(name, port, peer_port, peer_name, delay=0.05, resync=False):
    a = [PY, SERVER, "--name", name, "--port", str(port), "--bind", "127.0.0.1",
         "--data", os.path.join(BASE, name), "--peer", "127.0.0.1:%d" % peer_port,
         "--peer-name", peer_name, "--delay", str(delay)]
    if not resync:
        a.append("--no-resync")
    return subprocess.Popen(a, stdout=subprocess.DEVNULL)


def start_pair(delay=0.05):
    h3 = server("h3", 19103, 19102, "h2", delay)
    h2 = server("h2", 19102, 19103, "h3", delay)
    time.sleep(0.8)
    return h3, h2


def backup_cmd(src, workers=4):
    return [PY, CLIENT, "backup", src, "--servers", SPEC, "--chunk-size", str(128 * 1024), "--workers", str(workers)]


def make_file(mb):
    p = os.path.join(BASE, "src.bin")
    with open(p, "wb") as f:
        f.write(os.urandom(int(mb * 1024 * 1024)))
    return p


def fresh():
    os.system("rm -rf %s %s %s" % (BASE, os.environ["SDNB_META"], os.environ["SDNB_EVENTS"]))
    os.makedirs(BASE)


def restore_ok(fid, src):
    out = os.path.join(BASE, "restored.bin")
    r = subprocess.run([PY, CLIENT, "restore", fid, out, "--servers", SPEC], capture_output=True, text=True)
    return r.returncode == 0 and sha(out) == sha(src)


def test_resume():
    """Client killed mid-backup; a second run must skip chunks already stored and finish."""
    fresh()
    h3, h2 = start_pair(delay=0.1)
    src = make_file(8)                                  # 64 chunks
    c = subprocess.Popen(backup_cmd(src, workers=2), stdout=subprocess.DEVNULL)
    time.sleep(1.5)
    c.send_signal(signal.SIGKILL)
    c.wait()
    r = subprocess.run(backup_cmd(src), capture_output=True, text=True)
    m = re.search(r"(\d+) resumed", r.stdout)
    fid = re.search(r"file_id=(\w+)", r.stdout).group(1)
    resumed = int(m.group(1))
    assert r.returncode == 0, r.stdout
    assert resumed > 0, "second run should resume, got: " + r.stdout
    assert "Resuming an interrupted transfer" in r.stdout, "resume must be announced"
    assert "TRANSFER COMPLETE" in r.stdout and "Checksum Verified" in r.stdout, r.stdout
    assert "Time Taken" in r.stdout and "Throughput" in r.stdout and "MB/s" in r.stdout, r.stdout
    assert restore_ok(fid, src), "restore after resume must match"
    h3.kill(); h2.kill()
    print("resume OK (%d chunks skipped on second run)" % resumed)


def test_checksum_nack():
    fresh()
    h3, h2 = start_pair(delay=0)
    data = b"x" * 1000
    good = sha256_hex(data)
    r, _ = request(("127.0.0.1", 19103), {"op": "PUT", "file_id": "abc123", "idx": 0, "size": 1000,
                                           "sha": "0" * 64, "replicate": True}, data)
    assert r["ok"] is False and r["err"] == "checksum", r
    r, _ = request(("127.0.0.1", 19103), {"op": "LIST", "file_id": "abc123"})
    assert r["chunks"] == {}, "corrupt chunk must not be stored"
    r, _ = request(("127.0.0.1", 19103), {"op": "PUT", "file_id": "abc123", "idx": 0, "size": 1000,
                                           "sha": good, "replicate": True}, data)
    assert r["ok"] and r["replicated"] and r["peer"] == "h2", r
    r, _ = request(("127.0.0.1", 19102), {"op": "LIST", "file_id": "abc123"})
    assert r["chunks"] == {"0": good}, "standby must hold the replica"
    r, _ = request(("127.0.0.1", 19103), {"op": "PUT", "file_id": "../evil", "idx": 0, "size": 1000,
                                           "sha": good}, data)
    assert r["ok"] is False, "path traversal must be rejected"
    h3.kill(); h2.kill()
    print("checksum NACK / replication / path-traversal OK")


def test_total_outage_recovery():
    """Both servers die mid-backup and come back 3 s later; backup must still complete intact."""
    fresh()
    h3, h2 = start_pair(delay=0.1)
    src = make_file(6)
    c = subprocess.Popen(backup_cmd(src), stdout=subprocess.PIPE, text=True)
    time.sleep(1.2)
    h3.kill(); h2.kill()
    time.sleep(3)
    h3 = server("h3", 19103, 19102, "h2", 0.1)
    h2 = server("h2", 19102, 19103, "h3", 0.1)
    out, _ = c.communicate(timeout=120)
    fid = re.search(r"file_id=(\w+)", out).group(1)
    assert c.returncode == 0, out
    assert restore_ok(fid, src), "restore after total outage must match"
    h3.kill(); h2.kill()
    print("total outage + recovery OK")


def test_sdn_socket_failover():
    """Controller (fake, over the real UNIX-socket protocol) reports 'no path to h3' while h3 is still
    alive. The client must fail over to h2 because of the controller's answer, finish the backup, and the
    restored file must match. This is the socket <-> SDN integration."""
    import socket as sk
    import threading
    fresh()
    sock_path = os.path.join(BASE, "sdn.sock")
    os.environ["SDNB_SOCK"] = sock_path
    state = {"h3_ok": True}
    srv = sk.socket(sk.AF_UNIX, sk.SOCK_STREAM)
    srv.bind(sock_path)
    srv.listen(5)

    def serve():
        while True:
            try:
                c, _ = srv.accept()
            except OSError:
                return
            buf = b""
            while not buf.endswith(b"\n"):
                buf += c.recv(4096)
            req = json.loads(buf)
            paths = {}
            for d in req["dsts"]:
                if d == "h3" and not state["h3_ok"]:
                    paths[d] = {"reachable": False, "reason": "no path"}
                else:
                    paths[d] = {"reachable": True, "path": [1, 2, 4] if d == "h3" else [1, 3],
                                "hops": 2 if d == "h3" else 1, "max_util": 0.0}
            c.sendall((json.dumps({"ok": True, "paths": paths}) + "\n").encode())
            c.close()

    threading.Thread(target=serve, daemon=True).start()
    h3, h2 = start_pair(delay=0.1)
    src = make_file(6)
    c = subprocess.Popen(backup_cmd(src), stdout=subprocess.PIPE, text=True)
    time.sleep(1.5)
    state["h3_ok"] = False                      # controller now says: no path to h3 (h3 process still running!)
    out, _ = c.communicate(timeout=120)
    fid = re.search(r"file_id=(\w+)", out).group(1)
    assert c.returncode == 0, out
    assert "1 failovers" in out or "failovers" in out and " 0 failovers" not in out, out
    events = [json.loads(l) for l in open(os.environ["SDNB_EVENTS"])]
    types = [e["type"] for e in events]
    assert "sdn_path" in types, "client should log the path the controller reported"
    assert "sdn_unreachable" in types, "client must react to the controller's 'no path'"
    downs = [e for e in events if e["type"] == "server_state" and e["server"] == "h3" and e["state"] == "down"]
    assert downs and downs[0]["reason"] == "sdn: no path", downs
    assert not any(e["type"] == "failback" for e in events), "must not flap back while controller says no path"
    assert restore_ok(fid, src), "restore must match"
    srv.close()
    h3.kill(); h2.kill()
    del os.environ["SDNB_SOCK"]
    print("socket <-> SDN: controller said 'no path to h3' -> client failed over to h2, no flapping, hash OK")


def _events():
    return [json.loads(l) for l in open(os.environ["SDNB_EVENTS"])]


def test_h3_crashes_by_itself():
    """Nobody kills h3: it is armed to crash on its own after 10 chunks. Client must switch to h2 alone."""
    fresh()
    os.environ["SDNB_CRASH_DIR"] = BASE
    h3, h2 = start_pair(delay=0.05)
    open(os.path.join(BASE, "crash_h3"), "w").write("10")
    src = make_file(6)
    out = subprocess.run(backup_cmd(src), capture_output=True, text=True, timeout=120).stdout
    fid = re.search(r"file_id=(\w+)", out).group(1)
    assert h3.wait(timeout=5) == 1, "h3 should have crashed by itself"
    ev = _events()
    assert any(e["type"] == "server_state" and e["server"] == "h3" and e["reason"] == "crashed by itself" for e in ev)
    assert any(e["type"] == "failover" and e["dst"] == "h2" for e in ev), "client must fail over automatically"
    assert "0 failed" in out, out
    assert restore_ok(fid, src), "restore must match"
    h2.kill()
    del os.environ["SDNB_CRASH_DIR"]
    ft = [e["seconds"] for e in ev if e["type"] == "failover_done"]
    print("h3 crashed by itself -> client moved to h2 on its own (failover %.2fs), hash OK" % ft[0])


def test_h3_freezes():
    """h3 hangs (process alive, TCP accepts, never answers): the hardest failure to notice."""
    fresh()
    h3, h2 = start_pair(delay=0.1)
    src = make_file(6)
    c = subprocess.Popen(backup_cmd(src), stdout=subprocess.PIPE, text=True)
    time.sleep(1.2)
    h3.send_signal(signal.SIGSTOP)
    t0 = time.time()
    out, _ = c.communicate(timeout=120)
    took = time.time() - t0
    fid = re.search(r"file_id=(\w+)", out).group(1)
    ev = _events()
    down = [e for e in ev if e["type"] == "server_state" and e["server"] == "h3" and e["state"] == "down"]
    assert down and down[0]["reason"] == "heartbeat", down
    ft = [e["seconds"] for e in ev if e["type"] == "failover_done"]
    assert ft and ft[0] < 4, "failover from a frozen server must be quick, got %s" % ft
    assert "0 failed" in out, out
    assert restore_ok(fid, src), "restore must match"
    h3.send_signal(signal.SIGCONT); h3.kill(); h2.kill()
    print("h3 froze -> heartbeat lost -> in-flight chunks aborted -> h2 took over in %.2fs, hash OK" % ft[0])


if __name__ == "__main__":
    for t in (test_checksum_nack, test_resume, test_total_outage_recovery, test_sdn_socket_failover,
              test_h3_crashes_by_itself, test_h3_freezes):
        t()
    print("\nALL CORE TESTS PASSED")
