#!/usr/bin/env python3
"""Backup client: chunking, parallel upload, automatic failover, resume, restore.

Servers are given in priority order: the first is the primary (h3), the next
is the standby (h2). The client always writes to the first server that is not
marked DOWN. A server is marked DOWN by either
  * a failed request (connection refused / reset / timeout), or
  * (optional, off by default) the heartbeat monitor missing --hb-miss pings in a row.
    A frozen server is then detected by the --put-timeout of the in-flight upload.
Chunks that were in flight when the server died are retried on the standby.
Because the primary replicates every chunk to the standby before ACKing,
already-ACKed chunks are never lost.
"""
import argparse
import json
import math
import os
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (DEFAULT_CHUNK, META_DIR, append_result, emit, file_sha256,  # noqa: E402
                    parse_servers, recv_msg, request, send_msg, sha256_hex)

MAX_ATTEMPTS = 25
SDN_SOCK = os.environ.get("SDNB_SOCK", "/tmp/sdnbackup/sdn.sock")


def print_box(title, rows):
    """The result box shown in the Mininet terminal (and the dashboard shows the same facts)."""
    line = "=" * 66
    print("\n" + line + "\n" + title.center(66) + "\n" + line)
    for k, v in rows:
        print(" %-12s %s" % (k + ":", v))
    print(line, flush=True)


class SdnLink:
    """Socket link to the POX controller. Everything here is optional: if the controller is not
    running (e.g. local_demo.py) ask() returns None and the client works from its own health checks."""

    def __init__(self, path, my_host):
        self.path, self.host = path, my_host

    def ask(self, req, timeout=0.5):
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as c:
                c.settimeout(timeout)
                c.connect(self.path)
                c.sendall((json.dumps(req) + "\n").encode())
                buf = b""
                while not buf.endswith(b"\n"):
                    part = c.recv(65536)
                    if not part:
                        break
                    buf += part
            resp = json.loads(buf.decode())
            return resp if resp.get("ok") else None
        except (OSError, ValueError):
            return None

    def reach(self, names):
        r = self.ask({"op": "reach", "src": self.host, "dsts": names})
        return r["paths"] if r else None


class Server:
    def __init__(self, name, addr):
        self.name, self.addr = name, addr


class Backup:
    def __init__(self, servers, workers=4, sticky=False, hb_interval=0.0, hb_miss=3, sdn=None,
                 put_timeout=6.0):
        self.servers = [Server(n, a) for n, a in servers]
        self.workers = workers
        self.sticky = sticky            # keep promoted standby even if primary returns
        self.hb_interval = hb_interval
        self.hb_miss = hb_miss
        self.sdn = sdn
        self.put_timeout = put_timeout   # a frozen server is detected by this timeout (no heartbeat)
        self.last_path = {}
        self.sdn_blocked = set()   # servers the controller says have no path
        self.inflight = {}         # server name -> sockets of requests in progress
        self.down = set()
        self.miss = {s.name: 0 for s in self.servers}
        self.active = 0
        self.fail_t0 = None
        self.stats = {"resent": 0, "failovers": 0, "failover_time": None, "skipped": 0, "sent": 0}
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.meta = None
        self.meta_path = None

    # ---- health / failover ----------------------------------------------
    def _ping(self, s):
        try:
            r, _ = request(s.addr, {"op": "PING"}, timeout=0.4)
            return bool(r.get("ok"))
        except (OSError, ValueError):
            return False

    def _request(self, s, header, body, timeout):
        """Like common.request, but registers the socket so mark_down() can abort a hung transfer."""
        sock = socket.create_connection(s.addr, timeout=timeout)
        with self.lock:
            self.inflight.setdefault(s.name, set()).add(sock)
        try:
            send_msg(sock, header, body)
            return recv_msg(sock)
        finally:
            with self.lock:
                self.inflight.get(s.name, set()).discard(sock)
            sock.close()

    def _abort_inflight(self, s):
        with self.lock:
            socks = list(self.inflight.get(s.name, ()))
        for sk in socks:
            try:
                sk.shutdown(socket.SHUT_RDWR)   # wakes the blocked send/recv at once
            except OSError:
                pass

    def mark_down(self, s, reason):
        self._mark_down(s, reason)
        self._abort_inflight(s)     # a frozen server must not keep chunks hostage for the full timeout

    def _mark_down(self, s, reason):
        with self.lock:
            if s.name in self.down:
                return
            self.down.add(s.name)
            if self.servers[self.active] is s and self.fail_t0 is None:
                self.fail_t0 = time.time()
            # emitted under the lock so "DOWN" always precedes the "failover" it causes
            emit("server_state", server=s.name, state="down", reason=reason)

    def mark_up(self, s):
        with self.lock:
            if s.name not in self.down:
                return
            self.down.discard(s.name)
            self.miss[s.name] = 0
        emit("server_state", server=s.name, state="up", reason="heartbeat")

    def pick_server(self):
        """Server to use right now (priority order), or None if all are down."""
        with self.lock:
            cur = self.servers[self.active]
            if self.sticky and cur.name not in self.down:
                return cur
            for i, s in enumerate(self.servers):
                if s.name in self.down:
                    continue
                if i != self.active:
                    kind = "failover" if i > self.active or cur.name in self.down else "failback"
                    emit(kind, src=cur.name, dst=s.name)
                    self.active = i
                    self.stats["failovers"] += 1
                return s
            return None

    def refresh_down(self):
        for s in list(self.servers):
            if s.name in self.down and self._ping(s):
                self.mark_up(s)

    def _sdn_check(self):
        """Ask the controller which servers are reachable and over which path. If the controller says
        there is NO path to a server, fail over now instead of waiting for TCP timeouts."""
        paths = self.sdn.reach([s.name for s in self.servers])
        if not paths:
            return
        for s in self.servers:
            info = paths.get(s.name, {})
            if info.get("reachable"):
                self.sdn_blocked.discard(s.name)
                if self.last_path.get(s.name) != info["path"]:
                    self.last_path[s.name] = info["path"]
                    emit("sdn_path", server=s.name, path=info["path"], hops=info["hops"],
                         util=info["max_util"])
            elif info.get("reachable") is False:
                self.last_path.pop(s.name, None)
                self.sdn_blocked.add(s.name)
                if s.name not in self.down:
                    emit("sdn_unreachable", server=s.name, reason=info.get("reason", "no path"))
                    self.mark_down(s, "sdn: no path")

    def _heartbeat(self):
        while not self.stop.wait(self.hb_interval):
            if self.sdn:
                self._sdn_check()
            for s in self.servers:
                if self._ping(s):
                    self.miss[s.name] = 0
                    if s.name not in self.sdn_blocked:   # controller still says "no path": stay down
                        self.mark_up(s)
                else:
                    self.miss[s.name] += 1
                    if self.miss[s.name] >= self.hb_miss and s.name not in self.down:
                        emit("heartbeat_lost", server=s.name, misses=self.miss[s.name])
                        self.mark_down(s, "heartbeat")

    # ---- metadata (the chunk map; enables resume + restore) ---------------
    def _load_meta(self, fid):
        os.makedirs(META_DIR, exist_ok=True)
        self.meta_path = os.path.join(META_DIR, fid + ".json")
        if os.path.exists(self.meta_path):
            with open(self.meta_path) as f:
                return json.load(f)
        return None

    def _save_meta(self):
        tmp = self.meta_path + ".tmp"
        with self.lock:
            data = json.dumps(self.meta)
        with open(tmp, "w") as f:
            f.write(data)
        os.replace(tmp, self.meta_path)

    def verify_remote(self):
        """Integrity check after the upload: ask every server what it really holds and compare with the
        SHA-256 of every chunk of the source file. Returns (verified_chunks, total_chunks)."""
        listings = []
        for s in self.servers:
            try:
                r, _ = request(s.addr, {"op": "LIST", "file_id": self.meta["file_id"]}, timeout=3)
                if r.get("ok"):
                    listings.append(r["chunks"])
            except (OSError, ValueError):
                pass
        verified = sum(1 for idx, rec in self.meta["chunks"].items()
                       if any(l.get(idx) == rec["sha"] for l in listings))
        return verified, self.meta["total"]

    # ---- backup ------------------------------------------------------------
    def _send_chunk(self, fid, idx, data, sha, total):
        size = len(data)
        attempts = 0
        while attempts < MAX_ATTEMPTS and not self.stop.is_set():
            s = self.pick_server()
            if s is None:
                attempts += 1
                emit("chunk", idx=idx, state="retry", server=None, attempts=attempts, size=size)
                self.refresh_down()
                time.sleep(min(2.0, 0.2 * 2 ** min(attempts, 4)))
                continue
            emit("chunk", idx=idx, state="sending" if attempts == 0 else "retry",
                 server=s.name, attempts=attempts, size=size)
            try:
                resp, _ = self._request(s, {"op": "PUT", "file_id": fid, "idx": idx, "total": total,
                                            "size": size, "sha": sha, "replicate": True},
                                        data, timeout=self.put_timeout)
            except (OSError, ValueError) as e:
                attempts += 1
                self.mark_down(s, type(e).__name__)   # next pick_server() fails over
                continue
            if not resp.get("ok"):                      # NACK (checksum) -> retry
                attempts += 1
                time.sleep(0.1)
                continue
            holders = [s.name] + ([resp["peer"]] if resp.get("replicated") and resp.get("peer") else [])
            with self.lock:
                self.stats["sent"] += size
                if attempts:
                    self.stats["resent"] += 1
                done_failover = self.fail_t0 is not None and self.servers[self.active] is s
                if done_failover:
                    ft = time.time() - self.fail_t0
                    self.stats["failover_time"] = ft
                    self.fail_t0 = None
                self.meta["chunks"][str(idx)] = {"sha": sha, "size": size, "servers": holders}
            if done_failover:
                emit("failover_done", server=s.name, seconds=round(ft, 3))
            emit("chunk", idx=idx, state="stored", server=s.name,
                 replicated=bool(resp.get("replicated")), attempts=attempts, size=size, sha=sha)
            return True
        emit("chunk", idx=idx, state="failed", server=None, attempts=attempts, size=size)
        return False

    def backup(self, path, chunk_size):
        t_start = time.time()
        size = os.path.getsize(path)
        total = math.ceil(size / chunk_size)
        file_sha = file_sha256(path)
        fid = file_sha[:16]
        old = self._load_meta(fid)
        self.meta = old if old and old.get("chunk_size") == chunk_size else {
            "file_id": fid, "name": os.path.basename(path), "size": size,
            "chunk_size": chunk_size, "total": total, "file_sha": file_sha, "chunks": {}}
        emit("backup_start", file_id=fid, name=os.path.basename(path), size=size,
             total=total, chunk_size=chunk_size,
             servers=[s.name for s in self.servers])

        # what do the servers already hold? (resume after an interrupted run)
        remote = {}
        for s in self.servers:
            try:
                r, _ = request(s.addr, {"op": "LIST", "file_id": fid}, timeout=3)
                if r.get("ok"):
                    remote[s.name] = {int(k): v for k, v in r["chunks"].items()}
            except (OSError, ValueError):
                pass

        already = max([len(m) for m in remote.values()] or [0])
        if already:
            print("[*] Resuming an interrupted transfer: %d of %d chunks are already stored on the servers" %
                  (already, total), flush=True)
            emit("resume_found", file_id=fid, already=already, total=total)

        if self.sdn:
            self._sdn_check()
        if self.hb_interval > 0:        # heartbeat monitor is optional (off by default)
            threading.Thread(target=self._heartbeat, daemon=True).start()

        def work(idx):
            with open(path, "rb") as f:
                f.seek(idx * chunk_size)
                data = f.read(chunk_size)
            sha = sha256_hex(data)
            have = [n for n, m in remote.items() if m.get(idx) == sha]
            if have:  # already stored and verified remotely -> resume, don't resend
                with self.lock:
                    self.stats["skipped"] += 1
                    self.meta["chunks"][str(idx)] = {"sha": sha, "size": len(data), "servers": have}
                emit("chunk", idx=idx, state="stored", server=have[0], replicated=len(have) > 1,
                     attempts=0, size=len(data), sha=sha, resumed=True)
                return True
            ok = self._send_chunk(fid, idx, data, sha, total)
            if idx % 10 == 0:
                self._save_meta()
            return ok

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            results = list(ex.map(work, range(total)))
        self.stop.set()
        self._save_meta()
        failed = results.count(False)
        secs = max(time.time() - t_start, 1e-6)
        verified, vtotal = self.verify_remote()
        integrity_ok = failed == 0 and verified == total
        mbps = size * 8 / secs / 1e6
        emit("backup_done", file_id=fid, seconds=round(secs, 2), mbps=round(mbps, 2), failed=failed,
             integrity_ok=integrity_ok, verified=verified, total=total, **self.stats)
        ft = self.stats["failover_time"]
        append_result({"kind": "backup", "file_id": fid, "file_name": os.path.basename(path),
                       "size_mb": round(size / 1048576, 2), "chunk_kb": chunk_size // 1024,
                       "total_chunks": total, "workers": self.workers,
                       "servers": ">".join(s.name for s in self.servers),
                       "seconds": round(secs, 2), "throughput_mbps": round(mbps, 2),
                       "chunks_resent": self.stats["resent"], "chunks_resumed": self.stats["skipped"],
                       "chunks_failed": failed, "failovers": self.stats["failovers"],
                       "failover_time_s": "" if ft is None else round(ft, 3),
                       "hash_ok": "yes" if integrity_ok else "no"})
        holders = {}
        for rec in self.meta["chunks"].values():
            for n in rec["servers"]:
                holders[n] = holders.get(n, 0) + 1
        rows = [("File", os.path.basename(path)),
                ("Size", "%.2f MB in %d chunks of %d KB" % (size / 1048576.0, total, chunk_size // 1024)),
                ("Integrity", ("Checksum Verified - all %d chunks match their SHA-256 on the servers" % total)
                 if integrity_ok else "FAILED - only %d of %d chunks verified (%d failed)" % (verified, total, failed)),
                ("Time Taken", "%.2f seconds" % secs),
                ("Throughput", "%.2f MB/s (%.1f Mbps)" % (size / secs / 1048576.0, mbps)),
                ("Stored on", ", ".join("%s: %d chunks" % (n, c) for n, c in sorted(holders.items())) or "-"),
                ("Failover", ("%s -> %s in %.2f s, %d chunks re-sent" % (
                    self.servers[0].name, self.servers[self.active].name, ft, self.stats["resent"]))
                 if self.stats["failovers"] and ft is not None
                 else ("%d chunks re-sent" % self.stats["resent"] if self.stats["resent"] else "none needed")),
                ("Resumed", ("%d chunks were already safe on the servers, only %d chunks (%.2f MB) sent now" % (
                    self.stats["skipped"], total - self.stats["skipped"], self.stats["sent"] / 1048576.0))
                 if self.stats["skipped"] else "no (fresh transfer)")]
        print_box("TRANSFER COMPLETE" if integrity_ok else "TRANSFER INCOMPLETE", rows)
        print("backup %s: %d chunks, %d failed, %d resumed, %d re-sent, %d failovers, "
              "failover_time=%s, %.2fs" % (fid, total, failed, self.stats["skipped"], self.stats["resent"],
                                           self.stats["failovers"], self.stats["failover_time"], secs), flush=True)
        print("file_id=%s" % fid, flush=True)
        return failed == 0

    # ---- restore ----------------------------------------------------------
    def restore(self, fid, out_path):
        self.meta = self._load_meta(fid)
        if not self.meta:
            print("no metadata for file_id %s" % fid, file=sys.stderr)
            return False
        meta = self.meta
        total, cs = meta["total"], meta["chunk_size"]
        emit("restore_start", file_id=fid, total=total)
        t_start = time.time()
        with open(out_path, "wb") as f:
            f.truncate(meta["size"])
        io_lock = threading.Lock()
        by_name = {s.name: s for s in self.servers}

        def fetch(idx):
            rec = meta["chunks"].get(str(idx))
            if not rec:
                emit("chunk", idx=idx, state="failed", server=None, size=0)
                return False
            order = [by_name[n] for n in rec["servers"] if n in by_name]
            order += [s for s in self.servers if s not in order]
            for s in order:
                try:
                    r, data = request(s.addr, {"op": "GET", "file_id": fid, "idx": idx}, timeout=5)
                except (OSError, ValueError):
                    continue
                if r.get("ok") and sha256_hex(data) == rec["sha"]:
                    with io_lock, open(out_path, "r+b") as f:
                        f.seek(idx * cs)
                        f.write(data)
                    emit("chunk", idx=idx, state="verified", server=s.name, size=len(data), sha=rec["sha"])
                    return True
            emit("chunk", idx=idx, state="failed", server=None, size=rec["size"])
            return False

        with ThreadPoolExecutor(max_workers=self.workers) as ex:
            ok = all(list(ex.map(fetch, range(total))))
        match = ok and file_sha256(out_path) == meta["file_sha"]
        secs = max(time.time() - t_start, 1e-6)
        emit("restore_done", file_id=fid, ok=bool(match), out=out_path, seconds=round(secs, 2),
             mbps=round(meta["size"] * 8 / secs / 1e6, 2))
        append_result({"kind": "restore", "file_id": fid, "file_name": meta.get("name"),
                       "size_mb": round(meta["size"] / 1048576, 2), "chunk_kb": cs // 1024,
                       "total_chunks": total, "workers": self.workers,
                       "servers": ">".join(s.name for s in self.servers),
                       "seconds": round(secs, 2),
                       "throughput_mbps": round(meta["size"] * 8 / secs / 1e6, 2),
                       "hash_ok": "yes" if match else "no"})
        print("restore %s -> %s : %s" % (fid, out_path, "HASH OK" if match else "FAILED"), flush=True)
        print_box("RESTORE COMPLETE" if match else "RESTORE FAILED", [
            ("File", meta.get("name")),
            ("Integrity", "Checksum Verified - every chunk and the whole file match the original SHA-256"
             if match else "FAILED - restored file does not match the original"),
            ("Time Taken", "%.2f seconds" % secs),
            ("Throughput", "%.2f MB/s (%.1f Mbps)" % (meta["size"] / secs / 1048576.0, meta["size"] * 8 / secs / 1e6)),
            ("Saved to", out_path)])
        return bool(match)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def common(p):
        p.add_argument("--servers", required=True,
                       help="priority order, e.g. h3=10.0.0.3:9000,h2=10.0.0.2:9000")
        p.add_argument("--workers", type=int, default=4)
        p.add_argument("--sticky", action="store_true", help="do not fail back to the primary")
        p.add_argument("--hb-interval", type=float, default=0.0,
                       help="heartbeat ping interval in seconds; 0 = heartbeat disabled (default)")
        p.add_argument("--put-timeout", type=float, default=6.0,
                       help="seconds before an unanswered chunk upload marks the server DOWN")
        p.add_argument("--hb-miss", type=int, default=3)
        p.add_argument("--sdn-sock", default=SDN_SOCK, help="controller socket (UNIX)")
        p.add_argument("--sdn-host", default="h1", help="this client's host name in the topology")
        p.add_argument("--no-sdn", action="store_true", help="do not talk to the controller")

    b = sub.add_parser("backup")
    b.add_argument("file")
    b.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK)
    common(b)
    r = sub.add_parser("restore")
    r.add_argument("file_id")
    r.add_argument("out")
    common(r)
    sub.add_parser("list")
    e = sub.add_parser("export", help="save test results as CSV")
    e.add_argument("kind", choices=["results", "events"],
                   help="results = one row per backup/restore run; events = failover/reroute timeline")
    e.add_argument("out")
    a = ap.parse_args()

    if a.cmd == "export":
        import shutil
        from common import EVENT_FILE, RESULTS_FILE, events_to_csv
        if a.kind == "results":
            shutil.copyfile(RESULTS_FILE, a.out)
        else:
            events_to_csv(EVENT_FILE, a.out)
        print("saved", a.out)
        return 0

    if a.cmd == "list":
        os.makedirs(META_DIR, exist_ok=True)
        for fn in sorted(os.listdir(META_DIR)):
            if fn.endswith(".json"):
                m = json.load(open(os.path.join(META_DIR, fn)))
                print(m["file_id"], m["name"], m["size"], "bytes", len(m["chunks"]), "/", m["total"], "chunks")
        return 0

    sdn = None if a.no_sdn else SdnLink(a.sdn_sock, a.sdn_host)
    bk = Backup(parse_servers(a.servers), a.workers, a.sticky, a.hb_interval, a.hb_miss, sdn,
                a.put_timeout)
    if a.cmd == "backup":
        return 0 if bk.backup(a.file, a.chunk_size) else 1
    return 0 if bk.restore(a.file_id, a.out) else 1


if __name__ == "__main__":
    sys.exit(main())
