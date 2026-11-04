#!/usr/bin/env python3
"""Backup chunk server.

Run one per backup host (h3 = primary, h2 = standby). When a client PUTs a
chunk with replicate=True, this server forwards it to its --peer before ACKing,
so the standby already holds the data when the primary dies.

On startup the server pulls any chunks it is missing from its peer (re-sync),
which is how a recovered primary catches up after a failover.
"""
import argparse
import os
import socketserver
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (emit, parse_addr, recv_msg, request, send_msg,  # noqa: E402
                    sha256_hex)


class App:
    def __init__(self, name, data_dir, peer, peer_name, delay):
        self.name = name
        self.data_dir = data_dir
        self.peer = peer
        self.peer_name = peer_name
        self.delay = delay
        self.sha_cache = {}
        self.peer_down_until = 0.0   # circuit breaker: skip replication while the partner is unresponsive
        os.makedirs(data_dir, exist_ok=True)

    # ---- spontaneous failure (for the demo) --------------------------------
    # Writing a chunk number N into <crash dir>/crash_<name> arms the server: it crashes BY ITSELF
    # the moment chunk N (the tile "CHUNK 00N" in the dashboard) arrives. Chunks before N are stored.
    # No kill command, no goodbye: the client just sees the connection drop.
    def _crash_file(self):
        return os.path.join(os.environ.get("SDNB_CRASH_DIR", "/tmp/sdnbackup"), "crash_%s" % self.name)

    def maybe_crash(self, idx):
        f = self._crash_file()
        if not os.path.exists(f) or not isinstance(idx, int):
            return
        try:
            at = max(0, int(open(f).read().strip()))
        except (OSError, ValueError):
            return
        if idx < at:
            return
        try:
            os.remove(f)
        except OSError:
            pass
        emit("server_state", server=self.name, state="down", reason="crashed by itself at chunk %d" % idx)
        os._exit(1)

    # ---- storage ---------------------------------------------------------
    def _path(self, fid, idx):
        return os.path.join(self.data_dir, fid, "%d.chunk" % idx)

    @staticmethod
    def _valid_id(fid):
        return isinstance(fid, str) and fid.isalnum() and len(fid) <= 64

    def _store(self, fid, idx, data, sha):
        d = os.path.join(self.data_dir, fid)
        os.makedirs(d, exist_ok=True)
        tmp = self._path(fid, idx) + ".tmp%d" % threading.get_ident()
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, self._path(fid, idx))
        self.sha_cache[(fid, idx)] = sha

    def _sha_of(self, fid, idx):
        key = (fid, idx)
        if key not in self.sha_cache:
            with open(self._path(fid, idx), "rb") as f:
                self.sha_cache[key] = sha256_hex(f.read())
        return self.sha_cache[key]

    def _list(self, fid):
        d = os.path.join(self.data_dir, fid)
        out = {}
        if os.path.isdir(d):
            for fn in os.listdir(d):
                if fn.endswith(".chunk"):
                    idx = int(fn[:-6])
                    try:
                        out[str(idx)] = self._sha_of(fid, idx)
                    except OSError:
                        pass
        return out

    # ---- protocol --------------------------------------------------------
    def dispatch(self, h, body):
        op = h.get("op")
        if op == "PING":
            return {"ok": True, "name": self.name}, b""
        if op == "PUT":
            return self._put(h, body), b""
        if op == "GET":
            return self._get(h)
        if op == "LIST":
            if not self._valid_id(h.get("file_id")):
                return {"ok": False, "err": "bad file_id"}, b""
            return {"ok": True, "chunks": self._list(h["file_id"])}, b""
        if op == "LISTALL":
            files = {}
            for fid in os.listdir(self.data_dir):
                if self._valid_id(fid):
                    files[fid] = self._list(fid)
            return {"ok": True, "files": files}, b""
        return {"ok": False, "err": "unknown op"}, b""

    def _put(self, h, body):
        self.maybe_crash(h.get("idx"))
        fid, idx = h.get("file_id"), h.get("idx")
        if not self._valid_id(fid) or not isinstance(idx, int) or idx < 0:
            return {"ok": False, "err": "bad header"}
        if len(body) != h.get("size") or sha256_hex(body) != h.get("sha"):
            emit("chunk_rejected", server=self.name, file_id=fid, idx=idx)
            return {"ok": False, "err": "checksum"}  # NACK -> client retries
        if self.delay:
            time.sleep(self.delay)  # simulate disk/WAN latency for demos
        self._store(fid, idx, body, h["sha"])
        replicated = False
        if h.get("replicate") and self.peer and time.time() >= self.peer_down_until:
            try:
                r, _ = request(self.peer, dict(h, replicate=False), body, timeout=2)
                replicated = bool(r.get("ok"))
            except (OSError, ValueError):
                replicated = False
                self.peer_down_until = time.time() + 3.0   # partner is dead or frozen: stop waiting on it
                emit("peer_unreachable", server=self.name, peer=self.peer_name)
        return {"ok": True, "server": self.name, "replicated": replicated,
                "peer": self.peer_name if replicated else None}

    def _get(self, h):
        fid, idx = h.get("file_id"), h.get("idx")
        if not self._valid_id(fid) or not isinstance(idx, int):
            return {"ok": False, "err": "bad header"}, b""
        try:
            with open(self._path(fid, idx), "rb") as f:
                data = f.read()
        except OSError:
            return {"ok": False, "err": "not found"}, b""
        return {"ok": True, "size": len(data), "sha": sha256_hex(data)}, data

    # ---- re-sync from peer (failback) ----------------------------------
    def resync(self):
        if not self.peer:
            return
        listing = None
        for _ in range(5):
            try:
                listing, _ = request(self.peer, {"op": "LISTALL"}, timeout=3)
                break
            except (OSError, ValueError):
                time.sleep(1)
        if not listing or not listing.get("ok"):
            return
        emit("resync_start", server=self.name, peer=self.peer_name)
        pulled = 0
        for fid, chunks in listing["files"].items():
            for sidx, sha in chunks.items():
                idx = int(sidx)
                try:
                    if os.path.exists(self._path(fid, idx)) and self._sha_of(fid, idx) == sha:
                        continue
                    r, data = request(self.peer, {"op": "GET", "file_id": fid, "idx": idx}, timeout=10)
                    if r.get("ok") and sha256_hex(data) == sha:
                        self._store(fid, idx, data, sha)
                        pulled += 1
                except (OSError, ValueError):
                    continue
        emit("resync_done", server=self.name, chunks=pulled)


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        app = self.server.app
        try:
            self.request.settimeout(30)
            header, body = recv_msg(self.request)
            resp, rbody = app.dispatch(header, body)
            send_msg(self.request, resp, rbody)
        except (OSError, ValueError):
            pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--name", required=True)
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--bind", default="0.0.0.0")
    ap.add_argument("--data", required=True, help="directory for stored chunks")
    ap.add_argument("--peer", help="host:port of the other backup server")
    ap.add_argument("--peer-name", default="peer")
    ap.add_argument("--delay", type=float, default=0.0,
                    help="artificial seconds per stored chunk (demo/testing)")
    ap.add_argument("--no-resync", action="store_true")
    a = ap.parse_args()

    app = App(a.name, a.data, parse_addr(a.peer) if a.peer else None, a.peer_name, a.delay)
    try:                                   # forget any old "crash me" request
        os.remove(app._crash_file())
    except OSError:
        pass
    srv = Server((a.bind, a.port), Handler)
    srv.app = app
    emit("server_state", server=a.name, state="up", reason="started")
    print("[%s] listening on %s:%d, data=%s, peer=%s" % (a.name, a.bind, a.port, a.data, a.peer), flush=True)
    if not a.no_resync:
        threading.Thread(target=app.resync, daemon=True).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
