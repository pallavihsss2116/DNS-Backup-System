"""Shared helpers: framing protocol, hashing, event log (feeds the dashboard)."""
import hashlib
import json
import os
import socket
import struct
import time

EVENT_FILE = os.environ.get("SDNB_EVENTS", "/tmp/sdnbackup/events.jsonl")
META_DIR = os.environ.get("SDNB_META", "/tmp/sdnbackup/meta")
DEFAULT_CHUNK = 1024 * 1024

_HDR = struct.Struct(">II")  # header length, body length
_MAX_HDR = 1 << 20
_MAX_BODY = 256 << 20


def emit(type_, **fields):
    """Append one JSON line to the shared event file. The Tkinter dashboard tails it.

    Mininet hosts share the root filesystem, so this works from inside any host
    namespace (client, servers) and from the controller / runner processes.
    """
    rec = {"ts": time.time(), "type": type_}
    rec.update(fields)
    try:
        os.makedirs(os.path.dirname(EVENT_FILE), exist_ok=True)
        with open(EVENT_FILE, "a") as f:
            f.write(json.dumps(rec) + "\n")
    except OSError:
        pass  # never let logging break a transfer


RESULTS_FILE = os.environ.get("SDNB_RESULTS", "/tmp/sdnbackup/results.csv")
RESULT_FIELDS = ["time", "kind", "file_id", "file_name", "size_mb", "chunk_kb", "total_chunks",
                 "workers", "servers", "seconds", "throughput_mbps", "chunks_resent",
                 "chunks_resumed", "chunks_failed", "failovers", "failover_time_s", "hash_ok"]


def append_result(row):
    """Append one test-result row (one backup or restore run) to the results CSV."""
    import csv
    try:
        os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
        new = not os.path.exists(RESULTS_FILE) or os.path.getsize(RESULTS_FILE) == 0
        with open(RESULTS_FILE, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=RESULT_FIELDS, extrasaction="ignore")
            if new:
                w.writeheader()
            w.writerow(dict({"time": time.strftime("%Y-%m-%d %H:%M:%S")}, **row))
    except OSError:
        pass


def events_to_csv(src, dst, skip=("chunk", "link_util")):
    """Convert the JSON-lines event file to a CSV timeline (time, type, server, details).
    Per-chunk and per-poll utilisation events are skipped by default; they are very numerous."""
    import csv
    n = 0
    with open(src) as f, open(dst, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(["time", "type", "server", "details"])
        for line in f:
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            if ev.get("type") in skip:
                continue
            ts = ev.pop("ts", 0)
            typ = ev.pop("type", "")
            srv = ev.pop("server", "")
            w.writerow([time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts)) + ("%.3f" % (ts % 1))[1:],
                        typ, srv, json.dumps(ev, sort_keys=True)])
            n += 1
    return n


def sha256_hex(data):
    return hashlib.sha256(data).hexdigest()


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(1 << 20)
            if not block:
                break
            h.update(block)
    return h.hexdigest()


def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        part = sock.recv(min(1 << 18, n - len(buf)))
        if not part:
            raise ConnectionError("connection closed")
        buf += part
    return bytes(buf)


def send_msg(sock, header, body=b""):
    h = json.dumps(header).encode()
    sock.sendall(_HDR.pack(len(h), len(body)) + h)
    if body:
        sock.sendall(body)


def recv_msg(sock):
    hl, bl = _HDR.unpack(recv_exact(sock, _HDR.size))
    if hl > _MAX_HDR or bl > _MAX_BODY:
        raise ValueError("frame too large")
    header = json.loads(recv_exact(sock, hl))
    body = recv_exact(sock, bl) if bl else b""
    return header, body


def request(addr, header, body=b"", timeout=5.0):
    """One request / one response over a fresh TCP connection."""
    with socket.create_connection(addr, timeout=timeout) as s:
        send_msg(s, header, body)
        return recv_msg(s)


def parse_addr(text):
    host, port = text.rsplit(":", 1)
    return host, int(port)


def parse_servers(spec):
    """'h3=10.0.0.3:9000,h2=10.0.0.2:9000' -> [('h3', ('10.0.0.3', 9000)), ...]
    Order matters: first entry is the primary, the rest are standbys in order."""
    out = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        if "=" in item:
            name, addr = item.split("=", 1)
        else:
            name, addr = item, item
        out.append((name, parse_addr(addr)))
    return out
