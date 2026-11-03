"""Dashboard state model: turns the event stream into state. No Tk in here, so it can be tested headless."""
import time

COLORS = {
    "pending": "#4b5563",     # grey
    "sending": "#facc15",     # yellow
    "retry": "#f97316",       # orange
    "stored": "#22c55e",      # green   (on one server)
    "replicated": "#14b8a6",  # teal    (on primary + standby)
    "failed": "#ef4444",      # red
    "verified": "#3b82f6",    # blue    (hash-verified during restore)
}
LEGEND = [("Pending", "pending"), ("Sending", "sending"), ("Retrying", "retry"),
          ("Stored", "stored"), ("Replicated", "replicated"), ("Failed", "failed"),
          ("Verified (restore)", "verified")]

LINKS = [(1, 2), (2, 4), (1, 3), (3, 4)]  # switch-switch links of the diamond topology


def link_key(a, b):
    return (min(a, b), max(a, b))


class Model:
    def __init__(self):
        # network / server state survives from one backup to the next
        self.servers = {}                # name -> dict(state, reason)
        self.link_util = {}              # (a, b) -> 0..1
        self._util_dir = {}              # (from, to) -> 0..1
        self.links_down = set()
        self.path = []                   # latest dpid path for traffic from h1
        self._last_path = {}
        self.last_file_id = None
        self.reroute_ms = None
        self.banner = None               # (text, kind) kind: ok / bad / warn / info
        self.order = ["h3", "h2"]        # priority order (first = primary)
        self.new_backup()

    def new_backup(self):
        """Clear everything that belongs to a single backup run."""
        self.file_id = None
        self.name = None
        self.total = 0
        self.chunks = {}                 # idx -> dict(state, server, replicated, attempts, size, sha, holders)
        self.active = None
        self.failover_time = None
        self.done = None
        self.bytes = []                  # (ts, size) of stored chunks, for throughput

    # ---- derived ---------------------------------------------------------------
    def role(self, name):
        return "PRIMARY" if self.order and name == self.order[0] else "STANDBY"

    def counts(self):
        c = {k: 0 for k in COLORS}
        for ch in self.chunks.values():
            c[self.color_key(ch)] += 1
        return c

    @staticmethod
    def color_key(ch):
        st = ch.get("state", "pending")
        if st == "stored" and ch.get("replicated"):
            return "replicated"
        return st if st in COLORS else "pending"

    def chunk_color(self, idx):
        return COLORS[self.color_key(self.chunks.get(idx, {}))]

    def chunk_rows(self):
        """One row per chunk for CSV export: the chunk map as the dashboard sees it."""
        rows = []
        for idx in sorted(self.chunks):
            ch = self.chunks[idx]
            rows.append([idx, self.color_key(ch), ch.get("size", ""), "+".join(ch.get("holders", [])),
                         ch.get("attempts", 0), ch.get("sha", "")])
        return rows

    def holders_count(self, name):
        return sum(1 for ch in self.chunks.values() if name in ch.get("holders", ()))

    def throughput(self, now=None, span=60):
        """Mbps per second for the last `span` seconds (oldest first)."""
        now = now or time.time()
        buckets = [0] * span
        for ts, size in self.bytes:
            age = int(now - ts)
            if 0 <= age < span:
                buckets[span - 1 - age] += size
        return [b * 8 / 1e6 for b in buckets]

    def tooltip(self, idx):
        ch = self.chunks.get(idx)
        if ch is None:
            return None
        lines = ["chunk %d / %d" % (idx, self.total), "state: %s" % self.color_key(ch)]
        if ch.get("size"):
            lines.append("size: %d bytes" % ch["size"])
        if ch.get("holders"):
            lines.append("stored on: %s" % ", ".join(ch["holders"]))
        elif ch.get("server"):
            lines.append("server: %s" % ch["server"])
        lines.append("retries: %d" % ch.get("attempts", 0))
        if ch.get("sha"):
            lines.append("sha256: %s…" % ch["sha"][:16])
        return "\n".join(lines)

    # ---- event handling ------------------------------------------------------------
    def apply(self, ev):
        """Update state from one event. Returns (dirty, log) where dirty says what to redraw."""
        t = ev.get("type")
        dirty = {"grid": False, "chunks": set(), "servers": False, "topo": False, "stats": False, "banner": False}
        log = None
        stamp = time.strftime("%H:%M:%S", time.localtime(ev.get("ts", time.time())))

        def L(src, msg):
            return (stamp, src, msg)

        if t == "backup_start":
            self.new_backup()
            self.file_id = self.last_file_id = ev["file_id"]
            self.name, self.total = ev.get("name"), ev["total"]
            self.order = ev.get("servers") or self.order
            self.active = self.order[0]
            self.chunks = {i: {"state": "pending"} for i in range(self.total)}
            dirty.update(grid=True, servers=True, stats=True, banner=True)
            self.banner = ("BACKUP IN PROGRESS: %s (%d chunks)" % (ev.get("name"), self.total), "info")
            log = L("client", "backup %s started: %d chunks of %d KB, servers %s" % (
                ev.get("name"), self.total, ev.get("chunk_size", 0) // 1024, " -> ".join(self.order)))

        elif t == "chunk":
            idx = ev["idx"]
            ch = self.chunks.setdefault(idx, {})
            st = ev.get("state")
            ch["state"] = st
            ch["server"] = ev.get("server")
            ch["attempts"] = ev.get("attempts", ch.get("attempts", 0))
            ch["size"] = ev.get("size", ch.get("size"))
            if ev.get("sha"):
                ch["sha"] = ev["sha"]
            if st == "stored":
                ch["replicated"] = bool(ev.get("replicated"))
                srv = ev.get("server")
                ch["holders"] = [srv] + ([s for s in self.order if s != srv] if ch["replicated"] else [])
                if not ev.get("resumed"):
                    self.bytes.append((ev.get("ts", time.time()), ev.get("size") or 0))
                    if len(self.bytes) > 5000:
                        self.bytes = self.bytes[-3000:]
            dirty["chunks"].add(idx)
            dirty["stats"] = True
            if st == "failed":
                log = L("client", "chunk %d FAILED after %d attempts" % (idx, ch["attempts"]))

        elif t == "server_state":
            s = ev["server"]
            self.servers[s] = {"state": ev["state"], "reason": ev.get("reason", "")}
            dirty.update(servers=True)
            log = L(s, "%s (%s)" % (ev["state"].upper(), ev.get("reason", "")))

        elif t == "heartbeat_lost":
            log = L("monitor", "heartbeat lost from %s (%d missed pings)" % (ev["server"], ev.get("misses", 0)))

        elif t in ("failover", "failback"):
            self.active = ev["dst"]
            dirty.update(servers=True, stats=True)
            log = L("client", "%s: %s -> %s" % (t.upper(), ev["src"], ev["dst"]))

        elif t == "failover_done":
            self.failover_time = ev["seconds"]
            dirty["stats"] = True
            log = L("client", "failover complete in %.2f s (first chunk stored on %s)" % (ev["seconds"], ev["server"]))

        elif t == "backup_done":
            self.done = ev
            dirty["stats"] = True
            dirty["banner"] = True
            ok = ev.get("integrity_ok", ev.get("failed", 0) == 0)
            mb_s = ev["mbps"] / 8.0
            if ok:
                self.banner = ("TRANSFER COMPLETE   |   Integrity: Checksum Verified (%d/%d chunks SHA-256)   |   "
                               "Time Taken: %.2f s   |   Throughput: %.2f MB/s (%.1f Mbps)%s" % (
                                   ev.get("verified", self.total), ev.get("total", self.total), ev["seconds"],
                                   mb_s, ev["mbps"],
                                   ("   |   Failover: %.2f s" % ev["failover_time"]) if ev.get("failover_time") else ""),
                               "ok")
            else:
                self.banner = ("TRANSFER INCOMPLETE   |   Integrity check FAILED: %d of %d chunks verified, %d failed" % (
                    ev.get("verified", 0), ev.get("total", self.total), ev.get("failed", 0)), "bad")
            log = L("client", "TRANSFER %s: integrity %s, time %.2f s, throughput %.2f MB/s (%.1f Mbps), "
                    "%d re-sent, %d resumed, %d failed" % (
                        "COMPLETE" if ok else "INCOMPLETE", "Checksum Verified" if ok else "FAILED",
                        ev["seconds"], mb_s, ev["mbps"], ev.get("resent", 0), ev.get("skipped", 0),
                        ev.get("failed", 0)))

        elif t == "restore_start":
            log = L("client", "restore started")

        elif t == "restore_done":
            dirty["stats"] = True
            dirty["banner"] = True
            if ev["ok"]:
                self.banner = ("RESTORE COMPLETE   |   Integrity: Checksum Verified (whole-file SHA-256 matches the original)"
                               "   |   Time Taken: %.2f s   |   Throughput: %.2f MB/s" % (
                                   ev.get("seconds", 0), ev.get("mbps", 0) / 8.0), "ok")
            else:
                self.banner = ("RESTORE FAILED   |   restored file does not match the original", "bad")
            log = L("client", "restore finished: %s" % ("Checksum Verified, file hash MATCHES original" if ev["ok"] else "FAILED"))

        elif t in ("resync_start", "resync_done"):
            extra = "" if t == "resync_start" else " (%d chunks pulled)" % ev.get("chunks", 0)
            log = L(ev["server"], t.replace("_", " ") + extra)

        elif t == "chunk_rejected":
            log = L(ev["server"], "chunk %d rejected: checksum mismatch" % ev["idx"])

        elif t == "link_util":
            # controller reports each direction separately; show the busier one
            self._util_dir[(ev["a"], ev["b"])] = ev["util"]
            k = link_key(ev["a"], ev["b"])
            self.link_util[k] = max(self._util_dir.get((k[0], k[1]), 0.0),
                                    self._util_dir.get((k[1], k[0]), 0.0))
            dirty["topo"] = True

        elif t == "link_down":
            self.links_down.add(link_key(ev["a"], ev["b"]))
            dirty["topo"] = True
            log = L("controller", "link s%d-s%d DOWN" % (ev["a"], ev["b"]))

        elif t == "link_up":
            self.links_down.discard(link_key(ev["a"], ev["b"]))
            dirty["topo"] = True

        elif t == "link_state":
            log = L("mininet", "link %s-%s set %s" % (ev["a"], ev["b"], ev["state"]))

        elif t == "path_installed":
            key = (ev["src"], ev["dst"])
            if ev["src"] == "h1":
                self.path = ev["path"]
                dirty["topo"] = True
            if self._last_path.get(key) != ev["path"]:
                self._last_path[key] = ev["path"]
                log = L("controller", "path %s -> %s via %s" % (
                    ev["src"], ev["dst"], "-".join("s%d" % d for d in ev["path"])))

        elif t == "reroute_done":
            self.reroute_ms = ev["ms"]
            dirty["stats"] = True
            log = L("controller", "REROUTE %s->%s done in %d ms via %s" % (
                ev["src"], ev["dst"], ev["ms"], "-".join("s%d" % d for d in ev["path"])))

        elif t == "resume_found":
            dirty["banner"] = True
            self.banner = ("RESUMING INTERRUPTED TRANSFER   |   %d of %d chunks already stored, sending only the rest" % (
                ev["already"], ev["total"]), "info")
            log = L("client", "resuming: %d of %d chunks already on the servers" % (ev["already"], ev["total"]))

        elif t == "transfer_interrupted":
            lost = 0
            for idx, ch in self.chunks.items():
                if ch.get("state") in ("sending", "retry"):
                    ch["state"] = "pending"
                    dirty["chunks"].add(idx)
                    lost += 1
            done = sum(1 for c in self.chunks.values() if c.get("state") in ("stored", "verified"))
            dirty.update(banner=True, stats=True)
            self.banner = ("TRANSFER INTERRUPTED   |   %d of %d chunks are safely stored   |   press Resume to continue" % (
                done, self.total), "warn")
            log = L("client", "TRANSFER INTERRUPTED at %d/%d chunks (%d in flight lost); stored chunks are kept" % (
                done, self.total, lost))

        elif t == "peer_unreachable":
            log = L(ev["server"], "partner %s not answering: standby copy skipped for now" % ev.get("peer", "?"))

        elif t == "sdn_path":
            log = L("client", "controller says path to %s: %s (%d hops, util %d%%)" % (
                ev["server"], "-".join("s%d" % d for d in ev["path"]), ev["hops"], int(ev["util"] * 100)))

        elif t == "sdn_unreachable":
            log = L("controller", "NO PATH to %s (%s): client fails over" % (ev["server"], ev.get("reason", "")))

        elif t == "reroute_congestion":
            log = L("controller", "congestion reroute %s->%s" % (ev["src"], ev["dst"]))

        elif t == "topology_ready":
            log = L("mininet", "topology ready")

        elif t == "ui_msg":
            log = L("ui", ev["msg"])

        return dirty, log

    def status_text(self):
        if not self.file_id:
            return "No backup yet. Create a test file, then press Backup."
        c = self.counts()
        done = c["stored"] + c["replicated"] + c["verified"]
        s = "file %s | %d chunks | done %d (replicated %d) | retrying %d | failed %d" % (
            self.file_id[:8], self.total, done, c["replicated"], c["retry"], c["failed"])
        if self.active:
            s += " | active server: %s" % self.active
        if self.failover_time is not None:
            s += " | failover %.2f s" % self.failover_time
        if self.reroute_ms is not None:
            s += " | reroute %d ms" % self.reroute_ms
        return s
