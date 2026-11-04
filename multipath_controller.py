"""POX component: utilization-aware routing + automatic link-failure rerouting.

Copy this file to  <pox>/ext/multipath_controller.py  and start POX with:

    ./pox.py log.level --DEBUG openflow.discovery openflow.spanning_tree multipath_controller

What it does (OpenFlow 1.0)
  * openflow.discovery gives us switch-to-switch links (LLDP).
  * openflow.spanning_tree makes OFPP_FLOOD loop-free, so ARP/broadcast
    works on the diamond (meshed) topology.
  * Unicast traffic is routed by THIS component: it learns host locations,
    runs Dijkstra over the discovered links and installs a flow entry on
    every switch of the chosen path (both directions).
  * Link cost = 1 + 10 * utilization, where utilization comes from port
    statistics polled every POLL seconds. A busy link therefore becomes
    expensive and new flows avoid it.
  * If a flow's path becomes congested (>HIGH) and a clearly better path
    exists (<LOW), that flow is removed and re-routed.
  * On link down, only the flows that used the link are deleted; the next
    packet re-triggers path selection over the remaining links. The time
    from link-down to the new path being installed is measured and logged.

All notable events are appended as JSON lines to the shared event file so the
Tkinter dashboard can display them.

Written to run on both Python 2.7 (older POX) and Python 3 (newer POX).
"""
import heapq
import json
import os
import socket
import threading
import time

from pox.core import core
import pox.openflow.libopenflow_01 as of
import pox.lib.packet as pkt
from pox.lib.recoco import Timer

log = core.getLogger()

EVENT_FILE = os.environ.get("SDNB_EVENTS", "/tmp/sdnbackup/events.jsonl")
API_SOCK = os.environ.get("SDNB_SOCK", "/tmp/sdnbackup/sdn.sock")  # app <-> controller
IDLE_TIMEOUT = 10   # seconds; idle flows expire and get re-evaluated
HIGH_UTIL = 0.8     # path considered congested above this
LOW_UTIL = 0.5      # alternative must be below this to be worth switching to


def emit(type_, **fields):
    rec = {"ts": time.time(), "type": type_}
    rec.update(fields)
    try:
        d = os.path.dirname(EVENT_FILE)
        if not os.path.isdir(d):
            os.makedirs(d)
        with open(EVENT_FILE, "a") as f:
            f.write(json.dumps(rec) + "\n")
    except (IOError, OSError):
        pass


def host_name(mac):
    """00:00:00:00:00:03 -> 'h3' (Mininet autoSetMacs)."""
    try:
        return "h%d" % int(mac.toStr().split(":")[-1], 16)
    except Exception:
        return str(mac)


class SDNBackup(object):
    def __init__(self, capacity_mbps, poll):
        self.capacity = capacity_mbps * 1e6
        self.conns = {}          # dpid -> connection
        self.adj = {}            # dpid -> {neighbor dpid: local port}
        self.port_peer = {}      # (dpid, port) -> neighbor dpid
        self.hosts = {}          # mac -> (dpid, port)
        self.flows = {}          # (src, dst) -> {"path": [dpids], "matches": [(dpid, match)]}
        self.last_bytes = {}     # (dpid, port) -> (tx_bytes, time)
        self.util = {}           # (dpid, port) -> 0..1
        self.pending = {}        # (src, dst) -> link-down time, for reroute timing
        core.openflow.addListeners(self)
        core.openflow_discovery.addListeners(self)
        Timer(poll, self._poll, recurring=True)
        self._start_api()
        log.info("multipath_controller started (capacity %.0f Mbps, poll %.1fs)", capacity_mbps, poll)

    # ---- switches ---------------------------------------------------------
    def _handle_ConnectionUp(self, event):
        self.conns[event.dpid] = event.connection
        self.adj.setdefault(event.dpid, {})
        emit("switch_up", dpid=event.dpid)

    def _handle_ConnectionDown(self, event):
        self.conns.pop(event.dpid, None)
        self._drop_flows(lambda path: event.dpid in path, "switch %s down" % event.dpid)
        for peer in list(self.adj.pop(event.dpid, {})):
            self.adj.get(peer, {}).pop(event.dpid, None)
        emit("switch_down", dpid=event.dpid)

    # ---- links (from openflow.discovery) -----------------------------------
    def _handle_LinkEvent(self, event):
        l = event.link
        a, b = l.dpid1, l.dpid2
        if event.added:
            self.adj.setdefault(a, {})[b] = l.port1
            self.adj.setdefault(b, {})[a] = l.port2
            self.port_peer[(a, l.port1)] = b
            self.port_peer[(b, l.port2)] = a
            emit("link_up", a=a, b=b)
            log.info("link up %s <-> %s", a, b)
        elif event.removed:
            known = b in self.adj.get(a, {})
            p1 = self.adj.get(a, {}).pop(b, None)
            p2 = self.adj.get(b, {}).pop(a, None)
            self.port_peer.pop((a, p1), None)
            self.port_peer.pop((b, p2), None)
            if not known:
                return  # second direction of the same physical link
            emit("link_down", a=a, b=b)
            log.warning("link down %s <-> %s", a, b)
            self._drop_flows(lambda path: self._uses_link(path, a, b),
                             "link %s-%s down" % (a, b))

    @staticmethod
    def _uses_link(path, a, b):
        for x, y in zip(path, path[1:]):
            if (x, y) in ((a, b), (b, a)):
                return True
        return False

    def _drop_flows(self, pred, why):
        now = time.time()
        for key in list(self.flows):
            f = self.flows[key]
            if pred(f["path"]):
                for dpid, match in f["matches"]:
                    con = self.conns.get(dpid)
                    if con is not None:
                        con.send(of.ofp_flow_mod(command=of.OFPFC_DELETE, match=match))
                del self.flows[key]
                self.pending[key] = now
                log.info("flow %s->%s removed (%s)", host_name(key[0]), host_name(key[1]), why)

    # ---- statistics / utilization -------------------------------------------
    def _poll(self):
        for con in list(self.conns.values()):
            con.send(of.ofp_stats_request(body=of.ofp_port_stats_request()))
        self._rebalance()

    def _handle_PortStatsReceived(self, event):
        now = time.time()
        for st in event.stats:
            key = (event.connection.dpid, st.port_no)
            prev = self.last_bytes.get(key)
            self.last_bytes[key] = (st.tx_bytes, now)
            if prev is None or now <= prev[1]:
                continue
            bps = (st.tx_bytes - prev[0]) * 8.0 / (now - prev[1])
            u = max(0.0, min(1.0, bps / self.capacity))
            self.util[key] = u
            peer = self.port_peer.get(key)
            if peer is not None:
                emit("link_util", a=event.connection.dpid, b=peer, util=round(u, 3), bps=int(bps))

    def _link_util(self, a, b):
        pa, pb = self.adj.get(a, {}).get(b), self.adj.get(b, {}).get(a)
        return max(self.util.get((a, pa), 0.0), self.util.get((b, pb), 0.0))


    # ---- socket API for the backup application ---------------------------------
    # The client (running on h1) connects to a UNIX socket (Mininet hosts share the
    # filesystem) and asks the controller about the network. One JSON line in, one out.
    #   {"op": "reach", "src": "h1", "dsts": ["h3", "h2"]}
    #       -> {"ok": true, "paths": {"h3": {"reachable": true, "path": [1,2,4], "hops": 2,
    #                                        "max_util": 0.1}, "h2": {...}}}
    #   {"op": "status"} -> links, utilisation and known hosts
    def _start_api(self):
        try:
            d = os.path.dirname(API_SOCK)
            if not os.path.isdir(d):
                os.makedirs(d)
            if os.path.exists(API_SOCK):
                os.remove(API_SOCK)
            srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            srv.bind(API_SOCK)
            os.chmod(API_SOCK, 0o777)       # Mininet hosts run as root, POX may not
            srv.listen(8)
        except (OSError, IOError) as e:
            log.warning("socket API disabled: %s", e)
            return
        t = threading.Thread(target=self._api_loop, args=(srv,))
        t.daemon = True
        t.start()
        log.info("socket API listening on %s", API_SOCK)

    def _api_loop(self, srv):
        while True:
            try:
                conn, _ = srv.accept()
            except (OSError, IOError):
                return
            t = threading.Thread(target=self._api_serve, args=(conn,))
            t.daemon = True
            t.start()

    def _api_serve(self, conn):
        try:
            conn.settimeout(3)
            buf = b""
            while b"\n" not in buf:
                part = conn.recv(4096)
                if not part:
                    return
                buf += part
            try:
                req = json.loads(buf.split(b"\n")[0].decode())
                resp = self._api_handle(req)
            except ValueError:
                resp = {"ok": False, "err": "bad json"}
            conn.sendall((json.dumps(resp) + "\n").encode())
        except (OSError, IOError):
            pass
        finally:
            conn.close()

    def _api_handle(self, req):
        for _ in range(3):   # POX thread may change the dicts while we read them: retry
            try:
                return self._api_answer(req)
            except RuntimeError:
                continue
        return {"ok": False, "err": "busy"}

    def _api_answer(self, req):
        names = dict((host_name(m), loc) for m, loc in self.hosts.items())
        op = req.get("op")
        if op == "reach":
            src = req.get("src")
            if src not in names:
                return {"ok": False, "err": "unknown host %s" % src}
            out = {}
            for d in req.get("dsts", []):
                if d not in names:
                    out[d] = {"reachable": None, "reason": "host not learned yet"}
                    continue
                path = self._shortest_path(names[src][0], names[d][0])
                if path is None:
                    out[d] = {"reachable": False, "reason": "no path"}
                else:
                    out[d] = {"reachable": True, "path": path, "hops": len(path) - 1,
                              "max_util": round(self._path_max_util(path), 3)}
            return {"ok": True, "paths": out}
        if op == "status":
            links = sorted(set((min(a, b), max(a, b)) for a in self.adj for b in self.adj[a]))
            return {"ok": True, "switches": sorted(self.conns), "links": links,
                    "hosts": dict((n, list(l)) for n, l in names.items()),
                    "util": dict(("%s-%s" % (a, b), round(self._link_util(a, b), 3)) for a, b in links)}
        return {"ok": False, "err": "unknown op"}

    # ---- routing -------------------------------------------------------------
    def _shortest_path(self, src, dst):
        """Dijkstra; edge cost = 1 + 10 * utilization."""
        if src == dst:
            return [src]
        dist, prev, heap = {src: 0.0}, {}, [(0.0, src)]
        while heap:
            d, u = heapq.heappop(heap)
            if u == dst:
                break
            if d > dist.get(u, 1e18):
                continue
            for v in self.adj.get(u, {}):
                nd = d + 1.0 + 10.0 * self._link_util(u, v)
                if nd < dist.get(v, 1e18):
                    dist[v], prev[v] = nd, u
                    heapq.heappush(heap, (nd, v))
        if dst not in prev:
            return None
        path = [dst]
        while path[-1] != src:
            path.append(prev[path[-1]])
        return list(reversed(path))

    def _path_max_util(self, path):
        return max([self._link_util(a, b) for a, b in zip(path, path[1:])] or [0.0])

    def _rebalance(self):
        """Re-route a flow whose path is congested if a clearly better one exists."""
        for key, f in list(self.flows.items()):
            if self._path_max_util(f["path"]) < HIGH_UTIL:
                continue
            alt = self._shortest_path(f["path"][0], f["path"][-1])
            if alt and alt != f["path"] and self._path_max_util(alt) < LOW_UTIL:
                emit("reroute_congestion", src=host_name(key[0]), dst=host_name(key[1]),
                     old=f["path"], new=alt)
                log.info("congestion reroute %s -> %s", f["path"], alt)
                self._drop_flows(lambda p, old=f["path"]: p == old, "congestion")

    def _install(self, src, dst, path, announce=True):
        """Install src->dst and dst->src along `path` (list of dpids)."""
        for s, d, p in ((src, dst, path), (dst, src, list(reversed(path)))):
            matches = []
            for i, dpid in enumerate(p):
                if i == len(p) - 1:
                    out = self.hosts[d][1]
                else:
                    out = self.adj[dpid][p[i + 1]]
                match = of.ofp_match(dl_src=s, dl_dst=d)
                fm = of.ofp_flow_mod(match=match, idle_timeout=IDLE_TIMEOUT, hard_timeout=0,
                                     priority=10)
                fm.actions.append(of.ofp_action_output(port=out))
                self.conns[dpid].send(fm)
                matches.append((dpid, match))
            self.flows[(s, d)] = {"path": p, "matches": matches}
            t0 = self.pending.pop((s, d), None)
            if t0 is not None:
                ms = int((time.time() - t0) * 1000)
                emit("reroute_done", src=host_name(s), dst=host_name(d), path=p, ms=ms)
                log.info("reroute %s->%s done in %d ms via %s", host_name(s), host_name(d), ms, p)
        if announce:
            emit("path_installed", src=host_name(src), dst=host_name(dst), path=path)

    # ---- packets ---------------------------------------------------------------
    def _handle_PacketIn(self, event):
        packet = event.parsed
        if not packet.parsed or packet.type == pkt.ethernet.LLDP_TYPE:
            return
        dpid, in_port = event.dpid, event.port

        # learn host location only on edge ports (ports not used by a switch link)
        if (dpid, in_port) not in self.port_peer and not packet.src.is_multicast:
            self.hosts[packet.src] = (dpid, in_port)

        if packet.dst.is_multicast or packet.dst not in self.hosts or packet.src not in self.hosts:
            self._flood(event)
            return

        s_loc, d_loc = self.hosts[packet.src], self.hosts[packet.dst]
        path = self._shortest_path(s_loc[0], d_loc[0])
        if path is None:
            log.warning("no path %s -> %s", host_name(packet.src), host_name(packet.dst))
            return
        key = (packet.src, packet.dst)
        # A packet-in for a known pair means the switch has NO flow for it (idle timeout expired, or the
        # flow-mod has not arrived yet). c.flows is only our own record and goes stale when switch entries
        # expire, so ALWAYS (re)install; only announce the path when it is new or changed.
        known = key in self.flows and self.flows[key]["path"] == path
        self._install(packet.src, packet.dst, path, announce=not known)
        # send the packet that triggered this on its way, out of the switch that RAISED it
        here = event.dpid
        if here in path:
            i = path.index(here)
            out = d_loc[1] if i == len(path) - 1 else self.adj[here][path[i + 1]]
        else:
            alt = self._shortest_path(here, d_loc[0])
            if alt is None:
                return
            out = d_loc[1] if len(alt) == 1 else self.adj[here][alt[1]]
        msg = of.ofp_packet_out(data=event.ofp, in_port=in_port)
        msg.actions.append(of.ofp_action_output(port=out))
        event.connection.send(msg)

    @staticmethod
    def _flood(event):
        msg = of.ofp_packet_out(data=event.ofp, in_port=event.port)
        msg.actions.append(of.ofp_action_output(port=of.OFPP_FLOOD))
        event.connection.send(msg)


def launch(capacity=10, poll=2):
    """capacity = link capacity in Mbps used to compute utilization (match topology.py)."""
    def start():
        core.registerNew(SDNBackup, float(capacity), float(poll))
    core.call_when_ready(start, ["openflow", "openflow_discovery"])
