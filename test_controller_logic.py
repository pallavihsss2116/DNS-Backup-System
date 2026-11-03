"""Exercise the POX component's routing logic against a stubbed POX API (no POX/Mininet needed).

    python3 tests/test_controller_logic.py

This checks OUR logic (Dijkstra over discovered links, flow install in both directions, link-failure
reroute, congestion reroute). It does not prove the real POX/OpenFlow wiring: do the Mininet run for that.
"""
import json
import os
import sys
import types

os.environ["SDNB_EVENTS"] = "/tmp/sdnbackup/ctrltest_events.jsonl"
os.environ["SDNB_SOCK"] = "/tmp/sdnbackup/ctrltest.sock"
if os.path.exists(os.environ["SDNB_EVENTS"]):
    os.remove(os.environ["SDNB_EVENTS"])


# ----------------------------------------------------------------- POX stubs
class Obj(object):
    def __init__(self, **kw):
        self.__dict__.update(kw)


class FlowMod(Obj):
    def __init__(self, **kw):
        self.actions = []
        self.command = None
        self.match = None
        Obj.__init__(self, **kw)


of = types.ModuleType("pox.openflow.libopenflow_01")
of.OFPFC_DELETE = 3
of.OFPP_FLOOD = 0xfffb
of.ofp_flow_mod = FlowMod
of.ofp_match = lambda **kw: Obj(**kw)
of.ofp_action_output = lambda port: Obj(port=port)
of.ofp_packet_out = lambda **kw: Obj(actions=[], **kw)
of.ofp_stats_request = lambda **kw: Obj(**kw)
of.ofp_port_stats_request = lambda: Obj()

core_mod = types.ModuleType("pox.core")
core_mod.core = Obj(getLogger=lambda: Obj(info=lambda *a: None, warning=lambda *a: None,
                                          debug=lambda *a: None),
                    openflow=Obj(addListeners=lambda x: None),
                    openflow_discovery=Obj(addListeners=lambda x: None))
packet_mod = types.ModuleType("pox.lib.packet")
packet_mod.ethernet = Obj(LLDP_TYPE=0x88cc)
recoco = types.ModuleType("pox.lib.recoco")
recoco.Timer = lambda *a, **k: None
for name, mod in (("pox", types.ModuleType("pox")), ("pox.core", core_mod),
                  ("pox.openflow", types.ModuleType("pox.openflow")),
                  ("pox.openflow.libopenflow_01", of), ("pox.lib", types.ModuleType("pox.lib")),
                  ("pox.lib.packet", packet_mod), ("pox.lib.recoco", recoco)):
    sys.modules[name] = mod
sys.modules["pox"].openflow = sys.modules["pox.openflow"]
sys.modules["pox.openflow"].libopenflow_01 = of
sys.modules["pox"].lib = sys.modules["pox.lib"]
sys.modules["pox.lib"].packet = packet_mod
sys.modules["pox.lib"].recoco = recoco

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "controller"))
import multipath_controller as sdn_backup  # noqa: E402


class Mac(str):
    @property
    def is_multicast(self):
        return self == "ff:ff:ff:ff:ff:ff"

    def toStr(self):
        return str(self)


class Conn(object):
    def __init__(self, dpid):
        self.dpid, self.sent = dpid, []

    def send(self, m):
        self.sent.append(m)


def events():
    with open(os.environ["SDNB_EVENTS"]) as f:
        return [json.loads(l) for l in f]


# --------------------------------------------------------------------- setup
c = sdn_backup.SDNBackup(10, 2)
conns = {d: Conn(d) for d in (1, 2, 3, 4)}
for d, con in conns.items():
    c._handle_ConnectionUp(Obj(dpid=d, connection=con))

# diamond: s1-s2 (p2/p1), s2-s4 (p2/p2), s1-s3 (p3/p1), s3-s4 (p2/p3); hosts on port 1 of s1, s3, s4... edge ports
LINKS = [(1, 2, 2, 1), (2, 2, 4, 2), (1, 3, 3, 2), (3, 3, 4, 3)]
for a, pa, b, pb in LINKS:
    for l in (Obj(dpid1=a, port1=pa, dpid2=b, port2=pb), Obj(dpid1=b, port1=pb, dpid2=a, port2=pa)):
        c._handle_LinkEvent(Obj(link=l, added=True, removed=False))

H1, H2, H3 = Mac("00:00:00:00:00:01"), Mac("00:00:00:00:00:02"), Mac("00:00:00:00:00:03")
BCAST = Mac("ff:ff:ff:ff:ff:ff")


def pin(dpid, port, src, dst):
    ev = Obj(parsed=Obj(parsed=True, type=0x0800, src=src, dst=dst), dpid=dpid, port=port,
             ofp=Obj(), connection=conns[dpid])
    c._handle_PacketIn(ev)


# hosts announce themselves (ARP-like broadcast) -> controller learns locations, floods
pin(1, 1, H1, BCAST)
pin(3, 1, H2, BCAST)
pin(4, 1, H3, BCAST)
assert c.hosts == {H1: (1, 1), H2: (3, 1), H3: (4, 1)}, c.hosts
assert all(m.actions[-1].port == of.OFPP_FLOOD for con in conns.values() for m in con.sent
           if hasattr(m, "in_port")), "broadcasts must be flooded"
# a packet arriving on an inter-switch port must NOT be learned as a host location
pin(2, 1, H1, BCAST)
assert c.hosts[H1] == (1, 1)
print("host learning + flood OK")

# ----------------------------------------------------------- initial routing
for con in conns.values():
    del con.sent[:]
pin(1, 1, H1, H3)
first = c.flows[(H1, H3)]["path"]
assert first[0] == 1 and first[-1] == 4 and len(first) == 3, first
assert c.flows[(H3, H1)]["path"] == list(reversed(first)), "reverse direction must use the same path"
installed = [(d, m.actions[0].port) for d in conns for m in conns[d].sent
             if isinstance(m, FlowMod) and m.command is None and m.match.dl_src == H1]
assert len(installed) == 3, installed                 # one entry per switch on the path
assert (4, 1) in installed, "last hop must output to h3's edge port"
print("initial path", first, "installed OK")

# --------------------------------------------------- link failure -> reroute
a, b = first[0], first[1]
for con in conns.values():
    del con.sent[:]
for l in (Obj(dpid1=a, port1=c.adj[a][b], dpid2=b, port2=c.adj[b][a]),
          Obj(dpid1=b, port1=c.adj[b][a], dpid2=a, port2=c.adj[a][b])):
    c._handle_LinkEvent(Obj(link=l, added=False, removed=True))
assert (H1, H3) not in c.flows and (H3, H1) not in c.flows, "affected flows must be deleted"
deleted = [m for con in conns.values() for m in con.sent if getattr(m, "command", None) == of.OFPFC_DELETE]
assert len(deleted) == 6, len(deleted)                # 3 switches x 2 directions
ev_types = [e["type"] for e in events()]
assert ev_types.count("link_down") == 1, "link_down must be reported once, not once per direction"

pin(1, 1, H1, H3)                                     # next packet triggers re-routing
second = c.flows[(H1, H3)]["path"]
assert second != first and not c._uses_link(second, a, b), (first, second)
rd = [e for e in events() if e["type"] == "reroute_done"]
assert rd and rd[0]["path"] == second, rd
print("link %d-%d down -> rerouted %s -> %s in %d ms OK" % (a, b, first, second, rd[0]["ms"]))

# traffic for H2 (on s3) is unaffected by an unrelated flow
pin(1, 1, H1, H2)
assert c.flows[(H1, H2)]["path"][-1] == 3

# ---------------------------------------------------- congestion -> reroute
# bring the broken link back, then make the current H1<->H3 path busy and the other one idle
for aa, pa, bb, pb in LINKS:
    for l in (Obj(dpid1=aa, port1=pa, dpid2=bb, port2=pb), Obj(dpid1=bb, port1=pb, dpid2=aa, port2=pa)):
        c._handle_LinkEvent(Obj(link=l, added=True, removed=False))
cur = c.flows[(H1, H3)]["path"]
for x, y in zip(cur, cur[1:]):
    c.util[(x, c.adj[x][y])] = 0.95
c._rebalance()
assert (H1, H3) not in c.flows, "congested flow should be dropped for re-routing"
pin(1, 1, H1, H3)
third = c.flows[(H1, H3)]["path"]
assert third != cur, (cur, third)
print("congestion on %s -> moved to %s OK" % (cur, third))

# no alternative better than LOW_UTIL -> keep the path (no flapping)
for k in list(c.util):
    c.util[k] = 0.95
c._rebalance()
assert (H1, H3) in c.flows, "must not reroute when every path is congested"
print("no-flap guard OK")

print("\nCONTROLLER LOGIC TEST PASSED")


# ------------------------------------------------ socket API used by the backup client
import socket as _socket
import threading as _threading

print("\n-- socket API --")
SOCK = os.environ["SDNB_SOCK"]
assert os.path.exists(SOCK), "controller must create its UNIX socket"


def api(req):
    s = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
    s.connect(SOCK)
    s.sendall((json.dumps(req) + "\n").encode())
    buf = b""
    while not buf.endswith(b"\n"):
        buf += s.recv(65536)
    s.close()
    return json.loads(buf.decode())


# restore all links, re-learn path state
for aa, pa, bb, pb in LINKS:
    for l in (Obj(dpid1=aa, port1=pa, dpid2=bb, port2=pb), Obj(dpid1=bb, port1=pb, dpid2=aa, port2=pa)):
        c._handle_LinkEvent(Obj(link=l, added=True, removed=False))
for k in list(c.util):
    c.util[k] = 0.0
r = api({"op": "reach", "src": "h1", "dsts": ["h3", "h2", "h9"]})
assert r["ok"] and r["paths"]["h3"]["reachable"] and r["paths"]["h3"]["path"][0] == 1 \
    and r["paths"]["h3"]["path"][-1] == 4, r
assert r["paths"]["h2"]["path"] == [1, 3], r
assert r["paths"]["h9"]["reachable"] is None, "unknown host must not be reported as unreachable"
print("reach h1 -> h3 via", r["paths"]["h3"]["path"], "| h2 via", r["paths"]["h2"]["path"], "OK")

# cut BOTH ways to s4: h3 becomes unreachable, h2 (on s3) still reachable
for a_, b_ in ((2, 4), (3, 4)):
    for l in (Obj(dpid1=a_, port1=c.adj[a_][b_], dpid2=b_, port2=c.adj[b_][a_]),
              Obj(dpid1=b_, port1=c.adj[b_][a_], dpid2=a_, port2=c.adj[a_][b_])):
        c._handle_LinkEvent(Obj(link=l, added=False, removed=True))
r = api({"op": "reach", "src": "h1", "dsts": ["h3", "h2"]})
assert r["paths"]["h3"]["reachable"] is False, r
assert r["paths"]["h2"]["reachable"] is True, r
print("s4 isolated -> h3 unreachable, h2 reachable OK")
assert api({"op": "status"})["ok"]
assert api({"op": "nope"})["ok"] is False
print("SOCKET API TEST PASSED")
