#!/usr/bin/env python3
r"""Mininet topology + control socket for the dashboard.  Run with sudo.

              s2
             /  \
   h1 --- s1      s4 --- h3   (primary, 10.0.0.3)
             \  /
              s3
              |
              h2              (standby, 10.0.0.2)

  s1-s2-s4 : fast path (5 ms links)      s1-s3-s4 : slower path (15 ms links)
  h1 = backup client (10.0.0.1)

The dashboard talks to this process over 127.0.0.1:9998 (JSON lines) to
inject failures: link down/up, kill/start/freeze a server, start a backup
or restore on h1.
"""
import argparse
import json
import os
import socketserver
import subprocess
import sys
import threading
import time

try:
    from mininet.cli import CLI
    from mininet.link import TCLink
    from mininet.log import info, setLogLevel
    from mininet.net import Mininet
    from mininet.node import OVSKernelSwitch, RemoteController
except ImportError:
    sys.exit("Mininet python module not found. Run this on the Mininet VM with sudo.")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "backup"))
from common import emit  # noqa: E402

SERVER = os.path.join(ROOT, "backup", "server.py")
CLIENT = os.path.join(ROOT, "backup", "client.py")
LOGDIR = "/tmp/sdnbackup/logs"
DATA = "/tmp/sdnbackup"
PORT = 9000
SERVERS = "h3=10.0.0.3:%d,h2=10.0.0.2:%d" % (PORT, PORT)


class Runner:
    def __init__(self, args):
        self.args = args
        os.makedirs(LOGDIR, exist_ok=True)
        self.procs = {}
        self.net = None
        self.client_proc = None

    # ---- topology --------------------------------------------------------
    def build(self):
        net = Mininet(controller=None, switch=OVSKernelSwitch, link=TCLink, autoSetMacs=True)
        net.addController("c0", controller=RemoteController, ip="127.0.0.1", port=6633)
        s = {i: net.addSwitch("s%d" % i, protocols="OpenFlow10") for i in range(1, 5)}
        h1 = net.addHost("h1", ip="10.0.0.1/24")
        h2 = net.addHost("h2", ip="10.0.0.2/24")
        h3 = net.addHost("h3", ip="10.0.0.3/24")
        net.addLink(h1, s[1])
        net.addLink(h2, s[3])
        net.addLink(h3, s[4])
        bw = self.args.bw
        net.addLink(s[1], s[2], bw=bw, delay="5ms")    # fast path
        net.addLink(s[2], s[4], bw=bw, delay="5ms")
        net.addLink(s[1], s[3], bw=bw, delay="15ms")   # slower path
        net.addLink(s[3], s[4], bw=bw, delay="15ms")
        self.net = net

    def start_server(self, host):
        h = self.net.get(host)
        peer, pname = ("10.0.0.2", "h2") if host == "h3" else ("10.0.0.3", "h3")
        log = open(os.path.join(LOGDIR, "%s.log" % host), "a")
        self.procs[host] = h.popen(
            [sys.executable, SERVER, "--name", host, "--port", str(PORT),
             "--data", os.path.join(DATA, host), "--peer", "%s:%d" % (peer, PORT),
             "--peer-name", pname, "--delay", str(self.args.delay)],
            stdout=log, stderr=subprocess.STDOUT)

    # ---- commands from the dashboard ---------------------------------------
    def handle(self, c):
        cmd = c.get("cmd")
        if cmd == "link":
            a, b, state = c["a"], c["b"], c["state"]
            self.net.configLinkStatus(a, b, state)
            emit("link_state", a=a, b=b, state=state)
            return "link %s-%s %s" % (a, b, state)
        if cmd == "server":
            host, action = c["host"], c["action"]
            p = self.procs.get(host)
            if action == "kill":
                if p and p.poll() is None:
                    p.kill()
                emit("server_state", server=host, state="down", reason="killed")
            elif action == "freeze" and p:
                p.send_signal(19)  # SIGSTOP: process hangs, TCP connects still succeed
                emit("server_state", server=host, state="down", reason="frozen")
            elif action == "thaw" and p:
                p.send_signal(18)  # SIGCONT
                emit("server_state", server=host, state="up", reason="thawed")
            elif action == "start":
                if p is None or p.poll() is not None:
                    self.start_server(host)
            return "%s %s" % (host, action)
        if cmd == "backup":
            self.run_client([sys.executable, CLIENT, "backup", c["file"], "--servers", SERVERS,
                             "--chunk-size", str(c.get("chunk", 1048576)),
                             "--workers", str(c.get("workers", 4))],
                            "backup", os.path.basename(c["file"]))
            return "backup started"
        if cmd == "interrupt":
            return self.interrupt_client()
        if cmd == "restore":
            self.run_client([sys.executable, CLIENT, "restore", c["file_id"], c["out"],
                             "--servers", SERVERS], "restore", os.path.basename(c["out"]))
            return "restore started"
        return "unknown command"

    def run_client(self, args, kind, name):
        """Run the backup/restore client on h1 and show its result box in THIS (Mininet) terminal."""
        logf = open(os.path.join(LOGDIR, "client.log"), "a")
        p = self.net.get("h1").popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     universal_newlines=True)
        p.interrupted = False
        self.client_proc = p
        print("\n*** %s of %s started on h1" % (kind.upper(), name))
        sys.stdout.flush()

        def watch():
            lines = []
            for line in p.stdout:
                logf.write(line)
                logf.flush()
                lines.append(line.rstrip("\n"))
            rc = p.wait()
            if p.interrupted:
                return                       # the Interrupt command already printed its own message
            text = "\n".join(lines)
            if rc != 0 and "FAILED" not in text and "INCOMPLETE" not in text:
                text += "\n*** %s FAILED (exit code %d)" % (kind.upper(), rc)
            print("\n" + text + "\nmininet> ", end="")
            sys.stdout.flush()

        threading.Thread(target=watch, daemon=True).start()

    def interrupt_client(self):
        """Simulate an interrupted transfer: kill the client while it is sending."""
        p = self.client_proc
        if p is None or p.poll() is not None:
            return "no transfer is running"
        p.interrupted = True
        p.kill()
        emit("transfer_interrupted")
        print("\n" + "*" * 66 + "\n*** TRANSFER INTERRUPTED. Chunks already stored on the servers are kept."
              "\n*** Press Resume (run the same backup again) to continue where it stopped."
              "\n" + "*" * 66 + "\nmininet> ", end="")
        sys.stdout.flush()
        return "transfer interrupted"

    def serve_control(self):
        runner = self

        class H(socketserver.StreamRequestHandler):
            def handle(self):
                for line in self.rfile:
                    try:
                        msg = runner.handle(json.loads(line))
                        self.wfile.write((json.dumps({"ok": True, "msg": msg}) + "\n").encode())
                    except Exception as e:  # report errors to the dashboard instead of dying
                        self.wfile.write((json.dumps({"ok": False, "msg": str(e)}) + "\n").encode())

        class S(socketserver.ThreadingTCPServer):
            allow_reuse_address = True
            daemon_threads = True

        srv = S(("127.0.0.1", 9998), H)
        threading.Thread(target=srv.serve_forever, daemon=True).start()

    # ---- main ----------------------------------------------------------------
    def run(self):
        self.build()
        self.net.start()
        info("*** waiting %ds for POX discovery / spanning tree\n" % self.args.wait)
        time.sleep(self.args.wait)
        self.net.pingAll()          # teaches the controller where every host is
        self.start_server("h3")
        self.start_server("h2")
        self.serve_control()
        emit("topology_ready")
        info("*** ready. Control socket on 127.0.0.1:9998. Mininet CLI below.\n")
        CLI(self.net)
        for p in self.procs.values():
            if p.poll() is None:
                p.kill()
        self.net.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bw", type=float, default=10, help="inter-switch link bandwidth, Mbps")
    ap.add_argument("--delay", type=float, default=0.0, help="artificial per-chunk delay on servers (s)")
    ap.add_argument("--wait", type=int, default=6, help="seconds to wait for controller discovery")
    a = ap.parse_args()
    setLogLevel("info")
    Runner(a).run()
