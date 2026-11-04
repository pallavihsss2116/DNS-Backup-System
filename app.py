#!/usr/bin/env python3
"""Tkinter dashboard: per-chunk grid, servers, live topology, throughput, event log, failure controls.

    python3 dashboard/app.py            # follow live events
    python3 dashboard/app.py --replay   # also replay everything already in the event file

Events come from the shared JSON-lines file (backup client/servers, POX, Mininet runner).
Failure-injection buttons talk to topology/topology.py on 127.0.0.1:9998.
Socket/file threads never touch Tk: they put events on a queue that the UI drains with after().
"""
import argparse
import csv
import json
import math
import shutil
import os
import queue
import socket
import subprocess
import sys
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from model import COLORS, LEGEND, LINKS, Model, link_key  # noqa: E402
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "backup"))
from common import RESULTS_FILE, events_to_csv  # noqa: E402

EVENT_FILE = os.environ.get("SDNB_EVENTS", "/tmp/sdnbackup/events.jsonl")
CTRL = ("127.0.0.1", 9998)
WORKDIR = "/tmp/sdnbackup"

# topology drawing positions (canvas 520 x 330)
NODES = {"h1": (45, 150), "s1": (140, 150), "s2": (270, 60), "s3": (270, 240), "s4": (400, 150),
         "h3": (485, 150), "h2": (270, 310)}
SW = {1: "s1", 2: "s2", 3: "s3", 4: "s4"}
BG, FG, PANEL = "#111827", "#e5e7eb", "#1f2937"


class App:
    def __init__(self, root, replay):
        self.root = root
        self.m = Model()
        self.q = queue.Queue()
        self.tiles = {}
        self.thumb_images = {}       # keep Tk PhotoImage objects alive
        self.thumbnails = {}         # chunk index -> PNG path
        self.thumbnail_dir = None
        self.video_duration = None
        self.is_video = False
        self.cell = 10
        self.cols = 1
        self.tile_rects = {}          # chunk index -> (x1, y1, x2, y2)
        self.failover_events = []     # recent server promotion/failback transitions
        self.topo_dirty = True
        self.file_path = tk.StringVar(value="")
        root.title("SDN Resilient Backup — dashboard")
        sw, sh = root.winfo_screenwidth(), root.winfo_screenheight()
        root.geometry("%dx%d+0+0" % (min(1360, sw - 10), min(860, sh - 70)))   # never taller than the screen
        root.configure(bg=BG)
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", background=PANEL, fieldbackground=PANEL, foreground=FG, rowheight=20)
        style.configure("Treeview.Heading", background="#374151", foreground=FG)
        self._toolbar()
        self._log_panel()      # packed at the bottom first, so it is always visible
        self._body()
        threading.Thread(target=self._tail, args=(replay,), daemon=True).start()
        root.after(100, self._pump)
        root.after(500, self._tick)

    # ---------------------------------------------------------------- layout
    def _toolbar(self):
        bar = tk.Frame(self.root, bg=BG)
        bar.pack(fill="x", padx=8, pady=(8, 2))

        def btn(parent, text, cmd, color="#374151"):
            b = tk.Button(parent, text=text, command=cmd, bg=color, fg="white", relief="flat",
                          activebackground="#4b5563", padx=8, pady=3)
            b.pack(side="left", padx=2)
            return b

        row1 = tk.Frame(bar, bg=BG)
        row1.pack(fill="x")
        tk.Label(row1, text="Test file (MB):", bg=BG, fg=FG).pack(side="left")
        self.size_var = tk.StringVar(value="32")
        tk.Spinbox(row1, from_=1, to=2000, width=5, textvariable=self.size_var).pack(side="left", padx=2)
        btn(row1, "Make test file", self.make_file)
        btn(row1, "Choose video…", self.choose_file, "#7c3aed")
        btn(row1, "Make test video", self.make_video, "#7c3aed")
        tk.Label(row1, textvariable=self.file_path, bg=BG, fg="#9ca3af").pack(side="left", padx=6)
        tk.Label(row1, text="Chunk KB:", bg=BG, fg=FG).pack(side="left", padx=(12, 0))
        self.chunk_var = tk.StringVar(value="512")
        ttk.Combobox(row1, width=6, values=["128", "256", "512", "1024"], textvariable=self.chunk_var).pack(side="left")
        btn(row1, "▶ Backup", self.backup, "#15803d")
        btn(row1, "⏸ Interrupt", lambda: self.cmd({"cmd": "interrupt"}), "#b45309")
        btn(row1, "⏯ Resume", self.backup, "#15803d")
        btn(row1, "⟲ Restore", self.restore, "#1d4ed8")
        btn(row1, "▷ Play restored", self.play_restored, "#7c3aed")
        btn(row1, "Clear log", self.clear_log)
        row0 = tk.Frame(bar, bg=BG)
        row0.pack(fill="x", pady=(4, 0))
        tk.Label(row0, text="Export CSV:", bg=BG, fg="#93c5fd").pack(side="left")
        btn(row0, "Results", self.export_results, "#0f766e")
        btn(row0, "Events", self.export_events, "#0f766e")
        btn(row0, "Chunks", self.export_chunks, "#0f766e")

        row2 = tk.Frame(bar, bg=BG)
        row2.pack(fill="x", pady=(4, 0))
        tk.Label(row2, text="Failure injection:", bg=BG, fg="#fca5a5").pack(side="left")
        btn(row2, "Kill h3 (primary)", lambda: self.cmd({"cmd": "server", "host": "h3", "action": "kill"}), "#b91c1c")
        btn(row2, "Start h3", lambda: self.cmd({"cmd": "server", "host": "h3", "action": "start"}))
        btn(row2, "Kill h2 (standby)", lambda: self.cmd({"cmd": "server", "host": "h2", "action": "kill"}), "#b91c1c")
        btn(row2, "Start h2", lambda: self.cmd({"cmd": "server", "host": "h2", "action": "start"}))
        tk.Label(row2, text="   Link:", bg=BG, fg=FG).pack(side="left")
        self.link_var = tk.StringVar(value="s1-s2")
        ttk.Combobox(row2, width=7, values=["s1-s2", "s2-s4", "s1-s3", "s3-s4", "h3-s4", "h2-s3"],
                     textvariable=self.link_var, state="readonly").pack(side="left")
        btn(row2, "Link DOWN", lambda: self.link("down"), "#b91c1c")
        btn(row2, "Link UP", lambda: self.link("up"))

        row3 = tk.Frame(bar, bg=BG)
        row3.pack(fill="x", pady=(4, 0))
        tk.Label(row3, text="Natural failures (no kill):", bg=BG, fg="#fdba74").pack(side="left")
        tk.Label(row3, text=" h3 crashes by itself at chunk #", bg=BG, fg=FG).pack(side="left")
        self.crash_var = tk.StringVar(value="5")
        tk.Spinbox(row3, from_=0, to=5000, width=4, textvariable=self.crash_var).pack(side="left", padx=2)
        tk.Label(row3, text="chunks", bg=BG, fg=FG).pack(side="left")
        btn(row3, "Arm auto-crash", self.arm_crash, "#c2410c")
        btn(row3, "Freeze h3 (hang)", lambda: self.cmd({"cmd": "server", "host": "h3", "action": "freeze"}), "#c2410c")
        btn(row3, "Thaw h3", lambda: self.cmd({"cmd": "server", "host": "h3", "action": "thaw"}))
        btn(row3, "Cut h3 cable", lambda: self.cmd({"cmd": "link", "a": "h3", "b": "s4", "state": "down"}), "#c2410c")
        btn(row3, "Restore h3 cable", lambda: self.cmd({"cmd": "link", "a": "h3", "b": "s4", "state": "up"}))

    def _body(self):
        body = tk.PanedWindow(self.root, orient="horizontal", bg=BG, sashwidth=4)
        body.pack(fill="both", expand=True, padx=8, pady=4)

        left = tk.Frame(body, bg=BG)
        tk.Label(left, text="Video chunk timeline (each tile = one stored byte chunk; hover for details)",
                 bg=BG, fg=FG, anchor="w").pack(fill="x")
        gf = tk.Frame(left, bg=BG)
        gf.pack(fill="both", expand=True)
        self.grid = tk.Canvas(gf, bg="#0b1220", highlightthickness=0)
        sb = ttk.Scrollbar(gf, orient="vertical", command=self.grid.yview)
        self.grid.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        self.grid.pack(side="left", fill="both", expand=True)
        self.grid.bind("<Configure>", self._on_resize)
        self.grid.bind("<Motion>", self._hover)
        self.grid.bind("<Leave>", lambda e: self.tip.place_forget())
        self.tip = tk.Label(self.grid, bg="#fefce8", fg="black", justify="left", relief="solid", bd=1,
                            font=("TkDefaultFont", 9))
        legend = tk.Frame(left, bg=BG)
        legend.pack(fill="x", pady=2)
        for label, key in LEGEND:
            tk.Label(legend, text="  ", bg=COLORS[key]).pack(side="left", padx=(6, 2))
            tk.Label(legend, text=label, bg=BG, fg=FG).pack(side="left")
        body.add(left, minsize=520, stretch="always")

        right_outer = tk.Frame(body, bg=BG)
        rc = tk.Canvas(right_outer, bg=BG, highlightthickness=0)
        rsb = ttk.Scrollbar(right_outer, orient="vertical", command=rc.yview)
        rc.configure(yscrollcommand=rsb.set)
        rsb.pack(side="right", fill="y")
        rc.pack(side="left", fill="both", expand=True)
        right = tk.Frame(rc, bg=BG)
        win = rc.create_window((0, 0), window=right, anchor="nw")
        right.bind("<Configure>", lambda e: rc.configure(scrollregion=rc.bbox("all")))
        rc.bind("<Configure>", lambda e: rc.itemconfigure(win, width=e.width))

        def _wheel(e):      # mouse wheel scrolls the right column when the pointer is over it
            try:
                w = self.root.winfo_containing(*self.root.winfo_pointerxy())
            except (KeyError, tk.TclError):
                return
            if w is not None and str(w).startswith(str(right_outer)):
                rc.yview_scroll(-1 if (e.num == 4 or getattr(e, "delta", 0) > 0) else 1, "units")
        for ev in ("<MouseWheel>", "<Button-4>", "<Button-5>"):
            self.root.bind_all(ev, _wheel)
        tk.Label(right, text="Servers", bg=BG, fg=FG, anchor="w").pack(fill="x")
        sp = tk.Frame(right, bg=BG)
        sp.pack(fill="x")
        self.srv_widgets = {}
        for name in ("h3", "h2"):
            f = tk.Frame(sp, bg=PANEL, padx=8, pady=6)
            f.pack(side="left", fill="x", expand=True, padx=3)
            title = tk.Label(f, text=name, bg=PANEL, fg="white", font=("TkDefaultFont", 12, "bold"))
            title.pack(anchor="w")
            info = tk.Label(f, text="", bg=PANEL, fg=FG, justify="left")
            info.pack(anchor="w")
            self.srv_widgets[name] = (f, title, info)
        tk.Label(right, text="Failover visualization (latest server transition)",
                 bg=BG, fg=FG, anchor="w").pack(fill="x", pady=(6, 0))
        self.failover_canvas = tk.Canvas(right, bg="#0b1220", height=104, highlightthickness=0)
        self.failover_canvas.pack(fill="x")
        tk.Label(right, text="Network topology (live)", bg=BG, fg=FG, anchor="w").pack(fill="x", pady=(6, 0))
        self.topo = tk.Canvas(right, bg="#0b1220", height=300, highlightthickness=0)
        self.topo.pack(fill="x")
        self.topo.bind("<Configure>", lambda e: setattr(self, "topo_dirty", True))
        key = tk.Frame(right, bg=PANEL, padx=6, pady=4)
        key.pack(fill="x", pady=(2, 0))
        tk.Label(key, text="Colour key", bg=PANEL, fg="#9ca3af", font=("TkDefaultFont", 9, "bold")
                 ).grid(row=0, column=0, sticky="w")
        items = [("#22c55e", "link idle"), ("#f59e0b", "link busy"), ("#ef4444", "link down / congested"),
                 ("#3b82f6", "active path"), ("#7c3aed", "host (client / standby)"),
                 ("#16a34a", "ACTIVE server"), ("#ef4444", "server DOWN"), ("#0ea5e9", "switch")]
        for i, (col, text) in enumerate(items):
            r, cc = divmod(i, 4)
            cell = tk.Frame(key, bg=PANEL)
            cell.grid(row=1 + r, column=cc, sticky="w", padx=(0, 10), pady=1)
            tk.Label(cell, text="   ", bg=col).pack(side="left", padx=(0, 3))
            tk.Label(cell, text=text, bg=PANEL, fg=FG, font=("TkDefaultFont", 8)).pack(side="left")
        tk.Label(right, text="Throughput (Mbps, last 60 s)", bg=BG, fg=FG, anchor="w").pack(fill="x", pady=(6, 0))
        self.chart = tk.Canvas(right, bg="#0b1220", height=100, highlightthickness=0)
        self.chart.pack(fill="x")
        body.add(right_outer, minsize=540)

    def _log_panel(self):
        f = tk.Frame(self.root, bg=BG)
        f.pack(fill="x", padx=8, pady=(0, 4), side="bottom")
        self.banner = tk.Label(f, text="Ready", bg="#374151", fg="white", anchor="w", padx=8, pady=6,
                               font=("TkDefaultFont", 11, "bold"), wraplength=1300, justify="left")
        self.banner.pack(fill="x", pady=(0, 2))
        self.status = tk.Label(f, text=self.m.status_text(), bg="#064e3b", fg="white", anchor="w", padx=8, pady=4)
        self.status.pack(fill="x")
        self.tree = ttk.Treeview(f, columns=("time", "src", "msg"), show="headings", height=5)
        for col, w in (("time", 80), ("src", 90), ("msg", 1000)):
            self.tree.heading(col, text=col.capitalize())
            self.tree.column(col, width=w, anchor="w", stretch=(col == "msg"))
        self.tree.pack(fill="x")

    # ------------------------------------------------------------ event input
    def _tail(self, replay):
        os.makedirs(os.path.dirname(EVENT_FILE), exist_ok=True)
        open(EVENT_FILE, "a").close()
        with open(EVENT_FILE) as f:
            if not replay:
                f.seek(0, 2)
            buf = ""
            while True:
                chunk = f.readline()
                if not chunk:
                    if os.path.getsize(EVENT_FILE) < f.tell():  # file was truncated
                        f.seek(0)
                    time.sleep(0.1)
                    continue
                buf += chunk
                if not buf.endswith("\n"):
                    continue  # partial line, wait for the rest
                try:
                    self.q.put(json.loads(buf))
                except ValueError:
                    pass
                buf = ""

    def _pump(self):
        for _ in range(800):  # bounded work per tick keeps the UI responsive
            try:
                ev = self.q.get_nowait()
            except queue.Empty:
                break
            self._handle(ev)
        self.root.after(100, self._pump)

    def _handle(self, ev):
        # Keep a compact visual history independent of the text event log.
        if ev.get("type") in ("failover", "failback"):
            self.failover_events.append({
                "type": ev["type"], "src": ev.get("src", "?"),
                "dst": ev.get("dst", "?"), "ts": ev.get("ts", time.time()),
                "seconds": None,
            })
            self.failover_events = self.failover_events[-8:]
        elif ev.get("type") == "failover_done" and self.failover_events:
            self.failover_events[-1]["seconds"] = ev.get("seconds")
        dirty, log = self.m.apply(ev)
        if dirty["grid"]:
            self._draw_grid()
        for idx in dirty["chunks"]:
            tile = self.tiles.get(idx)
            if tile is not None:
                self._update_chunk_tile(idx)
        if dirty["topo"]:
            self.topo_dirty = True
        if dirty["stats"]:
            self.status.config(text=self.m.status_text())
        if dirty["banner"] and self.m.banner:
            text, kind = self.m.banner
            self.banner.config(text=text, bg={"ok": "#047857", "bad": "#b91c1c", "warn": "#b45309",
                                              "info": "#1d4ed8"}.get(kind, "#374151"))
        if log:
            self.tree.insert("", "end", values=log)
            kids = self.tree.get_children()
            if len(kids) > 400:
                self.tree.delete(*kids[:100])
            self.tree.see(self.tree.get_children()[-1])

    def _tick(self):
        self._draw_servers()
        self._draw_failover()
        if self.topo_dirty:
            self._draw_topo()
            self.topo_dirty = False
        self._draw_chart()
        self.root.after(500, self._tick)

    # --------------------------------------------------------------- chunk grid
    def _on_resize(self, _e):
        if self.m.total:
            self._draw_grid()

    def _draw_grid(self):
        """Draw a video-editor-style filmstrip; each tile still represents one byte chunk."""
        c = self.grid
        c.delete("all")
        self.tiles = {}
        self.tile_rects = {}
        self.thumb_images = {}
        n = self.m.total
        if not n:
            return

        tile_w, tile_h = 136, 100
        image_w, image_h = 124, 68
        pad_x, pad_y = 8, 8
        w = max(c.winfo_width(), 200)
        self.cols = max(1, (w - pad_x) // (tile_w + pad_x))
        rows = math.ceil(n / self.cols)
        total_h = pad_y + rows * (tile_h + pad_y)
        c.configure(scrollregion=(0, 0, w, max(c.winfo_height(), total_h)))

        for i in range(n):
            r, col = divmod(i, self.cols)
            x = pad_x + col * (tile_w + pad_x)
            y = pad_y + r * (tile_h + pad_y)
            color = self.m.chunk_color(i)
            img_path = self.thumbnails.get(i)
            if img_path is None and self.thumbnails:
                nearest = (i // max(1, math.ceil(n / 300))) * max(1, math.ceil(n / 300))
                img_path = self.thumbnails.get(nearest)
            if img_path and os.path.exists(img_path):
                try:
                    photo = tk.PhotoImage(file=img_path)
                    self.thumb_images[i] = photo
                    image_id = c.create_image(x + tile_w // 2, y + 36, image=photo)
                except tk.TclError:
                    image_id = c.create_rectangle(x + 6, y + 4, x + tile_w - 6, y + 72,
                                                  fill="#263449", outline="")
            else:
                image_id = c.create_rectangle(x + 6, y + 4, x + tile_w - 6, y + 72,
                                              fill="#263449", outline="")
                if self.is_video and self.video_duration:
                    c.create_text(x + tile_w // 2, y + 38, text="FRAME %02d" % (i + 1),
                                  fill="#9ca3af", font=("TkDefaultFont", 9))
                else:
                    c.create_text(x + tile_w // 2, y + 38, text="DATA CHUNK",
                                  fill="#9ca3af", font=("TkDefaultFont", 9))

            # The coloured outline is the live transfer state.
            border_id = c.create_rectangle(x + 4, y + 2, x + tile_w - 4, y + 74,
                                           outline=color, width=3)
            c.create_text(x + 8, y + 87, anchor="w", text="CHUNK %03d" % i,
                          fill=FG, font=("TkDefaultFont", 9, "bold"))
            c.create_text(x + tile_w - 8, y + 87, anchor="e",
                          text=self.m.color_key(self.m.chunks.get(i, {})).upper(),
                          fill=color, font=("TkDefaultFont", 8, "bold"),
                          tags=("chunk_status_%d" % i,))
            self.tiles[i] = border_id
            self.tile_rects[i] = (x + 4, y + 2, x + tile_w - 4, y + 74)

    def _update_chunk_tile(self, idx):
        """Update one tile's border and status label without rebuilding the filmstrip."""
        tile = self.tiles.get(idx)
        if tile is None:
            return
        color = self.m.chunk_color(idx)
        self.grid.itemconfig(tile, outline=color)
        # The status text is tagged per chunk so only that label changes.
        self.grid.itemconfig("chunk_status_%d" % idx,
                             text=self.m.color_key(self.m.chunks.get(idx, {})).upper(),
                             fill=color)

    def _prepare_video_thumbnails(self, path):
        """Extract representative frames for byte-range chunks using ffprobe/ffmpeg.

        These are visual previews associated with each byte chunk, not independently
        playable video segments. The backup protocol continues storing raw byte ranges.
        """
        self.is_video = os.path.splitext(path)[1].lower() in {
            ".mp4", ".mkv", ".avi", ".mov", ".webm", ".flv", ".wmv", ".m4v"
        }
        self.thumbnails = {}
        self.video_duration = None
        self.thumbnail_dir = None
        self._draw_grid()
        if not self.is_video or not shutil.which("ffmpeg") or not shutil.which("ffprobe"):
            return

        chunk_bytes = max(1, int(self.chunk_var.get()) * 1024)
        total = math.ceil(os.path.getsize(path) / chunk_bytes)
        cache_key = str(abs(hash((os.path.abspath(path), os.path.getsize(path), os.path.getmtime(path)))))
        out_dir = os.path.join(WORKDIR, "thumbnails", cache_key)
        self.thumbnail_dir = out_dir

        def work():
            os.makedirs(out_dir, exist_ok=True)
            probe = subprocess.run(
                ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                 "-of", "default=noprint_wrappers=1:nokey=1", path],
                capture_output=True, text=True)
            try:
                duration = float(probe.stdout.strip())
            except (ValueError, TypeError):
                duration = 0.0
            if duration <= 0:
                self.ui_msg("Could not read video duration; showing chunk tiles without thumbnails.")
                return

            self.video_duration = duration
            # Keep preview generation responsive for very large videos. Every chunk
            # still gets a tile; when there are >300 chunks, nearby tiles reuse
            # the nearest sampled representative frame.
            preview_stride = max(1, math.ceil(total / 300))
            for idx in range(0, total, preview_stride):
                # Map byte position to a representative point in the video's duration.
                timestamp = min(duration - 0.05, (idx + 0.5) / total * duration)
                out = os.path.join(out_dir, "chunk_%06d.png" % idx)
                if not os.path.exists(out):
                    result = subprocess.run(
                        ["ffmpeg", "-loglevel", "error", "-y", "-ss", "%.3f" % timestamp,
                         "-i", path, "-frames:v", "1", "-vf", "scale=124:68:force_original_aspect_ratio=decrease,"
                         "pad=124:68:(ow-iw)/2:(oh-ih)/2", out],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if result.returncode != 0:
                        continue
                self.thumbnails[idx] = out
                if idx % (8 * preview_stride) == 0:
                    self.root.after(0, self._draw_grid)
            self.root.after(0, self._draw_grid)
            self.ui_msg("Created %d representative video preview frames for %d chunks." %
                        (len(self.thumbnails), total))

        threading.Thread(target=work, daemon=True).start()

    def _hover(self, e):
        x, y = self.grid.canvasx(e.x), self.grid.canvasy(e.y)
        idx = None
        for chunk_idx, (x1, y1, x2, y2) in self.tile_rects.items():
            if x1 <= x <= x2 and y1 <= y <= y2:
                idx = chunk_idx
                break
        text = self.m.tooltip(idx) if idx is not None else None
        if text:
            self.tip.config(text=text)
            self.tip.place(x=min(e.x + 14, max(0, self.grid.winfo_width() - 190)), y=e.y + 14)
        else:
            self.tip.place_forget()

    # -------------------------------------------------------------- servers
    def _draw_servers(self):
        for name, (frame, title, info) in self.srv_widgets.items():
            st = self.m.servers.get(name, {}).get("state", "unknown")
            reason = self.m.servers.get(name, {}).get("reason", "")
            active = self.m.active == name and st != "down"
            color = {"up": "#065f46", "down": "#991b1b"}.get(st, PANEL)
            if active:
                color = "#15803d"
            for w in (frame, title, info):
                w.config(bg=color)
            badge = "ACTIVE" if active else ("DOWN" if st == "down" else st.upper())
            info.config(text="%s | %s\nchunks held: %d%s" % (
                self.m.role(name), badge, self.m.holders_count(name),
                ("\n(" + reason + ")") if st == "down" and reason else ""))

    # ------------------------------------------------------------ failover view
    def _draw_failover(self):
        """Show the actual primary->standby promotion and a short transition history."""
        c = self.failover_canvas
        c.delete("all")
        w = max(c.winfo_width(), 360)
        current = self.failover_events[-1] if self.failover_events else None
        active = self.m.active or "h3"
        primary_state = self.m.servers.get("h3", {}).get("state", "unknown")
        standby_state = self.m.servers.get("h2", {}).get("state", "unknown")

        def server_box(x, label, state, is_active):
            color = "#166534" if is_active else ("#991b1b" if state == "down" else "#374151")
            c.create_rectangle(x, 8, x + 112, 48, fill=color, outline="#9ca3af", width=1)
            c.create_text(x + 56, 20, text=label, fill="white", font=("TkDefaultFont", 10, "bold"))
            badge = "ACTIVE" if is_active else state.upper()
            c.create_text(x + 56, 36, text=badge, fill="#f9fafb", font=("TkDefaultFont", 8, "bold"))

        left_x = max(12, int(w * 0.12))
        right_x = max(left_x + 170, int(w * 0.58))
        server_box(left_x, "h3 · PRIMARY", primary_state, active == "h3")
        server_box(right_x, "h2 · STANDBY", standby_state, active == "h2")

        if current:
            src, dst = current["src"], current["dst"]
            if src == "h2" and dst == "h3":
                arrow_start, arrow_end = right_x - 8, left_x + 118
            else:
                arrow_start, arrow_end = left_x + 118, right_x - 8
            c.create_line(arrow_start, 28, arrow_end, 28, fill="#38bdf8", width=3,
                          arrow=tk.LAST, arrowshape=(10, 12, 4))
            label = "FAILBACK" if current["type"] == "failback" else "FAILOVER"
            c.create_text((left_x + right_x + 112) / 2, 12, text=label,
                          fill="#fbbf24", font=("TkDefaultFont", 8, "bold"))
            detail = "%s → %s" % (src, dst)
            if current.get("seconds") is not None:
                detail += "   |   recovery %.3f s" % current["seconds"]
            c.create_text(w / 2, 61, text=detail, fill=FG, font=("TkDefaultFont", 9, "bold"))
        else:
            c.create_text(w / 2, 68, text="Waiting for a failover / failback event…",
                          fill="#9ca3af", font=("TkDefaultFont", 9))

        if self.failover_events:
            recent = self.failover_events[-4:]
            labels = []
            for item in recent:
                kind = "FB" if item["type"] == "failback" else "FO"
                labels.append("%s %s→%s" % (kind, item["src"], item["dst"]))
            c.create_text(8, 96, anchor="w", text="Recent: " + "   |   ".join(labels),
                          fill="#93c5fd", font=("TkDefaultFont", 8))

    # --------------------------------------------------------------- topology
    def _draw_topo(self):
        c = self.topo
        c.delete("all")
        w, h = max(c.winfo_width(), 320), max(c.winfo_height(), 220)
        sx, sy = w / 540.0, h / 335.0            # base drawing is 540 x 335; scale it to the canvas
        r = max(13, int(18 * min(sx, sy)))

        def P(name):
            x, y = NODES[name]
            return x * sx + 4, y * sy

        for hn, sn in (("h1", "s1"), ("h2", "s3"), ("h3", "s4")):
            c.create_line(*P(hn), *P(sn), fill="#6b7280", width=2)
        on_path = set()
        for a, b in zip(self.m.path, self.m.path[1:]):
            on_path.add(link_key(a, b))
        for a, b in LINKS:
            k = link_key(a, b)
            (x1, y1), (x2, y2) = P(SW[a]), P(SW[b])
            if k in self.m.links_down:
                c.create_line(x1, y1, x2, y2, fill="#ef4444", width=4, dash=(6, 4))
                c.create_text((x1 + x2) / 2 + 16, (y1 + y2) / 2 - 10, text="DOWN", fill="#fca5a5",
                              font=("TkDefaultFont", 8, "bold"))
            else:
                u = self.m.link_util.get(k, 0.0)
                col = "#22c55e" if u < 0.3 else ("#f59e0b" if u < 0.7 else "#ef4444")
                if k in on_path:
                    c.create_line(x1, y1, x2, y2, fill="#3b82f6", width=9)
                c.create_line(x1, y1, x2, y2, fill=col, width=4)
                c.create_text((x1 + x2) / 2 + 16, (y1 + y2) / 2 - 10, text="%d%%" % int(u * 100), fill=FG)
        for name in NODES:
            x, y = P(name)
            is_host = name.startswith("h")
            fill = "#7c3aed" if is_host else "#0ea5e9"
            if is_host and self.m.servers.get(name, {}).get("state") == "down":
                fill = "#ef4444"
            elif name in ("h2", "h3") and self.m.active == name:
                fill = "#16a34a"
            c.create_oval(x - r, y - r, x + r, y + r, fill=fill, outline="white")
            c.create_text(x, y, text=name, fill="white", font=("TkDefaultFont", 9, "bold"))
        for name, text, dy in (("h1", "client", r + 10), ("h3", "primary", r + 10), ("h2", "standby", -(r + 10))):
            x, y = P(name)
            c.create_text(x, y + dy, text=text, fill="#9ca3af")

    # ---------------------------------------------------------------- chart
    def _draw_chart(self):
        c = self.chart
        c.delete("all")
        w, h = max(c.winfo_width(), 300), max(c.winfo_height(), 100)
        vals = self.m.throughput()
        peak = max(max(vals), 1.0)
        pts = []
        for i, v in enumerate(vals):
            pts += [10 + i * (w - 20) / (len(vals) - 1), h - 14 - v / peak * (h - 30)]
        c.create_line(10, h - 14, w - 10, h - 14, fill="#374151")
        c.create_line(*pts, fill="#22c55e", width=2)
        c.create_text(12, 8, anchor="w", text="%.1f Mbps peak" % peak, fill="#9ca3af")
        c.create_text(w - 10, 8, anchor="e", text="now: %.1f Mbps" % vals[-1], fill=FG)

    # ---------------------------------------------------------------- actions
    def clear_log(self):
        self.tree.delete(*self.tree.get_children())

    def ui_msg(self, msg):
        self.q.put({"type": "ui_msg", "msg": msg, "ts": time.time()})

    # ---- CSV export of test results -------------------------------------
    def _ask_save(self, default):
        return filedialog.asksaveasfilename(defaultextension=".csv", initialfile=default,
                                            filetypes=[("CSV", "*.csv")])

    def export_results(self):
        """One row per backup/restore run: time, throughput, re-sent chunks, failover time, hash check."""
        if not os.path.exists(RESULTS_FILE):
            return messagebox.showinfo("Export", "No results yet. Run a backup first.")
        dst = self._ask_save("backup_results.csv")
        if dst:
            shutil.copyfile(RESULTS_FILE, dst)
            self.ui_msg("saved results to %s" % dst)

    def export_events(self):
        """Full timeline: failovers, server up/down, link failures, reroute times (ms)."""
        dst = self._ask_save("backup_events.csv")
        if dst:
            n = events_to_csv(EVENT_FILE, dst)
            self.ui_msg("saved %d events to %s" % (n, dst))

    def export_chunks(self):
        """The chunk map of the current backup: state, servers holding it, retries, SHA-256."""
        if not self.m.chunks:
            return messagebox.showinfo("Export", "No backup on screen yet.")
        dst = self._ask_save("chunk_map_%s.csv" % (self.m.file_id or "backup")[:8])
        if dst:
            with open(dst, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["chunk", "state", "size_bytes", "stored_on", "retries", "sha256"])
                w.writerows(self.m.chunk_rows())
            self.ui_msg("saved %d chunk rows to %s" % (len(self.m.chunks), dst))

    def make_file(self):
        try:
            mb = max(1, int(float(self.size_var.get())))
        except ValueError:
            return messagebox.showerror("Size", "Enter the size in MB")
        os.makedirs(WORKDIR, exist_ok=True)
        path = os.path.join(WORKDIR, "testfile_%dMB.bin" % mb)

        def work():
            with open(path, "wb") as f:
                for _ in range(mb):
                    f.write(os.urandom(1024 * 1024))
            self.root.after(0, lambda: self.file_path.set(path))
            self.ui_msg("created %s" % path)

        threading.Thread(target=work, daemon=True).start()

    def choose_file(self):
        """Pick a video (or any file). Videos are backed up as raw bytes, chunk by chunk."""
        p = filedialog.askopenfilename(
            initialdir=os.path.expanduser("~"),
            filetypes=[("Video", "*.mp4 *.mkv *.avi *.mov *.webm *.flv *.wmv *.m4v"), ("All files", "*.*")])
        if p:
            self.file_path.set(p)
            self._prepare_video_thumbnails(p)
            mb = os.path.getsize(p) / 1048576.0
            self.ui_msg("selected %s (%.1f MB, about %d chunks of %s KB)" % (
                os.path.basename(p), mb, int(mb * 1024 / max(1, int(self.chunk_var.get()))) + 1,
                self.chunk_var.get()))

    def make_video(self):
        """Create a small test video with ffmpeg (if installed): a moving test pattern with a clock."""
        if not shutil.which("ffmpeg"):
            return messagebox.showinfo(
                "ffmpeg missing", "Install it with:  sudo apt install -y ffmpeg\n"
                "or press 'Choose video…' and pick any video file.")
        os.makedirs(WORKDIR, exist_ok=True)
        path = os.path.join(WORKDIR, "test_video.mp4")
        self.ui_msg("making test video (about 20 seconds of 720p)…")

        def work():
            r = subprocess.run(
                ["ffmpeg", "-y", "-f", "lavfi", "-i", "testsrc2=size=1280x720:rate=30",
                 "-t", "20", "-c:v", "mpeg4", "-q:v", "2", "-pix_fmt", "yuv420p", path],
                capture_output=True)
            if r.returncode == 0:
                self.root.after(0, lambda: (self.file_path.set(path), self._prepare_video_thumbnails(path)))
                self.ui_msg("created %s (%.1f MB)" % (path, os.path.getsize(path) / 1048576.0))
            else:
                self.ui_msg("ffmpeg failed: " + r.stderr.decode(errors="replace")[-200:])

        threading.Thread(target=work, daemon=True).start()

    def play_restored(self):
        """Open the restored video in the default player (proof the video survived the failover)."""
        p = getattr(self, "last_restored", None)
        if not p or not os.path.exists(p):
            return messagebox.showinfo("Play", "Restore a backup first.")
        if not shutil.which("xdg-open"):
            return messagebox.showinfo("Play", "Open this file yourself:\n" + p)
        subprocess.Popen(["xdg-open", p], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.ui_msg("playing " + p)

    def backup(self):
        if not self.file_path.get():
            return messagebox.showinfo("Backup", "Make or choose a file first.")
        if self.is_video:
            # Rebuild previews if the chosen chunk size changed since selection.
            self._prepare_video_thumbnails(self.file_path.get())
        self.cmd({"cmd": "backup", "file": self.file_path.get(), "chunk": int(self.chunk_var.get()) * 1024})

    def restore(self):
        fid = self.m.last_file_id
        if not fid:
            return messagebox.showinfo("Restore", "Run a backup first.")
        ext = os.path.splitext(self.m.name or "")[1] or ".bin"      # keep .mp4 etc. so it plays
        self.last_restored = os.path.join(WORKDIR, "restored_%s%s" % (fid[:8], ext))
        self.cmd({"cmd": "restore", "file_id": fid, "out": self.last_restored})

    def arm_crash(self):
        """h3 will crash BY ITSELF while handling its Nth chunk. Nobody presses Kill during the transfer."""
        try:
            n = max(0, int(self.crash_var.get()))
            os.makedirs(WORKDIR, exist_ok=True)
            with open(os.path.join(WORKDIR, "crash_h3"), "w") as f:
                f.write(str(n))
            self.ui_msg("armed: h3 will crash by itself when chunk %d arrives. Now press Backup." % n)
        except (OSError, ValueError) as e:
            self.ui_msg("could not arm auto-crash (try: sudo chmod -R 777 /tmp/sdnbackup): %s" % e)

    def link(self, state):
        a, b = self.link_var.get().split("-")
        self.cmd({"cmd": "link", "a": a, "b": b, "state": state})

    def cmd(self, payload):
        def work():
            try:
                with socket.create_connection(CTRL, timeout=3) as s:
                    s.sendall((json.dumps(payload) + "\n").encode())
                    s.settimeout(10)
                    reply = json.loads(s.makefile().readline() or "{}")
                self.ui_msg("%s: %s" % ("ok" if reply.get("ok") else "ERROR", reply.get("msg")))
            except OSError:
                self.ui_msg("Mininet runner not reachable on 127.0.0.1:9998 (start topology/topology.py)")

        threading.Thread(target=work, daemon=True).start()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay", action="store_true", help="replay events already in the event file")
    a = ap.parse_args()
    root = tk.Tk()
    App(root, a.replay)
    root.mainloop()


if __name__ == "__main__":
    main()
