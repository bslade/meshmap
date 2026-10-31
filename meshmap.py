#!/usr/bin/env python3
"""
meshmap.py - live web dashboard for a Meshtastic node on a serial port.
Shows mesh activity and every node's position on a map, updating as packets arrive.
Node icons on the map change to green when new packet is received from that node (then age to yellow, then grey)

Install:
    pip install meshtastic
    pip install pypubsub     (NOT pubsub.  That's a different, unrelated module)
Run:
    python mesh_map.py                      # auto-detect the radio, opens http://localhost:8080
    python mesh_map.py /dev/ttyUSB0         # Linux  (COM5 etc. on Windows)
    python mesh_map.py --tcp 192.168.1.50   # radio on Wi-Fi instead of USB
    python mesh_map.py --http-port 9000 --no-browser
    python mesh_map.py --listen 0.0.0.0     # let other devices on your LAN view the map

The map tiles come from OpenStreetMap, so the browser needs an internet connection.

Original verison of the program written by the Claude AI.  Tuned up by Ben Slade. Oct 2026
"""

import argparse
import json
import math
import sys
import threading
import time
import webbrowser
from collections import Counter, defaultdict, deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Note, TO INSTALL THIS USE: pip install pypubsub
# Do not do "pip install pubsub". That is an unrelated module with an unfortunate name
#
from pubsub import pub

MAX_EVENTS = 100      # activity entries kept in memory
TRAIL_POINTS = 50     # position history kept per node (drawn as a track)

lock = threading.Lock()
events = deque(maxlen=MAX_EVENTS)
port_counts = Counter()
trails = defaultdict(lambda: deque(maxlen=TRAIL_POINTS))   # node num -> [(lat, lon, t)]
last_packet = {}                                           # node num -> (time, type)
status = {"text": "Connecting to radio...", "connected": False,
          "started": time.time(), "total": 0}
iface = None


# ---------- location helpers ----------

def get_latlon(pos):
    """Return (lat, lon) from a position dict, or None if there's no fix."""
    if not pos:
        return None
    lat, lon = pos.get("latitude"), pos.get("longitude")
    if lat is None and "latitudeI" in pos:      # raw form is degrees * 1e7
        lat = pos["latitudeI"] / 1e7
    if lon is None and "longitudeI" in pos:
        lon = pos["longitudeI"] / 1e7
    if lat is None or lon is None or (lat == 0 and lon == 0):
        return None
    return lat, lon


def distance_km(a, b):
    """Great-circle distance between two (lat, lon) points (haversine)."""
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    h = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 6371.0 * 2 * math.asin(math.sqrt(h))


def compass(a, b):
    """Compass direction from point a to point b, e.g. 'NE'."""
    lat1, lon1, lat2, lon2 = map(math.radians, (*a, *b))
    x = math.sin(lon2 - lon1) * math.cos(lat2)
    y = (math.cos(lat1) * math.sin(lat2)
         - math.sin(lat1) * math.cos(lat2) * math.cos(lon2 - lon1))
    deg = (math.degrees(math.atan2(x, y)) + 360) % 360
    return ["N", "NE", "E", "SE", "S", "SW", "W", "NW"][round(deg / 45) % 8]


def fmt_distance(km):
    return f"{km * 1000:.0f} m" if km < 1 else f"{km:.1f} km"


def my_info():
    return (iface.getMyNodeInfo() if iface else None) or {}


# ---------- packet handling ----------

def node_name(node_id):
    """Turn '!a1b2c3d4' into a friendly name using the radio's node DB."""
    if node_id == "^all":
        return "everyone"
    if iface and iface.nodes and node_id in iface.nodes:
        user = iface.nodes[node_id].get("user", {})
        return user.get("longName") or user.get("shortName") or node_id
    return node_id or "?"


def summarize(packet):
    """Return (type, detail text, (lat, lon) or None) for one received packet."""
    decoded = packet.get("decoded")
    if not decoded:
        return "ENCRYPTED", "couldn't decrypt (different channel key)", None

    port = decoded.get("portnum", "UNKNOWN")
    detail, ll = "", None

    if port == "TEXT_MESSAGE_APP":
        detail = decoded.get("text", "")
    elif port == "POSITION_APP":
        pos = decoded.get("position", {})
        ll = get_latlon(pos)
        if ll:
            detail = f"{ll[0]:.5f}, {ll[1]:.5f}"
            if "altitude" in pos:
                detail += f", alt {pos['altitude']} m"
            me = get_latlon(my_info().get("position"))
            if me and packet.get("from") != my_info().get("num"):
                detail += f" ({fmt_distance(distance_km(me, ll))} {compass(me, ll)} of you)"
        else:
            detail = "no GPS fix"
    elif port == "TELEMETRY_APP":
        tel = decoded.get("telemetry", {})
        dev = tel.get("deviceMetrics", {})
        env = tel.get("environmentMetrics", {})
        parts = []
        if "batteryLevel" in dev:
            parts.append(f"battery {dev['batteryLevel']}%")
        if "voltage" in dev:
            parts.append(f"{dev['voltage']:.2f} V")
        if "channelUtilization" in dev:
            parts.append(f"channel util {dev['channelUtilization']:.1f}%")
        if "temperature" in env:
            parts.append(f"{env['temperature']:.1f} °C")
        if "relativeHumidity" in env:
            parts.append(f"{env['relativeHumidity']:.0f}% RH")
        detail = ", ".join(parts)
    elif port == "NODEINFO_APP":
        user = decoded.get("user", {})
        detail = f"{user.get('longName', '')} ({user.get('hwModel', '')})"
    elif port == "ROUTING_APP":
        detail = decoded.get("routing", {}).get("errorReason", "")

    return port.replace("_APP", ""), detail, ll

import pprint

# This is a call back, setup by the pub.subscribe(on_receive, "meshtastic.receive") call, which is listening
# for messages sent by the meshtastic.serial_interface.SerialInterface thread
#
def on_receive(packet, interface):
    kind, detail, ll = summarize(packet)
    frm = packet.get("from")
    # print("*********************************************************")
    # print(kind,frm)
    # pprint.pprint(packet)
    node_id = packet.get("fromId")
    # if( node_id is not None ):
    #     remote_node = interface.getNode(node_id)
    #     print(f"on_receive: node_id={node_id}, attributes and methods are:")
    #     print(remote_node.showInfo())
    now = time.time()

    hops = None
    if "hopStart" in packet and "hopLimit" in packet:
        hops = packet["hopStart"] - packet["hopLimit"]

    with lock:
        status["total"] += 1
        port_counts[kind] += 1
        last_packet[frm] = (now, kind)
        if ll:
            trails[frm].append((ll[0], ll[1], now))
        events.appendleft({
            "t": now,
            "time": datetime.now().strftime("%H:%M:%S"),
            "fromNum": frm,
            "from": node_name(packet.get("fromId")),
            "to": node_name(packet.get("toId")),
            "type": kind,
            "snr": packet.get("rxSnr"),
            "rssi": packet.get("rxRssi"),
            "hops": hops,
            "detail": detail,
            "lat": ll[0] if ll else None,
            "lon": ll[1] if ll else None,
            "lastHeard": int(time.time())  # I'm not sure if this is supposed to be here. I think it is supposed to be auto-generated, but that doesn't always work
                                           # Doing this seems to work
        })


def on_connected(interface, topic=pub.AUTO_TOPIC):
    name = (interface.getMyNodeInfo() or {}).get("user", {}).get("longName", "local node")
    with lock:
        status["connected"] = True
        status["text"] = f"Connected to {name}"


def on_lost(interface, topic=pub.AUTO_TOPIC):
    with lock:
        status["connected"] = False
        status["text"] = "Connection to radio lost"


# ---------- state for the web page ----------

def build_state():
    me = my_info()
    my_num = me.get("num")
    my_ll = get_latlon(me.get("position"))
    now = time.time()
    raw_nodes = list((iface.nodes or {}).values()) if iface else []

    with lock:
        nodes = []
        for n in raw_nodes:
            num = n.get("num")
            user = n.get("user", {}) or {}
            pos = n.get("position", {}) or {}
            ll = get_latlon(pos)
            metrics = n.get("deviceMetrics", {}) or {}
            lp = last_packet.get(num)
            is_me = num == my_num
            node_id = user.get("id") or (f"!{num:08x}" if isinstance(num, int) else "?")
            # print(f"build_state: node_id={node_id}, {user.get("longName")}")
            # pprint.pprint(n)
            item = {
                "num": num,
                "id": node_id,
                "name": user.get("longName") or user.get("shortName") or node_id,
                "short": user.get("shortName") or node_id[-4:],
                "hw": user.get("hwModel", ""),
                "isMe": is_me,
                "lat": ll[0] if ll else None,
                "lon": ll[1] if ll else None,
                "alt": pos.get("altitude"),
                "posTime": pos.get("time"),
                "lastHeard": now if (is_me and status["connected"]) else n.get("lastHeard"),
                "snr": n.get("snr"),
                "hopsAway": n.get("hopsAway"),
                "battery": metrics.get("batteryLevel"),
                "voltage": metrics.get("voltage"),
                "lastPacket": lp[0] if lp else None,
                "lastType": lp[1] if lp else None,
                "trail": [[a, b] for a, b, _ in trails.get(num, [])],
                "distance": None,
                "bearing": None,
            }
            if ll and my_ll and not is_me:
                item["distance"] = fmt_distance(distance_km(my_ll, ll))
                item["bearing"] = compass(my_ll, ll)
            nodes.append(item)

        return {
            "now": now,
            "status": status["text"],
            "connected": status["connected"],
            "uptime": int(now - status["started"]),
            "total": status["total"],
            "counts": dict(port_counts.most_common()),
            "myNum": my_num,
            "nodes": nodes,
            "events": list(events)[:60],
        }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/index.html"):
            body, ctype = PAGE.encode("utf-8"), "text/html; charset=utf-8"
        elif self.path.startswith("/api/state"):
            body = json.dumps(build_state(), default=str).encode("utf-8")
            ctype = "application/json"
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):   # keep the console quiet
        pass


# ---------- the web page (HTML + Leaflet map) ----------

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
<title>Mesh Map</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  :root {
    --bg: #f6f7f9; --panel: #ffffff; --text: #1d2330; --muted: #6b7385;
    --line: #e3e6ec; --accent: #2f6fed;
    --fresh: #17a34a; --recent: #e0a100; --stale: #8b93a3; --me: #2f6fed;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #12151b; --panel: #1a1e26; --text: #e6e9ef; --muted: #8d95a6;
      --line: #2a303b; --accent: #5b8cff;
    }
    .leaflet-tile-pane { filter: brightness(.8) invert(1) contrast(.9) hue-rotate(200deg) saturate(.35); }
    .leaflet-popup-content-wrapper, .leaflet-popup-tip { background: var(--panel); color: var(--text); }
  }
  * { box-sizing: border-box; }
  html, body { margin: 0; height: 100%; background: var(--bg); color: var(--text);
    font: 14px/1.4 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
  .app { display: grid; grid-template-columns: 1fr 400px; height: 100vh; }
  #map { height: 100%; }
  aside { display: flex; flex-direction: column; min-height: 0; border-left: 1px solid var(--line);
    background: var(--panel); }
  .status { padding: 14px 16px; border-bottom: 1px solid var(--line); }
  .status h1 { font-size: 16px; margin: 0 0 6px; display: flex; align-items: center; gap: 8px; }
  .dot { width: 10px; height: 10px; border-radius: 50%; background: var(--stale); flex: none; }
  .dot.on { background: var(--fresh); box-shadow: 0 0 0 3px color-mix(in srgb, var(--fresh) 25%, transparent); }
  .sub { color: var(--muted); font-size: 13px; }
  .chips { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
  .chip { font-size: 12px; padding: 2px 8px; border-radius: 999px; background: var(--bg);
    border: 1px solid var(--line); }
  section { display: flex; flex-direction: column; min-height: 0; }
  section h2 { font-size: 12px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted);
    margin: 0; padding: 10px 16px 6px; }
  .nodes { max-height: 40%; border-bottom: 1px solid var(--line); }
  .feed { flex: 1; }
  .list { overflow-y: auto; padding: 0 8px 8px; }
  .row { padding: 8px; border-radius: 8px; cursor: default; }
  .row.click { cursor: pointer; }
  .row.click:hover { background: var(--bg); }
  .row .top { display: flex; justify-content: space-between; gap: 8px; }
  .row .name { font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .row .meta { color: var(--muted); font-size: 12px; white-space: nowrap; }
  .row .detail { font-size: 13px; overflow-wrap: anywhere; margin-top: 2px; }
  .badge { font-size: 11px; font-weight: 600; padding: 1px 6px; border-radius: 4px;
    background: var(--bg); border: 1px solid var(--line); }
  .badge.TEXT_MESSAGE { color: var(--fresh); }
  .badge.POSITION { color: var(--accent); }
  .badge.TELEMETRY { color: var(--recent); }
  .badge.NODEINFO { color: #a855f7; }
  .badge.ENCRYPTED { color: var(--muted); }
  .sw { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 6px; }
  .empty { color: var(--muted); padding: 8px; font-size: 13px; }

  .pin { width: 34px; height: 34px; border-radius: 50%; display: grid; place-items: center;
    color: #fff; font-size: 10px; font-weight: 700; border: 2px solid #fff;
    box-shadow: 0 1px 4px rgba(0,0,0,.4); background: var(--stale); }
  .pin span { max-width: 30px; overflow: hidden; text-overflow: clip; white-space: nowrap; }
  .pin.fresh { background: var(--fresh); } .pin.recent { background: var(--recent); }
  .pin.me { background: var(--me); width: 38px; height: 38px; }
  .pin.pulse { animation: pulse 1.2s ease-out infinite; }
  @keyframes pulse {
    0% { box-shadow: 0 0 0 0 rgba(47,111,237,.7); }
    100% { box-shadow: 0 0 0 18px rgba(47,111,237,0); }
  }
  .mapbtns { position: absolute; top: 10px; right: 10px; z-index: 1000; display: flex; gap: 6px; }
  .mapbtns button { font: inherit; font-size: 13px; padding: 6px 10px; border-radius: 6px;
    border: 1px solid var(--line); background: var(--panel); color: var(--text); cursor: pointer;
    box-shadow: 0 1px 3px rgba(0,0,0,.15); }
  .legend { position: absolute; bottom: 24px; left: 10px; z-index: 1000; background: var(--panel);
    border: 1px solid var(--line); border-radius: 6px; padding: 6px 10px; font-size: 12px; }
  .mapwrap { position: relative; }
  .popup b { font-size: 14px; } .popup td { padding: 1px 8px 1px 0; vertical-align: top; }
  .popup td:first-child { color: var(--muted); }

  @media (max-width: 800px) {
    .app { grid-template-columns: 1fr; grid-template-rows: 55vh 1fr; }
    aside { border-left: none; border-top: 1px solid var(--line); }
  }
</style>
</head>
<body>
<div class="app">
  <div class="mapwrap">
    <div id="map"></div>
    <div class="mapbtns">
      <button id="btnMe">Center on me</button>
      <button id="btnFit">Show all</button>
    </div>
    <div class="legend">
      <span class="sw" style="background:var(--me)"></span>You
      &nbsp;<span class="sw" style="background:var(--fresh)"></span>&lt;15 min
      &nbsp;<span class="sw" style="background:var(--recent)"></span>&lt;2 h
      &nbsp;<span class="sw" style="background:var(--stale)"></span>older
    </div>
  </div>
  <aside>
    <div class="status">
      <h1><span id="dot" class="dot"></span><span id="statusText">Starting…</span></h1>
      <div class="sub" id="myLoc"></div>
      <div class="sub" id="stats"></div>
      <div class="chips" id="chips"></div>
    </div>
    <section class="nodes">
      <h2 id="nodesTitle">Nodes</h2>
      <div class="list" id="nodeList"></div>
    </section>
    <section class="feed">
      <h2>Activity</h2>
      <div class="list" id="feed"><div class="empty">Waiting for packets…</div></div>
    </section>
  </aside>
</div>

<script>
const map = L.map('map').setView([20, 0], 2);
L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19, attribution: '&copy; OpenStreetMap contributors'
}).addTo(map);

const markers = new Map();   // num -> {marker, key}
const tracks = new Map();    // num -> polyline
let state = null, fitted = false;

const esc = s => String(s ?? '').replace(/[&<>"']/g,
  c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

function ago(t, now) {
  if (!t) return '—';
  const s = Math.max(0, now - t);
  if (s < 60) return Math.floor(s) + 's ago';
  if (s < 3600) return Math.floor(s / 60) + 'm ago';
  if (s < 86400) return Math.floor(s / 3600) + 'h ago';
  return Math.floor(s / 86400) + 'd ago';
}

function freshness(n, now) {
  if (n.isMe) return 'me';
  if (!n.lastPacket ) return 'stale';
  // if (!n.lastHeard) return 'stale';
  const s = now - n.lastPacket;
  // const s = now - n.lastHeard;
  return s < 900 ? 'fresh' : s < 7200 ? 'recent' : 'stale';
}

function popupHtml(n, now) {
  const rows = [
    ['ID', n.id], ['Hardware', n.hw], ['Last heard', ago(n.lastPacket, now)],
    // ['ID', n.id], ['Hardware', n.hw], ['Last heard', ago(n.lastHeard, now)],
    ['Last packet', n.lastType ? `${n.lastType}, ${ago(n.lastPacket, now)}` : null],
    ['From you', n.distance ? `${n.distance} ${n.bearing}` : null],
    ['SNR', n.snr != null ? n.snr + ' dB' : null],
    ['Hops away', n.hopsAway],
    ['Battery', n.battery != null ? n.battery + '%' : null],
    ['Voltage', n.voltage != null ? n.voltage.toFixed(2) + ' V' : null],
    ['Altitude', n.alt != null ? n.alt + ' m' : null],
    ['Position', `${n.lat.toFixed(5)}, ${n.lon.toFixed(5)}`],
    ['Position fix', n.posTime ? ago(n.posTime, now) : null],
  ].filter(r => r[1] !== null && r[1] !== undefined && r[1] !== '');
  const osm = `https://www.openstreetmap.org/?mlat=${n.lat}&mlon=${n.lon}#map=16/${n.lat}/${n.lon}`;
  return `<div class="popup"><b>${esc(n.name)}${n.isMe ? ' (you)' : ''}</b>
    <table>${rows.map(r => `<tr><td>${esc(r[0])}</td><td>${esc(r[1])}</td></tr>`).join('')}</table>
    <a href="${osm}" target="_blank" rel="noopener">Open in OpenStreetMap</a></div>`;
}

function updateMap(s) {
  const seen = new Set();
  for (const n of s.nodes) {
    if (n.lat == null || n.lon == null) continue;
    seen.add(n.num);
    const cls = freshness(n, s.now);
    const active = n.lastPacket && s.now - n.lastPacket < 15;
    const key = `${cls}|${active}|${n.short}`;
    let m = markers.get(n.num);
    const icon = () => L.divIcon({
      className: '', iconSize: [38, 38], iconAnchor: [19, 19], popupAnchor: [0, -18],
      html: `<div class="pin ${cls}${active ? ' pulse' : ''}"><span>${esc(n.short)}</span></div>`
    });
    if (!m) {
      const marker = L.marker([n.lat, n.lon], {icon: icon(), zIndexOffset: n.isMe ? 1000 : 0})
        .bindPopup(popupHtml(n, s.now)).addTo(map);
      m = {marker, key};
      markers.set(n.num, m);
    } else {
      m.marker.setLatLng([n.lat, n.lon]);
      if (m.key !== key) { m.marker.setIcon(icon()); m.key = key; }
      m.marker.setPopupContent(popupHtml(n, s.now));
    }
    // Movement track from received position packets
    if (n.trail.length >= 2) {
      const color = n.isMe ? '#2f6fed' : '#17a34a';
      if (tracks.has(n.num)) tracks.get(n.num).setLatLngs(n.trail);
      else tracks.set(n.num, L.polyline(n.trail, {color, weight: 3, opacity: .6, dashArray: '6 6'}).addTo(map));
    }
  }
  for (const [num, m] of markers) {
    if (!seen.has(num)) { map.removeLayer(m.marker); markers.delete(num); }
  }
  if (!fitted && markers.size) { fitAll(); fitted = true; }
}

function fitAll() {
  if (!markers.size) return;
  const b = L.latLngBounds([...markers.values()].map(m => m.marker.getLatLng()));
  map.fitBounds(b, {padding: [40, 40], maxZoom: 14});
}

function focusNode(num) {
  const m = markers.get(Number(num));
  if (m) { map.flyTo(m.marker.getLatLng(), Math.max(map.getZoom(), 14)); m.marker.openPopup(); }
}

function updatePanel(s) {
  document.getElementById('dot').className = 'dot' + (s.connected ? ' on' : '');
  document.getElementById('statusText').textContent = s.status;

  const me = s.nodes.find(n => n.isMe);
  document.getElementById('myLoc').textContent = me && me.lat != null
    ? `Your location: ${me.lat.toFixed(5)}, ${me.lon.toFixed(5)}` + (me.alt != null ? `, alt ${me.alt} m` : '')
    : 'Your location: unknown (no GPS fix or fixed position set)';
  const up = s.uptime;
  document.getElementById('stats').textContent =
    `${s.total} packets · ${s.nodes.length} nodes known · up ${Math.floor(up / 60)}m ${up % 60}s`;
  document.getElementById('chips').innerHTML = Object.entries(s.counts)
    .map(([k, v]) => `<span class="chip">${esc(k)}: ${v}</span>`).join('');

  const others = s.nodes.filter(n => !n.isMe).sort((a, b) => (b.lastPacket || 0) - (a.lastPacket || 0));
  // const others = s.nodes.filter(n => !n.isMe).sort((a, b) => (b.lastHeard || 0) - (a.lastHeard || 0));
  const located = others.filter(n => n.lat != null).length;
  document.getElementById('nodesTitle').textContent = `Nodes · ${located} of ${others.length} on map`;
  document.getElementById('nodeList').innerHTML = others.length ? others.map(n => {
    const cls = freshness(n, s.now);
    const bits = [ago(n.lastPacket, s.now)];
    // const bits = [ago(n.lastHeard, s.now)];
    if (n.distance) bits.push(`${n.distance} ${n.bearing}`);
    else if (n.lat == null) bits.push('no position');
    if (n.battery != null) bits.push(`${n.battery}%`);
    return `<div class="row ${n.lat != null ? 'click' : ''}" data-num="${n.num}">
      <div class="top"><span class="name"><span class="sw" style="background:var(--${cls})"></span>${esc(n.name)}</span>
      <span class="meta">${esc(bits.join(' · '))}</span></div></div>`;
  }).join('') : '<div class="empty">No other nodes yet.</div>';

  document.getElementById('feed').innerHTML = s.events.length ? s.events.map(e => {
    const sig = [e.snr != null ? `SNR ${e.snr}` : '', e.rssi != null ? `RSSI ${e.rssi}` : '',
                 e.hops != null ? `${e.hops} hop${e.hops === 1 ? '' : 's'}` : ''].filter(Boolean).join(' · ');
    return `<div class="row ${e.lat != null ? 'click' : ''}" data-num="${e.fromNum}">
      <div class="top"><span class="name">${esc(e.from)} → ${esc(e.to)}</span>
      <span class="meta">${esc(e.time)}</span></div>
      <div class="detail"><span class="badge ${esc(e.type)}">${esc(e.type)}</span> ${esc(e.detail)}</div>
      ${sig ? `<div class="meta">${esc(sig)}</div>` : ''}</div>`;
  }).join('') : '<div class="empty">Waiting for packets…</div>';
}

document.addEventListener('click', ev => {
  const row = ev.target.closest('.row.click');
  if (row) focusNode(row.dataset.num);
});
document.getElementById('btnFit').onclick = fitAll;
document.getElementById('btnMe').onclick = () => {
  const me = state && state.nodes.find(n => n.isMe && n.lat != null);
  if (me) focusNode(me.num);
};

async function tick() {
  try {
    const r = await fetch('/api/state', {cache: 'no-store'});
    state = await r.json();
    updateMap(state);
    updatePanel(state);
  } catch (e) {
    document.getElementById('dot').className = 'dot';
    document.getElementById('statusText').textContent = 'Lost contact with mesh_map.py';
  }
  setTimeout(tick, 2000);
}
tick();
</script>
</body>
</html>
"""


# ---------- main ----------

def main():
    global iface
    ap = argparse.ArgumentParser(description="Live Meshtastic activity map")
    ap.add_argument("--port", help="serial port, e.g. /dev/ttyUSB0 or COM5 (default: auto-detect)")
    ap.add_argument("--tcp", metavar="HOST", help="connect to a Wi-Fi radio at this address instead of serial")
    ap.add_argument("--http-port", type=int, default=8080, help="web page port (default 8080)")
    ap.add_argument("--listen", default="127.0.0.1",
                    help="address to serve on; use 0.0.0.0 to allow other devices on your network")
    ap.add_argument("--no-browser", action="store_true", help="don't open a browser automatically")
    args = ap.parse_args()

    # Subscribe to messages sent by a meshtastic.serial_interface.SerialInterface() thread started below
    pub.subscribe(on_receive, "meshtastic.receive")
    pub.subscribe(on_connected, "meshtastic.connection.established")
    pub.subscribe(on_lost, "meshtastic.connection.lost")

    # Start the web server first so the page shows "Connecting..." while the radio syncs
    server = ThreadingHTTPServer((args.listen, args.http_port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://localhost:{args.http_port}/"
    print(f"Map running at {url}  (Ctrl+C to quit)")
    if not args.no_browser:
        webbrowser.open(url)

    try:
        if args.tcp:
            import meshtastic.tcp_interface
            iface = meshtastic.tcp_interface.TCPInterface(hostname=args.tcp)
        else:
            import meshtastic.serial_interface
            # This spawns a thread that, when a packet is received by this computer (from the Meshtastic node), it calls pub.sendMessage() 
            # sending a message to the 'meshtastic.receive' pubsub queue. See pub.subscribe(on_receive, "meshtastic.receive") above
            iface = meshtastic.serial_interface.SerialInterface(devPath=args.port)
    except Exception as e:
        server.shutdown()
        sys.exit(f"Could not connect to the Meshtastic radio: {e}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        iface.close()
        server.shutdown()


if __name__ == "__main__":
    main()

