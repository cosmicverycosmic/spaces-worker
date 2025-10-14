#!/usr/bin/env python3
# Generates WEBVTT + a syncable grouped transcript HTML from crawler JSONL.
# Inputs (env):
#   ARTDIR     — output dir (same place crawler wrote files)
#   BASE       — filename base (e.g., space-05-10-2025-<id>)
#   CC_JSONL   — path to captions JSONL (crawler output)
#   SHIFT_SECS — seconds trimmed from the head of audio (float). REQUIRED when head-trim is applied.
# Notes:
# - We ALWAYS subtract SHIFT_SECS from cue times so trimmed audio and captions stay aligned.
# - We DO NOT trim internal silence; only head/tail, matching your FFmpeg step.

import json, os, re, html, sys
from datetime import datetime, timezone

artdir = os.environ.get("ARTDIR") or ""
base   = os.environ.get("BASE") or ""
src    = os.environ.get("CC_JSONL") or ""
shift  = float(os.environ.get("SHIFT_SECS") or "0")

if not (artdir and base and src and os.path.isfile(src)):
    # Nothing to do; exit cleanly
    sys.exit(0)

# ---------- Helpers ----------

def parse_time_iso(s):
    """Parse an ISO-ish timestamp into epoch seconds (float) or None."""
    if not s:
        return None
    s = s.strip()
    try:
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        # Normalize +HHMM -> +HH:MM
        if re.search(r'[+-]\d{4}$', s):
            s = s[:-5] + s[-5:-2] + ':' + s[-2:]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

def to_float(x):
    try:
        if x is None: return None
        f = float(x)
        # Accept milliseconds if big:
        return f/1000.0 if f > 86400 else f
    except Exception:
        return None

def fmt_ts(t):
    """Format seconds -> HH:MM:SS.mmm for VTT."""
    if t < 0:
        t = 0.0
    h = int(t // 3600)
    m = int((t % 3600) // 60)
    s = t % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"

def clean_name(s):
    s = (s or "").strip()
    s = re.sub(r'[<>&]', '', s)
    # strip surrogate junk
    s = ''.join(ch for ch in s if (ord(ch) < 0x1F000 and not (0xD800 <= ord(ch) <= 0xDFFF)))
    return s or "Speaker"

def esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def norm_text(s: str) -> str:
    if not s: return ""
    s = s.replace("\u2028", " ").replace("\u2029", " ")
    s = re.sub(r'\s+', ' ', s).strip()
    # Drop obvious junk-only lines
    if s.lower() in {"n", "uh", "um"}:
        return ""
    return s

def first(*vals):
    for v in vals:
        if v not in (None, ""):
            return v
    return None

# ---------- Ingest ----------

raw_utts = []
with open(src, 'r', encoding='utf-8', errors='ignore') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        # Try twspace-crawler layered payloads first
        obj = None
        try:
            obj = json.loads(line)
        except Exception:
            obj = None

        layer = None
        if isinstance(obj, dict) and isinstance(obj.get("payload"), str):
            try:
                pl = json.loads(obj["payload"])
                if isinstance(pl, dict) and isinstance(pl.get("body"), str):
                    try:
                        layer = json.loads(pl["body"])
                        sender = pl.get("sender") or {}
                    except Exception:
                        layer = None
                        sender = {}
                else:
                    layer = None
                    sender = {}
            except Exception:
                layer = None
                sender = {}
        else:
            layer = obj
            sender = {}

        # Accept multiple shapes:
        # Shape A: layer["type"]==45, text in layer["body"], ISO time in programDateTime or ms in timestamp
        # Shape B: simple caption objects with start/startMs, end/endMs, text, username/displayName
        def append_item(ts_abs, txt, disp, uname, avatar):
            if ts_abs is None or txt is None:
                return
            txt2 = norm_text(txt)
            if not txt2:
                return
            raw_utts.append({
                "ts": float(ts_abs),
                "text": txt2,
                "name": clean_name(disp),
                "username": (uname or "").lstrip("@"),
                "avatar": avatar or "",
                # Optional end if present; we’ll fill otherwise
                "end_ts": None
            })

        # Try to read "Shape A"
        if isinstance(layer, dict) and layer:
            ttype = layer.get("type")
            txt   = first(layer.get("body"), layer.get("text"), layer.get("caption"))
            disp  = first(layer.get("displayName"), sender.get("display_name"), layer.get("speaker_name"), layer.get("speakerName"))
            uname = first(layer.get("username"), sender.get("screen_name"), layer.get("user_id"))
            avat  = first(sender.get("profile_image_url_https"), sender.get("profile_image_url"))

            ts_abs = first(
                to_float(layer.get("timestamp")),
                parse_time_iso(layer.get("programDateTime")),
                to_float(layer.get("start")), to_float(layer.get("startSec")), to_float(layer.get("startMs")), to_float(layer.get("ts")), to_float(layer.get("offset"))
            )
            te_abs = first(to_float(layer.get("end")), to_float(layer.get("endSec")), to_float(layer.get("endMs")))

            # Only accept type-less if fields look like captions,
            # or type==45 which twspace uses for speech chunks.
            if txt and (ttype is None or ttype == 45 or any(k in layer for k in ("start","startMs","timestamp","programDateTime"))):
                if ts_abs is not None:
                    raw_utts.append({
                        "ts": float(ts_abs),
                        "text": norm_text(txt),
                        "name": clean_name(disp or uname or "Speaker"),
                        "username": (uname or "").lstrip("@"),
                        "avatar": avat or "",
                        "end_ts": float(te_abs) if te_abs is not None else None
                    })
                continue

        # If nothing matched above, try a very permissive fallback
        if isinstance(obj, dict):
            txt = first(obj.get("text"), obj.get("caption"), obj.get("payloadText"))
            ts_abs = first(
                to_float(obj.get("timestamp")),
                parse_time_iso(obj.get("programDateTime")),
                to_float(obj.get("start")), to_float(obj.get("startMs")), to_float(obj.get("ts"))
            )
            disp  = first(obj.get("displayName"), obj.get("speaker"), obj.get("user"), obj.get("name"))
            uname = first(obj.get("username"), obj.get("handle"), obj.get("screen_name"))
            avat  = first(obj.get("profile_image_url_https"), obj.get("profile_image_url"))
            if txt and ts_abs is not None:
                append_item(ts_abs, txt, disp or uname or "Speaker", uname, avat)

# ---------- Normalize timeline ----------

if not raw_utts:
    # Make an empty VTT + empty transcript so downstream doesn’t break
    os.makedirs(artdir, exist_ok=True)
    open(os.path.join(artdir, f"{base}.vtt"), "w", encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(artdir, f"{base}_transcript.html"), "w", encoding="utf-8").write("")
    sys.exit(0)

# Sort by absolute time; enforce strictly increasing by tiny epsilon
raw_utts.sort(key=lambda x: x["ts"])
EPS = 0.0005
last_ts = -1e9
for u in raw_utts:
    if u["ts"] <= last_ts:
        u["ts"] = last_ts + EPS
    last_ts = u["ts"]

t0_abs = raw_utts[0]["ts"]

# Build rel times, subtract the head trim SHIFT_SECS so 0 aligns with trimmed audio
utts = []
for u in raw_utts:
    st_rel = (u["ts"] - t0_abs) - shift
    if st_rel < 0: st_rel = 0.0
    utts.append({
        "start_rel": st_rel,
        "end_rel": None,  # fill below
        "text": u["text"],
        "name": u["name"],
        "username": u["username"],
        "avatar": u["avatar"],
    })

# Fill end times from next start or heuristics; enforce durations/separations
MIN_DUR = 0.80     # sec
MAX_DUR = 10.0     # cap a single cue
GUARD   = 0.020    # 20 ms separation

for i, u in enumerate(utts):
    if i + 1 < len(utts):
        nxt = utts[i + 1]["start_rel"]
        dur = max(MIN_DUR, min(MAX_DUR, (nxt - u["start_rel"]) - GUARD))
        if u["start_rel"] + dur <= u["start_rel"]:  # pathological
            dur = MIN_DUR
        u["end_rel"] = u["start_rel"] + dur
    else:
        # last: heuristic from words
        words = max(1, len(u["text"].split()))
        dur = max(MIN_DUR, min(MAX_DUR, 0.33 * words + 0.7))
        u["end_rel"] = u["start_rel"] + dur

# Merge adjacent same-speaker fragments if close together
MERGE_GAP = 3.0
groups = []
cur = None
for u in utts:
    if (cur is not None and
        u["name"] == cur["name"] and
        u["username"] == cur["username"] and
        (u["start_rel"] - cur["end_rel"]) <= MERGE_GAP):
        sep = "" if re.search(r'[.!?]"?$', cur["text"]) else " "
        cur["text"] = (cur["text"] + sep + u["text"]).strip()
        cur["end_rel"] = max(cur["end_rel"], u["end_rel"])
    else:
        cur = {
            "name": u["name"],
            "username": u["username"],
            "avatar": u["avatar"],
            "start_rel": u["start_rel"],
            "end_rel": u["end_rel"],
            "text": u["text"],
        }
        groups.append(cur)

# Re-seal invariants after merges: guard & min duration, strictly increasing starts
prev_end = 0.0
for g in groups:
    if g["start_rel"] < prev_end + GUARD:
        g["start_rel"] = prev_end + GUARD
    if g["end_rel"] < g["start_rel"] + MIN_DUR:
        g["end_rel"] = g["start_rel"] + MIN_DUR
    prev_end = g["end_rel"]

# ---------- Write WEBVTT ----------
os.makedirs(artdir, exist_ok=True)
vtt_path = os.path.join(artdir, f"{base}.vtt")
with open(vtt_path, "w", encoding="utf-8") as vf:
    vf.write("WEBVTT\n\n")
    for idx, g in enumerate(groups, 1):
        # Use <v Speaker> voice tag for better player support
        vf.write(f"{idx}\n{fmt_ts(g['start_rel'])} --> {fmt_ts(g['end_rel'])}\n")
        name = esc(g["name"])
        text = esc(g["text"])
        vf.write(f"<v {name}> {text}\n\n")

# ---------- Syncable transcript HTML ----------
css = '''
<style>
.ss3k-transcript{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
.ss3k-seg{display:flex;gap:10px;padding:8px 10px;border-radius:10px;margin:6px 0}
.ss3k-seg.active{background:#eef6ff;outline:1px solid #bfdbfe}
.ss3k-avatar{width:26px;height:26px;border-radius:50%;flex:0 0 26px;margin-top:3px;background:#e5e7eb}
.ss3k-meta{font-size:12px;color:#64748b;margin-bottom:2px}
.ss3k-name a{color:#0f172a;text-decoration:none}
.ss3k-text{white-space:pre-wrap;word-break:break-word;cursor:pointer}
</style>
'''
js = '''
<script>
(function(){
  function time(s){return parseFloat(s||'0')||0}
  function within(t,seg){return t>=time(seg.dataset.start) && t<time(seg.dataset.end)}
  function bind(){
    var audio=document.getElementById('ss3k-audio')||document.querySelector('audio[data-ss3k-player]');
    var cont=document.querySelector('.ss3k-transcript'); if(!audio||!cont) return;
    var segs=[].slice.call(cont.querySelectorAll('.ss3k-seg'));
    function tick(){
      var t=audio.currentTime||0, found=null;
      for(var i=0;i<segs.length;i++){ if(within(t,segs[i])){found=segs[i];break;} }
      segs.forEach(function(s){ s.classList.toggle('active', s===found); });
    }
    audio.addEventListener('timeupdate', tick);
    audio.addEventListener('seeked', tick);
    segs.forEach(function(s){
      s.addEventListener('click', function(){
        if(audio){ audio.currentTime=time(s.dataset.start)+0.05; audio.play().catch(function(){}); }
      });
    });
    tick();
  }
  if(document.readyState!=='loading') bind(); else document.addEventListener('DOMContentLoaded', bind);
})();
</script>
'''

tr_path = os.path.join(artdir, f"{base}_transcript.html")
with open(tr_path, "w", encoding="utf-8") as tf:
    tf.write(css)
    tf.write('<div class="ss3k-transcript">\n')
    for i, g in enumerate(groups, 1):
        name = g["name"]
        uname = (g.get("username") or "").strip().lstrip("@")
        prof = f"https://x.com/{html.escape(uname, True)}" if uname else ""
        avatar = g.get("avatar") or (f"https://unavatar.io/x/{html.escape(uname, True)}" if uname else "")
        if avatar and prof:
            avtag = f'<a href="{prof}" target="_blank" rel="noopener"><img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt=""></a>'
        elif avatar:
            avtag = f'<img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt="">'
        else:
            avtag = '<div class="ss3k-avatar" aria-hidden="true"></div>'
        if prof:
            name_html = f'<span class="ss3k-name"><a href="{prof}" target="_blank" rel="noopener"><strong>{html.escape(name, True)}</strong></a></span>'
        else:
            name_html = f'<span class="ss3k-name"><strong>{html.escape(name, True)}</strong></span>'

        tf.write(
            f'<div class="ss3k-seg" id="seg-{i:04d}" data-start="{g["start_rel"]:.3f}" '
            f'data-end="{g["end_rel"]:.3f}" data-speaker="{html.escape(name, True)}"'
            f'{(" data-handle=\"@"+html.escape(uname, True)+"\"" if uname else "")}>'
        )
        tf.write(avtag)
        tf.write('<div class="ss3k-body">')
        tf.write(f'<div class="ss3k-meta">{name_html} · <time>{fmt_ts(g["start_rel"])}</time>–<time>{fmt_ts(g["end_rel"])}</time></div>')
        tf.write(f'<div class="ss3k-text">{html.escape(g["text"], True)}</div>')
        tf.write('</div></div>\n')
    tf.write('</div>\n')
    tf.write(js)

# Emit earliest absolute timestamp (useful for WP date)
start_iso = datetime.fromtimestamp(raw_utts[0]["ts"], timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')
with open(os.path.join(artdir, f"{base}.start.txt"), "w", encoding="utf-8") as sf:
    sf.write(start_iso + "\n")
