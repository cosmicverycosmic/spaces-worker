#!/usr/bin/env python3
# Generates WEBVTT + a syncable grouped transcript HTML from crawler JSONL.
# Inputs (env):
#   ARTDIR — output dir (same place crawler wrote files)
#   BASE   — filename base (e.g., space-05-10-2025-<id>)
#   CC_JSONL — path to captions JSONL (crawler output)

import json, os, re, html, sys
from datetime import datetime, timezone

artdir = os.environ.get("ARTDIR") or ""
base   = os.environ.get("BASE") or ""
src    = os.environ.get("CC_JSONL") or ""

if not (artdir and base and src and os.path.isfile(src)):
    # Nothing to do; exit cleanly
    sys.exit(0)

def parse_time_iso(s):
    """Parse an ISO-ish timestamp into epoch seconds (float) or None."""
    if not s:
        return None
    s = s.strip()
    try:
        # Normalize 'Z' to +00:00
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        # Normalize +HHMM to +HH:MM
        if re.search(r'[+-]\d{4}$', s):
            s = s[:-5] + s[-5:-2] + ':' + s[-2:]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
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
    s = s or ""
    s = re.sub(r'[<>&]', '', s)
    # strip non-BMP surrogate junk
    s = ''.join(ch for ch in s if (ord(ch) < 0x1F000 and not (0xD800 <= ord(ch) <= 0xDFFF)))
    s = s.strip()
    return s or "Speaker"

def esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

# Load utterances (type 45)
utt = []
with open(src, 'r', encoding='utf-8', errors='ignore') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        # Outer JSON
        try:
            outer = json.loads(line)
        except Exception:
            continue
        # JSON in "payload"
        payload_raw = outer.get("payload") or ""
        try:
            payload = json.loads(payload_raw)
        except Exception:
            continue
        # JSON in "body"
        body_raw = payload.get("body") or ""
        try:
            inner = json.loads(body_raw)
        except Exception:
            continue

        if inner.get("type") != 45:
            continue
        text = (inner.get("body") or "").strip()
        if not text:
            continue
        # skip non-final if flagged
        if "final" in inner and not inner.get("final"):
            continue

        name     = inner.get("displayName") \
                   or payload.get("sender", {}).get("display_name") \
                   or inner.get("username") \
                   or inner.get("user_id") \
                   or "Speaker"
        username = inner.get("username") \
                   or payload.get("sender", {}).get("screen_name") \
                   or ""
        avatar   = payload.get("sender", {}).get("profile_image_url_https") \
                   or payload.get("sender", {}).get("profile_image_url") \
                   or ""

        ts = parse_time_iso(inner.get("programDateTime"))
        if ts is None:
            try:
                ts = int(inner.get("timestamp")) / 1000.0
            except Exception:
                ts = None
        if ts is None:
            continue

        utt.append({
            "ts": ts,
            "name": clean_name(name),
            "username": username,
            "avatar": avatar,
            "text": text
        })

utt.sort(key=lambda x: x["ts"])
if not utt:
    sys.exit(0)

# Relative timings
t0_abs = utt[0]["ts"]
for i, u in enumerate(utt):
    u["start_rel"] = u["ts"] - t0_abs
    if i + 1 < len(utt):
        nxt = utt[i + 1]["ts"] - t0_abs
        # Leave small gap between cues
        u["end_rel"] = max(u["start_rel"] + 0.6, nxt - 0.1)
    else:
        # heuristic last segment duration
        words = max(1, len(u["text"].split()))
        u["end_rel"] = u["start_rel"] + min(6.0, max(1.2, 0.35 * words + 0.8))

# Group adjacent same-speaker fragments if close in time
groups = []
GAP_MAX = 4.0
cur = None
for u in utt:
    if (cur is not None and
        u["name"] == cur["name"] and
        u["username"] == cur["username"] and
        (u["start_rel"] - cur["end_rel"]) <= GAP_MAX):
        sep = "" if cur["text"].endswith(('.', '!', '?')) else " "
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

# Write WEBVTT
os.makedirs(artdir, exist_ok=True)
vtt_path = os.path.join(artdir, f"{base}.vtt")
with open(vtt_path, "w", encoding="utf-8") as vf:
    vf.write("WEBVTT\n\n")
    for idx, g in enumerate(groups, 1):
        vf.write(f"{idx}\n{fmt_ts(g['start_rel'])} --> {fmt_ts(g['end_rel'])}\n")
        vf.write(f"<b>{esc(g['name'])}</b>: {esc(g['text'])}\n\n")

# Syncable transcript HTML
css = '''
<style>
.ss3k-transcript{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
.ss3k-seg{display:flex;gap:10px;padding:8px 10px;border-radius:10px;margin:6px 0}
.ss3k-seg.active{background:#eef6ff;outline:1px solid #bfdbfe}
.ss3k-avatar{width:26px;height:26px;border-radius:50%;flex:0 0 26px;margin-top:3px}
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
    segs.forEach(function(s){
      s.addEventListener('click', function(){ if(audio){ audio.currentTime=time(s.dataset.start)+0.05; audio.play().catch(function(){}); }});
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
        uname = (g.get("username") or "").strip()
        prof = f"https://x.com/{html.escape(uname, True)}" if uname else ""
        avatar = g.get("avatar") or (f"https://unavatar.io/x/{html.escape(uname, True)}" if uname else "")
        if avatar and prof:
            avtag = f'<a href="{prof}" target="_blank" rel="noopener"><img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt=""></a>'
        elif avatar:
            avtag = f'<img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt="">'
        else:
            avtag = '<div style="width:26px;height:26px"></div>'
        if prof:
            name_html = f'<span class="ss3k-name"><a href="{prof}" target="_blank" rel="noopener"><strong>{html.escape(name, True)}</strong></a></span>'
        else:
            name_html = f'<span class="ss3k-name"><strong>{html.escape(name, True)}</strong></span>'

        tf.write(f'<div class="ss3k-seg" id="seg-{i:04d}" data-start="{g["start_rel"]:.3f}" data-end="{g["end_rel"]:.3f}" data-speaker="{html.escape(name, True)}">')
        tf.write(avtag)
        tf.write('<div class="ss3k-body">')
        tf.write(f'<div class="ss3k-meta">{name_html} · <time>{fmt_ts(g["start_rel"])}</time>–<time>{fmt_ts(g["end_rel"])}</time></div>')
        tf.write(f'<div class="ss3k-text">{html.escape(g["text"], True)}</div>')
        tf.write('</div></div>\n')
    tf.write('</div>\n')
    tf.write(js)

# Earliest absolute timestamp (fallback for WP date)
start_iso = datetime.fromtimestamp(utt[0]["ts"], timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')
with open(os.path.join(artdir, f"{base}.start.txt"), "w", encoding="utf-8") as sf:
    sf.write(start_iso + "\n")
