# file: .github/workflows/scripts/gen_vtt.py
#!/usr/bin/env python3
import json, os, re, html, sys, math
from datetime import datetime, timezone

artdir = os.environ.get("ARTDIR") or ""
base   = os.environ.get("BASE") or ""
src    = os.environ.get("CC_JSONL") or ""
shift_env  = float(os.environ.get("SHIFT_SECS") or "0")
trim_lead  = float(os.environ.get("TRIM_LEAD") or "0")   # audio was trimmed by this many seconds
START_ISO  = os.environ.get("START_ISO") or ""           # e.g., 2025-10-25T23:00:07Z
TOTAL_SHIFT = shift_env + trim_lead

if not (artdir and base and src and os.path.isfile(src)):
    os.makedirs(artdir, exist_ok=True)
    open(os.path.join(artdir,f"{base}.vtt"),"w",encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(artdir,f"{base}_emoji.vtt"),"w",encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(artdir,f"{base}_transcript.html"),"w",encoding="utf-8").write("")
    sys.exit(0)

def parse_time_iso(s: str):
    if not s: return None
    s = s.strip()
    try:
        # Allow trailing Z and compact +0000
        if s.endswith('Z'): s = s[:-1] + '+00:00'
        if re.search(r'[+-]\d{4}$', s):
            s = s[:-5] + s[-5:-2] + ':' + s[-2:]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except:
        return None

def parse_start_iso_env():
    ts = parse_time_iso(START_ISO)
    return ts

def to_float_seconds(x):
    """Heuristically parse numeric timestamps:
       - >= 1e12 : milliseconds since epoch → /1000
       - >= 1e9  : seconds since epoch      → as-is
       - else    : already relative seconds → as-is
    """
    try:
        if x is None: return None
        f = float(x)
        if f >= 1e12:   # ms epoch (e.g. 1690000000000)
            return f / 1000.0
        # Treat anything in [1e9, 1e12) as seconds-since-epoch (Unix time)
        # Otherwise assume it's already relative seconds.
        return f
    except:
        return None

def fmt_ts(t):
    if t < 0: t = 0.0
    # WebVTT permits hours >= 2 digits
    msec = int(round((t - math.floor(t)) * 1000))
    secs_i = int(math.floor(t))
    hh, rem = divmod(secs_i, 3600)
    mm, ss = divmod(rem, 60)
    return f"{hh:02d}:{mm:02d}:{ss:02d}.{msec:03d}"

def clean_name(s):
    s = (s or "").strip()
    s = re.sub(r'[<>&]', '', s)
    s = ''.join(ch for ch in s if (ord(ch) < 0x1F000 and not (0xD800 <= ord(ch) <= 0xDFFF)))
    return s or "Speaker"

def esc(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def norm_text(s):
    if not s: return ""
    s = s.replace("\u2028"," ").replace("\u2029"," ")
    s = re.sub(r'\s+',' ',s).strip()
    if s.lower() in {"n","uh","um"}: return ""
    return s

def first(*vals):
    for v in vals:
        if v not in (None, ""):
            return v
    return None

raw_utts = []
with open(src,'r',encoding='utf-8',errors='ignore') as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try:
            obj = json.loads(line)
        except:
            obj = None

        layer = None; sender = {}
        if isinstance(obj, dict) and isinstance(obj.get("payload"), str):
            try:
                pl = json.loads(obj["payload"])
                if isinstance(pl, dict) and isinstance(pl.get("body"), str):
                    try:
                        layer  = json.loads(pl["body"])
                        sender = pl.get("sender") or {}
                    except:
                        layer = None
            except:
                pass
        else:
            layer = obj

        def push(ts_abs, txt, disp, uname, avatar, te_abs=None):
            if ts_abs is None or not txt: return
            t2 = norm_text(txt)
            if not t2: return
            raw_utts.append({
                "ts": float(ts_abs),
                "text": t2,
                "name": clean_name(disp or uname or "Speaker"),
                "username": (uname or "").lstrip("@"),
                "avatar": avatar or "",
                "end_ts": float(te_abs) if te_abs is not None else None
            })

        if isinstance(layer, dict) and layer:
            ttype = layer.get("type")
            txt   = first(layer.get("body"), layer.get("text"), layer.get("caption"))
            disp  = first(layer.get("displayName"), (sender or {}).get("display_name"), layer.get("speaker_name"), layer.get("speakerName"))
            uname = first(layer.get("username"), (sender or {}).get("screen_name"), layer.get("user_id"))
            avat  = first((sender or {}).get("profile_image_url_https"), (sender or {}).get("profile_image_url"))

            ts_abs = first(
                to_float_seconds(layer.get("timestamp")),
                parse_time_iso(layer.get("programDateTime")),
                to_float_seconds(layer.get("start")), to_float_seconds(layer.get("startSec")), to_float_seconds(layer.get("startMs")),
                to_float_seconds(layer.get("ts")), to_float_seconds(layer.get("offset"))
            )
            te_abs = first(
                to_float_seconds(layer.get("end")),
                to_float_seconds(layer.get("endSec")),
                to_float_seconds(layer.get("endMs"))
            )

            if txt and (ttype is None or ttype == 45 or any(k in layer for k in ("start","startMs","timestamp","programDateTime","ts","offset"))):
                if ts_abs is not None:
                    push(ts_abs, txt, disp, uname, avat, te_abs)
                continue

        if isinstance(obj, dict):
            txt   = first(obj.get("text"), obj.get("caption"), obj.get("payloadText"))
            ts_abs = first(
                to_float_seconds(obj.get("timestamp")),
                parse_time_iso(obj.get("programDateTime")),
                to_float_seconds(obj.get("start")), to_float_seconds(obj.get("startMs")), to_float_seconds(obj.get("ts"))
            )
            disp  = first(obj.get("displayName"), obj.get("speaker"), obj.get("user"), obj.get("name"))
            uname = first(obj.get("username"), obj.get("handle"), obj.get("screen_name"))
            avat  = first(obj.get("profile_image_url_https"), obj.get("profile_image_url"))
            if txt and ts_abs is not None:
                push(ts_abs, txt, disp, uname, avat)

if not raw_utts:
    os.makedirs(artdir, exist_ok=True)
    open(os.path.join(artdir,f"{base}.vtt"),"w",encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(artdir,f"{base}_emoji.vtt"),"w",encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(artdir,f"{base}_transcript.html"),"w",encoding="utf-8").write("")
    sys.exit(0)

# ---------- Robust normalization: epoch vs relative ----------
# Decide if the stream is epoch-based (Unix seconds) or relative.
# Heuristic: if any timestamp >= 1e9, treat series as epoch seconds.
any_epoch = any(u["ts"] >= 1e9 for u in raw_utts)

epoch_base = None
if any_epoch:
    # Prefer explicit START_ISO if provided; else base on the minimum epoch ts observed.
    epoch_base = parse_start_iso_env()
    if epoch_base is None:
        epoch_base = min(u["ts"] for u in raw_utts if u["ts"] >= 1e9)

# Normalize absolute → relative (seconds from stream start), then subtract TOTAL_SHIFT.
for u in raw_utts:
    if any_epoch:
        u["ts"] = max(0.0, u["ts"] - epoch_base)
        if u.get("end_ts") is not None:
            u["end_ts"] = max(0.0, u["end_ts"] - epoch_base)
    # else: already relative seconds
    # Guard against accidental millisecond leftovers (rare edge)
    if u["ts"] > 86400 * 365 * 10:  # >10 years in seconds → clearly bad
        u["ts"] = u["ts"] / 1000.0
    if u.get("end_ts") and u["end_ts"] > 86400 * 365 * 10:
        u["end_ts"] = u["end_ts"] / 1000.0

# Ensure monotonic non-decreasing
raw_utts.sort(key=lambda x: x["ts"])
EPS = 0.0005
last = -1e9
for u in raw_utts:
    if u["ts"] <= last:
        u["ts"] = last + EPS
    last = u["ts"]

# Reference t0 is the earliest (now-relative) ts
t0 = raw_utts[0]["ts"]

# Build utterances with final relative timing (subtract TOTAL_SHIFT to align with trimmed audio)
utts = []
for u in raw_utts:
    st = (u["ts"] - t0) - TOTAL_SHIFT
    if st < 0: st = 0.0
    et = None
    if u.get("end_ts") is not None:
        et = (u["end_ts"] - t0) - TOTAL_SHIFT
        if et is not None and et <= st: et = None
    utts.append({
        "start_rel": st,
        "end_rel": et,
        "text": u["text"],
        "name": u["name"],
        "username": u["username"],
        "avatar": u["avatar"],
    })

# If no end times, synthesize based on next start; clamp duration
MIN_DUR = 0.80
MAX_DUR = 10.0
GUARD   = 0.020

for i, u in enumerate(utts):
    if u["end_rel"] is not None:
        # Clamp provided end
        dur = u["end_rel"] - u["start_rel"]
        if dur < MIN_DUR: u["end_rel"] = u["start_rel"] + MIN_DUR
        elif dur > MAX_DUR: u["end_rel"] = u["start_rel"] + MAX_DUR
        continue

    if i + 1 < len(utts):
        nxt = utts[i+1]["start_rel"]
        dur = max(MIN_DUR, min(MAX_DUR, (nxt - u["start_rel"]) - GUARD))
        if dur <= 0: dur = MIN_DUR
        u["end_rel"] = u["start_rel"] + dur
    else:
        words = max(1, len(u["text"].split()))
        dur = max(MIN_DUR, min(MAX_DUR, 0.33 * words + 0.7))
        u["end_rel"] = u["start_rel"] + dur

# Merge near-adjacent same-speaker segments
MERGE_GAP = 3.0
groups = []; cur = None
for u in utts:
    if (cur is not None and u["name"] == cur["name"] and u["username"] == cur["username"]
        and (u["start_rel"] - cur["end_rel"]) <= MERGE_GAP):
        sep = "" if re.search(r'[.!?]"?$', cur["text"]) else " "
        cur["text"] = (cur["text"] + sep + u["text"]).strip()
        cur["end_rel"] = max(cur["end_rel"], u["end_rel"])
    else:
        cur = {"name":u["name"], "username":u["username"], "avatar":u["avatar"],
               "start_rel":u["start_rel"], "end_rel":u["end_rel"], "text":u["text"]}
        groups.append(cur)

# Final pass: enforce non-overlap and min duration
prev = 0.0
for g in groups:
    if g["start_rel"] < prev + 0.02:
        g["start_rel"] = prev + 0.02
    if g["end_rel"] < g["start_rel"] + MIN_DUR:
        g["end_rel"] = g["start_rel"] + MIN_DUR
    prev = g["end_rel"]

os.makedirs(artdir, exist_ok=True)

# -------- Write full captions VTT --------
vtt_path = os.path.join(artdir, f"{base}.vtt")
with open(vtt_path, "w", encoding="utf-8") as vf:
    vf.write("WEBVTT\n\n")
    for i, g in enumerate(groups, 1):
        vf.write(f"{i}\n{fmt_ts(g['start_rel'])} --> {fmt_ts(g['end_rel'])}\n")
        vf.write(f"<v {esc(g['name'])}> {esc(g['text'])}\n\n")

# -------- Emoji-only VTT --------
EMOJI_RE = re.compile(
    "["                       
    "\U0001F1E6-\U0001F1FF"  # flags
    "\U0001F300-\U0001FAD6"  # misc pictographs
    "\U0001FAE0-\U0001FAFF"  # newer emoji
    "\U00002700-\U000027BF"  # dingbats
    "\U00002600-\U000026FF"  # misc symbols
    "\U0001F900-\U0001F9FF"  # supplemental
    "\U0001F680-\U0001F6FF"  # transport/map
    "\U0001F100-\U0001F5FF"  # enclosed alphanum/symbols
    "\U0001FA70-\U0001FAFF"  # more symbols
    "\U00002300-\U000023FF"  # misc tech
    "]+", flags=re.UNICODE
)
def only_emoji(s:str)->str:
    if not s: return ""
    return "".join(EMOJI_RE.findall(s))

evtt_path = os.path.join(artdir, f"{base}_emoji.vtt")
with open(evtt_path, "w", encoding="utf-8") as ef:
    ef.write("WEBVTT\n\n")
    j = 1
    for g in groups:
        em = only_emoji(g["text"])
        if not em: continue
        ef.write(f"{j}\n{fmt_ts(g['start_rel'])} --> {fmt_ts(g['end_rel'])}\n")
        ef.write(f"{em}\n\n")
        j += 1

# -------- Rich transcript HTML (avatars + names) --------
css = '''
<style>
.ss3k-transcript{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  max-height:70vh; overflow-y:auto; scroll-behavior:smooth; border:1px solid #e5e7eb; border-radius:12px; padding:6px 6px;}
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
    var segs=[].slice.call(cont.querySelectorAll('.ss3k-seg')); var lastId="";
    function tick(){
      var t=audio.currentTime||0, found=null;
      for(var i=0;i<segs.length;i++){ if(within(t,segs[i])){found=segs[i];break;} }
      segs.forEach(function(s){ s.classList.toggle('active', s===found); });
      if(found){
        var id=found.id||"";
        if(id!==lastId){
          var top = found.offsetTop - cont.offsetTop;
          if (Math.abs(cont.scrollTop - top) > 6) cont.scrollTop = top;
          lastId=id;
        }
      }
    }
    audio.addEventListener('timeupdate', tick);
    audio.addEventListener('seeked', tick);
    segs.forEach(function(s){
      s.addEventListener('click', function(){
        audio.currentTime = time(s.dataset.start)+0.05; audio.play().catch(function(){});
      });
    });
    tick();
  }
  if(document.readyState!=="loading") bind(); else document.addEventListener('DOMContentLoaded', bind);
})();
</script>
'''

tr_path = os.path.join(artdir, f"{base}_transcript.html")
with open(tr_path, "w", encoding="utf-8") as tf:
    tf.write(css)
    tf.write('<div class="ss3k-transcript">\n')
    for i, g in enumerate(groups, 1):
        name  = g["name"]
        uname = (g.get("username") or "").strip().lstrip("@")
        prof  = f"https://x.com/{html.escape(uname, True)}" if uname else ""
        avatar= g.get("avatar") or (f"https://unavatar.io/x/{html.escape(uname, True)}" if uname else "")
        if avatar and prof:
            avtag = f'<a href="{prof}" target="_blank" rel="noopener"><img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt=""></a>'
        elif avatar:
            avtag = f'<img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt="">'
        else:
            avtag = '<div class="ss3k-avatar" aria-hidden="true"></div>'
        name_html = f'<span class="ss3k-name"><strong>{html.escape(name, True)}</strong></span>'
        if prof:
            name_html = f'<span class="ss3k-name"><a href="{prof}" target="_blank" rel="noopener"><strong>{html.escape(name, True)}</strong></a></span>'

        tf.write(f'<div class="ss3k-seg" id="seg-{i:04d}" data-start="{g["start_rel"]:.3f}" data-end="{g["end_rel"]:.3f}" data-speaker="{html.escape(name, True)}"')
        if uname: tf.write(f' data-handle="@{html.escape(uname, True)}"')
        tf.write('>')
        tf.write(avtag)
        tf.write('<div class="ss3k-body">')
        tf.write(f'<div class="ss3k-meta">{name_html} · <time>{fmt_ts(g["start_rel"])}</time>–<time>{fmt_ts(g["end_rel"])}</time></div>')
        tf.write(f'<div class="ss3k-text">{html.escape(g["text"], True)}</div>')
        tf.write('</div></div>\n')
    tf.write('</div>\n')
    tf.write(js)

# Start marker for external consumers:
# Prefer START_ISO; else if we had epoch input, use computed epoch_base; else derive from "now" minus TOTAL_SHIFT.
start_epoch = None
if START_ISO:
    start_epoch = parse_start_iso_env()
elif any_epoch and epoch_base is not None:
    start_epoch = epoch_base
else:
    # best-effort: not epoch-based, fabricate from current UTC (not ideal, but keeps file present)
    start_epoch = datetime.now(timezone.utc).timestamp()

start_iso_out = datetime.fromtimestamp(start_epoch, timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
with open(os.path.join(artdir, f"{base}.start.txt"), "w", encoding="utf-8") as sf:
    sf.write(start_iso_out + "\n")
