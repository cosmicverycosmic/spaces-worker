#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build WEBVTT (speech + emoji) and an interactive transcript HTML.
- Emoji are REMOVED from transcript/speech VTT.
- Emoji VTT cues contain JSON with time+avatar+emoji+handle for perfect sync.
"""

import os, sys, re, json, html, unicodedata, math
from datetime import datetime, timezone
from statistics import median
from typing import Any, Dict, List, Optional

ARTDIR = (os.environ.get("ARTDIR") or ".").strip()
BASE   = (os.environ.get("BASE") or "space").strip()
SRC    = (os.environ.get("CC_JSONL") or "").strip()

SHIFT  = float((os.environ.get("SHIFT_SECS") or "0").strip() or "0")
TRIM   = float((os.environ.get("TRIM_LEAD")  or "0").strip() or "0")
TOTAL_SHIFT = SHIFT + TRIM

os.makedirs(ARTDIR, exist_ok=True)

VTT_PATH         = os.path.join(ARTDIR, f"{BASE}.vtt")
EVTT_PATH        = os.path.join(ARTDIR, f"{BASE}_emoji.vtt")
TRANSCRIPT_PATH  = os.path.join(ARTDIR, f"{BASE}_transcript.html")
START_PATH       = os.path.join(ARTDIR, f"{BASE}.start.txt")
SPEECH_JSON_PATH = os.path.join(ARTDIR, f"{BASE}_speech.json")
REACT_JSON_PATH  = os.path.join(ARTDIR, f"{BASE}_reactions.json")
META_JSON_PATH   = os.path.join(ARTDIR, f"{BASE}_meta.json")

def esc(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def nfc(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFC", s)
    return re.sub(r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF]", "", s)

def fmt_ts(t: float) -> str:
    if t < 0: t = 0.0
    ms = int(round((t - math.floor(t)) * 1000))
    s  = int(math.floor(t))
    h, rem = divmod(s, 3600)
    m, ss  = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{ss:02d}.{ms:03d}"

def parse_time_iso(s: Optional[str]) -> Optional[float]:
    if not s: return None
    s = s.strip()
    try:
        if s.endswith("Z"): s = s[:-1] + "+00:00"
        if re.search(r"[+-]\d{4}$", s):
            s = s[:-5] + s[-5:-3] + ":" + s[-3:]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

def to_secs(x: Any) -> Optional[float]:
    if x in (None, ""): return None
    try: v = float(x)
    except Exception: return None
    if v >= 1e12: v /= 1000.0
    return v

def first(*vals):
    for v in vals:
        if v not in (None, ""): return v
    return None

# Emoji detection/removal
EMOJI_RE = re.compile("[" +
    "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF" +
    "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF" +
    "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF" + "]+", re.UNICODE)
ONLY_PSPACE = re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")

def strip_emoji(s: str) -> str:
    return EMOJI_RE.sub("", s or "")

def is_emoji_only(s: str) -> bool:
    if not s or not s.strip(): return False
    t = ONLY_PSPACE.sub("", s)
    t = EMOJI_RE.sub("", t)
    return len(t.strip()) == 0

# Early empty
if not (SRC and os.path.isfile(SRC)):
    open(VTT_PATH,"w",encoding="utf-8").write("WEBVTT\n\n")
    open(EVTT_PATH,"w",encoding="utf-8").write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH,"w",encoding="utf-8").write("")
    open(SPEECH_JSON_PATH,"w",encoding="utf-8").write("[]")
    open(REACT_JSON_PATH,"w",encoding="utf-8").write("[]")
    open(META_JSON_PATH,"w",encoding="utf-8").write(json.dumps({"speech_segments":0,"reactions":0}))
    open(START_PATH,"w",encoding="utf-8").write("")
    sys.exit(0)

REL_KEYS  = ("offset","startSec","startMs","start")
ABS_KEYS  = ("programDateTime","timestamp","ts")
raw_speech: List[Dict[str,Any]] = []
raw_rx:     List[Dict[str,Any]] = []
abs_candidates: List[float] = []
idx = 0

def pick_rel_abs(d: Dict[str,Any]) -> (Optional[float], Optional[float]):
    rel = None; abs_ts = None
    for k in REL_KEYS:
        if k in d and d[k] not in (None, ""):
            v = to_secs(d[k]); 
            if v is not None: rel = v; break
    if "programDateTime" in d and d["programDateTime"] not in (None, ""):
        abs_ts = parse_time_iso(d["programDateTime"])
    for k in ("timestamp","ts"):
        if k in d and d[k] not in (None,""):
            v = to_secs(d[k]); 
            if v is None: continue
            if v >= 1e6: abs_ts = v
            elif rel is None: rel = v
    return rel, abs_ts

def harvest(d: Dict[str,Any]):
    global idx
    txt = first(d.get("body"), d.get("text"), d.get("caption"), d.get("payloadText"))
    if not txt: return
    txt = nfc(str(txt)).strip()

    sender = d.get("sender") or {}
    disp   = first(d.get("displayName"), d.get("speaker_name"), d.get("speakerName"),
                   (sender or {}).get("display_name"), d.get("name"), d.get("user"))
    uname  = first(d.get("username"), d.get("handle"), d.get("screen_name"),
                   d.get("user_id"), (sender or {}).get("screen_name"))
    avatar = first((sender or {}).get("profile_image_url_https"),
                   (sender or {}).get("profile_image_url"),
                   d.get("profile_image_url_https"), d.get("profile_image_url"))
    name   = nfc(first(disp, uname, "Speaker") or "Speaker")
    handle = (uname or "").lstrip("@")
    rel, abs_ts = pick_rel_abs(d)

    if is_emoji_only(txt):
        raw_rx.append({"idx":idx,"rel":rel,"abs":abs_ts,"emoji":txt,"name":name,"handle":handle,"avatar":avatar or ""})
    else:
        # Keep speech sans any embedded emoji
        txt_no_emoji = strip_emoji(txt).strip()
        if txt_no_emoji:
            raw_speech.append({"idx":idx,"rel":rel,"abs":abs_ts,"text":txt_no_emoji,"name":name,"handle":handle,"avatar":avatar or ""})
    if abs_ts is not None: abs_candidates.append(abs_ts)
    idx += 1

def ingest_line(ln: str):
    ln = (ln or "").strip()
    if not ln: return
    try:
        obj = json.loads(ln)
    except Exception:
        return
    layers: List[Dict[str,Any]] = []
    if isinstance(obj, dict):
        layers.append(obj)
        pl = obj.get("payload")
        if isinstance(pl, str):
            try:
                pj = json.loads(pl); 
                if isinstance(pj, dict):
                    layers.append(pj)
                    if isinstance(pj.get("body"), str):
                        try:
                            inner = json.loads(pj["body"])
                            if isinstance(inner, dict):
                                inner = dict(inner)
                                if isinstance(pj.get("sender"), dict):
                                    inner["sender"] = pj["sender"]
                                layers.append(inner)
                        except Exception:
                            pass
            except Exception:
                pass
        elif isinstance(pl, dict):
            layers.append(pl)
            if isinstance(pl.get("body"), str):
                try:
                    inner = json.loads(pl["body"])
                    if isinstance(inner, dict):
                        inner = dict(inner)
                        if isinstance(pl.get("sender"), dict):
                            inner["sender"] = pl["sender"]
                        layers.append(inner)
                except Exception:
                    pass
    for d in layers:
        if isinstance(d, dict): harvest(d)

with open(SRC,"r",encoding="utf-8",errors="ignore") as fh:
    for ln in fh: ingest_line(ln)

if not raw_speech and not raw_rx:
    open(VTT_PATH,"w",encoding="utf-8").write("WEBVTT\n\n")
    open(EVTT_PATH,"w",encoding="utf-8").write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH,"w",encoding="utf-8").write("")
    open(SPEECH_JSON_PATH,"w",encoding="utf-8").write("[]")
    open(REACT_JSON_PATH,"w",encoding="utf-8").write("[]")
    open(META_JSON_PATH,"w",encoding="utf-8").write(json.dumps({"speech_segments":0,"reactions":0}))
    open(START_PATH,"w",encoding="utf-8").write("")
    sys.exit(0)

# --- clock alignment ---
abs0 = min(abs_candidates) if abs_candidates else None
if abs0 is not None:
    deltas = []
    for it in raw_speech:
        if it["abs"] is not None and it["rel"] is not None:
            deltas.append((it["abs"] - abs0) - it["rel"])
    for r in raw_rx:
        if r["abs"] is not None and r["rel"] is not None:
            deltas.append((r["abs"] - abs0) - r["rel"])
    delta = median(deltas) if deltas else 0.0
else:
    delta = 0.0

def rel_time(rel: Optional[float], abs_ts: Optional[float]) -> float:
    if rel is not None: t = rel + delta
    elif abs_ts is not None and abs0 is not None: t = abs_ts - abs0
    else: t = 0.0
    return max(0.0, t - TOTAL_SHIFT)

norm = []
for it in raw_speech:
    t = rel_time(it["rel"], it["abs"])
    norm.append({"t":float(t), **it})
norm.sort(key=lambda x: (x["t"], x["idx"]))
EPS=5e-4; last=-1e9
for u in norm:
    if u["t"] <= last: u["t"] = last + EPS
    last = u["t"]

# --- build segments ---
MIN_DUR=0.80; MAX_DUR=10.0; GUARD=0.020; MERGE_GAP=3.0
segs = [{"start":u["t"], "end":u["t"]+MIN_DUR, "text":u["text"],
         "name":u["name"],"handle":u["handle"],"avatar":u["avatar"]} for u in norm]

merged=[]; cur=None
def ends_sentence(s:str)->bool: return bool(re.search(r'[.!?]"?$', (s or "").strip()))
for s in segs:
    if cur and s["name"]==cur["name"] and s["handle"]==cur["handle"] and s["start"]-cur["end"]<=MERGE_GAP:
        cur["text"] = (cur["text"] + ("" if ends_sentence(cur["text"]) else " ") + s["text"]).strip()
        cur["end"]  = max(cur["end"], s["end"])
    else:
        cur = dict(s); merged.append(cur)

for i,g in enumerate(merged):
    if i+1 < len(merged):
        nxt = merged[i+1]["start"]
        dur = max(MIN_DUR, min(MAX_DUR, (nxt - g["start"]) - GUARD))
        g["end"] = g["start"] + (dur if dur>0 else MIN_DUR)
    else:
        words = max(1, len((g["text"] or "").split()))
        g["end"] = g["start"] + max(MIN_DUR, min(MAX_DUR, 0.33*words + 0.7))

prev=0.0
for g in merged:
    if g["start"] < prev + GUARD: g["start"] = prev + GUARD
    if g["end"]   < g["start"] + MIN_DUR: g["end"] = g["start"] + MIN_DUR
    prev = g["end"]

# --- speech WEBVTT (emoji-free) ---
with open(VTT_PATH,"w",encoding="utf-8") as f:
    f.write("WEBVTT\n\n")
    for i,g in enumerate(merged,1):
        f.write(f"{i}\n{fmt_ts(g['start'])} --> {fmt_ts(g['end'])}\n")
        f.write(f"<v {esc(g['name'])}> {esc(g['text'])}\n\n")

# --- emoji VTT + JSON sidecar ---
rx_out=[]
for r in raw_rx:
    t = rel_time(r["rel"], r["abs"])
    rx_out.append({
        "t": round(t,3),
        "emoji": r["emoji"],
        "name": r["name"],
        "handle": r["handle"],
        "avatar": r["avatar"]
    })

with open(EVTT_PATH,"w",encoding="utf-8") as f:
    f.write("WEBVTT\n\n")
    for j,r in enumerate(sorted(rx_out, key=lambda x:x["t"]), 1):
        st = max(0.0, r["t"]); en = st + 2.0
        payload = json.dumps(r, ensure_ascii=False)
        f.write(f"{j}\n{fmt_ts(st)} --> {fmt_ts(en)}\n{payload}\n\n")

open(REACT_JSON_PATH,"w",encoding="utf-8").write(json.dumps(rx_out, ensure_ascii=False))

# --- interactive transcript (no timestamps; single 50x50 avatar) ---
CSS = '''
<style>
.ss3k-transcript{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  max-height:70vh; overflow-y:auto; scroll-behavior:smooth; border:1px solid #e5e7eb; border-radius:12px; padding:6px}
.ss3k-row{display:flex; align-items:flex-start; gap:10px; padding:8px 10px; border-radius:10px; margin:6px 0}
.ss3k-row.active{background:#eef6ff; outline:1px solid #bfdbfe}
.ss3k-avatar{width:50px; height:50px; border-radius:50%; flex:0 0 50px; background:#e5e7eb}
.ss3k-line{display:flex; flex-wrap:wrap; gap:6px; min-width:0}
.ss3k-name{font-weight:700; color:#0f172a; white-space:nowrap}
.ss3k-text{white-space:pre-wrap; word-break:break-word; cursor:pointer; min-width:0}
.ss3k-name a{color:inherit; text-decoration:none}
</style>
'''
JS = r'''
<script>
(function(){
  function tnum(s){return parseFloat(s||'0')||0}
  function inwin(t,el){return t>=tnum(el.dataset.start)&&t<tnum(el.dataset.end)}
  function bind(){
    var audio=document.getElementById('ss3k-audio')||document.querySelector('audio[data-ss3k-player]');
    var box=document.querySelector('.ss3k-transcript'); if(!audio||!box) return;
    var rows=[].slice.call(box.querySelectorAll('.ss3k-row'));
    var userScrolling=false, scrollTimer=null;

    box.addEventListener('scroll', function(){
      userScrolling = true;
      clearTimeout(scrollTimer);
      scrollTimer = setTimeout(function(){ userScrolling=false; }, 1200);
    });

    function ensureVisible(el){
      if(userScrolling) return;
      var top = el.offsetTop - box.offsetTop;
      var desired = top - 8;
      if (Math.abs(box.scrollTop - desired) > 6) box.scrollTop = desired;
    }
    function tick(){
      var t=audio.currentTime||0, found=null;
      for(var i=0;i<rows.length;i++){ if(inwin(t,rows[i])){ found=rows[i]; break; } }
      rows.forEach(function(r){ r.classList.toggle('active', r===found); });
      if(found) ensureVisible(found);
    }
    rows.forEach(function(r){
      r.addEventListener('click', function(){
        audio.currentTime = tnum(r.dataset.start) + 0.05;
        if(audio.play) audio.play().catch(function(){});
      });
    });
    audio.addEventListener('timeupdate', tick);
    audio.addEventListener('seeked', tick);
    tick();
  }
  if(document.readyState!=="loading") bind(); else document.addEventListener('DOMContentLoaded', bind);
})();
</script>
'''
with open(TRANSCRIPT_PATH,"w",encoding="utf-8") as tf:
    tf.write(CSS+"\n<div class=\"ss3k-transcript\">\n")
    for i,g in enumerate(merged,1):
        uname = (g.get("handle") or "").lstrip("@")
        prof  = f"https://x.com/{html.escape(uname, True)}" if uname else ""
        avatar= g.get("avatar") or (f"https://unavatar.io/x/{html.escape(uname, True)}" if uname else "")
        if avatar and prof:
            av = f'<a href="{prof}" target="_blank" rel="noopener"><img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt=""></a>'
        elif avatar:
            av = f'<img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt="">'
        else:
            av = '<div class="ss3k-avatar" aria-hidden="true"></div>'
        name_html = html.escape(g["name"], True)
        if prof: name_html = f'<a href="{prof}" target="_blank" rel="noopener">{name_html}</a>'
        tf.write(f'<div class="ss3k-row" id="seg-{i:04d}" data-start="{g["start"]:.3f}" data-end="{g["end"]:.3f}">')
        tf.write(av)
        tf.write('<div class="ss3k-line">')
        tf.write(f'<span class="ss3k-name">{name_html}:</span> ')
        tf.write(f'<span class="ss3k-text">{esc(g["text"])}</span>')
        tf.write('</div></div>\n')
    tf.write("</div>\n"+JS+"\n")

# Sidecars + start
speech_out = [{
    "start": round(g["start"],3), "end": round(g["end"],3),
    "text": g["text"], "name": g["name"], "handle": g["handle"], "avatar": g["avatar"]
} for g in merged]
open(SPEECH_JSON_PATH,"w",encoding="utf-8").write(json.dumps(speech_out, ensure_ascii=False))

rx_out = json.loads(open(REACT_JSON_PATH,"r",encoding="utf-8").read()) if os.path.exists(REACT_JSON_PATH) else []
open(META_JSON_PATH,"w",encoding="utf-8").write(json.dumps({
    "speech_segments": len(merged),
    "reactions": len(rx_out),
    "timing": {"shift_secs": SHIFT, "trim_lead": TRIM, "total_shift": TOTAL_SHIFT}
}, ensure_ascii=False, indent=2))

start_iso = ""
env_start = os.environ.get("START_ISO") or ""
if env_start:
    ts = parse_time_iso(env_start)
    if ts: start_iso = datetime.fromtimestamp(ts, timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
open(START_PATH,"w",encoding="utf-8").write((start_iso or "")+"\n")
