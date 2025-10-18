#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gen_vtt.py
----------
Builds a WEBVTT file + interactive transcript HTML from a crawler JSONL,
while separating out emoji/tap reactions to a sidecar JSON for UI animation.

ENV (provided by workflow):
  ARTDIR             - output directory
  BASE               - base filename (no extension)
  CC_JSONL           - path to crawler JSONL (captions/reactions stream)
  SHIFT_SECS         - seconds to shift left (lead-silence trim)
  ABS_ANCHOR_EPOCH   - (optional) absolute epoch seconds for the Space start

Outputs:
  {BASE}.vtt
  {BASE}_transcript.html
  {BASE}.start.txt          (ISO-8601 Z when absolute start known)
  {BASE}_speech.json        (segments: start,end,text,name,handle,avatar)
  {BASE}_reactions.json     (reaction events for UI overlay)
  {BASE}_meta.json          (diagnostics)
"""

import os, sys, re, json, html, unicodedata
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# ---------------- Env ----------------
ARTDIR = os.environ.get("ARTDIR", "").strip() or "."
BASE   = os.environ.get("BASE", "space").strip() or "space"
SRC    = os.environ.get("CC_JSONL", "").strip()
SHIFT  = float(os.environ.get("SHIFT_SECS", "0").strip() or "0")

ABS_ANCHOR = os.environ.get("ABS_ANCHOR_EPOCH", "").strip()
try:
    ABS_ANCHOR = float(ABS_ANCHOR) if ABS_ANCHOR else None
except Exception:
    ABS_ANCHOR = None

os.makedirs(ARTDIR, exist_ok=True)

VTT_PATH         = os.path.join(ARTDIR, f"{BASE}.vtt")
TRANSCRIPT_PATH  = os.path.join(ARTDIR, f"{BASE}_transcript.html")
START_PATH       = os.path.join(ARTDIR, f"{BASE}.start.txt")
SPEECH_JSON_PATH = os.path.join(ARTDIR, f"{BASE}_speech.json")
REACT_JSON_PATH  = os.path.join(ARTDIR, f"{BASE}_reactions.json")
META_JSON_PATH   = os.path.join(ARTDIR, f"{BASE}_meta.json")

# ---------------- Utils ----------------
def esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt_ts(t: float) -> str:
    if t < 0: t = 0.0
    h = int(t // 3600); m = int((t % 3600) // 60); s = t % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"

def parse_time_iso(s: Optional[str]) -> Optional[float]:
    if not s: return None
    s = s.strip()
    try:
        if s.endswith("Z"): s = s[:-1] + "+00:00"
        if re.search(r"[+-]\d{4}$", s):  # +0000 → +00:00
            s = s[:-5] + s[-5:-3] + ":" + s[-3:]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

def to_secs(x: Any) -> Optional[float]:
    if x is None: return None
    try:
        v = float(x)
    except Exception:
        return None
    # if it looks like milliseconds, scale down
    if v >= 4_000_000:  # ~46 days
        v = v / 1000.0
    return v

def looks_like_epoch(v: float) -> bool:
    return v >= 1_000_000  # ~11.6 days

def first(*vals):
    for v in vals:
        if v not in (None, ""):
            return v
    return None

def is_emoji_or_punct_only(s: str) -> bool:
    """True if the string contains no letters/digits and is only emoji/symbol/punct/space."""
    text = (s or "").strip()
    if not text: return True
    # letters or digits present? then it's not emoji-only
    for ch in text:
        cat = unicodedata.category(ch)
        if cat[0] in ("L", "N"):  # letters/numbers
            return False
    # allow only symbols, marks, punctuation, and whitespace
    for ch in text:
        if ch.isspace(): continue
        cat = unicodedata.category(ch)
        if cat[0] in ("S", "M", "P"):
            continue
        return False
    return True

def sanitize_text(s: str) -> str:
    """Keep text faithful to source; just remove control/format chars and collapse whitespace."""
    if not s: return ""
    # Remove zero-width and other format controls
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Cf" and ch not in ("\u200b", "\ufeff"))
    # Remove other control characters except \n and \t
    s = "".join(ch for ch in s if (unicodedata.category(ch) != "Cc") or ch in ("\n", "\t"))
    # Collapse whitespace to single spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ---------------- Input guard ----------------
if not (SRC and os.path.isfile(SRC)):
    # write empty artifacts so downstream never breaks
    open(VTT_PATH, "w", encoding="utf-8").write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH, "w", encoding="utf-8").write("")
    open(SPEECH_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(REACT_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(META_JSON_PATH, "w", encoding="utf-8").write(json.dumps({
        "speech_segments": 0, "reactions": 0, "notes": "no input"
    }, ensure_ascii=False))
    open(START_PATH, "w", encoding="utf-8").write("")
    sys.exit(0)

# ---------------- Parse JSONL ----------------
raw_speech: List[Dict[str, Any]] = []
reactions: List[Dict[str, Any]] = []
abs_candidates: List[float] = []

REL_KEYS = ("offset","startSec","startMs","start","t")
ABS_KEYS = ("programDateTime","timestamp","ts","eventAt","createdAt")

def classify_and_collect(text: str, disp: Optional[str], uname: Optional[str], avatar: Optional[str], record: Dict[str, Any]):
    # times
    ts_abs = None
    for k in ABS_KEYS:
        if k in record and record[k] not in (None, ""):
            if k == "programDateTime":
                ts_abs = parse_time_iso(record[k]); 
            else:
                val = to_secs(record[k])
                if val is not None and looks_like_epoch(val):
                    ts_abs = val
            if ts_abs is not None: break

    ts_rel = None
    for k in REL_KEYS:
        if k in record and record[k] not in (None, ""):
            val = to_secs(record[k])
            if val is not None:
                ts_rel = val; break

    # ambiguous small "timestamp" could be relative
    if ts_abs is None and "timestamp" in record:
        v = to_secs(record.get("timestamp"))
        if v is not None and not looks_like_epoch(v):
            ts_rel = v

    if ts_abs is not None:
        abs_candidates.append(ts_abs)

    t = sanitize_text(text)
    if not t: return
    name = sanitize_text(first(disp, uname, "Speaker") or "Speaker")
    handle = (uname or "").lstrip("@")
    avatar_url = avatar or ""

    if is_emoji_or_punct_only(t):
        # store to reactions if we have any timing; else ignore
        if ts_abs is None and ts_rel is None:
            return
        reactions.append({
            "t_abs": ts_abs, "t_rel": ts_rel,
            "emoji": t, "name": name, "username": handle, "avatar": avatar_url
        })
        return

    if ts_abs is None and ts_rel is None:
        return

    raw_speech.append({
        "t_abs": ts_abs, "t_rel": ts_rel,
        "text": t, "name": name, "username": handle, "avatar": avatar_url
    })

def harvest_from_obj(obj: Dict[str, Any]):
    txt = first(obj.get("body"), obj.get("text"), obj.get("caption"), obj.get("payloadText"))
    disp = first(obj.get("displayName"), obj.get("speaker_name"), obj.get("speakerName"),
                 obj.get("name"), obj.get("user"))
    uname = first(obj.get("username"), obj.get("handle"), obj.get("screen_name"), obj.get("user_id"))
    avatar = first(obj.get("profile_image_url_https"), obj.get("profile_image_url"))

    # nested sender
    sender = obj.get("sender") or {}
    if isinstance(sender, dict):
        disp = first(disp, sender.get("display_name"))
        uname = first(uname, sender.get("screen_name"))
        avatar = first(avatar, sender.get("profile_image_url_https"), sender.get("profile_image_url"))

    if txt:
        classify_and_collect(txt, disp, uname, avatar, obj)

def ingest_line(line: str):
    line = (line or "").strip()
    if not line: return
    try:
        obj = json.loads(line)
    except Exception:
        return

    if isinstance(obj, dict) and "payload" in obj and isinstance(obj["payload"], (str, dict)):
        pl = obj["payload"]
        if isinstance(pl, str):
            try:
                pl = json.loads(pl)
            except Exception:
                pl = {}
        if isinstance(pl, dict):
            body = pl.get("body")
            if isinstance(body, str):
                try:
                    inner = json.loads(body)
                    if isinstance(inner, dict):
                        rec = dict(inner)
                        if isinstance(pl.get("sender"), dict):
                            rec["sender"] = pl["sender"]
                        harvest_from_obj(rec); return
                except Exception:
                    pass
            rec = dict(pl)
            harvest_from_obj(rec); return

    if isinstance(obj, dict):
        harvest_from_obj(obj)

with open(SRC, "r", encoding="utf-8", errors="ignore") as f:
    for ln in f:
        ingest_line(ln)

# If nothing collected, emit empties
if not raw_speech and not reactions:
    open(VTT_PATH, "w", encoding="utf-8").write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH, "w", encoding="utf-8").write("")
    open(SPEECH_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(REACT_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(META_JSON_PATH, "w", encoding="utf-8").write(json.dumps({
        "speech_segments": 0, "reactions": 0, "notes": "no speech/reactions"
    }, ensure_ascii=False))
    open(START_PATH, "w", encoding="utf-8").write("")
    sys.exit(0)

# ---------------- Time normalization ----------------
# Choose absolute base:
abs0: Optional[float] = None
if ABS_ANCHOR is not None:
    abs0 = ABS_ANCHOR
elif abs_candidates:
    abs0 = min(abs_candidates)

def to_rel_time(t_rel: Optional[float], t_abs: Optional[float]) -> float:
    if t_rel is not None:
        t = t_rel
    elif t_abs is not None and abs0 is not None:
        t = t_abs - abs0
    else:
        t = 0.0
    t = max(0.0, t - SHIFT)  # apply lead-trim shift from audio
    return float(t)

for it in raw_speech:
    it["t"] = to_rel_time(it.get("t_rel"), it.get("t_abs"))
for r in reactions:
    r["t"] = to_rel_time(r.get("t_rel"), r.get("t_abs"))

# sort and stabilize event order
raw_speech.sort(key=lambda x: (x["t"], x["name"], x["username"]))
EPS = 0.0005
last = -1e9
for it in raw_speech:
    if it["t"] <= last: it["t"] = last + EPS
    last = it["t"]

# ---------------- Build segments ----------------
MIN_DUR = 0.80
MAX_DUR = 10.0
GUARD   = 0.020

segments: List[Dict[str, Any]] = []
for i, u in enumerate(raw_speech):
    st = u["t"]
    if i + 1 < len(raw_speech):
        nxt = raw_speech[i + 1]["t"]
        dur = max(MIN_DUR, min(MAX_DUR, (nxt - st) - GUARD))
        if dur <= 0: dur = MIN_DUR
    else:
        words = max(1, len(u["text"].split()))
        dur = max(MIN_DUR, min(MAX_DUR, 0.33 * words + 0.7))
    segments.append({
        "start": st, "end": st + dur,
        "text": u["text"], "name": u["name"],
        "username": u["username"], "avatar": u["avatar"]
    })

# Merge adjacent segments by same speaker if gap <= MERGE_GAP
MERGE_GAP = 3.0
merged: List[Dict[str, Any]] = []
cur: Optional[Dict[str, Any]] = None

def end_sentence_punct(s: str) -> bool:
    return bool(re.search(r'[.!?]"?$', s.strip()))

for seg in segments:
    if (cur is not None
        and seg["name"] == cur["name"]
        and seg["username"] == cur["username"]
        and seg["start"] - cur["end"] <= MERGE_GAP):
        sep = "" if end_sentence_punct(cur["text"]) else " "
        cur["text"] = (cur["text"] + sep + seg["text"]).strip()
        cur["end"] = max(cur["end"], seg["end"])
    else:
        cur = dict(seg)
        merged.append(cur)

# Ensure increasing times
prev_end = 0.0
for g in merged:
    if g["start"] < prev_end + GUARD:
        g["start"] = prev_end + GUARD
    if g["end"] < g["start"] + MIN_DUR:
        g["end"] = g["start"] + MIN_DUR
    prev_end = g["end"]

# ---------------- Emit WEBVTT ----------------
with open(VTT_PATH, "w", encoding="utf-8") as vf:
    vf.write("WEBVTT\n\n")
    for i, g in enumerate(merged, 1):
        vf.write(f"{i}\n{fmt_ts(g['start'])} --> {fmt_ts(g['end'])}\n")
        vf.write(f"<v {esc(g['name'])}> {esc(g['text'])}\n\n")

# ---------------- Interactive transcript HTML ----------------
CSS = '''
<style>
.ss3k-transcript{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  max-height:70vh; overflow-y:auto; scroll-behavior:smooth; border:1px solid #e5e7eb; border-radius:12px; padding:6px;}
.ss3k-seg{display:flex;gap:10px;padding:8px 10px;border-radius:10px;margin:6px 0}
.ss3k-seg.active{background:#eef6ff;outline:1px solid #bfdbfe}
.ss3k-avatar{width:26px;height:26px;border-radius:50%;flex:0 0 26px;margin-top:3px;background:#e5e7eb}
.ss3k-meta{font-size:12px;color:#64748b;margin-bottom:2px}
.ss3k-name a{color:#0f172a;text-decoration:none}
.ss3k-text{white-space:pre-wrap;word-break:break-word;cursor:pointer}
</style>
'''.strip()

JS = r'''
<script>
(function(){
  function time(v){ var x = parseFloat(v||'0'); return isFinite(x) ? x : 0; }
  function within(t,seg){ return t>=time(seg.dataset.start) && t<time(seg.dataset.end); }
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
        audio.currentTime = time(s.dataset.start)+0.05; audio.play && audio.play().catch(function(){});
      });
    });
    tick();
  }
  if(document.readyState!=="loading") bind(); else document.addEventListener('DOMContentLoaded', bind);
})();
</script>
'''.strip()

with open(TRANSCRIPT_PATH, "w", encoding="utf-8") as tf:
    tf.write(CSS + "\n")
    tf.write('<div class="ss3k-transcript">\n')
    for i, g in enumerate(merged, 1):
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

        name_html = f'<strong>{html.escape(name, True)}</strong>'
        if prof:
            name_html = f'<a href="{prof}" target="_blank" rel="noopener"><strong>{html.escape(name, True)}</strong></a>'

        tf.write(
            f'<div class="ss3k-seg" id="seg-{i:04d}" data-start="{g["start"]:.3f}" data-end="{g["end"]:.3f}"'
        )
        if uname:
            tf.write(f' data-handle="@{html.escape(uname, True)}"')
        tf.write('>')
        tf.write(avtag)
        tf.write('<div class="ss3k-body">')
        tf.write(f'<div class="ss3k-meta"><span class="ss3k-name">{name_html}</span> · '
                 f'<time>{fmt_ts(g["start"])}</time>–<time>{fmt_ts(g["end"])}</time></div>')
        tf.write(f'<div class="ss3k-text">{esc(g["text"])}</div>')
        tf.write('</div></div>\n')
    tf.write('</div>\n' + JS + "\n")

# ---------------- Sidecars & meta ----------------
speech_out = [{
    "start": round(g["start"], 3),
    "end": round(g["end"], 3),
    "text": g["text"],
    "name": g["name"],
    "handle": g["username"],
    "avatar": g["avatar"],
} for g in merged]
open(SPEECH_JSON_PATH, "w", encoding="utf-8").write(json.dumps(speech_out, ensure_ascii=False))

reactions.sort(key=lambda r: r["t"])
react_out = [{
    "t": round(r["t"], 3),
    "emoji": r["emoji"],
    "name": r["name"],
    "handle": r["username"],
    "avatar": r["avatar"],
} for r in reactions]
open(REACT_JSON_PATH, "w", encoding="utf-8").write(json.dumps(react_out, ensure_ascii=False))

open(META_JSON_PATH, "w", encoding="utf-8").write(json.dumps({
    "speech_segments": len(merged),
    "reactions": len(react_out),
    "inputs": {
        "raw_speech_rows": len(raw_speech),
        "raw_reaction_rows": len(reactions),
        "abs_time_rows": len(abs_candidates),
    },
    "shift_secs_applied": SHIFT,
    "abs_anchor_applied": ABS_ANCHOR if ABS_ANCHOR is not None else (min(abs_candidates) if abs_candidates else None)
}, ensure_ascii=False))

# best-guess absolute start (ISO Z)
start_iso = ""
if ABS_ANCHOR is not None:
    start_iso = datetime.fromtimestamp(ABS_ANCHOR, timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
elif abs_candidates:
    start_iso = datetime.fromtimestamp(min(abs_candidates), timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
open(START_PATH, "w", encoding="utf-8").write((start_iso or "") + "\n")
