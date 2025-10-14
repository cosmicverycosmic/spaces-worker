#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gen_vtt.py
----------
Builds a WEBVTT file + interactive transcript HTML from a crawler JSONL,
while separating out emoji/tap reactions to a sidecar JSON for UI animation.

ENV:
  ARTDIR      - output directory
  BASE        - base filename (no extension)
  CC_JSONL    - path to crawler JSONL (captions/reactions stream)
  SHIFT_SECS  - seconds to shift left (lead-silence trim); float, optional

OUTPUTS:
  {BASE}.vtt
  {BASE}_transcript.html
  {BASE}.start.txt          (ISO-8601 UTC when absolute start known)
  {BASE}_speech.json        (processed speech segments: start,end,text, name, handle, avatar)
  {BASE}_reactions.json     (reaction events: t, emoji, name, handle, avatar)
  {BASE}_meta.json          (counts, timing diagnostics)

Design notes:
- We *exclude* emoji-only rows from speech; they are emitted to *_reactions.json*.
- We robustly classify timestamps as epoch vs relative and normalize both into
  a single relative timeline, then apply SHIFT_SECS.
- We merge nearby segments by the same speaker and produce sensible durations.
"""

import os, sys, re, json, html
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import unicodedata

# ---------------- Env ----------------
ARTDIR = os.environ.get("ARTDIR", "").strip() or "."
BASE   = os.environ.get("BASE", "space").strip() or "space"
SRC    = os.environ.get("CC_JSONL", "").strip()
SHIFT  = float(os.environ.get("SHIFT_SECS", "0").strip() or "0")

os.makedirs(ARTDIR, exist_ok=True)

VTT_PATH         = os.path.join(ARTDIR, f"{BASE}.vtt")
TRANSCRIPT_PATH  = os.path.join(ARTDIR, f"{BASE}_transcript.html")
START_PATH       = os.path.join(ARTDIR, f"{BASE}.start.txt")
SPEECH_JSON_PATH = os.path.join(ARTDIR, f"{BASE}_speech.json")
REACT_JSON_PATH  = os.path.join(ARTDIR, f"{BASE}_reactions.json")
META_JSON_PATH   = os.path.join(ARTDIR, f"{BASE}_meta.json")

# ---------------- Utils ----------------
def write_empty_outputs():
    # Always produce files so later steps can read them safely
    with open(VTT_PATH, "w", encoding="utf-8") as f:
        f.write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH, "w", encoding="utf-8").write("")
    open(SPEECH_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(REACT_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(META_JSON_PATH, "w", encoding="utf-8").write(json.dumps({
        "speech_segments": 0, "reactions": 0, "notes": "no input"
    }, ensure_ascii=False))
    # Start time is optional; only write if we *know* it later.
    return 0

def esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt_ts(t: float) -> str:
    if t < 0:
        t = 0.0
    h = int(t // 3600)
    m = int((t % 3600) // 60)
    s = t % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"

def parse_time_iso(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    s = s.strip()
    try:
        # '2025-10-14T10:20:33Z' or with offset
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # '...+0000' → '...+00:00'
        if re.search(r"[+-]\d{4}$", s):
            s = s[:-5] + s[-5:-3] + ":" + s[-3:]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

def to_secs(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    # ms vs s
    if v >= 1e12:  # very likely ms
        v = v / 1000.0
    return v

def looks_like_epoch(v: float) -> bool:
    # Rough check: > 1e6 seconds (~11.6 days) means not a tiny relative offset
    return v >= 1e6

def is_letter_or_digit(ch: str) -> bool:
    cat = unicodedata.category(ch)
    return cat and (cat[0] == "L" or cat[0] == "N")

def has_letters_or_digits(s: str) -> bool:
    return any(is_letter_or_digit(ch) for ch in s)

def is_emoji_or_punct_only(s: str) -> bool:
    """Return True if string has no letters/digits and consists of emoji/symbol/punct/space."""
    text = (s or "").strip()
    if not text:
        return True
    if has_letters_or_digits(text):
        return False
    # If there are no letters/digits, ensure the remaining characters are symbol/mark/punct/space
    for ch in text:
        cat = unicodedata.category(ch)
        if cat[0] in ("L", "N"):   # letters/numbers (shouldn't happen here)
            return False
        if ch.strip() == "":
            continue  # whitespace ok
        # Allow symbols (S*), marks (M*), punctuation (P*)
        if cat[0] in ("S", "M", "P"):
            continue
        # Otherwise (e.g., unhandled categories), treat conservatively as not emoji-only
        return False
    return True

def sentenceish(text: str) -> str:
    """Light normalization to keep word-like content together."""
    if not text:
        return ""
    t = text.replace("\u2028", " ").replace("\u2029", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t

def clean_name(name: Optional[str]) -> str:
    s = (name or "").strip()
    s = re.sub(r"[<>&]", "", s)
    # odds are these IDs are 10–16 alnum w/o space; fall back in that case
    if re.fullmatch(r"[A-Za-z0-9]{10,16}", s):
        return "Speaker"
    # strip astral weirdness used sometimes in handles
    s = "".join(ch for ch in s if not (0xD800 <= ord(ch) <= 0xDFFF))
    return s or "Speaker"

def first(*vals):
    for v in vals:
        if v not in (None, ""):
            return v
    return None

# ---------------- Parsing ----------------
if not (SRC and os.path.isfile(SRC)):
    write_empty_outputs()
    sys.exit(0)

raw_speech: List[Dict[str, Any]] = []
reactions: List[Dict[str, Any]] = []
abs_times: List[float] = []   # absolute epoch seconds candidates, for start.txt

def classify_and_collect(text: str,
                         disp: Optional[str],
                         uname: Optional[str],
                         avatar: Optional[str],
                         record: Dict[str, Any]):
    """Decide whether a row is speech or a reaction, and collect with timestamp kind."""
    # Choose the best timestamp from the record
    # Prefer explicit absolute time from programDateTime
    ts_abs = parse_time_iso(record.get("programDateTime"))
    # Then numeric fields
    if ts_abs is None:
        # 'timestamp' is ambiguous → decide by scale
        ts_unk = to_secs(record.get("timestamp"))
        if ts_unk is not None and looks_like_epoch(ts_unk):
            ts_abs = ts_unk
    ts_rel = None
    if ts_abs is None:
        ts_rel = first(to_secs(record.get("start")),
                       to_secs(record.get("startSec")),
                       to_secs(record.get("startMs")),
                       to_secs(record.get("ts")),
                       to_secs(record.get("offset")))
        # If only 'timestamp' exists and small, consider it relative
        if ts_rel is None and ts_unk is not None and not looks_like_epoch(ts_unk):
            ts_rel = ts_unk

    # Also bank any absolute we see for start.txt
    if ts_abs is not None:
        abs_times.append(ts_abs)

    t = sentenceish(text)
    name = clean_name(first(disp, uname, "Speaker"))
    handle = (uname or "").lstrip("@")
    avatar_url = avatar or ""

    # Speech admission rule:
    #  - drop emoji/punct-only rows (put into reactions instead)
    #  - otherwise accept as speech
    if is_emoji_or_punct_only(t):
        # Store as reaction if we have any time notion; else drop
        kind = "epoch" if ts_abs is not None else ("rel" if ts_rel is not None else None)
        if kind is None:
            return
        reactions.append({
            "t_kind": kind,
            "ts": float(ts_abs if kind == "epoch" else ts_rel),
            "emoji": t,
            "name": name,
            "username": handle,
            "avatar": avatar_url
        })
        return

    # Speech
    kind = "epoch" if ts_abs is not None else ("rel" if ts_rel is not None else None)
    if kind is None:
        # no time? drop safely
        return
    raw_speech.append({
        "t_kind": kind,
        "ts": float(ts_abs if kind == "epoch" else ts_rel),
        "text": t,
        "name": name,
        "username": handle,
        "avatar": avatar_url
    })

def harvest_from_obj(obj: Dict[str, Any]):
    """
    Attempt to pull text + user from different shapes we see in the JSONL.
    """
    # Common text fields
    txt = first(obj.get("body"), obj.get("text"), obj.get("caption"), obj.get("payloadText"))
    disp = first(obj.get("displayName"), obj.get("speaker_name"), obj.get("speakerName"),
                 obj.get("name"), obj.get("user"))
    uname = first(obj.get("username"), obj.get("handle"), obj.get("screen_name"), obj.get("user_id"))
    avatar = first(obj.get("profile_image_url_https"), obj.get("profile_image_url"))

    # Some logs include nested "sender" for user info
    sender = obj.get("sender") or {}
    if isinstance(sender, dict):
        disp = first(disp, sender.get("display_name"))
        uname = first(uname, sender.get("screen_name"))
        avatar = first(avatar, sender.get("profile_image_url_https"), sender.get("profile_image_url"))

    # Try to classify this record if we have any text-like content
    if txt:
        classify_and_collect(txt, disp, uname, avatar, obj)

def ingest_line(line: str):
    """
    Each line may be:
      - a dict row with caption or reaction fields
      - a wrapper with 'payload' -> JSON, possibly with 'body' stringified JSON
    """
    line = (line or "").strip()
    if not line:
        return
    try:
        obj = json.loads(line)
    except Exception:
        return

    # Some crawlers wrap: {"payload": "...json..."} or {"payload": {"body": "..."}}
    if isinstance(obj, dict) and "payload" in obj and isinstance(obj["payload"], (str, dict)):
        pl = obj["payload"]
        if isinstance(pl, str):
            try:
                pl = json.loads(pl)
            except Exception:
                pl = {}
        if isinstance(pl, dict):
            # Some store JSON in pl["body"] as string
            body = pl.get("body")
            if isinstance(body, str):
                try:
                    inner = json.loads(body)
                    if isinstance(inner, dict):
                        # Merge known fields into a record-like dict
                        record = dict(inner)
                        # also attach sender for names/avatars
                        if isinstance(pl.get("sender"), dict):
                            record["sender"] = pl["sender"]
                        harvest_from_obj(record)
                        return
                except Exception:
                    pass
            # If body wasn't a stringified JSON, still try to use payload fields
            record = dict(pl)
            harvest_from_obj(record)
            return

    # Otherwise use obj directly
    if isinstance(obj, dict):
        harvest_from_obj(obj)

# -------- Read JSONL --------
with open(SRC, "r", encoding="utf-8", errors="ignore") as f:
    for ln in f:
        ingest_line(ln)

# If nothing collected, output empty artifacts
if not raw_speech and not reactions:
    write_empty_outputs()
    # Write start if we have any absolute times
    if abs_times:
        start_iso = datetime.fromtimestamp(min(abs_times), timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        open(START_PATH, "w", encoding="utf-8").write(start_iso + "\n")
    sys.exit(0)

# ------------- Time normalization -------------
def normalize_events(evts: List[Dict[str, Any]], shift_secs: float):
    """
    Normalize mixed epoch/relative events into a single relative timeline.
    Adds 't_rel' (seconds >= 0) to each item.
    """
    epochs = [e for e in evts if e["t_kind"] == "epoch"]
    rels   = [e for e in evts if e["t_kind"] == "rel"]

    base_epoch = min((e["ts"] for e in epochs), default=None)
    # 'rels' are already relative; we use their own base as 0
    for e in epochs:
        e["t_rel0"] = e["ts"] - (base_epoch or 0.0)
    for e in rels:
        e["t_rel0"] = e["ts"]  # they are already relative

    # Global zero is the min among the two clusters
    t0 = min([*(e["t_rel0"] for e in epochs), *(e["t_rel0"] for e in rels)]) if (epochs or rels) else 0.0
    for e in evts:
        e["t_rel"] = max(0.0, e["t_rel0"] - t0 - shift_secs)

# Apply normalization separately to speech and reactions
normalize_events(raw_speech, SHIFT)
normalize_events(reactions, SHIFT)

# Keep for meta
diag = {
    "speech_input": len(raw_speech),
    "reactions_input": len(reactions),
    "shift_secs": SHIFT,
}

# ------------- Build speech segments -------------
# Sort by relative start
raw_speech.sort(key=lambda x: (x["t_rel"], x["name"], x["username"]))

# Compute naive end times from next start, constrained
MIN_DUR = 0.80
MAX_DUR = 10.0
GUARD   = 0.020

segments: List[Dict[str, Any]] = []
for i, u in enumerate(raw_speech):
    st = u["t_rel"]
    if i + 1 < len(raw_speech):
        nxt = raw_speech[i + 1]["t_rel"]
        dur = max(MIN_DUR, min(MAX_DUR, (nxt - st) - GUARD))
        if dur <= 0:
            dur = MIN_DUR
    else:
        # Estimate final duration by words
        words = max(1, len(u["text"].split()))
        dur = max(MIN_DUR, min(MAX_DUR, 0.33 * words + 0.7))
    segments.append({
        "start": st,
        "end": st + dur,
        "text": u["text"],
        "name": u["name"],
        "username": u["username"],
        "avatar": u["avatar"],
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

# Ensure strictly increasing, padded by GUARD
prev_end = 0.0
for g in merged:
    if g["start"] < prev_end + GUARD:
        g["start"] = prev_end + GUARD
    if g["end"] < g["start"] + MIN_DUR:
        g["end"] = g["start"] + MIN_DUR
    prev_end = g["end"]

# ------------- Emit WEBVTT -------------
with open(VTT_PATH, "w", encoding="utf-8") as vf:
    vf.write("WEBVTT\n\n")
    for i, g in enumerate(merged, 1):
        vf.write(f"{i}\n{fmt_ts(g['start'])} --> {fmt_ts(g['end'])}\n")
        vf.write(f"<v {esc(g['name'])}> {esc(g['text'])}\n\n")

# ------------- Interactive transcript HTML -------------
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

# ------------- Sidecar JSONs -------------
# Speech sidecar
speech_out = [{
    "start": round(g["start"], 3),
    "end": round(g["end"], 3),
    "text": g["text"],
    "name": g["name"],
    "handle": g["username"],
    "avatar": g["avatar"],
} for g in merged]
open(SPEECH_JSON_PATH, "w", encoding="utf-8").write(
    json.dumps(speech_out, ensure_ascii=False)
)

# Reactions sidecar
reactions.sort(key=lambda r: r["t_rel"])
react_out = [{
    "t": round(r["t_rel"], 3),
    "emoji": r["emoji"],
    "name": r["name"],
    "handle": r["username"],
    "avatar": r["avatar"],
} for r in reactions]
open(REACT_JSON_PATH, "w", encoding="utf-8").write(
    json.dumps(react_out, ensure_ascii=False)
)

# Meta
open(META_JSON_PATH, "w", encoding="utf-8").write(json.dumps({
    "speech_segments": len(merged),
    "reactions": len(react_out),
    "inputs": {
        "raw_speech_rows": len(raw_speech),
        "raw_reaction_rows": len(reactions),
        "abs_time_rows": len(abs_times),
    },
    "shift_secs_applied": SHIFT,
}, ensure_ascii=False))

# ------------- Start time (ISO Z) -------------
if abs_times:
    start_iso = datetime.fromtimestamp(min(abs_times), timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    open(START_PATH, "w", encoding="utf-8").write(start_iso + "\n")
