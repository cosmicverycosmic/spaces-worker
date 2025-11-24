#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gen_vtt.py
----------
Builds a WEBVTT file + interactive transcript HTML from:
  - Deepgram diarized, smart_formatted utterances (primary), and
  - Crawler JSONL captions (secondary; used for speaker/handle mapping and fallback).

ENV:
  ARTDIR         - output directory
  BASE           - base filename (no extension)
  CC_JSONL       - path to crawler JSONL (captions/reactions stream), optional
  DEEPGRAM_JSON  - path to Deepgram JSON (utterances+diarization), optional
                   (alias: DG_JSON)
  SHIFT_SECS     - seconds to shift CC stream left (lead-silence trim); float, optional

OUTPUTS:
  {BASE}.vtt
  {BASE}_transcript.html
  {BASE}.start.txt          (ISO-8601 UTC when absolute start known from CC)
  {BASE}_speech.json        (speech segments: start,end,text, name, handle, avatar)
  {BASE}_reactions.json     (reaction events normalized to same clock, if any)
  {BASE}_meta.json          (counts, timing diagnostics)

Design goals:
- Prefer Deepgram utterances (smart_format + diarize) for segmenting and text.
- Use crawler CC only to:
    * map Deepgram speaker IDs → X handles / display names / avatars, and
    * provide a fallback transcript if Deepgram output is missing.
- Robust clock alignment for CC stream so mapping is stable.
- Unicode-safe normalization.
"""

import os, sys, re, json, html, unicodedata
from datetime import datetime, timezone
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

# ---------------- Absolute timestamp sanity ----------------
def sanitize_abs_epoch(v: Optional[float]) -> Optional[float]:
    """Return a plausible epoch seconds value or None."""
    if v is None:
        return None
    try:
        dt = datetime.fromtimestamp(float(v), timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    if 2000 <= dt.year <= 2100:
        return float(v)
    return None

# ---------------- Env ----------------
ARTDIR = os.environ.get("ARTDIR", "").strip() or "."
BASE   = os.environ.get("BASE", "space").strip() or "space"
SRC    = os.environ.get("CC_JSONL", "").strip()
SHIFT  = float(os.environ.get("SHIFT_SECS", "0").strip() or "0")

DG_JSON = (
    os.environ.get("DEEPGRAM_JSON", "").strip()
    or os.environ.get("DG_JSON", "").strip()
)

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

def nfc(s: str) -> str:
    """Normalize to NFC and strip zero-width / bidi controls that cause odd glyphs."""
    if not s:
        return ""
    s = unicodedata.normalize("NFC", s)
    return re.sub(r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF]", "", s)

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
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        if re.search(r"[+-]\d{4}$", s):
            s = s[:-5] + s[-5:-3] + ":" + s[-3:]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

def to_secs(x: Any) -> Optional[float]:
    """Float conversion with ms→s guard (epoch ms ~ 1e12)."""
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if v >= 1e12:  # very likely milliseconds epoch
        v = v / 1000.0
    return v

def first(*vals):
    for v in vals:
        if v not in (None, ""):
            return v
    return None

# Emoji / punctuation classification
EMOJI_RE = re.compile("[" +
    "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF" +
    "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF" +
    "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF" + "]+", re.UNICODE)
ONLY_PUNCT_SPACE = re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")

def is_emoji_only(s: str) -> bool:
    if not s or not s.strip():
        return False
    t = ONLY_PUNCT_SPACE.sub("", s)
    t = EMOJI_RE.sub("", t)
    return len(t.strip()) == 0

def has_letters_or_digits(s: str) -> bool:
    return bool(re.search(r"[A-Za-z0-9]", s or ""))

# ---------------- Early exit if no sources ----------------
if not (
    (SRC and os.path.isfile(SRC)) or
    (DG_JSON and os.path.isfile(DG_JSON))
):
    with open(VTT_PATH, "w", encoding="utf-8") as f:
        f.write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH, "w", encoding="utf-8").write("")
    open(SPEECH_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(REACT_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(META_JSON_PATH, "w", encoding="utf-8").write(json.dumps({
        "speech_segments": 0,
        "reactions": 0,
        "inputs": {"raw_items": 0, "abs_candidates": 0, "deepgram_utterances": 0},
        "timing": {"shift_secs_applied": SHIFT, "abs0_present": False, "delta_used": 0.0}
    }, ensure_ascii=False))
    open(START_PATH, "w", encoding="utf-8").write("")
    sys.exit(0)

# ---------------- Parsing CC JSONL ----------------
REL_KEYS  = ("offset", "startSec", "startMs", "start")
ABS_KEYS  = ("programDateTime", "timestamp", "ts")

raw_items: List[Dict[str, Any]] = []
reactions: List[Dict[str, Any]] = []
abs_candidates: List[float] = []
ingest_idx = 0

def pick_rel_abs(d: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """Classify fields into relative seconds and absolute epoch seconds.

    Logic:
      - REL from explicit rel keys.
      - 'programDateTime' always ABS (ISO).
      - 'timestamp'/'ts':
           * if clearly epoch (>= 1e6), treat as ABS;
           * else treat as REL if REL not already set.
    """
    rel: Optional[float] = None
    abs_ts: Optional[float] = None

    for k in REL_KEYS:
        if k in d and d[k] not in (None, ""):
            v = to_secs(d[k])
            if v is not None:
                rel = v
                break

    if "programDateTime" in d and d["programDateTime"] not in (None, ""):
        abs_ts = parse_time_iso(d["programDateTime"])

    for key in ("timestamp", "ts"):
        if key in d and d[key] not in (None, ""):
            v = to_secs(d[key])
            if v is None:
                continue
            if v >= 1e6:
                abs_ts = v
            else:
                if rel is None:
                    rel = v

    return rel, abs_ts

def harvest_from_dict(d: Dict[str, Any]):
    global ingest_idx
    txt = first(d.get("body"), d.get("text"), d.get("caption"), d.get("payloadText"))
    if not txt:
        return

    sender = d.get("sender") or {}
    disp = first(
        d.get("displayName"), d.get("speaker_name"), d.get("speakerName"),
        sender.get("display_name"), d.get("name"), d.get("user")
    )
    uname = first(
        d.get("username"), d.get("handle"), d.get("screen_name"),
        d.get("user_id"), sender.get("screen_name")
    )
    avatar = first(
        sender.get("profile_image_url_https"),
        sender.get("profile_image_url"),
        d.get("profile_image_url_https"),
        d.get("profile_image_url")
    )

    rel, abs_ts = pick_rel_abs(d)
    abs_ts = sanitize_abs_epoch(abs_ts)

    text = nfc(str(txt)).strip()
    if not has_letters_or_digits(text) and not is_emoji_only(text):
        return

    name = nfc(first(disp, uname, "Speaker") or "Speaker")
    handle = (uname or "").lstrip("@")
    avatar_url = avatar or ""

    if is_emoji_only(text):
        if abs_ts is not None or rel is not None:
            reactions.append({
                "idx": ingest_idx, "rel": rel, "abs": abs_ts,
                "emoji": text, "name": name, "handle": handle, "avatar": avatar_url
            })
        ingest_idx += 1
        return

    raw_items.append({
        "idx": ingest_idx, "rel": rel, "abs": abs_ts, "text": text,
        "name": name, "username": handle, "avatar": avatar_url
    })
    ingest_idx += 1
    if abs_ts is not None:
        abs_candidates.append(abs_ts)

def ingest_line(line: str):
    line = (line or "").strip()
    if not line:
        return
    try:
        obj = json.loads(line)
    except Exception:
        return

    layers: List[Dict[str, Any]] = []
    if isinstance(obj, dict):
        layers.append(obj)
        pl = obj.get("payload")

        # payload as stringified JSON
        if isinstance(pl, str):
            try:
                plj = json.loads(pl)
                if isinstance(plj, dict):
                    layers.append(plj)
                    body = plj.get("body")
                    if isinstance(body, str):
                        try:
                            inner = json.loads(body)
                            if isinstance(inner, dict):
                                if isinstance(plj.get("sender"), dict):
                                    inner = dict(inner)
                                    inner["sender"] = plj["sender"]
                                layers.append(inner)
                        except Exception:
                            pass
            except Exception:
                pass
        # payload already dict
        elif isinstance(pl, dict):
            layers.append(pl)
            body = pl.get("body")
            if isinstance(body, str):
                try:
                    inner = json.loads(body)
                    if isinstance(inner, dict):
                        if isinstance(pl.get("sender"), dict):
                            inner = dict(inner)
                            inner["sender"] = pl["sender"]
                        layers.append(inner)
                except Exception:
                    pass

    for d in layers:
        if isinstance(d, dict):
            harvest_from_dict(d)

# Read CC JSONL if present
if SRC and os.path.isfile(SRC):
    with open(SRC, "r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            ingest_line(ln)

# ---------------- Deepgram utterances ----------------
dg_utterances_raw: List[Dict[str, Any]] = []

if DG_JSON and os.path.isfile(DG_JSON):
    try:
        with open(DG_JSON, "r", encoding="utf-8") as df:
            dg_data = json.load(df)
        res = (dg_data or {}).get("results") or {}

        utts = res.get("utterances")
        if not isinstance(utts, list):
            # Try channels[0].alternatives[0].utterances
            channels = res.get("channels") or []
            for ch in channels:
                for alt in ch.get("alternatives", []):
                    if isinstance(alt.get("utterances"), list):
                        utts = alt["utterances"]
                        break
                if isinstance(utts, list):
                    break

        if isinstance(utts, list):
            for i, u in enumerate(utts):
                try:
                    st = float(u.get("start", 0.0))
                    en = float(u.get("end", st + 0.5))
                except Exception:
                    continue
                txt = nfc(str(u.get("transcript", "")).strip())
                if not has_letters_or_digits(txt):
                    continue
                spk = u.get("speaker")
                dg_utterances_raw.append({
                    "idx": i,
                    "start": max(0.0, st),
                    "end": max(0.0, en),
                    "speaker": spk,
                    "text": txt,
                })
    except Exception:
        dg_utterances_raw = []

dg_utterances_raw.sort(key=lambda u: (u["start"], u["idx"]))

# ---------------- If absolutely nothing, emit empties ----------------
if not raw_items and not dg_utterances_raw and not reactions:
    with open(VTT_PATH, "w", encoding="utf-8") as f:
        f.write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH, "w", encoding="utf-8").write("")
    open(SPEECH_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(REACT_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(META_JSON_PATH, "w", encoding="utf-8").write(json.dumps({
        "speech_segments": 0,
        "reactions": 0,
        "inputs": {"raw_items": 0, "abs_candidates": 0, "deepgram_utterances": 0},
        "timing": {"shift_secs_applied": SHIFT, "abs0_present": False, "delta_used": 0.0}
    }, ensure_ascii=False))
    open(START_PATH, "w", encoding="utf-8").write("")
    sys.exit(0)

# ------------- Time normalization for CC (Δ alignment) -------------
abs0 = min(abs_candidates) if abs_candidates else None
if abs0 is not None:
    deltas = []
    for it in raw_items:
        if it["abs"] is not None and it["rel"] is not None:
            deltas.append((it["abs"] - abs0) - it["rel"])
    for r in reactions:
        if r["abs"] is not None and r["rel"] is not None:
            deltas.append((r["abs"] - abs0) - r["rel"])
    delta = median(deltas) if deltas else 0.0
else:
    delta = 0.0

def rel_time_from_item(rel: Optional[float], abs_ts: Optional[float]) -> float:
    """Map mixed rel/abs clocks onto a single relative timeline, then apply SHIFT for CC."""
    if rel is not None:
        t = rel + delta
    elif abs_ts is not None and abs0 is not None:
        t = abs_ts - abs0
    else:
        t = 0.0
    return max(0.0, t - SHIFT)

# CC timeline
norm: List[Dict[str, Any]] = []
for it in raw_items:
    t = rel_time_from_item(it["rel"], it["abs"])
    norm.append({**it, "t": float(t)})

norm.sort(key=lambda x: (x["t"], x["idx"]))
EPS = 5e-4
last = -1e9
for u in norm:
    if u["t"] <= last:
        u["t"] = last + EPS
    last = u["t"]

# Build avatar map by username for mapped speakers
avatar_by_username: Dict[str, str] = {}
for it in raw_items:
    uname = (it.get("username") or "").strip()
    av = it.get("avatar") or ""
    if uname and av and uname not in avatar_by_username:
        avatar_by_username[uname] = av

# ------------- Map Deepgram speakers → CC names/handles -------------
speaker_map: Dict[Any, Dict[str, str]] = {}
if dg_utterances_raw and norm:
    votes: Dict[Any, Dict[Tuple[str, str], int]] = {}
    tol = 1.0  # seconds window

    for cc in norm:
        t = cc["t"]
        best = None
        best_gap = None
        for u in dg_utterances_raw:
            if t < u["start"] - tol or t > u["end"] + tol:
                continue
            mid = 0.5 * (u["start"] + u["end"])
            gap = abs(t - mid)
            if best is None or gap < best_gap:
                best = u
                best_gap = gap
        if best is None:
            continue
        spk = best.get("speaker")
        if spk is None:
            continue
        uname = (cc.get("username") or "").strip()
        nm = (cc.get("name") or "").strip()
        if not (uname or nm):
            continue
        key = (uname, nm)
        votes.setdefault(spk, {})
        votes[spk][key] = votes[spk].get(key, 0) + 1

    for spk, options in votes.items():
        (uname, nm), _ = max(options.items(), key=lambda kv: kv[1])
        disp = nm or uname or f"Speaker {spk}"
        speaker_map[spk] = {
            "username": uname,
            "name": disp,
        }

# ------------- Build segments (Deepgram primary, CC fallback) -------------
final_segments: List[Dict[str, Any]] = []

if dg_utterances_raw:
    # Use Deepgram utterances as authoritative segments
    for u in dg_utterances_raw:
        spk = u.get("speaker")
        meta = speaker_map.get(spk, {}) if spk is not None else {}
        uname = (meta.get("username") or "").strip()
        name = meta.get("name") or (f"Speaker {spk}" if spk is not None else "Speaker")
        avatar = avatar_by_username.get(uname, "")

        st = max(0.0, float(u["start"]))
        en = max(st + 0.10, float(u["end"]))  # ensure non-zero
        final_segments.append({
            "start": st,
            "end": en,
            "text": u["text"],
            "name": name,
            "username": uname,
            "avatar": avatar,
            "speaker": spk,
        })
else:
    # Fallback to CC-only segmentation (original logic)
    MIN_DUR = 0.80
    MAX_DUR = 10.0
    GUARD   = 0.020
    MERGE_GAP = 3.0

    segments: List[Dict[str, Any]] = []
    for u in norm:
        st = u["t"]
        segments.append({
            "start": st,
            "end": st + MIN_DUR,
            "text": u["text"],
            "name": u["name"],
            "username": u["username"],
            "avatar": u["avatar"],
        })

    merged: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None

    def end_sentence_punct(s: str) -> bool:
        return bool(re.search(r'[.!?]"?$', (s or "").strip()))

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

    for i, g in enumerate(merged):
        if i + 1 < len(merged):
            nxt = merged[i + 1]["start"]
            dur = max(MIN_DUR, min(MAX_DUR, (nxt - g["start"]) - GUARD))
            g["end"] = g["start"] + dur
        else:
            words = max(1, len((g["text"] or "").split()))
            g["end"] = g["start"] + max(MIN_DUR, min(MAX_DUR, 0.33 * words + 0.7))

    prev_end = 0.0
    for g in merged:
        if g["start"] < prev_end + GUARD:
            g["start"] = prev_end + GUARD
        if g["end"] < g["start"] + MIN_DUR:
            g["end"] = g["start"] + MIN_DUR
        prev_end = g["end"]

    final_segments = merged

# ------------- Emit WEBVTT -------------
with open(VTT_PATH, "w", encoding="utf-8") as vf:
    vf.write("WEBVTT\n\n")
    for i, g in enumerate(final_segments, 1):
        vf.write(f"{i}\n{fmt_ts(g['start'])} --> {fmt_ts(g['end'])}\n")
        vf.write(f"<v {esc(g['name'])}> {esc(g['text'])}\n\n")

# ------------- Interactive transcript HTML -------------
CSS = '''
<style>
.ss3k-transcript{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  max-height:70vh; overflow-y:auto; scroll-behavior:smooth; border:1px solid #e5e7eb; border-radius:12px; padding:6px}
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
      if(found){
        var top = found.offsetTop - cont.offsetTop;
        if (Math.abs(cont.scrollTop - top) > 6) cont.scrollTop = top;
      }
    }
    audio.addEventListener('timeupdate', tick);
    audio.addEventListener('seeked', tick);
    segs.forEach(function(s){
      s.addEventListener('click', function(){
        audio.currentTime = time(s.dataset.start)+0.05;
        if (audio.play) audio.play().catch(function(){});
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
    for i, g in enumerate(final_segments, 1):
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

        name_html = f'<span class="ss3k-name"><strong>{html.escape(name, True)}</strong></span>'
        if prof:
            name_html = f'<span class="ss3k-name"><a href="{prof}" target="_blank" rel="noopener"><strong>{html.escape(name, True)}</strong></a></span>'

        tf.write(
            f'<div class="ss3k-seg" id="seg-{i:04d}" data-start="{g["start"]:.3f}" data-end="{g["end"]:.3f}"'
        )
        if uname:
            tf.write(f' data-handle="@{html.escape(uname, True)}"')
        tf.write('>')
        tf.write(avtag)
        tf.write('<div class="ss3k-body">')
        tf.write(f'<div class="ss3k-meta">{name_html} · <time>{fmt_ts(g["start"])}</time>–<time>{fmt_ts(g["end"])}</time></div>')
        tf.write(f'<div class="ss3k-text">{esc(g["text"])}</div>')
        tf.write('</div></div>\n')
    tf.write('</div>\n' + JS + "\n")

# ------------- Sidecar JSONs -------------
speech_out = [{
    "start": round(g["start"], 3),
    "end": round(g["end"], 3),
    "text": g["text"],
    "name": g["name"],
    "handle": g.get("username", ""),
    "avatar": g.get("avatar", ""),
} for g in final_segments]
open(SPEECH_JSON_PATH, "w", encoding="utf-8").write(
    json.dumps(speech_out, ensure_ascii=False)
)

# Reactions sidecar (normalize CC emoji onto same relative clock as CC timeline)
rx_out = []
for r in reactions:
    if r["rel"] is not None:
        t = r["rel"] + (delta or 0.0)
    elif r["abs"] is not None and abs0 is not None:
        t = r["abs"] - abs0
    else:
        continue
    t = max(0.0, t - SHIFT)
    rx_out.append({
        "t": round(t, 3),
        "emoji": r["emoji"],
        "name": r["name"],
        "handle": r["handle"],
        "avatar": r["avatar"],
    })
open(REACT_JSON_PATH, "w", encoding="utf-8").write(
    json.dumps(rx_out, ensure_ascii=False)
)

# ------------- Meta + start time -------------
start_iso = ""
if abs_candidates:
    try:
        start_iso = datetime.fromtimestamp(min(abs_candidates), timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        start_iso = ""
open(START_PATH, "w", encoding="utf-8").write((start_iso or "") + "\n")

meta = {
    "speech_segments": len(final_segments),
    "reactions": len(rx_out),
    "inputs": {
        "raw_items": len(raw_items),
        "abs_candidates": len(abs_candidates),
        "deepgram_utterances": len(dg_utterances_raw),
        "raw_reaction_rows": len(reactions),
    },
    "timing": {
        "shift_secs_applied": SHIFT,
        "abs0_present": abs0 is not None,
        "delta_used": round(delta, 6) if abs0 is not None else 0.0,
        "first_caption_start": round(final_segments[0]["start"], 3) if final_segments else None,
    }
}
open(META_JSON_PATH, "w", encoding="utf-8").write(json.dumps(meta, ensure_ascii=False, indent=2))
