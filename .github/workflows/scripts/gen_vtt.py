#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gen_vtt.py
----------
Builds a WEBVTT file + interactive transcript HTML from either:

  (A) Deepgram diarized transcript JSON (preferred, high-accuracy), optionally
      aligned with Twitter Space crawler CC.txt to replace "Speaker N"
      with real X/Twitter handles and display names.
  (B) Fallback: crawler JSONL captions/reactions stream (legacy mode).

ENV:
  ARTDIR        - output directory
  BASE          - base filename (no extension)
  CC_JSONL      - path to crawler JSONL (optional; used for metadata + fallback)
  CC_TXT        - path to crawler CC.txt (optional; used for speaker mapping)
  DG_JSON       - path to Deepgram JSON (optional; if missing, fall back to CC)
  SHIFT_SECS    - seconds to shift left (lead-silence trim); float, optional
                  NOTE: applied only in CC-fallback path; Deepgram times are
                  assumed to already match the trimmed audio.

OUTPUTS:
  {BASE}.vtt
  {BASE}_transcript.html
  {BASE}.start.txt          (ISO-8601 UTC when absolute start known)
  {BASE}_speech.json        (processed speech segments: start,end,text,name,handle,avatar)
  {BASE}_reactions.json     (reaction events normalized to same clock; may be empty)
  {BASE}_meta.json          (counts, timing diagnostics)

Design goals:
- Prefer Deepgram utterances (diarize + smart_format) for actual text/timing.
- Use crawler CC.txt to map Deepgram speaker IDs → real handles/names via
  content-based alignment and time offsets.
- Use crawler CC.jsonl only for:
    * Display-name lookup for handles.
    * Absolute start time (programDateTime / timestamp).
    * Fallback caption generation when DG is unavailable.
- Add redundancy and conservative checks to avoid misattributing speakers.
- Retain sidecar JSONs for UI, but drop emoji VTT generation entirely.
"""

import os
import sys
import re
import json
import html
import unicodedata
from datetime import datetime, timezone, timedelta
from statistics import median
from typing import Any, Dict, List, Optional, Tuple
from collections import Counter, defaultdict
import bisect

# ---------------- Env ----------------
ARTDIR = os.environ.get("ARTDIR", "").strip() or "."
BASE   = os.environ.get("BASE", "space").strip() or "space"
CC_JSONL = os.environ.get("CC_JSONL", "").strip()
CC_TXT   = os.environ.get("CC_TXT", "").strip()
DG_JSON  = os.environ.get("DG_JSON", "").strip()
SHIFT    = float(os.environ.get("SHIFT_SECS", "0").strip() or "0")

os.makedirs(ARTDIR, exist_ok=True)

VTT_PATH         = os.path.join(ARTDIR, f"{BASE}.vtt")
TRANSCRIPT_PATH  = os.path.join(ARTDIR, f"{BASE}_transcript.html")
START_PATH       = os.path.join(ARTDIR, f"{BASE}.start.txt")
SPEECH_JSON_PATH = os.path.join(ARTDIR, f"{BASE}_speech.json")
REACT_JSON_PATH  = os.path.join(ARTDIR, f"{BASE}_reactions.json")
META_JSON_PATH   = os.path.join(ARTDIR, f"{BASE}_meta.json")

# Try to infer CC_TXT if not explicitly provided
if not CC_TXT and CC_JSONL:
    root, ext = os.path.splitext(CC_JSONL)
    CC_TXT = root + ".txt"

# Try to infer DG_JSON if not explicitly provided
def find_first_existing(paths: List[str]) -> str:
    for p in paths:
        if p and os.path.isfile(p):
            return p
    return ""

if not DG_JSON:
    candidates = [
        os.path.join(ARTDIR, f"{BASE}.dg.json"),
        os.path.join(ARTDIR, f"{BASE}_dg.json"),
        os.path.join(ARTDIR, f"{BASE}.deepgram.json")
    ]
    DG_JSON = find_first_existing(candidates)

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
    if v >= 1e12:  # likely milliseconds epoch
        v /= 1000.0
    return v

def first(*vals):
    for v in vals:
        if v not in (None, ""):
            return v
    return None

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

def normalize_phrase(s: str) -> str:
    s = nfc(s or "")
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def text_tokens(s: str) -> List[str]:
    s = normalize_phrase(s)
    return s.split()

# Emoji / punctuation classification (for reactions; no more emoji VTT)
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

# ---------------- Crawler CC parsing (metadata + fallback captions) ----------------
REL_KEYS  = ("offset", "startSec", "startMs", "start")

raw_items: List[Dict[str, Any]] = []   # for CC-fallback captions
reactions: List[Dict[str, Any]] = []   # emoji/tap events
abs_candidates: List[float] = []       # for absolute start time estimation
ingest_idx = 0

def pick_rel_abs(d: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """Classify fields into relative seconds and absolute epoch seconds."""
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

def harvest_from_dict_for_cc(d: Dict[str, Any]):
    global ingest_idx
    txt = first(d.get("body"), d.get("text"), d.get("caption"), d.get("payloadText"))
    if not txt:
        return
    sender = d.get("sender") or {}
    disp = first(d.get("displayName"), d.get("speaker_name"), d.get("speakerName"),
                 (sender or {}).get("display_name"), d.get("name"), d.get("user"))
    uname = first(d.get("username"), d.get("handle"), d.get("screen_name"),
                  d.get("user_id"), (sender or {}).get("screen_name"))
    avatar = first((sender or {}).get("profile_image_url_https"),
                   (sender or {}).get("profile_image_url"),
                   d.get("profile_image_url_https"), d.get("profile_image_url"))

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

def ingest_cc_jsonl_line(line: str):
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
            harvest_from_dict_for_cc(d)

def load_cc_jsonl():
    if not (CC_JSONL and os.path.isfile(CC_JSONL)):
        return
    with open(CC_JSONL, "r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            ingest_cc_jsonl_line(ln)

# ---------------- CC.txt anchors + handle/display name mapping ----------------
CC_TXT_RE = re.compile(
    r"^(?P<hms>\d{2}:\d{2}:\d{2})\s*\|\s*(?P<handle>[^:]+):\s*(?P<text>.*)$"
)

def hms_to_seconds(hms: str) -> float:
    h, m, s = map(int, hms.split(":"))
    return float(h * 3600 + m * 60 + s)

def load_cc_txt_anchors(path: str):
    anchors = []  # [{abs_t, handle, text}]
    if not (path and os.path.isfile(path)):
        return anchors
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            m = CC_TXT_RE.match(ln)
            if not m:
                continue
            hms   = m.group("hms")
            handle = m.group("handle").strip()
            text   = m.group("text").strip()
            anchors.append({
                "abs_t": hms_to_seconds(hms),
                "handle": handle,
                "text": text,
            })
    anchors.sort(key=lambda a: a["abs_t"])
    return anchors

def build_handle_display_map_from_cc_jsonl(path: str):
    out: Dict[str, str] = {}
    if not (path and os.path.isfile(path)):
        return out
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            try:
                obj = json.loads(ln)
            except Exception:
                continue
            sender = (obj.get("payload") or {}).get("sender") or obj.get("sender") or {}
            handle = sender.get("screen_name") or sender.get("username")
            disp   = sender.get("display_name") or sender.get("name")
            if handle and disp and handle not in out:
                out[handle] = nfc(str(disp))
    return out

# ---------------- Deepgram utterance loading ----------------
def load_deepgram_utterances(path: str) -> List[Dict[str, Any]]:
    if not (path and os.path.isfile(path)):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []

    # Preferred: results.utterances
    utterances = []
    res = (data.get("results") if isinstance(data, dict) else None) or {}
    if isinstance(res.get("utterances"), list) and res["utterances"]:
        for u in res["utterances"]:
            try:
                start = float(u["start"])
                end   = float(u["end"])
            except Exception:
                continue
            if end <= start:
                continue
            transcript = u.get("transcript", "").strip()
            if not transcript:
                continue
            speaker = u.get("speaker")
            utterances.append({
                "start": start,
                "end": end,
                "speaker": speaker,
                "transcript": nfc(transcript),
            })

    if utterances:
        utterances.sort(key=lambda u: u["start"])
        return utterances

    # Fallback: build "utterances" from words if diarization present
    channels = res.get("channels") or []
    if not channels:
        return []
    alt = channels[0].get("alternatives") or []
    if not alt:
        return []
    alt0 = alt[0]
    words = alt0.get("words") or []
    if not words:
        return []

    # Group consecutive words by speaker ID and small gaps
    MAX_GAP = 1.0
    seg = None
    for w in words:
        try:
            ws = float(w["start"])
            we = float(w["end"])
        except Exception:
            continue
        if we <= ws:
            continue
        word = w.get("word", "").strip()
        if not word:
            continue
        spk = w.get("speaker")
        if seg is None:
            seg = {
                "start": ws,
                "end": we,
                "speaker": spk,
                "words": [word],
            }
            continue
        if spk == seg["speaker"] and ws - seg["end"] <= MAX_GAP:
            seg["words"].append(word)
            seg["end"] = we
        else:
            text = " ".join(seg["words"]).strip()
            if text:
                utterances.append({
                    "start": seg["start"],
                    "end": seg["end"],
                    "speaker": seg["speaker"],
                    "transcript": nfc(text),
                })
            seg = {
                "start": ws,
                "end": we,
                "speaker": spk,
                "words": [word],
            }
    if seg is not None:
        text = " ".join(seg["words"]).strip()
        if text:
            utterances.append({
                "start": seg["start"],
                "end": seg["end"],
                "speaker": seg["speaker"],
                "transcript": nfc(text),
            })

    utterances.sort(key=lambda u: u["start"])
    return utterances

# ---------------- DG ↔ CC alignment (text + time) ----------------
def estimate_offset_between_cc_and_dg(anchors, dg_utts, min_words=6, score_thresh=0.6, max_pairs=400):
    """
    Estimate absolute-time offset such that:
      CC_abs_time ≈ DG_start + offset_abs

    Uses content similarity between CC lines and DG utterances to pair them
    and then takes the median of (abs_t - start).
    """
    if not anchors or not dg_utts:
        return None

    offsets = []
    # Pre-tokenize CC anchors
    cc_tokens = []
    for a in anchors:
        toks = text_tokens(a.get("text", ""))
        if toks:
            cc_tokens.append((a["abs_t"], toks))

    if not cc_tokens:
        return None

    for utt in dg_utts[:max_pairs]:
        toks_u = text_tokens(utt.get("transcript", ""))
        if len(toks_u) < min_words:
            continue
        set_u = set(toks_u)
        best_score = 0.0
        best_abs_t = None
        for abs_t, toks_a in cc_tokens:
            set_a = set(toks_a)
            inter = len(set_u & set_a)
            if inter == 0:
                continue
            denom = min(len(set_u), len(set_a))
            if denom == 0:
                continue
            score = inter / denom
            if score > best_score:
                best_score = score
                best_abs_t = abs_t
        if best_abs_t is not None and best_score >= score_thresh:
            offsets.append(best_abs_t - utt["start"])

    if len(offsets) < 2:
        # Not enough matches to be confident
        return None
    return median(offsets)

def map_speakers_to_handles(anchors, dg_utts, offset_abs, window=4.0):
    """
    anchors: [{abs_t, handle, text}]
    dg_utts: [{start, end, speaker, transcript}]
    offset_abs: seconds; CC_abs_time ≈ dg_start + offset_abs
    window: allowed distance in seconds between utterance and nearest CC line
    """
    if not anchors or offset_abs is None:
        return {}

    times = [a["abs_t"] for a in anchors]

    def nearest_anchor(t_abs: float):
        if not times:
            return None
        idx = bisect.bisect_left(times, t_abs)
        best = None
        best_d = 1e9
        for j in (idx - 1, idx, idx + 1):
            if 0 <= j < len(anchors):
                a = anchors[j]
                d = abs(a["abs_t"] - t_abs)
                if d < best_d:
                    best_d = d
                    best = a
        if best is None or best_d > window:
            return None
        return best

    votes: Dict[int, Counter] = defaultdict(Counter)

    for utt in dg_utts:
        spk = utt.get("speaker")
        if spk is None:
            continue
        t_abs = utt["start"] + offset_abs
        anc = nearest_anchor(t_abs)
        if not anc:
            continue
        handle = anc["handle"]
        if not handle:
            continue
        # weight by utterance length (words) to favor richer matches
        w = max(1, len(text_tokens(utt.get("transcript", ""))) // 3)
        votes[int(spk)][handle] += w

    if not votes:
        return {}

    # First pass: per handle, keep the speaker with strongest vote to avoid
    # two different DG speakers being assigned the same handle.
    handle_best: Dict[str, Tuple[int, int]] = {}  # handle -> (spk, count)
    for spk, ctr in votes.items():
        for handle, cnt in ctr.items():
            prev = handle_best.get(handle)
            if prev is None or cnt > prev[1]:
                handle_best[handle] = (spk, cnt)

    speaker_to_handle: Dict[int, str] = {}
    for handle, (spk, _cnt) in handle_best.items():
        speaker_to_handle[spk] = handle

    return speaker_to_handle

# ---------------- Build segments from Deepgram ----------------
def segments_from_deepgram(
    dg_utts: List[Dict[str, Any]],
    anchors,
    handle_display_map: Dict[str, str]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Build merged segments from Deepgram utterances, with best-effort mapping
    of speaker IDs to real handles and display names using CC anchors.
    Returns (segments, speaker_mapping_meta)
    """
    speaker_mapping_meta: Dict[str, Any] = {
        "offset_abs": None,
        "speaker_to_handle": {},
        "handle_to_display": handle_display_map,
    }

    segments: List[Dict[str, Any]] = []
    if not dg_utts:
        return segments, speaker_mapping_meta

    offset_abs = estimate_offset_between_cc_and_dg(anchors, dg_utts)
    speaker_mapping_meta["offset_abs"] = offset_abs

    speaker_to_handle: Dict[int, str] = {}
    if anchors and offset_abs is not None:
        speaker_to_handle = map_speakers_to_handles(anchors, dg_utts, offset_abs)
        speaker_mapping_meta["speaker_to_handle"] = speaker_to_handle

    # Build per-speaker display map
    def label_for_utterance(utt):
        spk = utt.get("speaker")
        if spk is not None and int(spk) in speaker_to_handle:
            handle = speaker_to_handle[int(spk)]
            disp = handle_display_map.get(handle, handle)
            return disp or f"@{handle}", handle
        # fallback: stable pseudo-name
        if spk is None:
            return "Speaker", ""
        return f"Speaker {int(spk) + 1}", ""

    for utt in dg_utts:
        name, handle = label_for_utterance(utt)
        segments.append({
            "start": float(utt["start"]),
            "end": float(utt["end"]),
            "text": utt["transcript"],
            "name": nfc(name),
            "username": handle.lstrip("@"),
            "avatar": "",  # could be filled from handle later if desired
        })

    # Merge/smooth similar to legacy CC path
    segments.sort(key=lambda g: g["start"])
    MIN_DUR = 0.80
    MAX_DUR = 10.0
    GUARD   = 0.020
    MERGE_GAP = 3.0

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

    # Refine end times relative to next start or length
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

    return merged, speaker_mapping_meta

# ---------------- Fallback: segments from CC captions ----------------
def segments_from_cc(raw_items_local: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Legacy path: build segments from crawler CC captions when Deepgram is
    unavailable. Uses REL+ABS Δ alignment and SHIFT_SECS to match trimmed audio.
    """
    if not raw_items_local:
        return []

    # Δ alignment
    abs0 = min(abs_candidates) if abs_candidates else None
    if abs0 is not None:
        deltas = []
        for it in raw_items_local:
            if it["abs"] is not None and it["rel"] is not None:
                deltas.append((it["abs"] - abs0) - it["rel"])
        delta = median(deltas) if deltas else 0.0
    else:
        delta = 0.0

    def rel_time_from_item(rel: Optional[float], abs_ts: Optional[float]) -> float:
        if rel is not None:
            t = rel + delta
        elif abs_ts is not None and abs0 is not None:
            t = abs_ts - abs0
        else:
            t = 0.0
        return max(0.0, t - SHIFT)

    norm: List[Dict[str, Any]] = []
    for it in raw_items_local:
        t = rel_time_from_item(it["rel"], it["abs"])
        norm.append({**it, "t": float(t)})

    norm.sort(key=lambda x: (x["t"], x["idx"]))
    EPS = 5e-4
    last = -1e9
    for u in norm:
        if u["t"] <= last:
            u["t"] = last + EPS
        last = u["t"]

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

    return merged

# ---------------- Main pipeline ----------------
def write_empty_outputs(note: str):
    with open(VTT_PATH, "w", encoding="utf-8") as f:
        f.write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH, "w", encoding="utf-8").write("")
    open(SPEECH_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(REACT_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(START_PATH, "w", encoding="utf-8").write("")
    meta = {
        "speech_segments": 0,
        "reactions": 0,
        "inputs": {
            "raw_items": 0,
            "abs_candidates": 0,
            "dg_utterances": 0,
        },
        "timing": {
            "shift_secs_applied": 0.0,
            "abs0_present": False,
            "delta_used": 0.0,
            "first_caption_start": None,
        },
        "notes": note,
    }
    open(META_JSON_PATH, "w", encoding="utf-8").write(
        json.dumps(meta, ensure_ascii=False, indent=2)
    )
    sys.exit(0)

# Load CC JSONL (for metadata + fallback)
if CC_JSONL and os.path.isfile(CC_JSONL):
    load_cc_jsonl()

# Load Deepgram utterances
dg_utts = load_deepgram_utterances(DG_JSON)

# Load CC.txt anchors + handle display map
anchors = load_cc_txt_anchors(CC_TXT)
handle_display_map = build_handle_display_map_from_cc_jsonl(CC_JSONL)

# Decide on primary source
use_deepgram = bool(dg_utts)
use_cc_fallback = (not use_deepgram) and bool(raw_items)

if not use_deepgram and not use_cc_fallback and not reactions:
    write_empty_outputs("no Deepgram or CC caption input available")

# Build segments
speaker_mapping_meta = {}
if use_deepgram:
    merged, speaker_mapping_meta = segments_from_deepgram(dg_utts, anchors, handle_display_map)
    shift_applied = 0.0  # Deepgram times are already aligned to trimmed audio
else:
    merged = segments_from_cc(raw_items)
    shift_applied = SHIFT

if not merged:
    write_empty_outputs("no segments built from available inputs")

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

# ---------------- Sidecar JSONs ----------------
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

# Reactions sidecar (normalize onto same relative clock for CC path).
# For Deepgram-first mode, this is informational only.
rx_out = []
abs0_for_rx = min(abs_candidates) if abs_candidates else None
if reactions:
    # We reuse CC style Δ alignment for reactions
    if abs0_for_rx is not None:
        deltas = []
        for r in reactions:
            if r["abs"] is not None and r["rel"] is not None:
                deltas.append((r["abs"] - abs0_for_rx) - r["rel"])
        delta_rx = median(deltas) if deltas else 0.0
    else:
        delta_rx = 0.0

    for r in reactions:
        if r["rel"] is not None:
            t = r["rel"] + (delta_rx or 0.0)
        elif r["abs"] is not None and abs0_for_rx is not None:
            t = r["abs"] - abs0_for_rx
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

# ---------------- Meta + start time ----------------
start_iso = ""
if abs_candidates:
    try:
        start_iso = datetime.fromtimestamp(min(abs_candidates), timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    except (OverflowError, OSError, ValueError):
        start_iso = ""
open(START_PATH, "w", encoding="utf-8").write((start_iso or "") + "\n")

meta = {
    "speech_segments": len(merged),
    "reactions": len(rx_out),
    "inputs": {
        "raw_items": len(raw_items),
        "abs_candidates": len(abs_candidates),
        "dg_utterances": len(dg_utts),
        "cc_anchors": len(anchors),
        "has_cc_jsonl": bool(CC_JSONL and os.path.isfile(CC_JSONL)),
        "has_cc_txt": bool(CC_TXT and os.path.isfile(CC_TXT)),
        "has_dg_json": bool(DG_JSON and os.path.isfile(DG_JSON)),
    },
    "timing": {
        "shift_secs_applied": shift_applied,
        "abs0_present": bool(abs_candidates),
        "delta_used": None,  # detailed Δ used internally; omit here for brevity
        "first_caption_start": round(merged[0]["start"], 3) if merged else None,
    },
    "speaker_mapping": speaker_mapping_meta,
}
open(META_JSON_PATH, "w", encoding="utf-8").write(json.dumps(meta, ensure_ascii=False, indent=2))
