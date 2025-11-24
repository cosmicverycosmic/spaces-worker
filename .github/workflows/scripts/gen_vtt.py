#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gen_vtt.py
----------
Builds a WEBVTT file + interactive transcript HTML from:
  - twspace-crawler "CC" artifacts (JSONL + TXT)
  - optional Deepgram diarized JSON

ENV:
  ARTDIR      - output directory
  BASE        - base filename (no extension)
  CC_JSONL    - path to crawler JSONL (chat/cc stream), optional
  SHIFT_SECS  - seconds trimmed from the *front* of the original audio
  DG_JSON     - path to Deepgram JSON, optional

OUTPUTS (written to ARTDIR):
  {BASE}.vtt
  {BASE}_transcript.html
  {BASE}.start.txt          (ISO-8601 UTC when absolute start known)
  {BASE}_speech.json        (speech segments: start,end,text,name,handle,speaker_id)
  {BASE}_reactions.json     (reaction events, currently empty shell)
  {BASE}_meta.json          (counts, timing diagnostics)

Design goals:
- Prefer Deepgram diarized utterances for timing + text quality.
- Use crawler CC.txt to discover as many speaker handles/names as possible.
- Map Deepgram speaker IDs -> X handles via corpus-level text similarity.
- Group contiguous utterances from the same speaker into larger blocks
  for the HTML transcript (paragraph-style), while keeping VTT cues
  reasonably granular.
- Never attribute a human name/handle unless we're at least *somewhat*
  confident in the mapping; otherwise fall back to "Speaker #n".
"""

import json
import math
import os
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

# --------------------------- Utilities -------------------------------------


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def getenv(name: str, default: str = "") -> str:
    v = os.environ.get(name, default)
    return v


def safe_float(x: str, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def ensure_dir(path: str) -> None:
    if path and not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def seconds_to_timestamp(sec: float) -> str:
    """Return WEBVTT HH:MM:SS.mmm string for a non-negative second value."""
    if sec < 0:
        sec = 0.0
    ms = int(round(sec * 1000))
    h, rem = divmod(ms, 3600 * 1000)
    m, rem = divmod(rem, 60 * 1000)
    s, ms = divmod(rem, 1000)
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"


def parse_program_datetime(s: str) -> Optional[datetime]:
    """Parse programDateTime-style strings from the CC JSONL, return UTC."""
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None


def tokenize(text: str) -> List[str]:
    """Simple tokenizer for similarity."""
    text = text.lower()
    # Replace non-alphanumeric with spaces
    text = re.sub(r"[^a-z0-9]+", " ", text)
    toks = [t for t in text.split() if len(t) >= 3]
    return toks


def cosine_sim(c1: Counter, c2: Counter) -> float:
    if not c1 or not c2:
        return 0.0
    # dot
    dot = 0.0
    for k, v in c1.items():
        dot += v * c2.get(k, 0)
    if dot == 0:
        return 0.0
    n1 = math.sqrt(sum(v * v for v in c1.values()))
    n2 = math.sqrt(sum(v * v for v in c2.values()))
    if n1 == 0 or n2 == 0:
        return 0.0
    return float(dot / (n1 * n2))


# --------------------------- Data classes ----------------------------------


@dataclass
class DGUtterance:
    start: float
    end: float
    speaker_id: str
    text: str


@dataclass
class Block:
    start: float
    end: float
    speaker_id: str
    text: str


@dataclass
class SpeakerInfo:
    speaker_id: str
    handle: Optional[str] = None
    name: Optional[str] = None


# --------------------------- Load Deepgram ---------------------------------


def load_deepgram(path: str) -> List[DGUtterance]:
    if not path or not os.path.isfile(path):
        eprint("[gen_vtt] No Deepgram JSON found, falling back to CC-only.")
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            dg = json.load(f)
    except Exception as e:
        eprint(f"[gen_vtt] Failed to parse Deepgram JSON: {e}")
        return []

    # Prefer results.utterances[]
    utterances = dg.get("results", {}).get("utterances") or []
    out: List[DGUtterance] = []
    for u in utterances:
        try:
            start = float(u.get("start", 0.0))
            end = float(u.get("end", start + 0.5))
            if end <= start:
                end = start + 0.5
            spk = str(u.get("speaker", "0"))
            txt = (u.get("transcript") or "").strip()
            if not txt:
                continue
            out.append(DGUtterance(start=start, end=end, speaker_id=spk, text=txt))
        except Exception:
            continue

    # Sort by start time
    out.sort(key=lambda x: x.start)
    eprint(f"[gen_vtt] Loaded {len(out)} Deepgram utterances.")
    return out


# --------------------------- Load CC TXT -----------------------------------


def derive_cc_txt_path(cc_jsonl: str) -> Optional[str]:
    if not cc_jsonl:
        return None
    base, ext = os.path.splitext(cc_jsonl)
    cand = base + ".txt"
    if os.path.isfile(cand):
        return cand
    return None


def load_cc_txt(path: Optional[str]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Parse crawler CC .txt, aggregating text for each handle and capturing
    display names.

    Lines look roughly like:
      04:28:32 | Chelsea Belle 🇺🇸 ( @CHBMPorg ): Oh, my gosh...
    """
    handle_text: Dict[str, str] = defaultdict(str)
    handle_name: Dict[str, str] = {}

    if not path or not os.path.isfile(path):
        eprint("[gen_vtt] No CC.txt found for speaker mapping.")
        return dict(handle_text), handle_name

    pattern = re.compile(
        r"^(?P<clock>\d{2}:\d{2}:\d{2})"
        r"\s*\|\s*"
        r"(?P<name>.+?)"
        r"(?:\s*\(\s*@(?P<handle>[A-Za-z0-9_]+)\s*\))?"
        r"\s*:\s*(?P<text>.*)$"
    )

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip("\n")
            m = pattern.match(line)
            if not m:
                continue
            handle = m.group("handle")
            name = m.group("name").strip()
            text = m.group("text").strip()
            if handle:
                handle_name.setdefault(handle, name)
                if text:
                    handle_text[handle] += " " + text

    eprint(
        f"[gen_vtt] Parsed CC.txt: {len(handle_text)} handles with text, "
        f"{len(handle_name)} handles with names."
    )
    return dict(handle_text), handle_name


# ------------------------ Map speakers via similarity ----------------------


def build_corpus(tokens: List[str], max_terms: int = 500) -> Counter:
    c = Counter(tokens)
    if len(c) <= max_terms:
        return c
    # Keep top-N by frequency
    most_common = c.most_common(max_terms)
    return Counter(dict(most_common))


def map_speakers(
    dg_utts: List[DGUtterance],
    handle_text: Dict[str, str],
    handle_name: Dict[str, str],
) -> Dict[str, SpeakerInfo]:
    """
    Map Deepgram speaker IDs -> X handles using corpus-level cosine similarity.
    """
    speaker_ids = sorted({u.speaker_id for u in dg_utts})
    if not speaker_ids or not handle_text:
        return {sid: SpeakerInfo(speaker_id=sid) for sid in speaker_ids}

    # Build DG corpora per speaker
    dg_corpora: Dict[str, Counter] = {}
    for sid in speaker_ids:
        all_txt = " ".join(u.text for u in dg_utts if u.speaker_id == sid)
        toks = tokenize(all_txt)
        dg_corpora[sid] = build_corpus(toks)

    # Build handle corpora
    handle_corpora: Dict[str, Counter] = {}
    for h, txt in handle_text.items():
        toks = tokenize(txt)
        handle_corpora[h] = build_corpus(toks)

    mapping: Dict[str, SpeakerInfo] = {sid: SpeakerInfo(speaker_id=sid) for sid in speaker_ids}

    # For each DG speaker, find best matching handle
    for sid in speaker_ids:
        dg_c = dg_corpora.get(sid)
        if not dg_c:
            continue
        best_handle = None
        best_score = 0.0
        second_best = 0.0
        for h, hc in handle_corpora.items():
            score = cosine_sim(dg_c, hc)
            if score > best_score:
                second_best = best_score
                best_score = score
                best_handle = h
            elif score > second_best:
                second_best = score

        if best_handle is None:
            continue

        # Basic sanity gate: require some separation over runner-up
        # and an absolute similarity threshold.
        if best_score >= 0.18 and (best_score - second_best) >= 0.04:
            mapping[sid].handle = best_handle
            mapping[sid].name = handle_name.get(best_handle)
            eprint(
                f"[gen_vtt] Speaker {sid} -> @{best_handle} "
                f"({mapping[sid].name or 'unknown name'}), score={best_score:.3f}, "
                f"margin={best_score-second_best:.3f}"
            )
        else:
            eprint(
                f"[gen_vtt] Speaker {sid} NOT confidently mapped "
                f"(best={best_score:.3f}, second={second_best:.3f})."
            )

    return mapping


# --------------------------- Group utterances -------------------------------


def group_utterances(
    utterances: List[DGUtterance],
    max_gap: float = 1.5,
    max_chars: int = 600,
) -> List[Block]:
    """
    Group contiguous utterances from the same speaker into bigger blocks
    for the HTML transcript.
    """
    blocks: List[Block] = []
    if not utterances:
        return blocks

    cur = Block(
        start=utterances[0].start,
        end=utterances[0].end,
        speaker_id=utterances[0].speaker_id,
        text=utterances[0].text,
    )

    def append_cur():
        if cur.text.strip():
            blocks.append(Block(start=cur.start, end=cur.end, speaker_id=cur.speaker_id, text=cur.text.strip()))

    for u in utterances[1:]:
        gap = u.start - cur.end
        if (
            u.speaker_id == cur.speaker_id
            and gap >= 0
            and gap <= max_gap
            and len(cur.text) + 1 + len(u.text) <= max_chars
        ):
            # Merge into current
            sep = "" if cur.text.endswith((" ", "—", "-", "…")) else " "
            cur.text = cur.text + sep + u.text
            cur.end = max(cur.end, u.end)
        else:
            append_cur()
            cur = Block(start=u.start, end=u.end, speaker_id=u.speaker_id, text=u.text)

    append_cur()
    eprint(f"[gen_vtt] Grouped {len(utterances)} utterances into {len(blocks)} blocks.")
    return blocks


# --------------------------- Build VTT cues --------------------------------


def build_vtt_from_utterances(utterances: List[DGUtterance]) -> str:
    """
    Build WEBVTT text from Deepgram utterances.
    We keep them relatively granular (one cue per utterance).
    """
    lines = ["WEBVTT", ""]
    for idx, u in enumerate(utterances, start=1):
        start_ts = seconds_to_timestamp(u.start)
        end_ts = seconds_to_timestamp(u.end)
        lines.append(f"{idx}")
        lines.append(f"{start_ts} --> {end_ts}")
        lines.append(u.text.replace("\n", " ").strip())
        lines.append("")  # blank line
    return "\n".join(lines) + "\n"


# ------------------------- Build transcript HTML ---------------------------


CSS_BLOCK = """
<style>
.ss3k-transcript{
  font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  max-height:70vh;
  overflow-y:auto;
  scroll-behavior:smooth;
  border:1px solid #e5e7eb;
  border-radius:12px;
  padding:6px;
}
.ss3k-seg{
  display:flex;
  gap:10px;
  padding:8px 10px;
  border-radius:10px;
  margin:6px 0;
}
.ss3k-seg.active{
  background:#eef6ff;
  outline:1px solid #bfdbfe;
}
.ss3k-avatar{
  width:26px;
  height:26px;
  border-radius:50%;
  flex:0 0 26px;
  margin-top:3px;
  background:#e5e7eb;
}
.ss3k-meta{
  font-size:12px;
  color:#64748b;
  margin-bottom:2px;
}
.ss3k-name a{
  color:#0f172a;
  text-decoration:none;
}
.ss3k-text{
  white-space:pre-wrap;
  word-break:break-word;
  cursor:pointer;
}
</style>
""".strip()


def build_transcript_html(
    blocks: List[Block],
    speaker_map: Dict[str, SpeakerInfo],
) -> str:
    """
    Build clickable transcript HTML, grouping by blocks.
    Each block becomes a .ss3k-seg, with data-start/end attributes.
    """
    parts: List[str] = []
    parts.append(CSS_BLOCK)
    parts.append('<div class="ss3k-transcript">')

    for idx, b in enumerate(blocks, start=1):
        spk_info = speaker_map.get(b.speaker_id) or SpeakerInfo(speaker_id=b.speaker_id)
        if spk_info.handle:
            name_html = spk_info.name or spk_info.handle
            handle_html = f"@{spk_info.handle}"
            # Name line: "Name (@handle)" with link to X profile
            name_span = (
                f'<span class="ss3k-name"><a href="https://x.com/{spk_info.handle}" '
                f'target="_blank" rel="noopener"><strong>{name_html}</strong></a>'
                f' <span class="ss3k-handle">({handle_html})</span></span>'
            )
            avatar_html = '<div class="ss3k-avatar" aria-hidden="true"></div>'
        else:
            # Generic speaker label
            label = f"Speaker {b.speaker_id}" if b.speaker_id is not None else "Speaker"
            name_span = f'<span class="ss3k-name"><strong>{label}</strong></span>'
            avatar_html = '<div class="ss3k-avatar" aria-hidden="true"></div>'

        start_ts = seconds_to_timestamp(b.start)
        end_ts = seconds_to_timestamp(b.end)
        parts.append(
            f'<div id="seg-{idx:04d}" class="ss3k-seg" data-start="{b.start:.3f}" data-end="{b.end:.3f}">'
        )
        parts.append(avatar_html)
        parts.append('<div class="ss3k-body">')
        parts.append(
            f'<div class="ss3k-meta">{name_span} · '
            f'<time>{start_ts}</time>–<time>{end_ts}</time></div>'
        )
        # Escape minimal HTML in text (we keep it simple)
        text = (
            b.text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        parts.append(f'<div class="ss3k-text">{text}</div>')
        parts.append("</div>")  # ss3k-body
        parts.append("</div>")  # ss3k-seg

    parts.append("</div>")  # ss3k-transcript
    return "\n".join(parts) + "\n"


# ------------------------- Clock alignment (start.txt) ---------------------


def estimate_absolute_start(cc_jsonl: str, shift_secs: float) -> Optional[str]:
    """
    Use the earliest programDateTime we can find in the CC JSONL as a proxy
    for the absolute clock of the trimmed audio, minus SHIFT_SECS.
    """
    if not cc_jsonl or not os.path.isfile(cc_jsonl):
        return None

    earliest: Optional[datetime] = None

    try:
        with open(cc_jsonl, "r", encoding="utf-8") as f:
            for line in f:
                if "programDateTime" not in line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                pl = obj.get("payload")
                if isinstance(pl, str):
                    try:
                        plj = json.loads(pl)
                    except Exception:
                        plj = None
                else:
                    plj = pl if isinstance(pl, dict) else None

                if not isinstance(plj, dict):
                    continue
                body = plj.get("body")
                if isinstance(body, str):
                    try:
                        inner = json.loads(body)
                    except Exception:
                        inner = None
                else:
                    inner = body if isinstance(body, dict) else None

                if not isinstance(inner, dict):
                    continue
                pdt = inner.get("programDateTime")
                dt = parse_program_datetime(pdt) if isinstance(pdt, str) else None
                if dt is None:
                    continue
                if earliest is None or dt < earliest:
                    earliest = dt
    except Exception as e:
        eprint(f"[gen_vtt] Error while scanning CC JSONL for programDateTime: {e}")
        earliest = None

    if earliest is None:
        return None

    # Apply SHIFT_SECS (lead trim) backwards in time, if >0
    dt_start = earliest - timedelta(seconds=max(0.0, shift_secs))
    # ISO-8601 UTC
    return dt_start.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


# --------------------------- Main -----------------------------------------


def main() -> int:
    artdir = getenv("ARTDIR")
    base = getenv("BASE")
    cc_jsonl = getenv("CC_JSONL")
    dg_json = getenv("DG_JSON")
    shift_secs = safe_float(getenv("SHIFT_SECS", "0"))

    if not artdir or not base:
        eprint("[gen_vtt] ARTDIR and BASE are required.")
        return 1

    ensure_dir(artdir)

    # Load Deepgram diarization, if any
    dg_utterances = load_deepgram(dg_json)

    if not dg_utterances:
        # We *could* fall back to CC-only captions here, but historically
        # these were short / choppy. For now, we produce an empty shell
        # and let the worker handle the "no transcript" case.
        eprint("[gen_vtt] No Deepgram utterances; nothing to do.")
        # Still write minimal, valid artifacts to avoid breaking callers.
        vtt_path = os.path.join(artdir, f"{base}.vtt")
        with open(vtt_path, "w", encoding="utf-8") as f:
            f.write("WEBVTT\n\n")
        with open(os.path.join(artdir, f"{base}_transcript.html"), "w", encoding="utf-8") as f:
            f.write(CSS_BLOCK + '\n<div class="ss3k-transcript"></div>\n')
        with open(os.path.join(artdir, f"{base}_speech.json"), "w", encoding="utf-8") as f:
            json.dump([], f)
        with open(os.path.join(artdir, f"{base}_reactions.json"), "w", encoding="utf-8") as f:
            json.dump([], f)
        with open(os.path.join(artdir, f"{base}_meta.json"), "w", encoding="utf-8") as f:
            json.dump(
                {
                    "dg_utterances": 0,
                    "blocks": 0,
                    "has_cc_jsonl": bool(cc_jsonl),
                    "note": "Deepgram data missing; produced empty artifacts.",
                },
                f,
                indent=2,
            )
        return 0

    # Load CC TXT (for handle/name + similarity mapping)
    cc_txt_path = derive_cc_txt_path(cc_jsonl) if cc_jsonl else None
    handle_text, handle_name = load_cc_txt(cc_txt_path)

    # Map Deepgram speaker IDs to handles (if we can)
    speaker_map = map_speakers(dg_utterances, handle_text, handle_name)

    # Group utterances for transcript HTML
    blocks = group_utterances(dg_utterances)

    # Build and write VTT
    vtt_text = build_vtt_from_utterances(dg_utterances)
    vtt_path = os.path.join(artdir, f"{base}.vtt")
    with open(vtt_path, "w", encoding="utf-8") as f:
        f.write(vtt_text)
    eprint(f"[gen_vtt] Wrote VTT to {vtt_path}")

    # Build and write transcript HTML
    html = build_transcript_html(blocks, speaker_map)
    tr_path = os.path.join(artdir, f"{base}_transcript.html")
    with open(tr_path, "w", encoding="utf-8") as f:
        f.write(html)
    eprint(f"[gen_vtt] Wrote transcript HTML to {tr_path}")

    # Write speech.json
    speech_out = []
    for b in blocks:
        spk = speaker_map.get(b.speaker_id) or SpeakerInfo(speaker_id=b.speaker_id)
        speech_out.append(
            {
                "start": round(b.start, 3),
                "end": round(b.end, 3),
                "text": b.text,
                "speaker_id": spk.speaker_id,
                "handle": spk.handle,
                "name": spk.name,
            }
        )
    sp_path = os.path.join(artdir, f"{base}_speech.json")
    with open(sp_path, "w", encoding="utf-8") as f:
        json.dump(speech_out, f, ensure_ascii=False, indent=2)
    eprint(f"[gen_vtt] Wrote speech JSON to {sp_path}")

    # Reactions sidecar: currently empty shell (we can fill from CC JSONL later).
    reactions_path = os.path.join(artdir, f"{base}_reactions.json")
    with open(reactions_path, "w", encoding="utf-8") as f:
        json.dump([], f)
    eprint(f"[gen_vtt] Wrote empty reactions JSON to {reactions_path}")

    # start.txt (absolute clock)
    abs_start_iso = estimate_absolute_start(cc_jsonl, shift_secs) if cc_jsonl else None
    if abs_start_iso:
        with open(os.path.join(artdir, f"{base}.start.txt"), "w", encoding="utf-8") as f:
            f.write(abs_start_iso + "\n")
        eprint(f"[gen_vtt] Wrote absolute start time {abs_start_iso}")

    # meta
    meta = {
        "dg_utterances": len(dg_utterances),
        "blocks": len(blocks),
        "speakers": [asdict(s) for s in speaker_map.values()],
        "has_cc_jsonl": bool(cc_jsonl),
        "cc_txt_path": cc_txt_path,
        "shift_secs": shift_secs,
        "abs_start_iso": abs_start_iso,
    }
    meta_path = os.path.join(artdir, f"{base}_meta.json")
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    eprint(f"[gen_vtt] Wrote meta JSON to {meta_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
