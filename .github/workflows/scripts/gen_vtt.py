#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate:
- captions VTT:  ARTDIR/BASE.vtt
- emoji VTT:     ARTDIR/BASE_emoji.vtt  (cue text is compact JSON: {"e":"😀","user":"@name"})
- raw transcript HTML lines: ARTDIR/BASE_transcript.html (speaker: text; no visible timestamps)
Input: a crawler JSONL containing speech segments and emoji reactions.
We accept many shapes; see _classify().
"""
from __future__ import annotations
import os, sys, json, math, random, re
from typing import Any, Dict, List, Tuple
from utils import ensure_dir, log, html_escape, cue_time, merge_adjacent_by_speaker, first, safe_int

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
JSONL_PATH = os.environ.get("JSONL_PATH","") or os.environ.get("CRAWL_JSONL","") or os.path.join(ARTDIR, f"{BASE}.jsonl")
LOG_PATH   = os.path.join(ARTDIR, f"{BASE}_genvtt.log")

OUT_VTT         = os.path.join(ARTDIR, f"{BASE}.vtt")
OUT_EMOJI_VTT   = os.path.join(ARTDIR, f"{BASE}_emoji.vtt")
OUT_TRANS_HTML  = os.path.join(ARTDIR, f"{BASE}_transcript.html")
OUT_PARA_HTML   = os.path.join(ARTDIR, f"{BASE}_transcript_paragraphs.html")

ensure_dir(ARTDIR)

def load_jsonl(path: str) -> List[Dict[str,Any]]:
    rows = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line: continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows

def _classify(o: Dict[str,Any]) -> Tuple[str, Dict[str,Any]]:
    """
    Try to classify each JSONL row as 'segment' (speech) or 'emoji' (reaction) or 'other'.
    Returns (kind, normalized_dict)
    segment: {begin:float, end:float, speaker:str, text:str}
    emoji:   {time:float, emoji:str, user:str}
    """
    # Heuristics for speech
    for cand in (o, o.get("segment") or {}, o.get("asr") or {}, o.get("speech") or {}, o.get("transcript") or {}):
        text = first(cand.get("text"), cand.get("content"), cand.get("utterance"), "")
        begin = first(cand.get("start"), cand.get("begin"), cand.get("t0"), None)
        end   = first(cand.get("end"), cand.get("t1"), None)
        speaker = first(cand.get("speaker"), cand.get("user"), cand.get("name"), "")
        if text and begin is not None:
            # fallback duration heuristic
            try:
                begin = float(begin)
            except Exception:
                begin = 0.0
            try:
                end = float(end) if end is not None else (begin + max(1.5, 0.06*len(str(text))))
            except Exception:
                end = begin + max(1.5, 0.06*len(str(text)))
            return "segment", {"begin": max(0.0, begin), "end": max(begin, end), "speaker": str(speaker or "").strip(), "text": str(text).strip()}

    # Heuristics for reactions
    for cand in (o, o.get("reaction") or {}, o.get("emoji") or {}, o.get("event") or {}):
        e = first(cand.get("emoji"), cand.get("e"), cand.get("value"), "")
        t = first(cand.get("time"), cand.get("ts"), cand.get("start"), cand.get("t"), None)
        user = first(cand.get("user"), cand.get("handle"), cand.get("name"), "")
        if e and t is not None:
            try:
                t = float(t)
            except Exception:
                t = 0.0
            return "emoji", {"time": max(0.0, t), "emoji": str(e), "user": str(user or "").strip()}
    return "other", {}

def build_vtts(rows: List[Dict[str,Any]]) -> Tuple[str, str]:
    segs, emos = [], []
    for o in rows:
        kind, data = _classify(o)
        if kind == "segment":
            if data.get("text","").strip():
                segs.append(data)
        elif kind == "emoji":
            emos.append(data)

    segs.sort(key=lambda r: (r["begin"], r["end"]))
    emos.sort(key=lambda r: r["time"])

    # Captions VTT (text only; UI handles highlighting)
    parts = ["WEBVTT", ""]
    for i, r in enumerate(segs, 1):
        parts.append(f"{i}")
        parts.append(f"{cue_time(r['begin'])} --> {cue_time(r['end'])}")
        # Keep text only; no timestamps or speaker here
        parts.append(r["text"].replace("\n", " ").strip())
        parts.append("")  # blank between cues
    captions_vtt = "\n".join(parts).rstrip() + "\n"

    # Emoji VTT: json payload per cue
    ep = ["WEBVTT", ""]
    for i, e in enumerate(emos, 1):
        start = e["time"]
        end = start + 2.5  # short float window
        payload = {"e": e["emoji"], "user": e.get("user","")}
        ep.append(f"{i}")
        ep.append(f"{cue_time(start)} --> {cue_time(end)}")
        ep.append(json.dumps(payload, ensure_ascii=False))
        ep.append("")
    emoji_vtt = "\n".join(ep).rstrip() + "\n"
    return captions_vtt, emoji_vtt, segs

def render_transcript_html(segs: List[Dict[str,Any]]) -> Tuple[str, str]:
    # Line-based (no visible timestamps)
    lines = []
    for r in segs:
        spk = (r.get("speaker") or "").strip()
        txt = r.get("text","").strip()
        lines.append(
            f'<div class="ss3k-line" data-begin="{r["begin"]:.3f}" data-end="{r["end"]:.3f}" data-speaker="{html_escape(spk)}">'
            f'<span class="speaker">{html_escape(spk)}</span>: {html_escape(txt)}</div>'
        )
    line_html = "\n".join(lines)

    # Paragraphs (merge adjacent same-speaker)
    merged = merge_adjacent_by_speaker(segs, gap=6.0)
    paras = []
    for r in merged:
        spk = (r.get("speaker") or "").strip()
        txt = r.get("text","").strip()
        paras.append(
            f'<p class="ss3k-para" data-begin="{r["begin"]:.3f}" data-end="{r["end"]:.3f}" data-speaker="{html_escape(spk)}">'
            f'<span class="speaker">{html_escape(spk)}</span>: {html_escape(txt)}</p>'
        )
    para_html = "\n".join(paras)
    return line_html, para_html

def main():
    try:
        if not JSONL_PATH or not os.path.isfile(JSONL_PATH):
            log("JSONL not found; writing minimal outputs.", LOG_PATH)
            with open(OUT_VTT, "w", encoding="utf-8") as fh: fh.write("WEBVTT\n\n")
            with open(OUT_EMOJI_VTT, "w", encoding="utf-8") as fh: fh.write("WEBVTT\n\n")
            with open(OUT_TRANS_HTML, "w", encoding="utf-8") as fh: fh.write("")
            with open(OUT_PARA_HTML, "w", encoding="utf-8") as fh: fh.write("")
            return 0

        raw = load_jsonl(JSONL_PATH)
        captions_vtt, emoji_vtt, segs = build_vtts(raw)
        line_html, para_html = render_transcript_html(segs)

        with open(OUT_VTT, "w", encoding="utf-8") as fh: fh.write(captions_vtt)
        with open(OUT_EMOJI_VTT, "w", encoding="utf-8") as fh: fh.write(emoji_vtt)
        with open(OUT_TRANS_HTML, "w", encoding="utf-8") as fh: fh.write(line_html)
        with open(OUT_PARA_HTML, "w", encoding="utf-8") as fh: fh.write(para_html)

        log(f"Wrote {OUT_VTT}, {OUT_EMOJI_VTT}, {OUT_TRANS_HTML}, {OUT_PARA_HTML}", LOG_PATH)
        return 0
    except Exception as e:
        log(f"ERROR: {e}", LOG_PATH)
        return 1

if __name__ == "__main__":
    sys.exit(main())
