#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
polish_transcript.py
--------------------
Polishes the HTML transcript ONLY (does not touch VTT).
Default behavior is conservative and *faithful to the speaker text*.

ENV:
  ARTDIR                         - output directory
  BASE                           - base filename
  TRANSCRIPT_GEC                 - "true"/"false" (default false in workflow)
  TRANSCRIPT_GEC_MODEL           - optional HF model id (if GEC true)
  TRANSCRIPT_GEC_MAX_LINES       - max lines to pass to GEC (int)
  TRANSCRIPT_GEC_BATCH           - batch size (int)
  TRANSCRIPT_SMART_QUOTES        - "true"/"false" (default false)
  TRANSCRIPT_STRIP_EMOJI         - "true"/"false" (default true)
  TRANSCRIPT_DROP_EMOJI_ONLY     - "true"/"false" (default true)
  TRANSCRIPT_REPORT_JSON         - optional path for stats report JSON
"""

import os, re, html, json, time, unicodedata
from typing import List, Tuple

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")

INP    = os.path.join(ARTDIR, f"{BASE}_transcript.html")
OUT    = os.path.join(ARTDIR, f"{BASE}_transcript_polished.html")
REPORT = os.environ.get("TRANSCRIPT_REPORT_JSON", os.path.join(ARTDIR, f"{BASE}_transcript_polish_report.json"))

USE_GEC = os.environ.get("TRANSCRIPT_GEC", "false").lower() in ("1","true","yes","on")
GEC_MODEL = os.environ.get("TRANSCRIPT_GEC_MODEL", "prithivida/grammar_error_correcter_v1")
GEC_MAX_LINES = int(os.environ.get("TRANSCRIPT_GEC_MAX_LINES", "10"))
GEC_BATCH_SIZE = int(os.environ.get("TRANSCRIPT_GEC_BATCH", "4"))

SMART_QUOTES    = os.environ.get("TRANSCRIPT_SMART_QUOTES", "false").lower() in ("1","true","yes","on")
STRIP_EMOJI     = os.environ.get("TRANSCRIPT_STRIP_EMOJI", "true").lower() in ("1","true","yes","on")
DROP_EMOJI_ONLY = os.environ.get("TRANSCRIPT_DROP_EMOJI_ONLY", "true").lower() in ("1","true","yes","on")

if not os.path.exists(INP) or os.path.getsize(INP) == 0:
    raise SystemExit(0)

t0 = time.time()
raw_html = open(INP, "r", encoding="utf-8", errors="ignore").read()

TEXT_NODE = re.compile(r'(<(?:div|span)\s+class="ss3k-text"[^>]*>)(.*?)(</(?:div|span)>)', re.S | re.I)
URL_RE = re.compile(r"https?://[^\s<>\"]+")

# emoji detection
EMOJI_RE = re.compile("[" +
    "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF" +
    "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF" +
    "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF" + "]+", re.UNICODE)
ONLY_PUNCT_SPACE = re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")

def strip_controls(s: str) -> str:
    if not s: return ""
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Cf" and ch not in ("\u200b", "\ufeff"))
    s = "".join(ch for ch in s if (unicodedata.category(ch) != "Cc") or ch in ("\n", "\t"))
    return s

def is_emoji_only(s: str) -> bool:
    if not s or not s.strip(): return False
    t = ONLY_PUNCT_SPACE.sub("", s)
    t = EMOJI_RE.sub("", t)
    return len(t.strip()) == 0

def to_smart_quotes(s: str) -> str:
    s = s.replace("---","—").replace("--","–")
    s = re.sub(r'(^|[\s\(\[])"', r'\1“', s)
    s = s.replace('"', '”')
    s = re.sub(r"(^|[\s\(\[])'", r"\1‘", s)
    s = s.replace("'", "’")
    return s

def ensure_end_punct(s: str) -> str:
    t = s.rstrip()
    if not t: return s
    if t[-1] in ".!?\":)”’'»]>": return s
    if URL_RE.search(t[-80:]): return s
    # add a soft period for long lines with no terminal punctuation
    if len(re.findall(r"\b\w+\b", t)) >= 8: return t + "."
    return s

def base_clean(txt: str, stats) -> str:
    if txt is None: return ""
    orig = txt
    txt = strip_controls(html.unescape(txt))
    if DROP_EMOJI_ONLY and is_emoji_only(txt):
        stats["emoji_dropped"] += 1
        return ""
    if STRIP_EMOJI:
        before = txt
        txt = EMOJI_RE.sub("", txt)
        if txt != before: stats["emoji_stripped"] += 1

    # collapse whitespace
    txt = re.sub(r"\s+", " ", txt).strip()

    # simple spacing around punctuation
    txt = re.sub(r"\s+([,.;:!?])", r"\1", txt)
    txt = re.sub(r"([,;:])([^\s])", r"\1 \2", txt)

    # keep casing as-is (faithful), but fix lone lowercase " i " → " I "
    txt = re.sub(r"\bi\b", "I", txt)

    # ensure some terminal punctuation if missing for long lines
    txt = ensure_end_punct(txt)

    if SMART_QUOTES:
        txt = to_smart_quotes(txt)

    # Escape HTML
    txt = txt.replace("<","&lt;").replace(">","&gt;")

    # heuristic guard: if we somehow introduced suspicious IPA range chars or digit-sandwich, revert
    def looks_glitchy(s: str) -> bool:
        if re.search(r"[\u0250-\u02AF]", s):  # IPA block
            return True
        if "\uFFFD" in s:
            return True
        if re.search(r"(?<=\w)\s*[0-9]\s*(?=\w)", s):
            return True
        return False

    ratio = (len(txt)+1)/(len(orig)+1)
    if looks_glitchy(html.unescape(txt)) or not (0.4 <= ratio <= 2.0):
        return orig  # fall back to original if anything feels off

    return txt

# collect raw text nodes in order
spans: List[str] = []
def _collect(m):
    spans.append(m.group(2))
    return m.group(0)
TEXT_NODE.sub(_collect, raw_html)

stats = {"total_nodes": len(spans), "emoji_stripped": 0, "emoji_dropped": 0,
         "gec_attempted": 0, "gec_applied": 0, "changed_nodes": 0, "duration_sec": 0.0}

cleaned = [base_clean(t, stats) for t in spans]

# Optional: light GEC on a small subset (OFF by default)
def apply_gec(lines: List[str]) -> List[str]:
    try:
        from transformers import AutoTokenizer, AutoModelForSeq2SeqLM  # heavy, optional
        import torch  # noqa
    except Exception:
        return lines
    try:
        tok = AutoTokenizer.from_pretrained(GEC_MODEL)
        mdl = AutoModelForSeq2SeqLM.from_pretrained(GEC_MODEL)
        mdl.eval()
    except Exception:
        return lines

    outs: List[str] = []
    buf: List[str] = []

    def flush():
        nonlocal outs, buf
        if not buf: return
        enc = tok(buf, return_tensors="pt", padding=True, truncation=True, max_length=512)
        gen = mdl.generate(input_ids=enc["input_ids"], attention_mask=enc["attention_mask"],
                           max_new_tokens=96, num_beams=4, length_penalty=1.0, early_stopping=True)
        dec = tok.batch_decode(gen, skip_special_tokens=True)
        for o in dec:
            o = o.strip().replace("<","&lt;").replace(">","&gt;")
            o = ensure_end_punct(o)
            outs.append(o)
        buf.clear()

    for line in lines:
        buf.append(html.unescape(line))
        if len(buf) >= GEC_BATCH_SIZE: flush()
    flush()
    return outs

if USE_GEC:
    # choose lines with no punctuation & longer than ~10 words (best ROI)
    cand_idx = [i for i, s in enumerate(cleaned) if s and not re.search(r"[.!?]", s) and len(re.findall(r"\b\w+\b", s)) >= 12]
    cand_idx = cand_idx[:GEC_MAX_LINES]
    if cand_idx:
        stats["gec_attempted"] = len(cand_idx)
        corrected = apply_gec([cleaned[i] for i in cand_idx])
        for pos, idx in enumerate(cand_idx):
            if pos < len(corrected) and corrected[pos].strip():
                new = corrected[pos]
                if new != cleaned[idx]:
                    cleaned[idx] = new
                    stats["gec_applied"] += 1

# Rebuild HTML
it = iter(cleaned)
def _replace(m):
    open_tag, old_text, close_tag = m.group(1), m.group(2), m.group(3)
    try:
        new_text = next(it)
    except StopIteration:
        new_text = old_text
    return f"{open_tag}{new_text}{close_tag}"

polished_html = TEXT_NODE.sub(_replace, raw_html)
polished_html = re.sub(r"\n{3,}", "\n\n", polished_html)

with open(OUT, "w", encoding="utf-8") as f:
    f.write(polished_html)

stats["changed_nodes"] = sum(1 for a, b in zip(spans, cleaned) if a != b)
stats["duration_sec"] = round(time.time() - t0, 3)
try:
    open(REPORT, "w", encoding="utf-8").write(json.dumps(stats, ensure_ascii=False, indent=2))
except Exception:
    pass
