# file: .github/workflows/scripts/polish_transcript.py
#!/usr/bin/env python3
import os, re, html
from typing import List

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
INP    = os.path.join(ARTDIR, f"{BASE}_transcript.html")
OUT    = os.path.join(ARTDIR, f"{BASE}_transcript_polished.html")

if not os.path.exists(INP) or os.path.getsize(INP) == 0:
    raise SystemExit(0)

raw_html = open(INP, "r", encoding="utf-8", errors="ignore").read()

TEXT_NODE = re.compile(r'(<(?:div|span)\s+class="ss3k-text"[^>]*>)(.*?)(</(?:div|span)>)', re.S | re.I)
URL_RE = re.compile(r"https?://[^\s<>\"']+")

FILLER_WORDS = [r"uh+", r"um+", r"er+", r"ah+", r"mm+h*", r"hmm+", r"eh+", r"uh\-huh", r"uhhuh", r"uh\-uh", r"uhuh"]
FILLER_PHRASES = [r"you\s+know", r"i\s+mean", r"kind\s+of", r"sort\s+of", r"you\s+see"]
FILLERS_RE = re.compile(r"\b(?:" + "|".join(FILLER_WORDS + FILLER_PHRASES) + r")\b", re.I)
STUTTER_RE = re.compile(r"\b([A-Za-z])(?:\s+\1\b){1,5}")
REPEAT_RE  = re.compile(r"\b([A-Za-z]{2,})\b(?:\s+\1\b){1,4}", re.I)

def sentence_case(s: str) -> str:
    s = re.sub(r"\bi\b", "I", s)
    def cap_first(m):
        pre = m.group(1) or ""
        ch  = m.group(2).upper()
        return pre + ch
    return re.sub(r"(^|\.\s+|\?\s+|!\s+)([a-z])", cap_first, s)

def ensure_end_punct(s: str) -> str:
    t = s.rstrip()
    if not t: return s
    if t[-1] in ".!?\":)””’'»]>": return s
    if URL_RE.search(t[-80:]): return s
    if len(re.findall(r"\b\w+\b", t)) >= 6:
        return t + "."
    return s

def apply_rules(txt: str) -> str:
    if not txt.strip(): return txt
    txt = FILLERS_RE.sub("", txt)
    txt = STUTTER_RE.sub(lambda m: m.group(1), txt)
    txt = REPEAT_RE.sub(lambda m: m.group(1), txt)
    txt = re.sub(r"\s{2,}", " ", txt).strip()
    txt = re.sub(r"\bi\b", "I", txt)
    txt = re.sub(r"\s+([,.;:!?])", r"\1", txt)
    txt = re.sub(r"([,;:])([^\s])", r"\1 \2", txt)
    txt = sentence_case(txt)
    txt = ensure_end_punct(txt)
    txt = txt.replace("<","&lt;").replace(">","&gt;")
    return txt

spans: List[str] = []
def _collect(m):
    spans.append(m.group(2))
    return m.group(0)
TEXT_NODE.sub(_collect, raw_html)

cleaned = [apply_rules(t) for t in spans]
it = iter(cleaned)
def _replace(m):
    open_tag, _, close_tag = m.group(1), m.group(2), m.group(3)
    try:
        new_text = next(it)
    except StopIteration:
        new_text = m.group(2)
    return f"{open_tag}{new_text}{close_tag}"

polished_html = TEXT_NODE.sub(_replace, raw_html)
polished_html = re.sub(r"\n{3,}", "\n\n", polished_html)
with open(OUT, "w", encoding="utf-8") as f:
    f.write(polished_html)
