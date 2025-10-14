#!/usr/bin/env python3
import os, re, html, json
from typing import List, Tuple

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
INP    = os.path.join(ARTDIR, f"{BASE}_transcript.html")
OUT    = os.path.join(ARTDIR, f"{BASE}_transcript_polished.html")

# ------------ Config (free, on-runner) ------------
USE_GEC = os.environ.get("TRANSCRIPT_GEC", "true").lower() in ("1","true","yes","on")
GEC_MODEL = os.environ.get("TRANSCRIPT_GEC_MODEL", "prithivida/grammar_error_correcter_v1")
GEC_MAX_LINES = int(os.environ.get("TRANSCRIPT_GEC_MAX_LINES", "10"))  # <= 10 as requested
GEC_BATCH_SIZE = int(os.environ.get("TRANSCRIPT_GEC_BATCH", "4"))      # small batches for CPU
GEC_SELECT = os.environ.get("TRANSCRIPT_GEC_SELECT", "auto")           # auto|longest|first

# ------------ Early exit ------------
if not os.path.exists(INP) or os.path.getsize(INP) == 0:
    raise SystemExit(0)

with open(INP, "r", encoding="utf-8", errors="ignore") as f:
    raw_html = f.read()

# ------------ Targets ------------
TEXT_SPAN_RE = re.compile(r'(<span\s+class="ss3k-text"[^>]*>)(.*?)(</span>)', re.S | re.I)
URL_RE = re.compile(r"https?://[^\s<>\"]+")

# ------------ Rule-based cleanup ------------
FILLER_WORDS = [
    r"uh+", r"um+", r"er+", r"ah+", r"mm+h*", r"hmm+", r"eh+", r"uh\-huh", r"uhhuh", r"uh\-uh", r"uhuh",
]
FILLER_PHRASES = [
    r"you\s+know", r"i\s+mean", r"kind\s+of", r"sort\s+of", r"you\s+see",
]
FILLERS_RE = re.compile(r"\b(?:" + "|".join(FILLER_WORDS + FILLER_PHRASES) + r")\b", re.I)
STUTTER_RE = re.compile(r"\b([A-Za-z])(?:\s+\1\b){1,5}")
REPEAT_RE  = re.compile(r"\b([A-Za-z]{2,})\b(?:\s+\1\b){1,4}", re.I)

def sentence_case(s: str) -> str:
    s = re.sub(r"\bi\b", "I", s)
    def cap_first(m):
        pre = m.group(1) or ""
        ch  = m.group(2).upper()
        return pre + ch
    s = re.sub(r"(^|\.\s+|\?\s+|!\s+)([a-z])", cap_first, s)
    return s

def ensure_end_punct(s: str) -> str:
    t = s.rstrip()
    if not t: return s
    if t[-1] in ".!?\":)””’'»]>": return s
    if len(re.findall(r"\b\w+\b", t)) >= 6 and not URL_RE.search(t[-40:]):
        return t + "."
    return s

def match_case(repl: str, orig: str) -> str:
    if orig.isupper(): return repl.upper()
    if orig.islower(): return repl.lower()
    if orig[:1].isupper() and orig[1:].islower(): return repl.capitalize()
    return repl

EGGCORNS = [
    (r"\byouth\s*[- ]\s*in\s*[- ]\s*asia\b", "euthanasia"),
    (r"\beuthin(?:a|e)sia\b", "euthanasia"),
    (r"\bcould\s+of\b", "could have"),
    (r"\bshould\s+of\b", "should have"),
    (r"\bwould\s+of\b", "would have"),
    (r"\bmute\s+point\b", "moot point"),
    (r"\bfor\s+all\s+intensive\s+purposes\b", "for all intents and purposes"),
    (r"\bcase\s+and\s+point\b", "case in point"),
    (r"\bdeep\s+seeded\b", "deep-seated"),
    (r"\bslight\s+of\s+hand\b", "sleight of hand"),
    (r"\bescape\s+goat\b", "scapegoat"),
    (r"\bbaited\s+breath\b", "bated breath"),
    (r"\bpeaked\s+my\s+interest\b", "piqued my interest"),
    (r"\bwet\s+your\s+appetite\b", "whet your appetite"),
    (r"\btounge\b", "tongue"),
    (r"\btounge\s*[- ]\s*in\s*[- ]\s*cheek\b", "tongue-in-cheek"),
    (r"\btow\s+the\s+line\b", "toe the line"),
    (r"\bplay\s+it\s+by\s+year\b", "play it by ear"),
    (r"\bfree\s+reign\b", "free rein"),
    (r"\bold\s*[- ]?\s*timer'?s\s+disease\b", "Alzheimer's disease"),
    (r"\bwreckless\s+driving\b", "reckless driving"),
    (r"\bminus\s+well\b", "might as well"),
    (r"\bfirst\s+come\s*,?\s*first\s+serve\b", "first come, first served"),
    (r"\bnip\s+it\s+in\s+the\s+butt\b", "nip it in the bud"),
    (r"\bhome\s+in\s+on\b", "home in on"),
    (r"\bhone\s+in\s+on\b", "home in on"),
    (r"\bchest\s*[- ]\s*of\s*[- ]\s*drawers\b", "chest of drawers"),
]
EGG_REPLACERS = [(re.compile(p, re.I), r) for p, r in EGGCORNS]

def apply_eggcorns(s: str) -> str:
    def sub_with_case(rx, rep, txt):
        def _f(m): return match_case(rep, m.group(0))
        return rx.sub(_f, txt)
    for rx, rep in EGG_REPLACERS:
        s = sub_with_case(rx, rep, s)
    return s

def protect_urls(s: str) -> Tuple[str, List[str]]:
    urls = []
    def stash(m):
        urls.append(m.group(0))
        return f"<<<URL{len(urls)-1}>>>"
    return URL_RE.sub(stash, s), urls

def restore_urls(s: str, urls: List[str]) -> str:
    for i,u in enumerate(urls):
        s = s.replace(f"<<<URL{i}>>>", u)
    return s

def clean_text(txt: str) -> str:
    if not txt.strip(): return txt
    txt, urls = protect_urls(txt)
    txt = FILLERS_RE.sub("", txt)
    txt = STUTTER_RE.sub(lambda m: m.group(1), txt)
    txt = REPEAT_RE.sub(lambda m: m.group(1), txt)
    txt = re.sub(r"\s{2,}", " ", txt).strip()
    txt = re.sub(r"\bi\b", "I", txt)
    txt = re.sub(r"\s+([,.;:!?])", r"\1", txt)
    txt = re.sub(r"([,;:])([^\s])", r"\1 \2", txt)
    txt = apply_eggcorns(txt)
    txt = sentence_case(txt)
    txt = ensure_end_punct(txt)
    txt = restore_urls(txt, urls)
    # Safety: ensure no stray angle brackets
    txt = txt.replace("<","&lt;").replace(">","&gt;")
    return txt

# ------------ Collect spans ------------
spans: List[str] = []
def _collect(m):
    spans.append(m.group(2))
    return m.group(0)
_ = TEXT_SPAN_RE.sub(_collect, raw_html)

# First pass rule-based
cleaned = [clean_text(t) for t in spans]

# ------------ Heuristic selection for ML (max 10) ------------
def quality_score(s: str) -> float:
    # Higher = more likely to need help
    plain = html.unescape(s)
    longness = min(len(plain), 2000) / 10.0
    no_punct = 15.0 if not re.search(r"[.!?]", plain) else 0.0
    mostly_lower = 12.0 if (re.sub(r"[^A-Za-z]+","",plain).islower() and len(plain) > 20) else 0.0
    many_commas = -3.0 if plain.count(",") >= 3 else 0.0
    return longness + no_punct + mostly_lower + many_commas

indices = list(range(len(cleaned)))
if GEC_SELECT == "first":
    candidate_idxs = indices[:GEC_MAX_LINES]
else:
    # auto (score-based) or longest
    if GEC_SELECT == "longest":
        scored = sorted(indices, key=lambda i: len(cleaned[i]), reverse=True)
    else:
        scored = sorted(indices, key=lambda i: quality_score(cleaned[i]), reverse=True)
    candidate_idxs = scored[:max(0, min(GEC_MAX_LINES, len(scored)))]

# ------------ Optional GEC model (free, local) ------------
def apply_gec(lines: List[str]) -> List[str]:
    try:
        from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
        import torch
    except Exception:
        return lines  # transformers unavailable

    try:
        tok = AutoTokenizer.from_pretrained(GEC_MODEL)
        mdl = AutoModelForSeq2SeqLM.from_pretrained(GEC_MODEL)
        mdl.eval()
        device = torch.device("cpu")
        mdl.to(device)
    except Exception:
        return lines

    out = []
    buf = []
    # Protect URLs before sending to model (models often mangle them)
    url_buckets: List[List[str]] = []

    def flush():
        nonlocal out, buf, url_buckets
        if not buf: return
        # Model expects "gec: " prefix (for prithivida model)
        inputs = [("gec: " + b) for b in buf]
        enc = tok(inputs, return_tensors="pt", padding=True, truncation=True, max_length=512)
        with torch.no_grad():
            gen = mdl.generate(
                input_ids=enc["input_ids"].to(device),
                attention_mask=enc["attention_mask"].to(device),
                max_new_tokens=96,
                num_beams=4,
                length_penalty=1.0,
                early_stopping=True,
            )
        outs = tok.batch_decode(gen, skip_special_tokens=True)
        # Post: restore URLs + HTML-escape angle brackets
        for i, o in enumerate(outs):
            o = o.strip()
            o = restore_urls(o, url_buckets[i])
            o = o.replace("<","&lt;").replace(">","&gt;")
            # Run eggcorns again just in case model missed some
            o = apply_eggcorns(o)
            o = ensure_end_punct(sentence_case(o))
            out.append(o)

        buf = []
        url_buckets = []

    for line in lines:
        # unescape HTML for better model context
        pl = html.unescape(line)
        # protect URLs
        pl, urls = protect_urls(pl)
        url_buckets.append(urls)
        buf.append(pl)
        if len(buf) >= GEC_BATCH_SIZE:
            flush()
    flush()
    return out

if USE_GEC and candidate_idxs:
    # Prepare selected lines for ML, others pass through unchanged
    selected = [cleaned[i] for i in candidate_idxs]
    corrected = apply_gec(selected)
    # Merge back
    for pos, idx in enumerate(candidate_idxs):
        cleaned[idx] = corrected[pos]

# ------------ Rebuild HTML ------------
it = iter(cleaned)
def _replace(m):
    open_tag, _, close_tag = m.group(1), m.group(2), m.group(3)
    try:
        new_text = next(it)
    except StopIteration:
        new_text = m.group(2)
    return f"{open_tag}{new_text}{close_tag}"

polished_html = TEXT_SPAN_RE.sub(_replace, raw_html)
polished_html = re.sub(r"\n{3,}", "\n\n", polished_html)

with open(OUT, "w", encoding="utf-8") as f:
    f.write(polished_html)
