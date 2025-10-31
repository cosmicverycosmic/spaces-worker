#!/usr/bin/env python3
import os, re, html, unicodedata, json, time

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
INP    = os.path.join(ARTDIR, f"{BASE}_transcript.html")
OUT    = os.path.join(ARTDIR, f"{BASE}_transcript_polished.html")
REPORT = os.path.join(ARTDIR, f"{BASE}_transcript_polish_report.json")

if not (os.path.exists(INP) and os.path.getsize(INP)>0):
    raise SystemExit(0)

t0=time.time()
raw = open(INP,"r",encoding="utf-8",errors="ignore").read()

# We only normalize spacing and remove pure-emoji lines accidentally emitted into text blocks.
TEXT_NODE = re.compile(r'(<(?:div|span)\s+class="ss3k-text"[^>]*>)(.*?)(</(?:div|span)>)', re.S|re.I)
EMOJI_RE  = re.compile("[" +
    "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF" +
    "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF" +
    "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF" + "]+", re.UNICODE)
ONLY_PSPACE = re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")

def nfc(s:str)->str:
    s = unicodedata.normalize("NFC", s or "")
    return re.sub(r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF]", "", s)

def is_emoji_only(s: str)->bool:
    if not s or not s.strip(): return False
    t = ONLY_PSPACE.sub("", s)
    t = EMOJI_RE.sub("", t)
    return len(t.strip())==0

def clean_text(s: str)->str:
    s = nfc(html.unescape(s))
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"\s+([,.;:!?])", r"\1", s)
    s = re.sub(r"([,;:])([^\s])", r"\1 \2", s)
    return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

stats={"nodes":0,"emoji_dropped":0,"changed":0}

def repl(m: re.Match)->str:
    stats["nodes"] += 1
    body = m.group(2)
    plain = html.unescape(body)
    if is_emoji_only(plain):
        stats["emoji_dropped"] += 1
        return m.group(1) + "" + m.group(3)
    cleaned = clean_text(body)
    if cleaned != body: stats["changed"] += 1
    return m.group(1) + cleaned + m.group(3)

polished = TEXT_NODE.sub(repl, raw)
polished = re.sub(r"\n{3,}", "\n\n", polished)

open(OUT,"w",encoding="utf-8").write(polished)
open(REPORT,"w",encoding="utf-8").write(json.dumps(stats,ensure_ascii=False,indent=2))
