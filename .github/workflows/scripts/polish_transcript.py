#!/usr/bin/env python3
import os, re

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
INP    = os.path.join(ARTDIR, f"{BASE}_transcript.html")
OUT    = os.path.join(ARTDIR, f"{BASE}_transcript_polished.html")

if not (os.path.exists(INP) and os.path.getsize(INP)>0):
    raise SystemExit(0)

html = open(INP,"r",encoding="utf-8",errors="ignore").read()

# Final safety: strip any lingering emoji that slipped through
EMOJI_RE = re.compile("[" +
    "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF" +
    "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF" +
    "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF" + "]+", re.UNICODE)

def strip_emojis_in_text_nodes(s: str) -> str:
    return re.sub(r'(<(?:div|span)\s+class="ss3k-text"[^>]*>)(.*?)(</(?:div|span)>)',
                  lambda m: m.group(1) + EMOJI_RE.sub("", m.group(2)) + m.group(3),
                  s, flags=re.S|re.I)

html = strip_emojis_in_text_nodes(html)
html = re.sub(r"\n{3,}", "\n\n", html)

with open(OUT,"w",encoding="utf-8") as f:
    f.write(html)
