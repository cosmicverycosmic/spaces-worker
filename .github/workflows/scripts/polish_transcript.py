#!/usr/bin/env python3
# Light polishing pass:
# - ensure no emoji in transcript text nodes
# - normalize whitespace
# - keep markup minimal (no timestamps added here)

import os, re
from pathlib import Path

ARTDIR = Path(os.environ.get("ARTDIR","."))
BASE   = os.environ.get("BASE","space")

INP = ARTDIR / f"{BASE}_transcript.html"
OUT = ARTDIR / f"{BASE}_transcript_polished.html"
if not INP.is_file():
    raise SystemExit(0)

EMOJI_RE = re.compile(
    r"[\U0001F300-\U0001F5FF"
    r"\U0001F600-\U0001F64F"
    r"\U0001F680-\U0001F6FF"
    r"\U0001F700-\U0001F77F"
    r"\U0001F780-\U0001F7FF"
    r"\U0001F800-\U0001F8FF"
    r"\U0001F900-\U0001F9FF"
    r"\U0001FA00-\U0001FA6F"
    r"\U0001FA70-\U0001FAFF"
    r"\U00002700-\U000027BF"
    r"\U00002600-\U000026FF"
    r"\U00002B00-\U00002BFF"
    r"]+", flags=re.UNICODE
)

s = INP.read_text(encoding="utf-8")

def strip_emojis(t): return EMOJI_RE.sub("", t)

# Remove any emojis in visible text nodes
s = re.sub(r'(<div class="ss3k-text">)(.*?)(</div>)',
           lambda m: m.group(1) + strip_emojis(m.group(2)) + m.group(3),
           s, flags=re.S)

# compact whitespace
s = re.sub(r'[ \t]+', ' ', s)
s = re.sub(r'\n{3,}', '\n\n', s)

OUT.write_text(s, encoding="utf-8")
print(f"Polished transcript -> {OUT}")
