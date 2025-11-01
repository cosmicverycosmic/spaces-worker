# .github/workflows/scripts/polish_transcript.py  (REPLACE)
#!/usr/bin/env python3
import os, re, html, unicodedata, json, time

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
INP    = os.path.join(ARTDIR, f"{BASE}_transcript.html")
OUT    = os.path.join(ARTDIR, f"{BASE}_transcript_polished.html")
REPORT = os.path.join(ARTDIR, f"{BASE}_transcript_polish_report.json")

if not os.path.exists(INP) or os.path.getsize(INP) == 0:
    raise SystemExit(0)

t0=time.time()
raw = open(INP, "r", encoding="utf-8", errors="ignore").read()

TEXT_NODE = re.compile(r'(<(?:div|span)\s+class="ss3k-text"[^>]*>)(.*?)(</(?:div|span)>)', re.S|re.I)
EMOJI_RE  = re.compile("[" +
    "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF" +
    "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF" +
    "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF" + "]+", re.UNICODE)
ONLY_PUNCT_SPACE = re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")

def nfc(s:str)->str:
    if not s: return ""
    s = unicodedata.normalize("NFC", s)
    # strip zero-width & bidi controls that can render weird glyphs
    return re.sub(r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF]", "", s)

def is_emoji_only(s: str) -> bool:
    if not s or not s.strip(): return False
    t = ONLY_PUNCT_SPACE.sub("", s)
    t = EMOJI_RE.sub("", t)
    return len(t.strip()) == 0

def clean_text(s: str) -> str:
    s = nfc(s)
    # collapse whitespace, but preserve single spaces
    s = re.sub(r"\s+", " ", s).strip()
    # normalize stray spaces around punctuation
    s = re.sub(r"\s+([,.;:!?])", r"\1", s)
    s = re.sub(r"([,;:])([^\s])", r"\1 \2", s)
    # don’t touch apostrophes/quotes/dashes (intent!)
    return s

stats={"nodes":0,"emoji_dropped":0,"changed":0,"duration_sec":0.0}

out = []
pos = 0
for m in TEXT_NODE.finditer(raw):
    out.append(raw[pos:m.start()])
    open_tag, body, close_tag = m.groups()
    stats["nodes"] += 1
    body_dec = body  # already escaped by generator; keep entities as-is
    plain = html.unescape(body_dec)

    if is_emoji_only(plain):
        # Drop pure emoji rows entirely
        stats["emoji_dropped"] += 1
        new_body = ""
    else:
        cleaned = clean_text(plain)
        if cleaned != plain: stats["changed"] += 1
        # re-escape only the critical chars (keep quotes as-is)
        new_body = cleaned.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

    out.append(f"{open_tag}{new_body}{close_tag}")
    pos = m.end()

out.append(raw[pos:])
polished = "".join(out)

open(OUT,"w",encoding="utf-8").write(polished)
stats["duration_sec"]=round(time.time()-t0,3)
open(REPORT,"w",encoding="utf-8").write(json.dumps(stats,ensure_ascii=False,indent=2))
