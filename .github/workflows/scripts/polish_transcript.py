#!/usr/bin/env python3
import os, re, html, unicodedata, json, time

ARTDIR=os.environ.get("ARTDIR",".")
BASE=os.environ.get("BASE","space")
INP=os.path.join(ARTDIR,f"{BASE}_transcript.html")
OUT=os.path.join(ARTDIR,f"{BASE}_transcript_polished.html")
REPORT=os.path.join(ARTDIR,f"{BASE}_transcript_polish_report.json")

if not os.path.exists(INP) or os.path.getsize(INP)==0:
    raise SystemExit(0)

t0=time.time()
raw=open(INP,"r",encoding="utf-8",errors="ignore").read()

TEXT_NODE=re.compile(r'(<div class="ss3k-text"[^>]*>)(.*?)(</div>)',re.S)
EMOJI_RE=re.compile("["+"\U0001F1E6-\U0001F1FF\U0001F300-\U0001FAD6\u2600-\u26FF\u2700-\u27BF"+"]+")
ONLY_PUNCT_SPACE=re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")

def nfc(s): return re.sub(r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF]","",unicodedata.normalize("NFC",s or ""))
def is_emoji_only(s):
    t=ONLY_PUNCT_SPACE.sub("",s or "")
    return len(EMOJI_RE.sub("",t).strip())==0

def clean(s):
    s=nfc(html.unescape(s))
    s=re.sub(r"\s+"," ",s).strip()
    s=re.sub(r"\s+([,.;:!?])",r"\1",s)
    s=re.sub(r"([,;:])([^\s])",r"\1 \2",s)
    return s

nodes=TEXT_NODE.findall(raw)
stats={"total":len(nodes),"changed":0,"emoji_dropped":0}
for o,b,c in nodes:
    plain=html.unescape(b)
    if is_emoji_only(plain):
        raw=raw.replace(o+b+c,"")
        stats["emoji_dropped"]+=1
    else:
        new=clean(plain)
        if new!=plain: stats["changed"]+=1
        raw=raw.replace(o+b+c,o+html.escape(new)+c)
open(OUT,"w",encoding="utf-8").write(raw)
stats["duration_sec"]=round(time.time()-t0,3)
open(REPORT,"w",encoding="utf-8").write(json.dumps(stats,ensure_ascii=False,indent=2))
