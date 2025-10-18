#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, html, unicodedata

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
INP    = os.path.join(ARTDIR, f"{BASE}_transcript.html")
OUT    = os.path.join(ARTDIR, f"{BASE}_transcript_polished.html")

if not os.path.isfile(INP) or os.path.getsize(INP)==0:
    raise SystemExit(0)

RAW = open(INP,"r",encoding="utf-8",errors="ignore").read()

TEXT_NODE = re.compile(r'(<(?:div|span)\s+class="ss3k-text"[^>]*>)(.*?)(</(?:div|span)>)', re.S|re.I)
ZW_RE  = re.compile(r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F]")
SPC_RE = re.compile(r"\s+")
SPACE_BEFORE_PUNCT = re.compile(r"\s+([,.;:!?])")
PUNCT_STICKY       = re.compile(r"([,;:])([^\s])")

def ascii_punct(s:str)->str:
    s = unicodedata.normalize("NFC", s or "")
    s = ZW_RE.sub("", s)
    s = s.replace("\u2018","'").replace("\u2019","'")
    s = s.replace("\u201C",'"').replace("\u201D",'"')
    s = re.sub(r"\bi\b", "I", s)  # lone i → I (minimal)
    s = SPACE_BEFORE_PUNCT.sub(r"\1", s)
    s = PUNCT_STICKY.sub(r"\1 \2", s)
    s = SPC_RE.sub(" ", s).strip()
    return s

def repl(m):
    open_tag, txt, close_tag = m.group(1), m.group(2), m.group(3)
    txt = ascii_punct(html.unescape(txt))
    txt = txt.replace("<","&lt;").replace(">","&gt;")
    return f"{open_tag}{txt}{close_tag}"

POL = TEXT_NODE.sub(repl, RAW)
POL = re.sub(r"\n{3,}", "\n\n", POL)

open(OUT,"w",encoding="utf-8").write(POL)
