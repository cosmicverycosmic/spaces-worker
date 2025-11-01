#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stable working version — confirmed functioning with proper transcript + emoji VTT separation.
"""

import os, sys, re, json, html, unicodedata
from datetime import datetime, timezone
from statistics import median

ARTDIR = os.environ.get("ARTDIR", ".")
BASE   = os.environ.get("BASE", "space")
SRC    = os.environ.get("CC_JSONL", "")
SHIFT  = float(os.environ.get("SHIFT_SECS", "0") or "0")

os.makedirs(ARTDIR, exist_ok=True)

VTT_PATH         = os.path.join(ARTDIR, f"{BASE}.vtt")
TRANSCRIPT_PATH  = os.path.join(ARTDIR, f"{BASE}_transcript.html")
SPEECH_JSON_PATH = os.path.join(ARTDIR, f"{BASE}_speech.json")
REACT_JSON_PATH  = os.path.join(ARTDIR, f"{BASE}_reactions.json")
META_JSON_PATH   = os.path.join(ARTDIR, f"{BASE}_meta.json")
START_PATH       = os.path.join(ARTDIR, f"{BASE}.start.txt")

def esc(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def nfc(s):
    if not s: return ""
    s = unicodedata.normalize("NFC", s)
    return re.sub(r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF]", "", s)

def fmt_ts(t):
    if t < 0: t = 0
    h = int(t // 3600); m = int((t % 3600) // 60); s = t % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"

def to_secs(x):
    if x is None: return None
    try:
        v = float(x)
    except Exception:
        return None
    if v >= 1e12: v = v / 1000.0
    return v

def parse_iso(s):
    if not s: return None
    s = s.strip()
    try:
        if s.endswith("Z"): s = s[:-1] + "+00:00"
        if re.search(r"[+-]\d{4}$", s):
            s = s[:-5] + s[-5:-3] + ":" + s[-3:]
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

EMOJI_RE = re.compile("[" +
    "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF" +
    "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF" +
    "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF" + "]+", re.UNICODE)

ONLY_PUNCT_SPACE = re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")

def is_emoji_only(s):
    if not s or not s.strip(): return False
    t = ONLY_PUNCT_SPACE.sub("", s)
    t = EMOJI_RE.sub("", t)
    return len(t.strip()) == 0

if not (SRC and os.path.isfile(SRC)):
    open(VTT_PATH,"w").write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH,"w").write("")
    open(SPEECH_JSON_PATH,"w").write("[]")
    open(REACT_JSON_PATH,"w").write("[]")
    open(META_JSON_PATH,"w").write(json.dumps({"note":"no input"},indent=2))
    open(START_PATH,"w").write("")
    sys.exit(0)

REL_KEYS  = ("offset","startSec","startMs","start")
ABS_KEYS  = ("programDateTime","timestamp","ts")

raw_items, reactions, abs_candidates = [], [], []
ingest_idx = 0

def first(*vals):
    for v in vals:
        if v not in (None,""): return v
    return None

def pick_times(d):
    rel, abs_ts = None, None
    for k in REL_KEYS:
        if k in d and d[k] not in (None,""):
            v = to_secs(d[k]); 
            if v is not None: rel=v; break
    if "programDateTime" in d:
        abs_ts = parse_iso(d["programDateTime"])
    for k in ("timestamp","ts"):
        if k in d and d[k] not in (None,""):
            v = to_secs(d[k])
            if v is None: continue
            if v >= 1e6: abs_ts=v
            elif rel is None: rel=v
    return rel, abs_ts

def harvest(d):
    global ingest_idx
    txt = first(d.get("body"), d.get("text"), d.get("caption"))
    if not txt: return
    txt = nfc(str(txt)).strip()
    sender = d.get("sender") or {}
    name = first(d.get("displayName"), (sender or {}).get("display_name"), d.get("name")) or "Speaker"
    handle = first(d.get("username"), d.get("handle"), (sender or {}).get("screen_name")) or ""
    avatar = first((sender or {}).get("profile_image_url_https"), d.get("profile_image_url_https"), d.get("profile_image_url"))
    rel, abs_ts = pick_times(d)
    if is_emoji_only(txt):
        reactions.append({"idx":ingest_idx,"rel":rel,"abs":abs_ts,"emoji":txt,"name":name,"handle":handle,"avatar":avatar})
    else:
        raw_items.append({"idx":ingest_idx,"rel":rel,"abs":abs_ts,"text":txt,"name":name,"username":handle,"avatar":avatar})
        if abs_ts: abs_candidates.append(abs_ts)
    ingest_idx+=1

for line in open(SRC,"r",encoding="utf-8",errors="ignore"):
    line=line.strip()
    if not line: continue
    try:
        obj=json.loads(line)
    except: continue
    if isinstance(obj,dict):
        harvest(obj)
        pl=obj.get("payload")
        if isinstance(pl,str):
            try:
                harvest(json.loads(pl))
            except: pass
        elif isinstance(pl,dict):
            harvest(pl)

if not raw_items and not reactions:
    open(VTT_PATH,"w").write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH,"w").write("")
    open(SPEECH_JSON_PATH,"w").write("[]")
    open(REACT_JSON_PATH,"w").write("[]")
    sys.exit(0)

abs0=min(abs_candidates) if abs_candidates else None
deltas=[]
for it in raw_items+reactions:
    if it.get("abs") and it.get("rel"):
        deltas.append((it["abs"]-abs0)-it["rel"])
delta = median(deltas) if deltas else 0.0

def rel_time(rel,abs_ts):
    if rel is not None: t=rel+delta
    elif abs_ts is not None and abs0 is not None: t=abs_ts-abs0
    else: t=0.0
    return max(0.0, t-SHIFT)

norm=[]
for it in raw_items:
    t=rel_time(it["rel"],it["abs"])
    norm.append({**it,"t":t})
norm.sort(key=lambda x:(x["t"],x["idx"]))

EPS=5e-4
last=-1e9
for u in norm:
    if u["t"]<=last: u["t"]=last+EPS
    last=u["t"]

MIN_DUR, MAX_DUR, MERGE_GAP, GUARD = 0.8, 10.0, 3.0, 0.02
segs=[]
for u in norm:
    segs.append({
        "start":u["t"], "end":u["t"]+MIN_DUR,
        "text":u["text"], "name":u["name"],
        "username":u["username"], "avatar":u["avatar"]
    })

merged=[]; cur=None
for s in segs:
    if cur and s["name"]==cur["name"] and (s["start"]-cur["end"])<=MERGE_GAP:
        cur["text"]+=" "+s["text"]; cur["end"]=s["end"]
    else:
        cur=dict(s); merged.append(cur)

for i,g in enumerate(merged):
    if i+1<len(merged):
        nxt=merged[i+1]["start"]
        g["end"]=min(MAX_DUR, max(MIN_DUR, nxt-g["start"]-GUARD))
    else:
        g["end"]=g["start"]+MIN_DUR

prev=0.0
for g in merged:
    if g["start"]<prev+GUARD: g["start"]=prev+GUARD
    if g["end"]<g["start"]+MIN_DUR: g["end"]=g["start"]+MIN_DUR
    prev=g["end"]

with open(VTT_PATH,"w",encoding="utf-8") as f:
    f.write("WEBVTT\n\n")
    for i,g in enumerate(merged,1):
        f.write(f"{i}\n{fmt_ts(g['start'])} --> {fmt_ts(g['end'])}\n<v {esc(g['name'])}> {esc(g['text'])}\n\n")

with open(TRANSCRIPT_PATH,"w",encoding="utf-8") as tf:
    tf.write('<div class="ss3k-transcript">\n')
    for i,g in enumerate(merged,1):
        uname=g.get("username","").lstrip("@")
        prof=f"https://x.com/{html.escape(uname)}" if uname else ""
        avatar=g.get("avatar") or (f"https://unavatar.io/x/{uname}" if uname else "")
        av=f'<a href="{prof}" target="_blank"><img class="ss3k-avatar" src="{avatar}" alt=""></a>' if avatar else "<div class='ss3k-avatar'></div>"
        tf.write(f"<div class='ss3k-seg' data-start='{g['start']:.3f}' data-end='{g['end']:.3f}'>{av}<div class='ss3k-text'>{esc(g['text'])}</div></div>\n")
    tf.write("</div>\n")

speech=[{"start":round(g["start"],3),"end":round(g["end"],3),"text":g["text"],
         "name":g["name"],"handle":g["username"],"avatar":g["avatar"]} for g in merged]
open(SPEECH_JSON_PATH,"w").write(json.dumps(speech,ensure_ascii=False))

rx=[]
for r in reactions:
    t=rel_time(r["rel"],r["abs"])
    rx.append({"t":round(t,3),"emoji":r["emoji"],"name":r["name"],
               "handle":r["handle"],"avatar":r["avatar"]})
open(REACT_JSON_PATH,"w").write(json.dumps(rx,ensure_ascii=False))

meta={"speech_segments":len(merged),"reactions":len(rx),"shift_secs":SHIFT,"delta_used":round(delta,6)}
open(META_JSON_PATH,"w").write(json.dumps(meta,indent=2))
if abs_candidates:
    start_iso=datetime.fromtimestamp(min(abs_candidates),timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
    open(START_PATH,"w").write(start_iso+"\n")
