#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Final robust gen_vtt.py — clock-aligned, single-avatar, emoji-synced.
"""
import os, sys, re, json, html, unicodedata
from datetime import datetime, timezone
from statistics import median
from typing import Any, Dict, List, Optional

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
SRC    = os.environ.get("CC_JSONL","")
SHIFT  = float(os.environ.get("SHIFT_SECS","0") or "0")

VTT_PATH = os.path.join(ARTDIR,f"{BASE}.vtt")
EMOJI_VTT_PATH = os.path.join(ARTDIR,f"{BASE}_emoji.vtt")
TRANS_PATH = os.path.join(ARTDIR,f"{BASE}_transcript.html")
START_PATH = os.path.join(ARTDIR,f"{BASE}.start.txt")

os.makedirs(ARTDIR,exist_ok=True)

def esc(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
def nfc(s):
    if not s: return ""
    s = unicodedata.normalize("NFC", s)
    return re.sub(r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF]", "", s)

def fmt_ts(t):
    if t<0: t=0
    h=int(t//3600); m=int((t%3600)//60); s=t%60
    return f"{h:02d}:{m:02d}:{s:06.3f}"

def parse_iso(s):
    if not s: return None
    try:
        if s.endswith("Z"): s=s[:-1]+"+00:00"
        if re.search(r"[+-]\d{4}$",s): s=s[:-5]+s[-5:-3]+":"+s[-3:]
        dt=datetime.fromisoformat(s)
        if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except: return None

def to_secs(x):
    if x in (None,""): return None
    try: v=float(x)
    except: return None
    if v>1e12: v/=1000.0
    return v

def first(*a):
    for x in a:
        if x not in (None,""): return x
    return None

EMOJI_RE=re.compile("["+
 "\U0001F1E6-\U0001F1FF\U0001F300-\U0001FAD6\U0001F900-\U0001F9FF\u2600-\u26FF\u2700-\u27BF]+")
ONLY_PUNCT=re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")
def is_emoji_only(s):
    if not s or not s.strip(): return False
    t=ONLY_PUNCT.sub("",s)
    t=EMOJI_RE.sub("",t)
    return not t.strip()

def has_letters(s): return bool(re.search(r"[A-Za-z0-9]",s or ""))

if not SRC or not os.path.isfile(SRC):
    open(VTT_PATH,"w").write("WEBVTT\n\n")
    open(EMOJI_VTT_PATH,"w").write("WEBVTT\n\n")
    open(TRANS_PATH,"w").write("")
    sys.exit(0)

items=[]; emojis=[]; abs_candidates=[]
def harvest(d):
    txt=first(d.get("text"),d.get("caption"),d.get("body"))
    if not txt: return
    disp=first(d.get("displayName"),d.get("speaker_name"),d.get("speakerName"),d.get("name"))
    uname=first(d.get("username"),d.get("handle"),d.get("screen_name"),d.get("user_id"))
    avatar=first(d.get("profile_image_url_https"),d.get("profile_image_url"),
                 (d.get("sender") or {}).get("profile_image_url_https"),
                 (d.get("sender") or {}).get("profile_image_url"))
    rel=first(to_secs(d.get("offset")),to_secs(d.get("startSec")),to_secs(d.get("start")),to_secs(d.get("startMs")))
    abs_ts=first(parse_iso(d.get("programDateTime")),to_secs(d.get("timestamp")),to_secs(d.get("ts")))
    if abs_ts and abs_ts>1e9: abs_candidates.append(abs_ts)
    text=nfc(str(txt)).strip()
    name=nfc(disp or uname or "Speaker")
    handle=(uname or "").lstrip("@")
    avatar=avatar or ""
    if is_emoji_only(text):
        emojis.append({"rel":rel,"abs":abs_ts,"emoji":text,"name":name,"handle":handle,"avatar":avatar})
    elif has_letters(text):
        items.append({"rel":rel,"abs":abs_ts,"text":text,"name":name,"handle":handle,"avatar":avatar})

with open(SRC,"r",encoding="utf-8",errors="ignore") as f:
    for ln in f:
        try: o=json.loads(ln.strip())
        except: continue
        if isinstance(o,dict):
            for k in ("payload","body"):
                if isinstance(o.get(k),str):
                    try: inner=json.loads(o[k])
                    except: inner=None
                    if isinstance(inner,dict): harvest(inner)
            harvest(o)

# --- clock alignment ---
abs0=min(abs_candidates) if abs_candidates else None
deltas=[]
for it in items+emojis:
    if it["abs"] and it["rel"]:
        deltas.append((it["abs"]-abs0)-it["rel"])
delta=median(deltas) if deltas else 0.0

def t_rel(rel,abs_ts):
    if rel is not None: t=rel+delta
    elif abs_ts and abs0: t=abs_ts-abs0
    else: t=0
    return max(0,t-SHIFT)

norm=[]
for it in items:
    t=t_rel(it["rel"],it["abs"])
    norm.append({**it,"t":t})
norm.sort(key=lambda x:x["t"])
eps=0.0005; last=-1e9
for n in norm:
    if n["t"]<=last: n["t"]=last+eps
    last=n["t"]

# --- segment build ---
MIN_DUR,MAX_DUR,MERGE_GAP=0.8,10.0,3.0
segments=[]
for n in norm:
    segments.append({**n,"start":n["t"],"end":n["t"]+MIN_DUR})
merged=[]; cur=None
for s in segments:
    if cur and s["name"]==cur["name"] and s["handle"]==cur["handle"] and s["start"]-cur["end"]<=MERGE_GAP:
        sep=" " if not re.search(r"[.!?]$",cur["text"]) else ""
        cur["text"]=(cur["text"]+sep+s["text"]).strip()
        cur["end"]=s["end"]
    else:
        cur=dict(s); merged.append(cur)

for i,g in enumerate(merged):
    if i+1<len(merged):
        nxt=merged[i+1]["start"]
        g["end"]=min(g["start"]+MAX_DUR,max(g["start"]+MIN_DUR,nxt-g["start"]-0.02))
    else:
        words=len(g["text"].split())
        g["end"]=g["start"]+max(MIN_DUR,min(MAX_DUR,0.33*words+0.7))

# --- write VTT ---
with open(VTT_PATH,"w",encoding="utf-8") as f:
    f.write("WEBVTT\n\n")
    for i,g in enumerate(merged,1):
        f.write(f"{i}\n{fmt_ts(g['start'])} --> {fmt_ts(g['end'])}\n<v {esc(g['name'])}> {esc(g['text'])}\n\n")

# --- emoji vtt ---
with open(EMOJI_VTT_PATH,"w",encoding="utf-8") as f:
    f.write("WEBVTT\n\n")
    j=1
    for e in emojis:
        if not e["emoji"]: continue
        t=t_rel(e["rel"],e["abs"])
        f.write(f"{j}\n{fmt_ts(t)} --> {fmt_ts(t+1.2)}\n{e['emoji']}\n\n")
        j+=1

# --- transcript html ---
CSS='''
<style>
.ss3k-transcript{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  max-height:70vh;overflow-y:auto;scroll-behavior:smooth;border:1px solid #e5e7eb;border-radius:12px;padding:6px}
.ss3k-seg{display:flex;align-items:flex-start;gap:10px;padding:8px 10px;margin:6px 0;border-radius:10px}
.ss3k-seg.active{background:#eef6ff;outline:1px solid #bfdbfe}
.ss3k-avatar{width:32px;height:32px;border-radius:50%;flex-shrink:0;background:#e5e7eb}
.ss3k-text{white-space:pre-wrap;word-break:break-word;cursor:pointer}
.ss3k-meta{font-size:13px;color:#475569;margin-bottom:2px}
.ss3k-name strong{color:#0f172a}
</style>
'''
JS='''
<script>
(function(){
function tnum(s){return parseFloat(s||'0')||0}
function within(t,s){return t>=tnum(s.dataset.start)&&t<tnum(s.dataset.end)}
function bind(){
 let a=document.querySelector('audio[data-ss3k-player],#ss3k-audio'); if(!a) return;
 let c=document.querySelector('.ss3k-transcript'); if(!c) return;
 let segs=[...c.querySelectorAll('.ss3k-seg')];
 function tick(){
   let t=a.currentTime||0, f=null;
   for(let s of segs){if(within(t,s)){f=s;break;}}
   segs.forEach(s=>s.classList.toggle('active',s===f));
   if(f){c.scrollTop=f.offsetTop-c.offsetTop;}
 }
 a.addEventListener('timeupdate',tick);
 a.addEventListener('seeked',tick);
 segs.forEach(s=>s.onclick=()=>{a.currentTime=tnum(s.dataset.start)+.05; a.play().catch(()=>{});});
 tick();
}
document.readyState!=='loading'?bind():document.addEventListener('DOMContentLoaded',bind);
})();
</script>
'''
with open(TRANS_PATH,"w",encoding="utf-8") as tf:
    tf.write(CSS+'\n<div class="ss3k-transcript">\n')
    for i,g in enumerate(merged,1):
        uname=(g["handle"] or "").lstrip("@")
        prof=f"https://x.com/{html.escape(uname)}" if uname else ""
        avatar=g["avatar"] or (f"https://unavatar.io/x/{uname}" if uname else "")
        avtag=f'<img class="ss3k-avatar" src="{html.escape(avatar)}" alt="">' if avatar else '<div class="ss3k-avatar"></div>'
        name_html=f'<strong>{html.escape(g["name"])}</strong>'
        if prof: name_html=f'<a href="{prof}" target="_blank" rel="noopener">{name_html}</a>'
        tf.write(f'<div class="ss3k-seg" data-start="{g["start"]:.3f}" data-end="{g["end"]:.3f}">{avtag}<div><div class="ss3k-meta">{name_html}</div><div class="ss3k-text">{esc(g["text"])}</div></div></div>\n')
    tf.write('</div>'+JS)
if abs_candidates:
    start_iso=datetime.fromtimestamp(min(abs_candidates),timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
    open(START_PATH,"w").write(start_iso+"\n")
