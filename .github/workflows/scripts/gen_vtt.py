#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, json, html, sys, unicodedata
from datetime import datetime, timezone

ARTDIR = os.environ.get("ARTDIR") or "."
BASE   = os.environ.get("BASE") or "space"
SRC    = os.environ.get("CC_JSONL") or ""
SHIFT  = float(os.environ.get("SHIFT_SECS") or "0")

VTT_PATH         = os.path.join(ARTDIR, f"{BASE}.vtt")
TRANSCRIPT_PATH  = os.path.join(ARTDIR, f"{BASE}_transcript.html")
START_PATH       = os.path.join(ARTDIR, f"{BASE}.start.txt")
REACT_JSON_PATH  = os.path.join(ARTDIR, f"{BASE}_reactions.json")
META_JSON_PATH   = os.path.join(ARTDIR, f"{BASE}_meta.json")
SPEECH_JSON_PATH = os.path.join(ARTDIR, f"{BASE}_speech.json")

DROP_NEGATIVE_BEFORE_ZERO = True     # drop captions that would occur before audio start
NEGATIVE_GRACE_SECONDS    = 0.15     # small grace so near-zero lines survive

def ensure_dirs():
    os.makedirs(ARTDIR, exist_ok=True)

def empty_outputs():
    ensure_dirs()
    open(VTT_PATH, "w", encoding="utf-8").write("WEBVTT\n\n")
    open(TRANSCRIPT_PATH, "w", encoding="utf-8").write("")
    open(REACT_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(SPEECH_JSON_PATH, "w", encoding="utf-8").write("[]")
    open(META_JSON_PATH, "w", encoding="utf-8").write(json.dumps({"notes":"no input"}))
    return 0

def esc(s:str)->str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

# —— text hygiene: ASCII quotes, strip zero-widths, NFC normalize ——
ZW_RE = re.compile(r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F]")
def ascii_punct(s:str)->str:
    if not s: return ""
    s = unicodedata.normalize("NFC", s)
    s = ZW_RE.sub("", s)
    s = s.replace("\u2018", "'").replace("\u2019", "'")  # curly ’ → '
    s = s.replace("\u201C", '"').replace("\u201D", '"')  # curly ” → "
    return s

def parse_iso(s):
    if not s: return None
    s=s.strip()
    try:
        if s.endswith("Z"): s=s[:-1]+"+00:00"
        if re.search(r"[+-]\d{4}$", s): s=s[:-5]+s[-5:-3]+":"+s[-3:]
        dt=datetime.fromisoformat(s)
        if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except: return None

def to_secs(x):
    if x is None: return None
    try:
        f=float(x)
        return f/1000.0 if f>=4_000_000 else f
    except: return None

def first(*vals):
    for v in vals:
        if v not in (None, ""): return v
    return None

if not (SRC and os.path.isfile(SRC)):
    empty_outputs(); sys.exit(0)

raw=[]            # {seq, rel?, abs?, text, name, username, avatar}
reactions=[]
abs_candidates=[]

def is_emoji_only(text:str)->bool:
    if not (text or "").strip(): return False
    # allow simple symbols/punct counts as reactions
    EMOJI = re.compile("[" +
        "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF" +
        "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF" +
        "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF" + "]+", re.UNICODE)
    ONLY_P = re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")
    t = ONLY_P.sub("", text)
    t = EMOJI.sub("", t)
    return len(t.strip())==0

seq_counter = 0
def push_item(rel, abs_ts, text, name, uname, avatar):
    global seq_counter
    if not text: return
    text = ascii_punct(text)
    name = ascii_punct(name or "Speaker")
    uname= (uname or "").lstrip("@")
    avatar = avatar or ""
    if is_emoji_only(text):
        if rel is not None or abs_ts is not None:
            reactions.append({"rel":rel, "abs":abs_ts, "emoji":text, "name":name, "handle":uname, "avatar":avatar})
        return
    raw.append({"seq":seq_counter, "rel":rel, "abs":abs_ts, "text":text, "name":name, "username":uname, "avatar":avatar})
    seq_counter += 1
    if abs_ts is not None: abs_candidates.append(abs_ts)

def harvest(d):
    txt = first(d.get("body"), d.get("text"), d.get("caption"), d.get("payloadText"))
    disp= first(d.get("displayName"), d.get("speaker_name"), d.get("speakerName"), d.get("name"), d.get("user"))
    uname=first(d.get("username"), d.get("handle"), d.get("screen_name"), d.get("user_id"))
    avatar=first(d.get("profile_image_url_https"), d.get("profile_image_url"))
    # nested sender
    snd = d.get("sender") or {}
    if isinstance(snd, dict):
        disp = first(disp, snd.get("display_name"))
        uname= first(uname, snd.get("screen_name"))
        avatar=first(avatar, snd.get("profile_image_url_https"), snd.get("profile_image_url"))
    # time families
    rel = first(to_secs(d.get("offset")), to_secs(d.get("startSec")), to_secs(d.get("startMs")), to_secs(d.get("start")))
    abs_ts = to_secs(d.get("timestamp"))
    if abs_ts is None: abs_ts = parse_iso(d.get("programDateTime"))
    if txt: push_item(rel, abs_ts, txt, disp, uname, avatar)

def ingest(line):
    line=line.strip()
    if not line: return
    try: obj=json.loads(line)
    except: return
    if isinstance(obj, dict) and "payload" in obj and isinstance(obj["payload"], (str, dict)):
        pl = obj["payload"]
        if isinstance(pl, str):
            try: pl = json.loads(pl)
            except: pl = {}
        if isinstance(pl, dict):
            body = pl.get("body")
            if isinstance(body, str):
                try:
                    inner = json.loads(body)
                    if isinstance(inner, dict):
                        rec=dict(inner)
                        if isinstance(pl.get("sender"), dict): rec["sender"]=pl["sender"]
                        harvest(rec); return
                except: pass
            harvest(pl); return
    if isinstance(obj, dict): harvest(obj)

with open(SRC,"r",encoding="utf-8",errors="ignore") as f:
    for ln in f: ingest(ln)

if not raw and not reactions:
    empty_outputs()
    if abs_candidates:
        start_iso = datetime.fromtimestamp(min(abs_candidates), timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
        open(START_PATH,"w",encoding="utf-8").write(start_iso+"\n")
    sys.exit(0)

# normalize to one relative clock
abs0 = min(abs_candidates) if abs_candidates else None
norm=[]
for it in raw:
    if it["rel"] is not None:
        t = it["rel"]
    elif it["abs"] is not None and abs0 is not None:
        t = it["abs"] - abs0
    else:
        t = 0.0
    norm.append({**it, "t0": float(t)})

# guard SHIFT that would nuke too many lines
neg_if_applied = sum(1 for u in norm if (u["t0"] - SHIFT) < -NEGATIVE_GRACE_SECONDS)
if neg_if_applied > max(3, int(0.35*len(norm))):
    # captions appear on their own zero; ignore SHIFT so we keep them intact
    effective_shift = 0.0
else:
    effective_shift = SHIFT

# apply shift, optionally drop negatives
kept=[]
dropped_neg=0
for u in norm:
    t = u["t0"] - effective_shift
    if DROP_NEGATIVE_BEFORE_ZERO and t < -NEGATIVE_GRACE_SECONDS:
        dropped_neg += 1
        continue
    if t < 0: t = 0.0
    kept.append({**u, "t": t})

# stable sort: (t, seq) — Python sort is stable; we keep seq for tie-break
kept.sort(key=lambda x: (x["t"], x["seq"]))

# de-jitter identical starts
EPS=5e-4
last=-1e9
for u in kept:
    if u["t"] <= last: u["t"] = last + EPS
    last = u["t"]

# build merged segments
MERGE_GAP=3.0
MIN_DUR=0.80
MAX_DUR=10.0
GUARD=0.020

groups=[]
cur=None
end_sentence = re.compile(r'[.!?]"?$')

for u in kept:
    if (cur is not None
        and u["username"]==cur["username"]
        and u["name"]==cur["name"]
        and (u["t"] - cur["end"]) <= MERGE_GAP):
        cur["text"] = (cur["text"] + ("" if end_sentence.search(cur["text"]) else " ") + u["text"]).strip()
        cur["end"]  = max(cur["end"], u["t"] + MIN_DUR)
    else:
        cur={"name":u["name"],"username":u["username"],"avatar":u["avatar"],"start":u["t"],"end":u["t"]+MIN_DUR,"text":u["text"]}
        groups.append(cur)

# durations
for i,g in enumerate(groups):
    if i+1 < len(groups):
        nxt=groups[i+1]["start"]
        dur = max(MIN_DUR, min(MAX_DUR, (nxt - g["start"]) - GUARD))
        g["end"] = g["start"] + dur
    else:
        words=max(1, len(g["text"].split()))
        g["end"] = g["start"] + max(MIN_DUR, min(MAX_DUR, 0.33*words + 0.7))

ensure_dirs()

# VTT
with open(VTT_PATH,"w",encoding="utf-8") as vf:
    vf.write("WEBVTT\n\n")
    for i,g in enumerate(groups,1):
        def fmt(t):
            if t<0: t=0.0
            h=int(t//3600); m=int((t%3600)//60); s=t%60
            return f"{h:02d}:{m:02d}:{s:06.3f}"
        vf.write(f"{i}\n{fmt(g['start'])} --> {fmt(g['end'])}\n")
        vf.write(f"<v {esc(g['name'])}> {esc(g['text'])}\n\n")

# Transcript HTML (simple, UTF-8 safe, ASCII quotes already applied)
CSS = '''
<style>
.ss3k-transcript{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  max-height:70vh; overflow-y:auto; scroll-behavior:smooth; border:1px solid #e5e7eb; border-radius:12px; padding:6px}
.ss3k-seg{display:flex;gap:10px;padding:8px 10px;border-radius:10px;margin:6px 0}
.ss3k-seg.active{background:#eef6ff;outline:1px solid #bfdbfe}
.ss3k-avatar{width:26px;height:26px;border-radius:50%;flex:0 0 26px;margin-top:3px;background:#e5e7eb}
.ss3k-meta{font-size:12px;color:#64748b;margin-bottom:2px}
.ss3k-name a{color:#0f172a;text-decoration:none}
.ss3k-text{white-space:pre-wrap;word-break:break-word;cursor:pointer}
</style>
'''.strip()

JS = r'''
<script>
(function(){
  function time(s){return parseFloat(s||'0')||0}
  function within(t,seg){return t>=time(seg.dataset.start) && t<time(seg.dataset.end)}
  function bind(){
    var audio=document.getElementById('ss3k-audio')||document.querySelector('audio[data-ss3k-player]');
    var cont=document.querySelector('.ss3k-transcript'); if(!audio||!cont) return;
    var segs=[].slice.call(cont.querySelectorAll('.ss3k-seg')); var last="";
    function tick(){
      var t=audio.currentTime||0, found=null;
      for(var i=0;i<segs.length;i++){ if(within(t,segs[i])){found=segs[i];break;} }
      segs.forEach(function(s){ s.classList.toggle('active', s===found); });
      if(found){
        var top = found.offsetTop - cont.offsetTop;
        if (Math.abs(cont.scrollTop - top) > 6) cont.scrollTop = top;
      }
    }
    audio.addEventListener('timeupdate', tick);
    audio.addEventListener('seeked', tick);
    segs.forEach(function(s){
      s.addEventListener('click', function(){
        audio.currentTime = time(s.dataset.start)+0.05; audio.play && audio.play().catch(function(){});
      });
    });
    tick();
  }
  if(document.readyState!=="loading") bind(); else document.addEventListener('DOMContentLoaded', bind);
})();
</script>
'''.strip()

with open(TRANSCRIPT_PATH,"w",encoding="utf-8") as tf:
    tf.write(CSS+"\n")
    tf.write('<div class="ss3k-transcript">\n')
    for i,g in enumerate(groups,1):
        uname=(g.get("username") or "").lstrip("@")
        prof = f"https://x.com/{html.escape(uname, True)}" if uname else ""
        avatar=g.get("avatar") or (f"https://unavatar.io/x/{html.escape(uname, True)}" if uname else "")
        if avatar and prof:
            avtag=f'<a href="{prof}" target="_blank" rel="noopener"><img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt=""></a>'
        elif avatar:
            avtag=f'<img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt="">'
        else:
            avtag='<div class="ss3k-avatar" aria-hidden="true"></div>'
        name_html = f'<strong>{html.escape(g["name"], True)}</strong>'
        if prof: name_html = f'<a href="{prof}" target="_blank" rel="noopener"><strong>{html.escape(g["name"], True)}</strong></a>'

        tf.write(f'<div class="ss3k-seg" id="seg-{i:04d}" data-start="{g["start"]:.3f}" data-end="{g["end"]:.3f}"')
        if uname: tf.write(f' data-handle="@{html.escape(uname, True)}"')
        tf.write('>')
        tf.write(avtag)
        tf.write('<div class="ss3k-body">')
        def fmt(t):
            h=int(t//3600); m=int((t%3600)//60); s=t%60
            return f"{h:02d}:{m:02d}:{s:06.3f}"
        tf.write(f'<div class="ss3k-meta"><span class="ss3k-name">{name_html}</span> · <time>{fmt(g["start"])}</time>–<time>{fmt(g["end"])}</time></div>')
        tf.write(f'<div class="ss3k-text">{esc(g["text"])}</div>')
        tf.write('</div></div>\n')
    tf.write('</div>\n'+JS+"\n")

# reactions sidecar normalized to same clock
rx=[]
if reactions:
    for r in reactions:
        if r["rel"] is not None:
            t=r["rel"]
        elif r["abs"] is not None and abs0 is not None:
            t=r["abs"]-abs0
        else:
            continue
        t = t - effective_shift
        if t < 0: t = 0.0
        rx.append({"t": round(t,3), "emoji": r["emoji"], "name": r["name"], "handle": r["handle"], "avatar": r["avatar"]})
open(REACT_JSON_PATH,"w",encoding="utf-8").write(json.dumps(rx, ensure_ascii=False))

# speech sidecar
speech_out=[{
    "start": round(g["start"],3),
    "end":   round(g["end"],3),
    "text":  g["text"],
    "name":  g["name"],
    "handle": g.get("username",""),
    "avatar": g.get("avatar","")
} for g in groups]
open(SPEECH_JSON_PATH,"w",encoding="utf-8").write(json.dumps(speech_out, ensure_ascii=False))

# meta + absolute start time
meta={
  "inputs": {"rows": len(raw), "reactions": len(reactions)},
  "shift_secs_requested": SHIFT,
  "shift_secs_effective": effective_shift,
  "dropped_negative": dropped_neg,
  "tie_breaker": "seq",
}
open(META_JSON_PATH,"w",encoding="utf-8").write(json.dumps(meta, ensure_ascii=False, indent=2))

if abs_candidates:
    start_iso = datetime.fromtimestamp(min(abs_candidates), timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
    open(START_PATH,"w",encoding="utf-8").write(start_iso+"\n")
