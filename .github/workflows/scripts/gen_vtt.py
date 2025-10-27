# file: .github/workflows/scripts/gen_vtt.py
#!/usr/bin/env python3
import json, os, re, html, sys
from datetime import datetime, timezone

artdir = os.environ.get("ARTDIR") or ""
base   = os.environ.get("BASE") or ""
src    = os.environ.get("CC_JSONL") or ""
shift  = float(os.environ.get("SHIFT_SECS") or "0")

if not (artdir and base and src and os.path.isfile(src)):
    os.makedirs(artdir, exist_ok=True)
    open(os.path.join(artdir,f"{base}.vtt"),"w",encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(artdir,f"{base}_emoji.vtt"),"w",encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(artdir,f"{base}_transcript.html"),"w",encoding="utf-8").write("")
    sys.exit(0)

def parse_time_iso(s):
    if not s: return None
    s=s.strip()
    try:
        if s.endswith('Z'): s=s[:-1]+'+00:00'
        if re.search(r'[+-]\d{4}$', s):
            s=s[:-5]+s[-5:-2]+':'+s[-2:]
        dt=datetime.fromisoformat(s)
        if dt.tzinfo is None: dt=dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except: return None

def to_float(x):
    try:
        if x is None: return None
        f=float(x)
        return f/1000.0 if f>86400 else f
    except: return None

def fmt_ts(t):
    if t<0: t=0.0
    h=int(t//3600); m=int((t%3600)//60); s=t%60
    return f"{h:02d}:{m:02d}:{s:06.3f}"

def clean_name(s):
    s=(s or "").strip()
    s=re.sub(r'[<>&]','',s)
    s=''.join(ch for ch in s if (ord(ch)<0x1F000 and not (0xD800<=ord(ch)<=0xDFFF)))
    return s or "Speaker"

def esc(s): return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def norm_text(s):
    if not s: return ""
    s=s.replace("\u2028"," ").replace("\u2029"," ")
    s=re.sub(r'\s+',' ',s).strip()
    if s.lower() in {"n","uh","um"}: return ""
    return s

def first(*vals):
    for v in vals:
        if v not in (None,""): return v
    return None

raw_utts=[]
with open(src,'r',encoding='utf-8',errors='ignore') as f:
    for line in f:
        line=line.strip()
        if not line: continue
        try: obj=json.loads(line)
        except: obj=None

        layer=None; sender={}
        if isinstance(obj,dict) and isinstance(obj.get("payload"),str):
            try:
                pl=json.loads(obj["payload"])
                if isinstance(pl,dict) and isinstance(pl.get("body"),str):
                    try:
                        layer=json.loads(pl["body"])
                        sender=pl.get("sender") or {}
                    except: layer=None
            except: pass
        else:
            layer=obj

        def push(ts_abs, txt, disp, uname, avatar, te_abs=None):
            if ts_abs is None or not txt: return
            t2=norm_text(txt); 
            if not t2: return
            raw_utts.append({
                "ts": float(ts_abs),
                "text": t2,
                "name": clean_name(disp or uname or "Speaker"),
                "username": (uname or "").lstrip("@"),
                "avatar": avatar or "",
                "end_ts": float(te_abs) if te_abs is not None else None
            })

        if isinstance(layer,dict) and layer:
            ttype=layer.get("type")
            txt  = first(layer.get("body"),layer.get("text"),layer.get("caption"))
            disp = first(layer.get("displayName"), (sender or {}).get("display_name"), layer.get("speaker_name"), layer.get("speakerName"))
            uname= first(layer.get("username"), (sender or {}).get("screen_name"), layer.get("user_id"))
            avat = first((sender or {}).get("profile_image_url_https"), (sender or {}).get("profile_image_url"))

            ts_abs = first(
                to_float(layer.get("timestamp")),
                parse_time_iso(layer.get("programDateTime")),
                to_float(layer.get("start")), to_float(layer.get("startSec")), to_float(layer.get("startMs")), to_float(layer.get("ts")), to_float(layer.get("offset"))
            )
            te_abs = first(to_float(layer.get("end")), to_float(layer.get("endSec")), to_float(layer.get("endMs")))

            if txt and (ttype is None or ttype==45 or any(k in layer for k in ("start","startMs","timestamp","programDateTime"))):
                if ts_abs is not None:
                    push(ts_abs, txt, disp, uname, avat, te_abs)
                continue

        if isinstance(obj,dict):
            txt = first(obj.get("text"), obj.get("caption"), obj.get("payloadText"))
            ts_abs = first(
                to_float(obj.get("timestamp")),
                parse_time_iso(obj.get("programDateTime")),
                to_float(obj.get("start")), to_float(obj.get("startMs")), to_float(obj.get("ts"))
            )
            disp = first(obj.get("displayName"), obj.get("speaker"), obj.get("user"), obj.get("name"))
            uname= first(obj.get("username"), obj.get("handle"), obj.get("screen_name"))
            avat = first(obj.get("profile_image_url_https"), obj.get("profile_image_url"))
            if txt and ts_abs is not None:
                push(ts_abs, txt, disp, uname, avat)

if not raw_utts:
    os.makedirs(artdir, exist_ok=True)
    open(os.path.join(artdir,f"{base}.vtt"),"w",encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(artdir,f"{base}_emoji.vtt"),"w",encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(artdir,f"{base}_transcript.html"),"w",encoding="utf-8").write("")
    sys.exit(0)

raw_utts.sort(key=lambda x: x["ts"])
EPS=0.0005; last=-1e9
for u in raw_utts:
    if u["ts"]<=last: u["ts"]=last+EPS
    last=u["ts"]

t0=raw_utts[0]["ts"]

utts=[]
for u in raw_utts:
    st=(u["ts"]-t0)-shift
    if st<0: st=0.0
    utts.append({
        "start_rel": st,
        "end_rel": None,
        "text": u["text"],
        "name": u["name"],
        "username": u["username"],
        "avatar": u["avatar"],
    })

MIN_DUR=0.80; MAX_DUR=10.0; GUARD=0.020
for i,u in enumerate(utts):
    if i+1<len(utts):
        nxt=utts[i+1]["start_rel"]
        dur=max(MIN_DUR, min(MAX_DUR, (nxt-u["start_rel"])-GUARD))
        if dur<=0: dur=MIN_DUR
        u["end_rel"]=u["start_rel"]+dur
    else:
        words=max(1, len(u["text"].split()))
        dur=max(MIN_DUR, min(MAX_DUR, 0.33*words+0.7))
        u["end_rel"]=u["start_rel"]+dur

MERGE_GAP=3.0
groups=[]; cur=None
for u in utts:
    if (cur is not None and u["name"]==cur["name"] and u["username"]==cur["username"] and (u["start_rel"]-cur["end_rel"])<=MERGE_GAP):
        sep="" if re.search(r'[.!?]"?$', cur["text"]) else " "
        cur["text"]=(cur["text"]+sep+u["text"]).strip()
        cur["end_rel"]=max(cur["end_rel"], u["end_rel"])
    else:
        cur={"name":u["name"],"username":u["username"],"avatar":u["avatar"],
             "start_rel":u["start_rel"],"end_rel":u["end_rel"],"text":u["text"]}
        groups.append(cur)

prev=0.0
for g in groups:
    if g["start_rel"]<prev+0.02: g["start_rel"]=prev+0.02
    if g["end_rel"]<g["start_rel"]+MIN_DUR: g["end_rel"]=g["start_rel"]+MIN_DUR
    prev=g["end_rel"]

os.makedirs(artdir, exist_ok=True)
# full captions VTT
vtt_path=os.path.join(artdir,f"{base}.vtt")
with open(vtt_path,"w",encoding="utf-8") as vf:
    vf.write("WEBVTT\n\n")
    for i,g in enumerate(groups,1):
        vf.write(f"{i}\n{fmt_ts(g['start_rel'])} --> {fmt_ts(g['end_rel'])}\n")
        vf.write(f"<v {esc(g['name'])}> {esc(g['text'])}\n\n")

# emoji-only VTT
EMOJI_RE = re.compile(
    "["                       
    "\U0001F1E6-\U0001F1FF"  # flags
    "\U0001F300-\U0001FAD6"  # misc pictographs
    "\U0001FAE0-\U0001FAFF"  # newer emoji
    "\U00002700-\U000027BF"  # dingbats
    "\U00002600-\U000026FF"  # misc symbols
    "\U0001F900-\U0001F9FF"  # supplemental
    "\U0001F680-\U0001F6FF"  # transport/map
    "\U0001F100-\U0001F5FF"  # enclosed alphanum/symbols
    "\U0001FA70-\U0001FAFF"  # more symbols
    "\U00002300-\U000023FF"  # misc tech
    "]+", flags=re.UNICODE
)
def only_emoji(s:str)->str:
    if not s: return ""
    return "".join(EMOJI_RE.findall(s))

evtt_path=os.path.join(artdir,f"{base}_emoji.vtt")
with open(evtt_path,"w",encoding="utf-8") as ef:
    ef.write("WEBVTT\n\n")
    j=1
    for g in groups:
        em=only_emoji(g["text"])
        if not em: continue
        ef.write(f"{j}\n{fmt_ts(g['start_rel'])} --> {fmt_ts(g['end_rel'])}\n")
        ef.write(f"{em}\n\n")
        j+=1

# rich transcript HTML (avatars + names)
css='''
<style>
.ss3k-transcript{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  max-height:70vh; overflow-y:auto; scroll-behavior:smooth; border:1px solid #e5e7eb; border-radius:12px; padding:6px 6px;}
.ss3k-seg{display:flex;gap:10px;padding:8px 10px;border-radius:10px;margin:6px 0}
.ss3k-seg.active{background:#eef6ff;outline:1px solid #bfdbfe}
.ss3k-avatar{width:26px;height:26px;border-radius:50%;flex:0 0 26px;margin-top:3px;background:#e5e7eb}
.ss3k-meta{font-size:12px;color:#64748b;margin-bottom:2px}
.ss3k-name a{color:#0f172a;text-decoration:none}
.ss3k-text{white-space:pre-wrap;word-break:break-word;cursor:pointer}
</style>
'''
js='''
<script>
(function(){
  function time(s){return parseFloat(s||'0')||0}
  function within(t,seg){return t>=time(seg.dataset.start) && t<time(seg.dataset.end)}
  function bind(){
    var audio=document.getElementById('ss3k-audio')||document.querySelector('audio[data-ss3k-player]');
    var cont=document.querySelector('.ss3k-transcript'); if(!audio||!cont) return;
    var segs=[].slice.call(cont.querySelectorAll('.ss3k-seg')); var lastId="";
    function tick(){
      var t=audio.currentTime||0, found=null;
      for(var i=0;i<segs.length;i++){ if(within(t,segs[i])){found=segs[i];break;} }
      segs.forEach(function(s){ s.classList.toggle('active', s===found); });
      if(found){
        var id=found.id||"";
        if(id!==lastId){
          var top = found.offsetTop - cont.offsetTop;
          if (Math.abs(cont.scrollTop - top) > 6) cont.scrollTop = top;
          lastId=id;
        }
      }
    }
    audio.addEventListener('timeupdate', tick);
    audio.addEventListener('seeked', tick);
    segs.forEach(function(s){
      s.addEventListener('click', function(){
        audio.currentTime = time(s.dataset.start)+0.05; audio.play().catch(function(){});
      });
    });
    tick();
  }
  if(document.readyState!=="loading") bind(); else document.addEventListener('DOMContentLoaded', bind);
})();
</script>
'''

tr_path=os.path.join(artdir,f"{base}_transcript.html")
with open(tr_path,"w",encoding="utf-8") as tf:
    tf.write(css)
    tf.write('<div class="ss3k-transcript">\n')
    for i,g in enumerate(groups,1):
        name=g["name"]; uname=(g.get("username") or "").strip().lstrip("@")
        prof=f"https://x.com/{html.escape(uname, True)}" if uname else ""
        avatar=g.get("avatar") or (f"https://unavatar.io/x/{html.escape(uname, True)}" if uname else "")
        if avatar and prof:
            avtag=f'<a href="{prof}" target="_blank" rel="noopener"><img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt=""></a>'
        elif avatar:
            avtag=f'<img class="ss3k-avatar" src="{html.escape(avatar, True)}" alt="">'
        else:
            avtag='<div class="ss3k-avatar" aria-hidden="true"></div>'
        name_html = f'<span class="ss3k-name"><strong>{html.escape(name, True)}</strong></span>'
        if prof:
            name_html = f'<span class="ss3k-name"><a href="{prof}" target="_blank" rel="noopener"><strong>{html.escape(name, True)}</strong></a></span>'

        tf.write(f'<div class="ss3k-seg" id="seg-{i:04d}" data-start="{g["start_rel"]:.3f}" data-end="{g["end_rel"]:.3f}" data-speaker="{html.escape(name, True)}"')
        if uname: tf.write(f' data-handle="@{html.escape(uname, True)}"')
        tf.write('>')
        tf.write(avtag)
        tf.write('<div class="ss3k-body">')
        tf.write(f'<div class="ss3k-meta">{name_html} · <time>{fmt_ts(g["start_rel"])}</time>–<time>{fmt_ts(g["end_rel"])}</time></div>')
        tf.write(f'<div class="ss3k-text">{html.escape(g["text"], True)}</div>')
        tf.write('</div></div>\n')
    tf.write('</div>\n')
    tf.write(js)

start_iso = datetime.fromtimestamp(raw_utts[0]["ts"], timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
with open(os.path.join(artdir,f"{base}.start.txt"),"w",encoding="utf-8") as sf:
    sf.write(start_iso+"\n")
