#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, json, html, time
from collections import defaultdict
from datetime import datetime, timezone

# Optional deps (gracefully degrade if missing)
try:
    import tldextract
except Exception:
    tldextract = None

# ---- Config / env ----
ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
PURPLE = (os.environ.get("PURPLE_TWEET_URL","") or "").strip()

REPLIES_OUT = os.path.join(ARTDIR, f"{BASE}_replies.html")
LINKS_OUT   = os.path.join(ARTDIR, f"{BASE}_links.html")
START_PATH  = os.path.join(ARTDIR, f"{BASE}.start.txt")  # absolute ISO start from gen_vtt

LINK_LABEL_AI         = (os.environ.get("LINK_LABEL_AI","keybert") or "keybert").lower()  # keybert | off
LINK_LABEL_MODEL      = os.environ.get("LINK_LABEL_MODEL","sentence-transformers/all-MiniLM-L6-v2")

FETCH_TITLES          = (os.environ.get("LINK_LABEL_FETCH_TITLES","true") or "true").lower() in ("1","true","yes","on")
FETCH_TITLES_LIMIT    = int(os.environ.get("LINK_LABEL_FETCH_LIMIT","10") or "10")
FETCH_TIMEOUT_SEC     = int(os.environ.get("LINK_LABEL_TIMEOUT_SEC","4") or "4")

KEYBERT_TOPN          = int(os.environ.get("KEYBERT_TOPN","8") or "8")
KEYBERT_NGRAM_MIN     = int(os.environ.get("KEYBERT_NGRAM_MIN","1") or "1")
KEYBERT_NGRAM_MAX     = int(os.environ.get("KEYBERT_NGRAM_MAX","3") or "3")
KEYBERT_USE_MMR       = (os.environ.get("KEYBERT_USE_MMR","true") or "true").lower() in ("1","true","yes","on")
KEYBERT_DIVERSITY     = float(os.environ.get("KEYBERT_DIVERSITY","0.6") or "0.6")

# ---- Helpers ----
def esc(x:str)->str: return html.escape(x or "")

def write_empty():
    open(REPLIES_OUT,"w",encoding="utf-8").write("")
    open(LINKS_OUT,"w",encoding="utf-8").write("")

def parse_created_at(s):
    # Example: "Tue Oct 01 14:23:37 +0000 2025"
    try:  return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y").timestamp()
    except Exception: return 0.0

def fmt_utc(ts):
    try:  return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    except Exception: return ""

def load_space_start_epoch(path: str) -> float | None:
    """
    Reads {BASE}.start.txt (ISO-8601 Z) and returns epoch seconds.
    """
    try:
        if not os.path.isfile(path): return None
        iso = open(path,"r",encoding="utf-8",errors="ignore").read().strip()
        if not iso: return None
        if iso.endswith("Z"): iso = iso[:-1] + "+00:00"
        # handle +0000 → +00:00
        m = re.search(r"[+-]\d{4}$", iso)
        if m: iso = iso[:-5] + iso[-5:-3] + ":" + iso[-3:]
        dt = datetime.fromisoformat(iso)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None

def smart_words(text): return [w for w in re.findall(r"[A-Za-z0-9]+", text or "") if w]

STOPWORDS = {
    "the","and","for","to","of","in","on","with","a","an","is","are","be","this","that","from","by","at","as","into",
    "your","our","their","about","or","it","its","you","we","they","them","his","her","him","than","then","over","out"
}

def compress_to_2_3_words(text):
    if not text: return "Link"
    seg = re.split(r"\s+[-–—|:]\s+", text.strip(), maxsplit=1)[0]
    tokens = smart_words(seg)
    kept=[]
    for w in tokens:
        if len(kept)>=3: break
        lw=w.lower()
        if lw in STOPWORDS and len(tokens)>3: continue
        kept.append(w if w.isupper() else w.capitalize())
    if not kept:
        kept = [tokens[0].capitalize()] if tokens else ["Link"]
    return " ".join(kept[:3])

def slug_to_words(u):
    m = re.match(r"https?://[^/]+/(.+)", u or "")
    if not m: return []
    slug = m.group(1).strip("/").split("/")[-1]
    slug = re.sub(r"\.[A-Za-z0-9]{1,6}$","",slug)
    slug = slug.replace("_","-")
    parts=[p for p in slug.split("-") if p]
    out=[]
    for p in parts:
        out.extend(re.findall(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+", p))
    return out

def derive_from_context(text):
    if not text: return None
    tags = re.findall(r"#([A-Za-z0-9_]+)", text)
    if tags:
        tag = re.sub(r"([A-Za-z])(\d)", r"\1 \2", tags[0])
        tag = re.sub(r"(\d)([A-Za-z])", r"\1 \2", tag)
        return compress_to_2_3_words(tag)
    up = re.findall(r"\b([A-Z][A-Z0-9]+(?:\s+[A-Z][A-Z0-9]+){1,3})\b", text)
    if up: return compress_to_2_3_words(up[0])
    toks = [t for t in re.findall(r"[A-Za-z][A-Za-z0-9']+", text) if t.lower() not in STOPWORDS]
    toks.sort(key=lambda w:(-w[0].isupper(), -len(w)))
    if toks: return compress_to_2_3_words(" ".join(toks[:3]))
    return None

def label_from_domain(domain):
    if not domain: return "Link"
    d=domain.lower()
    if "youtube.com" in d or "youtu.be" in d: return "YouTube Video"
    if "substack.com" in d: return "Substack Post"
    if "x.com" in d or "twitter.com" in d: return "Tweet"
    if "pubmed." in d: return "PubMed Study"
    if "who.int" in d: return "WHO Page"
    if "github.com" in d: return "GitHub Repo"
    if "medium.com" in d: return "Medium Post"
    if d.endswith(".gov"): return "Gov Page"
    return (d.split(".")[-2].capitalize()+" Page") if "." in d else d.capitalize()

def http_get(url, timeout=FETCH_TIMEOUT_SEC, max_bytes=32768):
    import urllib.request
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data=r.read(max_bytes)
            charset="utf-8"
            ct=r.headers.get("Content-Type","")
            m=re.search(r"charset=([\w\-]+)", ct or "", re.I)
            if m: charset=m.group(1)
            try: return data.decode(charset,"ignore")
            except Exception: return data.decode("utf-8","ignore")
    except Exception:
        return None

def extract_title(html_text):
    if not html_text: return None
    m=re.search(r"<title[^>]*>(.*?)</title>", html_text, re.I|re.S)
    if not m: return None
    t=re.sub(r"\s+"," ", html.unescape(m.group(1))).strip()
    return t or None

# ---------- Optional KeyBERT ----------
_keybert = None
def kb_label(corpus_text):
    global _keybert
    if LINK_LABEL_AI != "keybert":
        return None
    try:
        if _keybert is None:
            from keybert import KeyBERT
            from sentence_transformers import SentenceTransformer
            st_model = SentenceTransformer(LINK_LABEL_MODEL)
            _keybert = KeyBERT(model=st_model)
        kws = _keybert.extract_keywords(
            corpus_text,
            keyphrase_ngram_range=(KEYBERT_NGRAM_MIN, KEYBERT_NGRAM_MAX),
            stop_words="english",
            use_mmr=KEYBERT_USE_MMR,
            diversity=KEYBERT_DIVERSITY,
            top_n=KEYBERT_TOPN
        )
        if not kws: return None
        best = sorted(kws, key=lambda x: x[1], reverse=True)[0][0]
        return compress_to_2_3_words(best)
    except Exception:
        return None

# ---------- Link index with labeling ----------
class LinkIndex:
    def __init__(self):
        self.data = defaultdict(dict)   # domain -> url -> info
        self.urls_seen=set()
        self.all_urls=[]

    def _domain_of(self, url):
        if tldextract:
            try:
                ext=tldextract.extract(url)
                return ".".join([p for p in (ext.domain, ext.suffix) if p]).lower()
            except Exception:
                pass
        m=re.match(r"https?://([^/]+)/?", url or "")
        return (m.group(1).lower() if m else (url or ""))

    def add(self, url, context=None):
        if not url: return
        dom=self._domain_of(url)
        if url not in self.data[dom]:
            self.data[dom][url]={"url":url, "contexts":set(), "label":None, "domain":dom, "title":None}
            if url not in self.urls_seen:
                self.urls_seen.add(url); self.all_urls.append(url)
        if context:
            ctx = re.sub(r"\s+"," ", context).strip()
            if ctx: self.data[dom][url]["contexts"].add(ctx[:240])

    def finalize_labels(self):
        fetched=0
        if FETCH_TITLES and FETCH_TITLES_LIMIT>0:
            for url in self.all_urls:
                if fetched>=FETCH_TITLES_LIMIT: break
                html_text=http_get(url, timeout=FETCH_TIMEOUT_SEC)
                title=extract_title(html_text)
                if title:
                    dom=self._domain_of(url)
                    self.data[dom][url]["title"]=title
                    fetched+=1
        for dom, bucket in self.data.items():
            for url, info in bucket.items():
                label=None
                if LINK_LABEL_AI=="keybert":
                    corpus = " ".join(
                        [info.get("title") or ""] + list(info["contexts"])[:1]
                    ).strip()
                    if corpus:
                        label = kb_label(corpus)
                if not label and info.get("title"):
                    label = compress_to_2_3_words(info["title"])
                if not label and info["contexts"]:
                    label = derive_from_context(next(iter(info["contexts"])))
                if not label:
                    words = slug_to_words(url)
                    if words: label = compress_to_2_3_words(" ".join(words[:4]))
                if not label: label = label_from_domain(dom)
                label = compress_to_2_3_words(label)
                info["label"]=label

    def render_grouped_html(self):
        out=[]
        out.append('''<style>
.ss3k-links h4{font:600 14px/1.4 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:12px 0 6px}
.ss3k-links ul{margin:0 0 16px; padding-left:18px}
.ss3k-links li{margin:6px 0}
.ss3k-linkmeta{color:#64748b; font:12px system-ui; margin-left:6px}
</style>''')
        out.append('<div class="ss3k-links">')
        for dom in sorted(self.data.keys()):
            out.append(f"<h4>{esc(dom)}</h4>")
            out.append("<ul>")
            items=[self.data[dom][u] for u in self.all_urls if u in self.data[dom]]
            for info in items:
                url = info["url"]; label=info.get("label") or label_from_domain(dom)
                out.append(
                    f'<li><a href="{esc(url)}" target="_blank" rel="noopener">{esc(label)}</a>'
                    f'<span class="ss3k-linkmeta">— {esc(dom)}</span></li>'
                )
            out.append("</ul>")
        out.append("</div>")
        return "\n".join(out)

# ---------- Low-level HTTP JSON helpers (X) ----------
def http_json(url, method="GET", headers=None, data=None, timeout=30):
    import urllib.request
    req=urllib.request.Request(url=url, method=method)
    if headers:
        for k,v in headers.items(): req.add_header(k,v)
    body = data.encode("utf-8") if isinstance(data,str) else data
    try:
        with urllib.request.urlopen(req, data=body, timeout=timeout) as r:
            txt=r.read().decode("utf-8","ignore")
            return json.loads(txt)
    except Exception:
        return None

def get_guest_token(bearer):
    if not bearer: return None
    headers={"authorization":bearer,"content-type":"application/json","user-agent":"Mozilla/5.0"}
    d=http_json("https://api.twitter.com/1.1/guest/activate.json", method="POST", headers=headers, data="{}")
    return (d or {}).get("guest_token")

def find_bottom_cursor(obj):
    if isinstance(obj, dict):
        if obj.get("cursorType")=="Bottom" and "value" in obj: return obj["value"]
        for v in obj.values():
            c=find_bottom_cursor(v)
            if c: return c
    elif isinstance(obj, list):
        for it in obj:
            c=find_bottom_cursor(it)
            if c: return c
    return None

# ---------- Fetch conversation via search/adaptive (cookie/bearer) ----------
def fetch_conversation_adaptive(root_id_str, screen_name_hint=None, max_pages=60, page_sleep=0.6):
    bearer = (os.environ.get("TWITTER_AUTHORIZATION","") or "").strip()
    at     = (os.environ.get("TWITTER_AUTH_TOKEN","") or "").strip()
    ct0    = (os.environ.get("TWITTER_CSRF_TOKEN","") or "").strip()
    if not (at and ct0) and not bearer: return None, None

    headers={
        "user-agent":"Mozilla/5.0",
        "accept":"application/json, text/plain, */*",
        "pragma":"no-cache","cache-control":"no-cache",
        "referer": f"https://x.com/{screen_name_hint}/status/{root_id_str}" if screen_name_hint else "https://x.com/"
    }
    if at and ct0:
        if bearer and not bearer.startswith("Bearer "): bearer=""
        headers.update({"authorization": bearer or "Bearer","x-csrf-token": ct0,"cookie": f"auth_token={at}; ct0={ct0}"})
    else:
        if not bearer: return None, None
        gt=get_guest_token(bearer)
        if not gt: return None, None
        headers.update({"authorization": bearer,"x-guest-token": gt})

    base="https://twitter.com/i/api/2/search/adaptive.json"
    q=f"conversation_id:{root_id_str}"

    tweets, users, cursor, pages = {}, {}, None, 0
    while pages < max_pages:
        params={
            "q": q, "count": 100, "tweet_search_mode":"live","query_source":"typed_query","tweet_mode":"extended",
            "pc":"ContextualServices","spelling_corrections":"1",
            "include_quote_count":"true","include_reply_count":"true",
            "ext":"mediaStats,highlightedLabel,hashtags,antispam_media_platform,voiceInfo,superFollowMetadata,unmentionInfo,editControl,emoji_reaction"
        }
        if cursor: params["cursor"]=cursor
        url = base + "?" + "&".join(f"{k}={json.dumps(v)[1:-1]}" for k,v in params.items())
        data=http_json(url, headers=headers)
        if not data: break
        g=(data.get("globalObjects") or {})
        tw=g.get("tweets") or {}
        us=g.get("users") or {}
        if tw: tweets.update(tw)
        if us: users.update(us)
        nxt=find_bottom_cursor(data.get("timeline") or data)
        pages+=1
        if not nxt or nxt==cursor: break
        cursor=nxt
        time.sleep(page_sleep)
    return tweets or None, users or None

# ---------- Build threaded tree + metadata ----------
def build_thread_tree(root_id, tweets):
    children=defaultdict(list)
    meta={}
    uid_of={}
    for tid, t in (tweets or {}).items():
        if str(t.get("conversation_id_str") or t.get("conversation_id") or "") != str(root_id): continue
        parent=t.get("in_reply_to_status_id_str")
        if not parent: continue
        created=parse_created_at(t.get("created_at",""))
        uid=str(t.get("user_id_str") or t.get("user_id") or "")
        meta[tid]={"created_ts":created,"uid":uid}
        uid_of[tid]=uid
        children[str(parent)].append(tid)
    # Sort children by time
    for pid in list(children.keys()):
        children[pid].sort(key=lambda x: meta.get(x,{}).get("created_ts",0.0))
    roots = children.get(str(root_id), [])
    return roots, children, meta, uid_of

# ---------- Text/link helpers ----------
def expand_text_with_entities(text, entities):
    text = text or ""
    esc_text = esc(text)
    if entities:
        urls = entities.get("urls") or []
        urls_sorted = sorted(urls, key=lambda u: len(u.get("url","")), reverse=True)
        for u in urls_sorted:
            short = u.get("url") or ""
            expanded = u.get("expanded_url") or u.get("unwound_url") or short
            if not short: continue
            esc_text = esc_text.replace(esc(short), f'<a href="{esc(expanded)}" target="_blank" rel="noopener">{esc(expanded)}</a>')
    esc_text = re.sub(r'(https?://\S+)', r'<a href="\1" target="_blank" rel="noopener">\1</a>', esc_text)
    return esc_text

def extract_urls_from_text(text): return re.findall(r'(https?://\S+)', text or "")

# ---------- Rendering (with self-reply flatten + time pulse) ----------
CSS = '''
<style>
.ss3k-threads{font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
.ss3k-controls{margin:8px 0 12px}
.ss3k-controls button{font:12px system-ui;padding:6px 10px;border:1px solid #d1d5db;border-radius:8px;background:#fff;cursor:pointer}
.ss3k-reply{display:flex;gap:10px;padding:10px;border:1px solid #e5e7eb;border-radius:12px;margin:8px 0;background:#fff;position:relative}
.ss3k-ravatar{width:32px;height:32px;border-radius:50%;background:#e5e7eb;flex:0 0 32px}
.ss3k-rcontent{flex:1 1 auto}
.ss3k-rmeta{font-size:12px;color:#64748b;margin-bottom:4px}
.ss3k-rmeta a{color:#334155;text-decoration:none}
.ss3k-rname{font-weight:600;color:#0f172a}
.ss3k-rtext{white-space:pre-wrap;word-break:break-word;margin-top:4px}
.ss3k-children{margin-left:42px;border-left:2px solid #e5e7eb;padding-left:10px}
.ss3k-toggle{font:12px system-ui;color:#0ea5e9;cursor:pointer;margin:4px 0 0 42px}
.ss3k-hidden{display:none}

/* Pulse effect on time hit */
@keyframes ss3k-pulse {
  0%   { box-shadow: 0 0 0 0 rgba(59,130,246,.7); }
  70%  { box-shadow: 0 0 0 10px rgba(59,130,246,0); }
  100% { box-shadow: 0 0 0 0 rgba(59,130,246,0); }
}
.ss3k-reply.pulse { animation: ss3k-pulse 1s ease-out; }
.ss3k-dot {
  position:absolute; right:10px; top:10px; width:8px; height:8px; border-radius:50%;
  background:#60a5fa; opacity:.0;
}
.ss3k-reply.pulse .ss3k-dot { opacity:1; }
</style>
'''.strip()

JS = r'''
<script>
(function(){
  function toggle(el, show){
    if(show===true){ el.classList.remove('ss3k-hidden'); }
    else if(show===false){ el.classList.add('ss3k-hidden'); }
    else { el.classList.toggle('ss3k-hidden'); }
  }
  function expandAll(root){ root.querySelectorAll('.ss3k-children').forEach(c=>c.classList.remove('ss3k-hidden')); }
  function collapseAll(root){ root.querySelectorAll('.ss3k-children').forEach(c=>c.classList.add('ss3k-hidden')); }

  function bind(){
    var root=document.querySelector('.ss3k-threads'); if(!root) return;

    // expand/collapse toggles
    root.querySelectorAll('[data-toggle-for]').forEach(btn=>{
      btn.addEventListener('click', ()=>{
        var id=btn.getAttribute('data-toggle-for');
        var box=document.getElementById(id); if(!box) return;
        toggle(box);
        var collapsed=box.classList.contains('ss3k-hidden');
        btn.textContent = collapsed ? btn.getAttribute('data-collapsed-label') : btn.getAttribute('data-expanded-label');
      });
    });
    var exp=root.querySelector('[data-ss3k-expand-all]');
    var col=root.querySelector('[data-ss3k-collapse-all]');
    if(exp) exp.addEventListener('click', ()=>expandAll(root));
    if(col) col.addEventListener('click', ()=>collapseAll(root));

    // time-synced pulse (requires an <audio id="ss3k-audio">)
    var audio = document.getElementById('ss3k-audio') || document.querySelector('audio[data-ss3k-player]');
    var items = [].slice.call(root.querySelectorAll('.ss3k-reply[data-trel]')).map(function(el){
      var t = parseFloat(el.getAttribute('data-trel') || 'NaN');
      return {el: el, t: t, fired: false};
    }).filter(function(x){ return isFinite(x.t); }).sort(function(a,b){ return a.t - b.t; });

    if (audio && items.length){
      var last = 0;
      audio.addEventListener('seeked', function(){
        // reset flags after big backward seek so pulses can fire again
        if ((audio.currentTime||0) < last - 0.5) {
          items.forEach(function(it){ it.fired = false; });
        }
        last = audio.currentTime || 0;
      });
      audio.addEventListener('timeupdate', function(){
        var now = audio.currentTime || 0;
        if (now >= last){
          // forward: fire for crossings (last, now]
          for (var i=0;i<items.length;i++){
            var it = items[i];
            if (!it.fired && it.t > last && it.t <= now){
              it.fired = true;
              it.el.classList.add('pulse');
              setTimeout(function(el){ el.classList.remove('pulse'); }, 1100, it.el);
            }
          }
        } else {
          // backward scrub: optional re-fire if close to current time
          for (var j=0;j<items.length;j++){
            var jt = items[j];
            if (Math.abs(jt.t - now) < 0.15){
              jt.el.classList.add('pulse');
              setTimeout(function(el){ el.classList.remove('pulse'); }, 1100, jt.el);
            }
          }
        }
        last = now;
      });
    }
  }
  if(document.readyState!=='loading') bind(); else document.addEventListener('DOMContentLoaded', bind);
})();
</script>
'''.strip()

def render_node_html(tid, tweets, users, children_map, meta, uid_of, linkdex, visited, start_epoch, collapsed_by_default=True):
    """
    Renders a node + flattens self-reply chains (same author replying to themselves).
    'visited' prevents double rendering when we flatten.
    """
    if tid in visited: return ""
    visited.add(tid)

    t = tweets.get(tid) or {}
    uid = uid_of.get(tid, str(t.get("user_id_str") or t.get("user_id") or ""))
    u   = (users or {}).get(uid, {}) if uid else {}
    name   = u.get("name") or "User"
    handle = u.get("screen_name") or ""
    avatar = (u.get("profile_image_url_https") or u.get("profile_image_url") or "").replace("_normal.","_bigger.")
    created = meta.get(tid,{}).get("created_ts", parse_created_at(t.get("created_at","")))
    time_str = fmt_utc(created)

    # relative time to Space start (for pulsing)
    trel = None
    if isinstance(start_epoch,(int,float)) and start_epoch:
        trel = max(0.0, created - start_epoch)

    text_raw = t.get("full_text") or t.get("text") or ""
    ent = t.get("entities") or {}
    text_html = expand_text_with_entities(text_raw, ent)

    # index links
    seen=set()
    for uu in (ent.get("urls") or []):
        expanded = uu.get("expanded_url") or uu.get("unwound_url") or uu.get("url")
        if expanded and expanded not in seen:
            seen.add(expanded); linkdex.add(expanded, context=text_raw)
    for raw in extract_urls_from_text(text_raw):
        if raw not in seen:
            seen.add(raw); linkdex.add(raw, context=text_raw)

    tweet_id = t.get("id_str") or str(t.get("id") or tid)
    profile_url = f"https://x.com/{handle}" if handle else None
    tweet_url   = f"https://x.com/{handle}/status/{tweet_id}" if handle else f"https://x.com/i/status/{tweet_id}"

    # ---- collect a self-reply chain starting from this tweet ----
    flat_chain = []
    cur = tweet_id
    while True:
        kids = children_map.get(cur, [])
        # among the children, find first that is by the same user (self-reply)
        same = [k for k in kids if uid_of.get(k) == uid and k not in visited]
        if not same:
            break
        # pick chronological first; typical self-chain is linear
        k = same[0]
        flat_chain.append(k)
        visited.add(k)  # mark so we don't render nested
        cur = k  # continue chasing further self replies

    # ---- render the current tweet card (with data-trel for pulse) ----
    parts=[]
    data_trel = f' data-trel="{trel:.3f}"' if trel is not None else ""
    parts.append(f'<div class="ss3k-reply"{data_trel}>')
    parts.append('<span class="ss3k-dot" aria-hidden="true"></span>')
    if profile_url:
        parts.append(f'  <a href="{esc(profile_url)}" target="_blank" rel="noopener"><img class="ss3k-ravatar" src="{esc(avatar)}" alt=""></a>')
    else:
        parts.append(f'  <img class="ss3k-ravatar" src="{esc(avatar)}" alt="">')
    parts.append('  <div class="ss3k-rcontent">')
    meta_bits=[]
    if profile_url:
        meta_bits.append(f'<span class="ss3k-rname"><a href="{esc(profile_url)}" target="_blank" rel="noopener">{esc(name)}</a></span>')
    else:
        meta_bits.append(f'<span class="ss3k-rname">{esc(name)}</span>')
    if handle: meta_bits.append(f' <span>@{esc(handle)}</span>')
    if tweet_url: meta_bits.append(f' · <a href="{esc(tweet_url)}" target="_blank" rel="noopener">{esc(time_str)}</a>')
    parts.append(f'    <div class="ss3k-rmeta">{"".join(meta_bits)}</div>')
    parts.append(f'    <div class="ss3k-rtext">{text_html}</div>')
    parts.append('  </div>')
    parts.append('</div>')

    # ---- render the flattened self-reply chain directly after (no indent) ----
    for child_id in flat_chain:
        parts.append(render_node_html(child_id, tweets, users, children_map, meta, uid_of, linkdex, visited, start_epoch, collapsed_by_default=True))

    # ---- render "other users" children nested (exclude anything already visited) ----
    other_kids = [k for k in children_map.get(tweet_id, []) if k not in visited]
    if other_kids:
        box_id = f"ss3k-children-{tweet_id}"
        collapsed = " ss3k-hidden" if collapsed_by_default else ""
        collapsed_label = f"Show {len(other_kids)} repl{'y' if len(other_kids)==1 else 'ies'}"
        expanded_label  = f"Hide repl{'y' if len(other_kids)==1 else 'ies'}"
        parts.append(
            f'<div class="ss3k-toggle" role="button" tabindex="0" '
            f'data-toggle-for="{box_id}" data-collapsed-label="{esc(collapsed_label)}" '
            f'data-expanded-label="{esc(expanded_label)}">{esc(collapsed_label) if collapsed_by_default else esc(expanded_label)}</div>'
        )
        parts.append(f'<div class="ss3k-children{collapsed}" id="{box_id}">')
        for kid in other_kids:
            parts.append(render_node_html(kid, tweets, users, children_map, meta, uid_of, linkdex, visited, start_epoch, collapsed_by_default=True))
        parts.append('</div>')

    return "\n".join(parts)

def render_thread_html(roots, tweets, users, children_map, meta, uid_of, linkdex, start_epoch):
    out=[CSS, '<div class="ss3k-threads">']
    out.append('<div class="ss3k-controls"><button type="button" data-ss3k-expand-all>Expand all</button> '
               '<button type="button" data-ss3k-collapse-all>Collapse all</button></div>')
    visited=set()
    for rid in roots:
        out.append(render_node_html(rid, tweets, users, children_map, meta, uid_of, linkdex, visited, start_epoch, collapsed_by_default=True))
    out.append('</div>')
    out.append(JS)
    return "\n".join(out)

# ---------- main ----------
def main():
    if not PURPLE: write_empty(); return
    m=re.search(r"/([^/]+)/status/(\d+)", PURPLE)
    if not m: write_empty(); return
    screen_name=m.group(1)
    tid_str    =m.group(2)

    tweets, users = fetch_conversation_adaptive(tid_str, screen_name_hint=screen_name)
    if not tweets: write_empty(); return

    roots, children_map, meta, uid_of = build_thread_tree(tid_str, tweets)

    # Build link index from ALL tweets (root + replies + nested)
    linkdex=LinkIndex()
    for t in (tweets or {}).values():
        text_raw=(t.get("full_text") or t.get("text") or "")
        ent=(t.get("entities") or {})
        seen=set()
        for uu in (ent.get("urls") or []):
            expanded=uu.get("expanded_url") or uu.get("unwound_url") or uu.get("url")
            if expanded and expanded not in seen:
                seen.add(expanded); linkdex.add(expanded, context=text_raw)
        for raw in extract_urls_from_text(text_raw):
            if raw not in seen:
                seen.add(raw); linkdex.add(raw, context=text_raw)
    linkdex.finalize_labels()

    # Try to read absolute Space start for timeline pulses
    start_epoch = load_space_start_epoch(START_PATH)

    html_out  = render_thread_html(roots, tweets, users or {}, children_map, meta, uid_of, linkdex, start_epoch)
    links_out = linkdex.render_grouped_html()

    open(REPLIES_OUT,"w",encoding="utf-8").write(html_out)
    open(LINKS_OUT,"w",encoding="utf-8").write(links_out)

if __name__=="__main__":
    main()
