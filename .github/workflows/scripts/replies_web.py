#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Replies/links renderer with resilient fallback harvesting.

What’s new (vs prior):
- If no clean "reply-like" rows exist, walk any JSON/JSONL looking for
  Twitter GraphQL "legacy" tweet objects (typical crawler dumps), harvest
  them into reply-like dicts (id/text/user/stats/entities/media/quoted).
- Dedup by tweet id; can build URLs with @handle if known, else i/web/status/id.
- Restores per-reply stats/date and embeds quote-tweets via widgets.js.
- Expands t.co in text using entities (indices when present).
- Writes emotes to BASE_emotes.vtt (ignoring transcript blocks).
- Writes a quick debug summary to BASE_replies_debug.txt.

ENV:
  ARTDIR            default "."
  BASE              default "space"
  PURPLE_TWEET_URL  optional, shown on links page
  DEBUG             if set (any), writes extra info in debug file
"""

import os, re, json, html, time, traceback, sys
from urllib.parse import urlparse

# --------- ENV & Paths ----------
ARTDIR = os.environ.get("ARTDIR", ".")
BASE   = os.environ.get("BASE", "space")
PURPLE = (os.environ.get("PURPLE_TWEET_URL", "") or "").strip()
DEBUG  = os.environ.get("DEBUG")

OUT_REPLIES = os.path.join(ARTDIR, f"{BASE}_replies.html")
OUT_LINKS   = os.path.join(ARTDIR, f"{BASE}_links.html")
OUT_EMOTES  = os.path.join(ARTDIR, f"{BASE}_emotes.vtt")
OUT_DEBUG   = os.path.join(ARTDIR, f"{BASE}_replies_debug.txt")
LOG_PATH    = os.path.join(ARTDIR, f"{BASE}_replies.log")

_NEED_TWITTER_WIDGET = False

# --------- Utility ----------
def log(msg: str) -> None:
    os.makedirs(os.path.dirname(LOG_PATH) or ".", exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

def write_text(path, text):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def read_text(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def ts_to_local_str(ts_iso_or_ms):
    try:
        if isinstance(ts_iso_or_ms, (int, float)):
            sec = ts_iso_or_ms / 1000.0 if ts_iso_or_ms > 10**12 else float(ts_iso_or_ms)
            return time.strftime("%Y-%m-%d %H:%M", time.localtime(sec))
        if isinstance(ts_iso_or_ms, str):
            s = ts_iso_or_ms.strip()
            if re.fullmatch(r"\d{10}(?:\.\d+)?", s):
                return time.strftime("%Y-%m-%d %H:%M", time.localtime(float(s)))
            # Try common Twitter formats
            for fmt in ("%a %b %d %H:%M:%S %z %Y",    # "Wed Oct 30 12:34:56 +0000 2025"
                        "%Y-%m-%dT%H:%M:%S.%fZ",
                        "%Y-%m-%dT%H:%M:%SZ",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d %H:%M"):
                try:
                    if "%z" in fmt:
                        return time.strftime("%Y-%m-%d %H:%M", time.localtime(time.mktime(time.strptime(s, fmt))))
                    t = time.strptime(s.replace("Z",""), fmt.replace("Z",""))
                    return time.strftime("%Y-%m-%d %H:%M", t)
                except Exception:
                    pass
    except Exception:
        pass
    return ""

def fmt_int(n):
    try: n = int(n)
    except Exception: return "0"
    if n >= 1_000_000:
        s = f"{n/1_000_000:.1f}".rstrip("0").rstrip("."); return f"{s}M"
    if n >= 1_000:
        s = f"{n/1_000:.1f}".rstrip("0").rstrip("."); return f"{s}K"
    return str(n)

def escape_text(s): return html.escape(s or "")
def domain_of(url):
    try: return urlparse(url).netloc
    except Exception: return ""
def coalesce(*vals, default=None):
    for v in vals:
        if v is not None: return v
    return default

# --------- Input discovery ----------
def list_candidates():
    # Strong hints first
    c = [
        os.path.join(ARTDIR, f"{BASE}_replies.jsonl"),
        os.path.join(ARTDIR, f"{BASE}.replies.jsonl"),
        os.path.join(ARTDIR, f"{BASE}_crawler.jsonl"),
        os.path.join(ARTDIR, f"{BASE}_all.jsonl"),
        os.path.join(ARTDIR, f"{BASE}_replies.json"),
        os.path.join(ARTDIR, f"{BASE}.replies.json"),
        os.path.join(ARTDIR, f"{BASE}_crawler.json"),
        os.path.join(ARTDIR, f"{BASE}_all.json"),
    ]
    # Also scan ARTDIR for any json/jsonl to salvage from (fallback)
    try:
        for name in sorted(os.listdir(ARTDIR)):
            if name.endswith((".json",".jsonl")) and name not in [os.path.basename(x) for x in c]:
                c.append(os.path.join(ARTDIR, name))
    except Exception:
        pass
    # Keep only existing files
    return [p for p in c if os.path.isfile(p)]

def read_lines_jsonl(path):
    items = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try: items.append(json.loads(line))
            except Exception as e: log(f"Bad JSONL line in {os.path.basename(path)} skipped: {e}")
    return items

def read_json_any(path):
    try:
        raw = read_text(path)
        obj = json.loads(raw)
        if isinstance(obj, list): return obj
        if isinstance(obj, dict):
            # common shapes
            if "replies" in obj and isinstance(obj["replies"], list): return obj["replies"]
            if "items" in obj and isinstance(obj["items"], list): return obj["items"]
            return [obj]
    except Exception as e:
        log(f"Failed reading {os.path.basename(path)}: {e}")
    return []

# --------- Classification ----------
def is_emote_record(r):
    t = (r.get("type") or r.get("kind") or "").lower()
    if t in {"emote","emoji","emotes","emoji_vtt","vtt_emoji"}: return True
    # some crawlers file emotes as {"track":"emotes","start":...}
    if (r.get("track") or "").lower() in {"emotes","emoji"}: return True
    return False

def looks_like_tweet_minimal(r):
    # v1-ish
    if ("text" in r or "full_text" in r) and isinstance(r.get("user",{}), dict):
        return True
    # v2-like with public_metrics + author
    if "public_metrics" in r and ("author" in r or "user" in r):
        return True
    # legacy grafted (after our GraphQL harvest)
    if "full_text" in r and ("id" in r or "id_str" in r):
        return True
    return False

# --------- Normalizers ----------
def normalize_user(u):
    if not isinstance(u, dict): return {"name":"", "screen_name":"", "avatar":""}
    return {
        "name": coalesce(u.get("name"), u.get("full_name"), ""),
        "screen_name": coalesce(u.get("screen_name"), u.get("username"), u.get("handle"), ""),
        "avatar": coalesce(u.get("profile_image_url_https"), u.get("profile_image_url"), u.get("avatar"), ""),
    }

def normalize_media_from_entities(entities):
    out = []
    if not entities: return out
    media = []
    if isinstance(entities, dict) and isinstance(entities.get("media"), list):
        media = entities["media"]
    elif isinstance(entities, list):
        media = entities
    for m in media:
        if not isinstance(m, dict): continue
        mtype = m.get("type") or ""
        if mtype == "photo":
            url = coalesce(m.get("media_url_https"), m.get("media_url"), m.get("url"))
            if url: out.append({"type":"photo","url":url})
        elif mtype in ("video","animated_gif"):
            v = (m.get("video_info") or {}).get("variants", [])
            mp4s = [x for x in v if "mp4" in (x.get("content_type") or "")]
            mp4s.sort(key=lambda x: x.get("bitrate", 0), reverse=True)
            url = mp4s[0]["url"] if mp4s else None
            thumb = coalesce(m.get("media_url_https"), m.get("media_url"))
            if url: out.append({"type":"video","url":url,"poster":thumb})
    return out

def normalize_media(r):
    # prefer extended_entities, else entities, else r["media"]
    return (normalize_media_from_entities(r.get("extended_entities")) or
            normalize_media_from_entities(r.get("entities")) or
            normalize_media_from_entities(r.get("media")))

def normalize_urls(entities):
    urls = []
    if not isinstance(entities, dict): return urls
    for u in entities.get("urls", []):
        expanded = coalesce(u.get("unwound_url"), u.get("expanded_url"), u.get("url"))
        display  = u.get("display_url") or expanded
        short    = u.get("url") or ""
        indices  = tuple(u.get("indices", [])) if isinstance(u.get("indices"), list) else None
        host     = domain_of(expanded or "")
        is_stat  = False
        if expanded:
            try:
                up = urlparse(expanded)
                if up.netloc and ("twitter.com" in up.netloc.lower() or "x.com" in up.netloc.lower()):
                    if re.search(r"/status/\d+", up.path or ""): is_stat = True
            except Exception:
                pass
        if expanded:
            urls.append({"expanded":expanded,"display":display,"short":short,"indices":indices,"is_status":is_stat,"host":host})
    return urls

def expand_text_urls(text, url_entities, media_entities=None):
    if not text: return ""
    s = text
    media_short = set()
    if isinstance(media_entities, dict):
        for m in media_entities.get("media", []) or []:
            if m.get("url"): media_short.add(m["url"])
    repl = []
    for u in url_entities:
        short = u.get("short") or ""
        expanded = u.get("expanded") or short
        display = u.get("display") or expanded
        if not short: continue
        if short in media_short:
            if u.get("indices"): repl.append((u["indices"][0], u["indices"][1], ""))
            else: s = s.replace(short, "")
            continue
        a_tag = f'<a href="{html.escape(expanded)}" target="_blank" rel="noopener noreferrer">{html.escape(display)}</a>'
        if u.get("indices"): repl.append((u["indices"][0], u["indices"][1], a_tag))
        else: s = s.replace(short, a_tag)
    if repl:
        repl.sort(key=lambda x: x[0], reverse=True)
        out=[]; last=len(s)
        for a,b,rep in repl:
            a=max(0,min(a,len(s))); b=max(0,min(b,len(s)))
            out.append(s[b:last]); out.append(rep); last=a
        out.append(s[0:last]); s="".join(reversed(out))
    return re.sub(r"\s{2,}", " ", s).strip()

def normalize_tweet(r):
    tid = coalesce(r.get("id_str"), r.get("rest_id"), r.get("id"))
    text = coalesce(r.get("full_text"), r.get("text"), "")
    created = coalesce(r.get("created_at"), r.get("createdAt"), r.get("time"))
    stats = r.get("public_metrics") or {}
    likes   = coalesce(r.get("favorite_count"), stats.get("like_count"), 0)
    retw    = coalesce(r.get("retweet_count"), stats.get("retweet_count"), stats.get("repost_count"), 0)
    replies = coalesce(r.get("reply_count"),  stats.get("reply_count"), 0)
    quotes  = coalesce(r.get("quote_count"),  stats.get("quote_count"), 0)

    # entities from v1 or legacy
    entities = r.get("entities") or {}
    ext_entities = r.get("extended_entities") or {}
    media = normalize_media(r)
    urls = normalize_urls(entities)

    user = normalize_user(r.get("user") or r.get("author") or {})
    # Build URL
    url = ""
    if tid:
        if user.get("screen_name"):
            url = f"https://x.com/{user['screen_name']}/status/{tid}"
        else:
            url = f"https://x.com/i/web/status/{tid}"

    expanded_html = expand_text_urls(text, urls, entities)

    quoted_raw = r.get("quoted_status") or r.get("quoted") or {}
    quoted = None
    if isinstance(quoted_raw, dict) and quoted_raw:
        quoted = normalize_tweet(quoted_raw)

    status_urls = [u for u in urls if u.get("is_status")]
    return {
        "id": str(tid) if tid is not None else "",
        "url": url,
        "text": text,
        "text_expanded_html": expanded_html,
        "created_str": ts_to_local_str(created) or "",
        "user": user,
        "likes": int(likes) if isinstance(likes,(int,str)) and str(likes).isdigit() else 0,
        "retweets": int(retw) if isinstance(retw,(int,str)) and str(retw).isdigit() else 0,
        "replies": int(replies) if isinstance(replies,(int,str)) and str(replies).isdigit() else 0,
        "quotes": int(quotes) if isinstance(quotes,(int,str)) and str(quotes).isdigit() else 0,
        "media": media,
        "urls": urls,
        "status_urls": status_urls,
        "quoted": quoted,
    }

# --------- GraphQL fallback harvesting ----------
def graft_user_from_graphql(node):
    """
    Try to pull a user legacy from a tweet node (GraphQL style).
    """
    # common places:
    # node["core"]["user_results"]["result"]["legacy"]
    try:
        core = node.get("core") or {}
        ures = (core.get("user_results") or {}).get("result") or {}
        uleg = ures.get("legacy") or {}
        if uleg:
            return {
                "name": uleg.get("name",""),
                "screen_name": uleg.get("screen_name",""),
                "avatar": uleg.get("profile_image_url_https") or uleg.get("profile_image_url") or ""
            }
    except Exception:
        pass
    # sometimes directly under "author"
    a = node.get("author") or {}
    if a:
        return normalize_user(a)
    return {"name":"","screen_name":"","avatar":""}

def legacy_to_v1ish(node, legacy):
    # Convert a GraphQL tweet (rest_id + legacy) to v1-ish dict our normalizer understands.
    d = {
        "id": node.get("rest_id") or legacy.get("id_str"),
        "id_str": node.get("rest_id") or legacy.get("id_str"),
        "full_text": legacy.get("full_text") or legacy.get("text") or "",
        "created_at": legacy.get("created_at"),
        "favorite_count": legacy.get("favorite_count"),
        "retweet_count": legacy.get("retweet_count"),
        "reply_count": legacy.get("reply_count"),
        "quote_count": legacy.get("quote_count"),
        "entities": legacy.get("entities") or {},
        "extended_entities": legacy.get("extended_entities") or {},
        "user": graft_user_from_graphql(node),
    }
    # Quoted
    try:
        qres = (node.get("quoted_status_result") or node.get("quoted_status_result", {}))
        if isinstance(qres, dict):
            qnode = qres.get("result") or {}
            qleg = qnode.get("legacy") or {}
            if qleg:
                d["quoted_status"] = legacy_to_v1ish(qnode, qleg)
    except Exception:
        pass
    return d

def harvest_graphql_legacy(obj):
    """
    Recursively find GraphQL tweet nodes that have 'legacy' and return list of v1-ish dicts.
    """
    harvested = []

    def walk(x):
        if isinstance(x, dict):
            # tweet_results / tweetResult
            if "tweet_results" in x and isinstance(x["tweet_results"], dict):
                res = x["tweet_results"].get("result") or {}
                leg = res.get("legacy")
                if leg:
                    harvested.append(legacy_to_v1ish(res, leg))
            if "tweetResult" in x and isinstance(x["tweetResult"], dict):
                res = x["tweetResult"].get("result") or {}
                leg = res.get("legacy")
                if leg:
                    harvested.append(legacy_to_v1ish(res, leg))
            # nested result with legacy (common)
            if "result" in x and isinstance(x["result"], dict):
                res = x["result"]; leg = res.get("legacy")
                if leg:
                    harvested.append(legacy_to_v1ish(res, leg))
            # plain node with rest_id + legacy
            if "legacy" in x and ("rest_id" in x or "id_str" in x):
                leg = x["legacy"]
                harvested.append(legacy_to_v1ish(x, leg))
            for v in x.values(): walk(v)
        elif isinstance(x, list):
            for v in x: walk(v)

    walk(obj)
    return harvested

# --------- Emotes (emoji VTT) ----------
def collect_emotes(records):
    emotes = []
    for r in records:
        if not is_emote_record(r): continue
        start = float(coalesce(r.get("start"), r.get("begin"), 0.0))
        end   = float(coalesce(r.get("end"), r.get("finish"), start + 0.8))
        speaker = coalesce(r.get("speaker"), r.get("user"), r.get("uid"), "")
        e = r.get("emotes") or r.get("emoji") or r.get("data")
        if isinstance(e, list): emstr = " ".join(map(str, e))
        else: emstr = str(e or "").strip()
        if emstr:
            emotes.append((start, end, str(speaker), emstr))

    if emotes:
        lines = ["WEBVTT", ""]
        def vtt_time(secs):
            h=int(secs//3600); m=int((secs%3600)//60); s=secs%60
            return f"{h:02d}:{m:02d}:{s:06.3f}".replace(".", ",").replace(",", ".")
        for i,(a,b,sp,txt) in enumerate(sorted(emotes, key=lambda x:x[0]), start=1):
            lines.append(str(i))
            lines.append(f"{vtt_time(a)} --> {vtt_time(b)}")
            lines.append(f"<v {html.escape(sp)}> {txt}")
            lines.append("")
        write_text(OUT_EMOTES, "\n".join(lines).strip()+"\n")
        log(f"Emotes VTT written ({len(emotes)} cues).")
    else:
        if not os.path.exists(OUT_EMOTES):
            write_text(OUT_EMOTES, "WEBVTT\n\n")
    return len(emotes)

# --------- HTML builders ----------
CSS = """
<style>
:root{--bg:#0b0d10;--fg:#e8eef6;--muted:#8fa1b3;--card:#12161b;--alt:#0e1217;--line:#1e2630;--accent:#1da1f2;}
*{box-sizing:border-box}
body{margin:0;padding:16px;color:var(--fg);background:transparent;font:14px/1.4 system-ui,Segoe UI,Roboto,Helvetica,Arial}
.replies{display:flex;flex-direction:column;gap:10px}
.reply{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px;display:grid;grid-template-columns:50px 1fr;gap:10px}
.reply.alt{background:var(--alt)}
.ava{width:50px;height:50px;border-radius:50%;overflow:hidden;background:#222}
.ava img{width:100%;height:100%;object-fit:cover;display:block}
.hdr{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.hdr .name{font-weight:600}
.hdr .handle{color:var(--muted)}
.txt{white-space:pre-wrap;word-break:break-word;margin-top:6px}
.media{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.media img{max-width:100%;height:auto;border-radius:10px;border:1px solid var(--line)}
.media video{max-width:100%;height:auto;border-radius:10px;border:1px solid var(--line)}
.card{border:1px solid var(--line);background:#0c1116;border-radius:10px;padding:8px;margin-top:8px}
.card .url{font-size:12px;color:var(--muted);margin-bottom:4px}
.card .title{font-weight:600;margin-bottom:4px}
.meta{display:flex;gap:12px;align-items:center;justify-content:flex-end;margin-top:10px;color:var(--muted);font-size:12px}
.meta a{color:var(--muted);text-decoration:none}
.meta .stat{display:inline-flex;gap:6px;align-items:center}
.meta .dot{opacity:.6}
.links{display:flex;flex-direction:column;gap:10px}
.link{border:1px solid var(--line);background:var(--card);border-radius:12px;padding:10px}
.link .top{display:flex;justify-content:space-between;gap:8px}
.link a{color:#9bd1ff;text-decoration:none;word-break:break-all}
.twitter-embed-fallback{font-size:13px;color:var(--muted);margin-top:6px}
</style>
"""

def render_youtube(url):
    try:
        u = urlparse(url); host=(u.netloc or "").lower()
        if "youtube.com" in host:
            q=dict([p.split("=",1) for p in (u.query or "").split("&") if "=" in p]); vid=q.get("v")
        elif "youtu.be" in host:
            vid = u.path.strip("/").split("/")[0]
        else:
            vid=None
        if vid:
            return f'<div class="card"><div class="url">youtube.com</div><div class="title"><a href="{html.escape(url)}" target="_blank" rel="noopener noreferrer">YouTube</a></div><div style="position:relative;padding-top:56.25%;border-radius:10px;overflow:hidden;border:1px solid var(--line);margin-top:6px"><iframe src="https://www.youtube.com/embed/{html.escape(vid)}" allowfullscreen style="position:absolute;inset:0;width:100%;height:100%;border:0"></iframe></div></div>'
    except Exception:
        pass
    return ""

def render_url_cards(urls):
    out=[]
    for u in urls or []:
        if u.get("is_status"): continue
        expanded=u.get("expanded") or ""
        if not expanded: continue
        dom=u.get("host") or domain_of(expanded)
        yt = render_youtube(expanded)
        if yt: out.append(yt); continue
        out.append(
            '<div class="card">'
            f'<div class="url">{html.escape(dom)}</div>'
            f'<div class="title"><a href="{html.escape(expanded)}" target="_blank" rel="noopener noreferrer">{html.escape(expanded)}</a></div>'
            '</div>'
        )
    return "".join(out)

def render_twitter_embeds(status_urls):
    global _NEED_TWITTER_WIDGET
    if not status_urls: return ""
    _NEED_TWITTER_WIDGET=True
    blocks=[]
    for u in status_urls:
        link = u.get("expanded") or u.get("short") or ""
        if not link: continue
        blocks.append(
            '<div class="card">'
            '<blockquote class="twitter-tweet"><a href="'+html.escape(link)+'"></a></blockquote>'
            '<div class="twitter-embed-fallback">If the embed doesn’t load, open: '
            f'<a href="{html.escape(link)}" target="_blank" rel="noopener noreferrer">{html.escape(link)}</a></div>'
            '</div>'
        )
    return "".join(blocks)

def render_media(media):
    if not media: return ""
    items=[]
    for m in media:
        if m.get("type")=="photo":
            items.append(f'<img loading="lazy" src="{html.escape(m["url"])}" alt="image"/>')
        elif m.get("type")=="video":
            poster=f' poster="{html.escape(m.get("poster",""))}"' if m.get("poster") else ""
            items.append(f'<video controls preload="metadata"{poster}><source src="{html.escape(m["url"])}" type="video/mp4"></video>')
    return f'<div class="media">{"".join(items)}</div>'

def render_quote(q):
    if not q: return ""
    hdr=f'<div class="hdr"><span class="name">{html.escape(q["user"]["name"])}</span><span class="handle">@{html.escape(q["user"]["screen_name"])}</span></div>'
    body_html = q.get("text_expanded_html") or escape_text(q.get("text",""))
    txt = f'<div class="txt">{body_html}</div>' if body_html else ""
    med=render_media(q.get("media"))
    cards=render_url_cards(q.get("urls"))
    meta=[]
    if q.get("url"):
        meta.append(f'<a href="{html.escape(q["url"])}" target="_blank" rel="noopener noreferrer">{html.escape(q.get("created_str",""))}</a>')
    st=[]
    if q.get("replies",0) or q.get("retweets",0) or q.get("likes",0):
        st.append(f'<span class="stat">💬 <b>{fmt_int(q["replies"])}</b></span>')
        st.append(f'<span class="stat">🔁 <b>{fmt_int(q["retweets"])}</b></span>')
        st.append(f'<span class="stat">❤️ <b>{fmt_int(q["likes"])}</b></span>')
    meta_html = f'<div class="meta'> + ("<span class=dot>•</span>".join(meta+st)) + '</div>' if (meta or st) else ""
    return f'<div class="card" style="border-left:3px solid var(--line);padding-left:8px">{hdr}{txt}{med}{cards}{meta_html}</div>'

def render_reply_item(t, alt=False):
    ava = f'<div class="ava"><img src="{html.escape(t["user"]["avatar"])}" alt="avatar"/></div>' if t["user"]["avatar"] else '<div class="ava"></div>'
    hdr = f'<div class="hdr"><span class="name">{html.escape(t["user"]["name"])}</span><span class="handle">@{html.escape(t["user"]["screen_name"])}</span></div>'
    body_html = t.get("text_expanded_html") or escape_text(t.get("text",""))
    txt = f'<div class="txt">{body_html}</div>' if body_html else ""
    med = render_media(t.get("media"))
    tw_embeds = render_twitter_embeds(t.get("status_urls"))
    cards = render_url_cards(t.get("urls"))
    quote = render_quote(t.get("quoted"))
    left=[]
    if t.get("url"):
        left.append(f'<a href="{html.escape(t["url"])}" target="_blank" rel="noopener noreferrer">{html.escape(t.get("created_str",""))}</a>')
    stats=[
        f'<span class="stat">💬 <b>{fmt_int(t["replies"])}</b></span>',
        f'<span class="stat">🔁 <b>{fmt_int(t["retweets"])}</b></span>',
        f'<span class="stat">❤️ <b>{fmt_int(t["likes"])}</b></span>',
    ]
    meta=f'<div class="meta">{"<span class=dot>•</span>".join(left+stats)}</div>'
    klass="reply alt" if alt else "reply"
    return f'<div class="{klass}">{ava}<div>{hdr}{txt}{med}{tw_embeds}{cards}{quote}{meta}</div></div>'

def build_replies_html(tweets):
    rows=[render_reply_item(t, alt=bool(i%2)) for i,t in enumerate(tweets)]
    script = '<script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>' if _NEED_TWITTER_WIDGET else ''
    return f'<!doctype html><html><head><meta charset="utf-8">{CSS}</head><body><div class="replies'>{"".join(rows) if rows else '<div class="reply"><div class="ava"></div><div>No replies found.</div></div>'}</div>{script}</body></html>'

def build_links_html(tweets):
    seen=set(); items=[]
    if PURPLE:
        items.append(f'<div class="link"><div class="top"><span>Purple</span><span>{time.strftime("%Y-%m-%d %H:%M")}</span></div><a href="{html.escape(PURPLE)}" target="_blank" rel="noopener noreferrer">{html.escape(PURPLE)}</a></div>')
    for t in tweets:
        for u in (t.get("urls") or []):
            expanded=u.get("expanded"); 
            if not expanded or expanded in seen: continue
            seen.add(expanded)
            items.append(f'<div class="link"><div class="top"><span>{html.escape(u.get("host") or domain_of(expanded))}</span><span>{html.escape(t.get("created_str",""))}</span></div><a href="{html.escape(expanded)}" target="_blank" rel="noopener noreferrer">{html.escape(expanded)}</a></div>')
        q=t.get("quoted")
        if q:
            for u in (q.get("urls") or []):
                expanded=u.get("expanded"); 
                if not expanded or expanded in seen: continue
                seen.add(expanded)
                items.append(f'<div class="link"><div class="top"><span>{html.escape(u.get("host") or domain_of(expanded))}</span><span>{html.escape(q.get("created_str",""))}</span></div><a href="{html.escape(expanded)}" target="_blank" rel="noopener noreferrer">{html.escape(expanded)}</a></div>')
    return f'<!doctype html><html><head><meta charset="utf-8">{CSS}</head><body><div class="links'>{"".join(items) if items else '<div class="link">No external links found.</div>'}</div></body></html>'

# --------- Main ----------
def main():
    try:
        files = list_candidates()
        if not files:
            write_text(OUT_REPLIES, build_replies_html([]))
            write_text(OUT_LINKS, build_links_html([]))
            log("No JSON/JSONL found; wrote placeholders.")
            print("No input files found; wrote placeholders.")
            return

        all_records=[]; emote_src_count=0
        for p in files:
            if p.endswith(".jsonl"): items=read_lines_jsonl(p)
            else: items=read_json_any(p)
            if not items: continue
            all_records.extend(items)

        # Emotes (from any file)
        emote_count = collect_emotes(all_records)

        # First pass: direct reply-like rows
        raw_replies = [r for r in all_records if looks_like_tweet_minimal(r)]
        harvested_count_direct = len(raw_replies)

        # Fallback: harvest GraphQL legacy tweets if we have few/no replies
        harvested=[]
        if harvested_count_direct == 0:
            seen_ids=set()
            for obj in all_records:
                try:
                    for tw in harvest_graphql_legacy(obj):
                        tid = str(coalesce(tw.get("id_str"), tw.get("id"), tw.get("rest_id"), ""))
                        if not tid or tid in seen_ids: continue
                        seen_ids.add(tid)
                        harvested.append(tw)
                except Exception:
                    continue
            raw_replies = harvested

        # Normalize & dedup
        dedup={},[]
        seen=set(); norm=[]
        for r in raw_replies:
            t = normalize_tweet(r)
            if not t.get("id"): continue
            if t["id"] in seen: continue
            seen.add(t["id"])
            norm.append(t)

        # Sort by time asc
        def sort_key(t):
            s=t.get("created_str","")
            try: return time.mktime(time.strptime(s,"%Y-%m-%d %H:%M"))
            except Exception: return 0.0
        norm.sort(key=sort_key)

        write_text(OUT_REPLIES, build_replies_html(norm))
        write_text(OUT_LINKS,   build_links_html(norm))

        # Debug summary
        if DEBUG is not None:
            dbg = []
            dbg.append(f"Files scanned: {len(files)}")
            dbg.extend([f"- {os.path.basename(p)} ({os.path.getsize(p)} bytes)" for p in files])
            dbg.append(f"Total records: {len(all_records)}")
            dbg.append(f"Emote cues written: {emote_count}")
            dbg.append(f"Direct reply-like records: {harvested_count_direct}")
            dbg.append(f"Harvested from GraphQL fallback: {len(harvested)}")
            dbg.append(f"Final replies rendered: {len(norm)}")
            write_text(OUT_DEBUG, "\n".join(dbg)+"\n")

        log(f"Done. replies={len(norm)} emotes={emote_count} files={len(files)} widget={_NEED_TWITTER_WIDGET}")
        print(f"Wrote {OUT_REPLIES} and {OUT_LINKS}. Replies: {len(norm)}. Emote cues: {emote_count}.")
    except Exception as e:
        tb = traceback.format_exc()
        log(f"FATAL: {e}\n{tb}")
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
