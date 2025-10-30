#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
replies_web.py — replies HTML (with OG cards + media grids, no cropping)

What’s new in this build:
- OpenGraph enrichment: if a URL entity lacks title/image, we fetch the page
  (small, fast GET with tight limits) and extract og:/twitter: meta to render
  proper content cards (thumb + title + host).
- Media layout rules (images/videos):
    1 item  -> 1 column, 400px tall
    2 items -> 2 columns, 400px tall
    3 items -> 2 columns, FIRST spans both columns on row 1 (400px tall),
               items 2–3 on row 2 (400px tall each)
    4 items -> 2×2 grid (400px tiles)
  All media are UN-CROPPED: height fixed to 400px; width is auto with
  max-width:100%; object-fit:contain (done inline to override theme CSS).
- Chronological ordering (as posted), while still emitting data-parent and a
  computed data-depth for optional visual threading in CSS.

Environment:
  ARTDIR, BASE, PURPLE_TWEET_URL, REPLIES_JSONL
  TWITTER_AUTHORIZATION, TWITTER_AUTH_TOKEN, TWITTER_CSRF_TOKEN (optional)
  WP_BASE_URL, WP_USER, WP_APP_PASSWORD, WP_POST_ID (optional)
"""

import os, re, json, html, time, traceback
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# ---------- Env ----------
ARTDIR = os.environ.get("ARTDIR", ".").strip() or "."
BASE   = os.environ.get("BASE", "space").strip() or "space"
PURPLE = (os.environ.get("PURPLE_TWEET_URL", "") or "").strip()
REPLIES_JSONL = (os.environ.get("REPLIES_JSONL", "") or "").strip()

OUT_REPLIES = os.path.join(ARTDIR, f"{BASE}_replies.html")
OUT_LINKS   = os.path.join(ARTDIR, f"{BASE}_links.html")
LOG_PATH    = os.path.join(ARTDIR, f"{BASE}_replies.log")
DBG_DIR     = os.path.join(ARTDIR, "debug")
DBG_PREFIX  = os.path.join(DBG_DIR, f"{BASE}_replies_page")

# X auth (optional — for API fallback)
AUTH        = (os.environ.get("TWITTER_AUTHORIZATION", "") or "").strip()
AUTH_COOKIE = (os.environ.get("TWITTER_AUTH_TOKEN", "") or "").strip()
CSRF        = (os.environ.get("TWITTER_CSRF_TOKEN", "") or "").strip()

MAX_PAGES   = int(os.environ.get("REPLIES_MAX_PAGES", "40") or "40")
SLEEP_SEC   = float(os.environ.get("REPLIES_SLEEP", "0.7") or "0.7")
SAVE_JSON   = (os.environ.get("REPLIES_SAVE_JSON", "1") or "1").lower() not in ("0","false")

# WP patch (optional)
WP_BASE = (os.environ.get("WP_BASE_URL", "") or os.environ.get("WP_URL", "") or "").rstrip("/")
WP_USER = os.environ.get("WP_USER", "") or ""
WP_PW   = os.environ.get("WP_APP_PASSWORD", "") or ""
WP_PID  = (os.environ.get("WP_POST_ID", "") or os.environ.get("POST_ID", "") or "").strip()

BASE_X = "https://x.com"
CONVO_URL = f"{BASE_X}/i/api/2/timeline/conversation/{{tid}}.json"
SEARCH_URL = f"{BASE_X}/i/api/2/search/adaptive.json"

# ---------- FS ----------
os.makedirs(ARTDIR, exist_ok=True)
os.makedirs(DBG_DIR, exist_ok=True)

def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    with open(LOG_PATH, "a", encoding="utf-8") as lf:
        lf.write(f"[{ts}Z] {msg}\n")

def save_debug_blob(kind: str, idx: int, raw: str) -> None:
    if not SAVE_JSON: return
    p = f"{DBG_PREFIX}_{kind}{idx:02d}.json"
    try:
        with open(p, "w", encoding="utf-8") as f: f.write(raw)
        log(f"Saved debug {kind} page {idx} to {p}")
    except Exception as e:
        log(f"Failed to save debug {kind} page {idx}: {e}")

# ---------- utils ----------
EMOJI_RE = re.compile("["+
    "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF"+
    "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF"+
    "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF"+
    "]+", re.UNICODE)
ONLY_PUNCT_SPACE = re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")

def is_emoji_only(s: str) -> bool:
    if not s or not s.strip(): return False
    t = ONLY_PUNCT_SPACE.sub("", s)
    t = EMOJI_RE.sub("", t)
    return len(t.strip()) == 0

def esc(s: Optional[str]) -> str:
    return html.escape(s or "", quote=True)

def iso_from_twitter(created_at: str) -> str:
    if not created_at: return ""
    try:
        dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc)
        return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        return created_at

def fmt_when(iso: str) -> str:
    try:
        if not iso: return ""
        d = datetime.fromisoformat(iso.replace("Z","+00:00")).astimezone(timezone.utc)
        return d.strftime("%b %d, %Y, %H:%M UTC")
    except Exception:
        return iso

def fmt_metric(n: Optional[int]) -> str:
    if n is None: return ""
    try: n = int(n)
    except Exception: return ""
    if n < 1000: return str(n)
    if n < 1_000_000: return f"{round(n/100)/10}K"
    if n < 1_000_000_000: return f"{round(n/100_000)/10}M"
    return f"{round(n/100_000_000)/10}B"

# ---------- t.co expansion ----------
def expand_text_with_entities(text: str, entities: Dict[str, Any]) -> Tuple[str, List[Dict[str,Any]], List[Dict[str,Any]]]:
    s = esc(text or "")
    urls = list((entities or {}).get("urls") or [])
    media = list((entities or {}).get("media") or [])

    rep = []
    for u in urls:
        short = u.get("url") or ""
        exp = u.get("unwound_url") or u.get("expanded_url") or short
        disp = u.get("display_url") or (exp.replace("https://","").replace("http://",""))
        start, end = None, None
        if isinstance(u.get("indices"), list) and len(u["indices"]) == 2:
            start, end = u["indices"]
        anchor = f'<a href="{esc(exp)}" target="_blank" rel="noopener">{esc(disp)}</a>'
        rep.append((start, end, esc(short), anchor))

    media_short = {m.get("url") for m in media if m.get("url")}

    rep_idx = [r for r in rep if r[0] is not None]
    for start, end, short, anchor in sorted(rep_idx, key=lambda x: x[0] or 0, reverse=True):
        try:
            s = s[:start] + anchor + s[end:]
        except Exception:
            s = s.replace(short, anchor)
    for start, end, short, anchor in rep:
        if start is None:
            s = s.replace(short, anchor)
    for short in media_short:
        s = s.replace(esc(short), "")

    s = re.sub(r'@([A-Za-z0-9_]{1,15})', r'<a href="https://x.com/\1" target="_blank" rel="noopener">@\1</a>', s)
    s = re.sub(r'#([A-Za-z0-9_]+)', r'<a href="https://x.com/hashtag/\1" target="_blank" rel="noopener">#\1</a>', s)
    return s, urls, media

# ---------- OG fetch ----------
OG_CACHE: Dict[str, Dict[str,str]] = {}
MAX_OG_FETCH = 30
OG_FETCHED = 0

META_RE = re.compile(
    r'<meta\b[^>]+?(?:property|name)\s*=\s*["\'](?P<key>og:[^"\']+|twitter:[^"\']+)["\'][^>]*?content\s*=\s*["\'](?P<val>[^"\']+)["\']',
    re.IGNORECASE)
TITLE_RE = re.compile(r'<title[^>]*>(?P<title>.*?)</title>', re.IGNORECASE|re.DOTALL)

def fetch_small(url: str) -> Optional[str]:
    try:
        req = Request(url, headers={
            "User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language":"en-US,en;q=0.9",
            "Referer": url,
        })
        with urlopen(req, timeout=8) as r:
            raw = r.read(524288)  # 512 KB max
            return raw.decode("utf-8", "ignore")
    except Exception as e:
        log(f"OG fetch fail {url}: {e}")
        return None

def fetch_og(url: str) -> Dict[str,str]:
    global OG_FETCHED
    if not url or url.split("://",1)[0] not in ("http","https"):
        return {}
    if url in OG_CACHE: return OG_CACHE[url]
    if OG_FETCHED >= MAX_OG_FETCH:
        return {}
    OG_FETCHED += 1

    html_src = fetch_small(url)
    if not html_src:
        OG_CACHE[url] = {}
        return OG_CACHE[url]

    out: Dict[str,str] = {}
    for m in META_RE.finditer(html_src):
        k = m.group("key").lower().strip()
        v = m.group("val").strip()
        if k in ("og:title","twitter:title"): out.setdefault("title", v)
        elif k in ("og:description","twitter:description"): out.setdefault("description", v)
        elif k in ("og:image","twitter:image","twitter:image:src"): out.setdefault("image", v)
        elif k == "og:site_name": out.setdefault("site", v)
    if "title" not in out:
        m = TITLE_RE.search(html_src)
        if m:
            t = re.sub(r"\s+"," ", m.group("title")).strip()
            out["title"] = t
    OG_CACHE[url] = out
    return out

def host_of(u: str) -> str:
    try:
        return urlsplit(u).netloc or ""
    except Exception:
        return ""

# ---------- Quote embed ----------
def is_status_url(u: str) -> bool:
    return bool(re.search(r"https?://(x|twitter)\.com/[^/]+/status/\d+", u or ""))

def render_quote_embed(u: str) -> str:
    safe = esc(u)
    return '<blockquote class="twitter-tweet" data-dnt="true"><a href="%s"></a></blockquote>' % safe

# ---------- Media rendering (no crop, fixed height) ----------
def media_container_style(n: int) -> str:
    # Use inline style to avoid depending on theme CSS
    if n <= 1:
        return "display:grid;grid-template-columns:1fr;gap:8px;grid-auto-rows:400px;"
    # 2,3,4 -> two columns
    return "display:grid;grid-template-columns:repeat(2,1fr);gap:8px;grid-auto-rows:400px;"

def media_item_style() -> str:
    # Every media item is a 400px-tall tile; content un-cropped
    return "width:100%;height:100%;display:flex;align-items:center;justify-content:center;background:#f8fafc;border:1px solid #e6eaf2;border-radius:10px;overflow:hidden;"

def fit_style() -> str:
    # Applied directly to <img>/<video>: no cropping, height fixed 400px, width variable
    return "height:400px;width:auto;max-width:100%;object-fit:contain;display:block;background:#000;"

def render_media(media_list: List[Dict[str,Any]], extended: Dict[str,Any]) -> str:
    if not media_list and not extended:
        return ""
    ext_media = []
    if isinstance(extended, dict):
        ext_media = list((extended.get("media") or []))
    by_id: Dict[str,Dict[str,Any]] = {}
    for m in media_list or []:
        mid = str(m.get("id_str") or m.get("id") or "")
        by_id[mid] = m
    if ext_media:
        for m in ext_media:
            mid = str(m.get("id_str") or m.get("id") or "")
            by_id[mid] = {**by_id.get(mid, {}), **m}

    # Normalize into ordered list (stable)
    items = []
    for mid, m in by_id.items():
        t = (m.get("type") or "").lower()
        if t == "photo" and m.get("media_url_https"):
            items.append(("img", m["media_url_https"]))
        elif t in ("video","animated_gif"):
            vi = (m.get("video_info") or {})
            src = best_mp4(vi.get("variants") or [])
            poster = m.get("media_url_https") or ""
            if src:
                items.append(("vid", src, poster))

    if not items: return ""

    n = len(items)
    html_items = []
    for i, it in enumerate(items, 1):
        # First figure wrapper provides the tile
        tile = ['<figure class="media-item" style="%s">' % media_item_style()]
        if it[0] == "img":
            tile.append('<img src="%s" alt="" style="%s"/>' % (esc(it[1]), fit_style()))
        else:
            poster_attr = ' poster="%s"' % esc(it[2]) if len(it) > 2 and it[2] else ""
            tile.append('<video controls preload="metadata" playsinline%s style="%s"><source src="%s" type="video/mp4">Your browser does not support the video tag.</video>' %
                        (poster_attr, fit_style(), esc(it[1])))
        tile.append("</figure>")
        html_items.append("".join(tile))

    # Layout tweaks: span first for 3-pack
    wrapper_open = '<div class="ss3k-media" data-media-count="%d" style="%s">' % (n, media_container_style(n))
    if n == 3:
        # Insert style to span cols for first item
        html_items[0] = html_items[0].replace(
            'class="media-item"',
            'class="media-item" style="%s grid-column:1 / -1;"' % media_item_style(), 1
        )
    out = [wrapper_open] + html_items + ["</div>"]
    return "".join(out)

def best_mp4(variants: List[Dict[str,Any]]) -> Optional[str]:
    best, best_br = None, -1
    for v in variants or []:
        if v.get("content_type") == "video/mp4" and v.get("url"):
            br = int(v.get("bitrate") or 0)
            if br > best_br: best_br, best = br, v["url"]
    return best

# ---------- Cards ----------
def render_link_card_from_url(url_entity: Dict[str,Any]) -> str:
    exp = url_entity.get("unwound_url") or url_entity.get("expanded_url") or url_entity.get("url") or ""
    if not exp: return ""
    if is_status_url(exp): return ""  # handled as quote-embed elsewhere

    # Prefer entity-provided meta if present; otherwise fetch OG
    title = url_entity.get("title") or ""
    desc  = url_entity.get("description") or ""
    images = url_entity.get("images") or []
    thumb = None
    if images and isinstance(images, list):
        thumb = images[0].get("url") or None

    if not (title and thumb):
        og = fetch_og(exp)
        title = title or og.get("title","")
        desc  = desc  or og.get("description","")
        thumb = thumb or og.get("image","")

    host = host_of(exp) or (url_entity.get("display_url") or "")
    # Minimal clean card; CSS in your PHP already styles .ss3k-card.ext
    return (
        '<div class="ss3k-card ext" data-og="1">'
        f'  <a class="wrap" href="{esc(exp)}" target="_blank" rel="noopener">'
        f'    {"<img class=\"thumb\" src=\"%s\" alt=\"\">" % esc(thumb) if thumb else ""}'
        f'    <div class="meta"><div class="ttl">{esc(title or host or "Open link")}</div>'
        f'    <div class="dom">{esc(host)}</div></div>'
        '  </a>'
        '</div>'
    )

# ---------- API (optional) ----------
def hdrs(screen_name: str, root_id: str) -> Dict[str,str]:
    ck = f"auth_token={AUTH_COOKIE}; ct0={CSRF}" if (AUTH_COOKIE and CSRF) else ""
    h = {
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"https://x.com/{screen_name}/status/{root_id}",
        "Origin":  "https://x.com",
    }
    if ck:
        h["Cookie"] = ck
        h["x-csrf-token"] = CSRF
    if AUTH.startswith("Bearer "):
        h["Authorization"] = AUTH
    return h

def fetch_json(url: str, headers: Dict[str,str], tag: str, attempt=1) -> Tuple[Optional[Dict], Optional[str]]:
    try:
        r = urlopen(Request(url, headers=headers), timeout=30)
        raw = r.read().decode("utf-8", "ignore")
        data = json.loads(raw) if raw.strip() else {}
        return data, raw
    except HTTPError as e:
        body = ""
        try: body = e.read().decode("utf-8","ignore")
        except Exception: pass
        log(f"{tag} HTTP {e.code} {url} body={body[:500]}")
        if e.code in (429,403) and attempt <= 3:
            time.sleep(2**attempt)
            return fetch_json(url, headers, tag, attempt+1)
        return None, None
    except URLError as e:
        log(f"{tag} URL error {getattr(e,'reason',e)} {url}")
        if attempt <= 3:
            time.sleep(2**attempt)
            return fetch_json(url, headers, tag, attempt+1)
        return None, None
    except Exception as e:
        log(f"{tag} EXC {e}\n{traceback.format_exc()}")
        return None, None

def find_bottom_cursor(data: Dict[str,Any]) -> Optional[str]:
    def rec(o):
        if isinstance(o, dict):
            if o.get("cursorType") == "Bottom" and o.get("value"):
                return o["value"]
            for v in o.values():
                x = rec(v)
                if x: return x
        elif isinstance(o, list):
            for v in o:
                x = rec(v)
                if x: return x
        return None
    return rec(data)

def parse_global_objects(data: Dict[str,Any], tweets: Dict[str,Any], users: Dict[str,Any]) -> None:
    g = (data.get("globalObjects") or {})
    tweets.update(g.get("tweets") or {})
    users.update(g.get("users") or {})

def collect_via_api(root_sn: str, root_id: str) -> Tuple[Dict[str,Any], Dict[str,Any]]:
    tweets, users, cursor, page = {}, {}, None, 0
    while page < MAX_PAGES:
        page += 1
        params = {"count": 100, "tweet_mode": "extended"}
        if cursor: params["cursor"] = cursor
        url = CONVO_URL.format(tid=root_id) + "?" + urlencode(params)
        log(f"[CONVO] page={page} cursor={cursor}")
        data, raw = fetch_json(url, hdrs(root_sn, root_id), "[CONVO]")
        if raw: save_debug_blob("convo", page, raw)
        if not data: break
        parse_global_objects(data, tweets, users)
        nxt = find_bottom_cursor(data)
        if not nxt or nxt == cursor: break
        cursor = nxt
        time.sleep(SLEEP_SEC)
    if not tweets:
        tweets, users, cursor, page = {}, {}, None, 0
        while page < MAX_PAGES:
            page += 1
            params = {
                "q": f"conversation_id:{root_id}",
                "count": 100,
                "tweet_search_mode": "live",
                "query_source": "typed_query",
                "tweet_mode": "extended",
            }
            url = SEARCH_URL + "?" + urlencode(params)
            log(f"[SEARCH] page={page} cursor={cursor}")
            data, raw = fetch_json(url, hdrs(root_sn, root_id), "[SEARCH]")
            if raw: save_debug_blob("search", page, raw)
            if not data: break
            parse_global_objects(data, tweets, users)
            nxt = find_bottom_cursor(data)
            if not nxt or nxt == cursor: break
            cursor = nxt
            time.sleep(SLEEP_SEC)
    return tweets, users

# ---------- Reply model ----------
class Reply:
    __slots__ = (
        "id","user_id","name","handle","verified","avatar",
        "text","html","created_iso","metrics","entities","extended_entities",
        "in_reply_to","quoted_url","parent_id","root_id",
        "media_html","link_cards_html","depth"
    )
    def __init__(self):
        self.id=""; self.user_id=""; self.name="User"; self.handle=""; self.verified=False
        self.avatar=""; self.text=""; self.html=""; self.created_iso=""
        self.metrics={"replies":None,"reposts":None,"likes":None,"quotes":None,"bookmarks":None,"views":None}
        self.entities={}; self.extended_entities={}
        self.in_reply_to=None; self.quoted_url=None; self.parent_id=None; self.root_id=None
        self.media_html=""; self.link_cards_html=""; self.depth=0

def from_tweet_obj(t: Dict[str,Any], users: Dict[str,Any]) -> Optional[Reply]:
    tid = str(t.get("id_str") or t.get("id") or "").strip()
    if not tid: return None
    r = Reply(); r.id = tid
    uid = str(t.get("user_id_str") or t.get("user_id") or (t.get("user") or {}).get("id_str") or (t.get("user") or {}).get("id") or "")
    u = users.get(uid) if uid and isinstance(users, dict) else (t.get("user") or {})
    r.user_id = uid or str(u.get("id_str") or u.get("id") or "")
    r.name = u.get("name") or (t.get("user") or {}).get("name") or "User"
    r.handle = u.get("screen_name") or (t.get("user") or {}).get("screen_name") or ""
    r.verified = bool(u.get("verified") or u.get("is_blue_verified") or u.get("ext_is_blue_verified"))
    r.avatar = (u.get("profile_image_url_https") or u.get("profile_image_url") or "").replace("_normal.", "_bigger.")

    text = t.get("full_text") or t.get("text") or ""
    r.entities = (t.get("entities") or {})
    r.extended_entities = (t.get("extended_entities") or {})
    html_text, url_entities, media_entities = expand_text_with_entities(text, r.entities)

    # Quote tweet from URL entities
    qt = None
    for uent in url_entities:
        ex = uent.get("unwound_url") or uent.get("expanded_url") or uent.get("url") or ""
        if is_status_url(ex):
            qt = ex; break
    r.quoted_url = qt

    # Media tiles
    r.media_html = render_media(media_entities, r.extended_entities)

    # External link cards (with OG fetch if needed)
    cards = []
    seen = set()
    for uent in url_entities:
        ex = uent.get("unwound_url") or uent.get("expanded_url") or uent.get("url") or ""
        if not ex or is_status_url(ex):  # quote tweets handled separately
            continue
        if ex in seen: continue
        seen.add(ex)
        cards.append(render_link_card_from_url(uent))
    r.link_cards_html = "".join(cards)

    r.text = text
    r.html = html_text

    ca = t.get("created_at") or ""
    r.created_iso = iso_from_twitter(ca) if ca else (t.get("created_at_iso") or t.get("created_at_utc") or "")

    pm = t.get("public_metrics") or {}
    r.metrics["replies"]   = pm.get("reply_count") if pm else (t.get("reply_count"))
    r.metrics["reposts"]   = pm.get("retweet_count") if pm else (t.get("retweet_count"))
    r.metrics["likes"]     = pm.get("like_count")    if pm else (t.get("favorite_count"))
    r.metrics["quotes"]    = pm.get("quote_count")    if pm else (t.get("quote_count"))
    r.metrics["bookmarks"] = t.get("bookmark_count")
    views = t.get("views") or t.get("ext_views") or {}
    if isinstance(views, dict): r.metrics["views"] = views.get("count")

    r.in_reply_to = str(t.get("in_reply_to_status_id_str") or t.get("in_reply_to_status_id") or "") or None
    for ref in (t.get("referenced_tweets") or []):
        if ref.get("type") == "replied_to" and ref.get("id"):
            r.in_reply_to = str(ref["id"]) or r.in_reply_to
        if ref.get("type") == "quoted" and ref.get("id"):
            if not r.quoted_url:
                r.quoted_url = f"https://x.com/i/web/status/{ref['id']}"
    return r

# ---------- JSONL ingestion ----------
def parse_jsonl(path: str) -> Tuple[List[Reply], Dict[str,Any]]:
    replies: List[Reply] = []
    users_index: Dict[str,Any] = {}

    if not path or not os.path.isfile(path):
        return replies, users_index

    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = (line or "").strip()
            if not line: continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            cand = None
            if isinstance(obj, dict):
                if obj.get("tweet") and isinstance(obj["tweet"], dict):
                    cand = obj["tweet"]
                elif obj.get("data") and isinstance(obj["data"], dict) and obj["data"].get("tweet"):
                    cand = obj["data"]["tweet"]
                else:
                    cand = obj
            if not isinstance(cand, dict): continue

            text = cand.get("full_text") or cand.get("text") or ""
            if text and is_emoji_only(text):  # drop emoji-only lines
                continue

            uobj = cand.get("user")
            if isinstance(uobj, dict):
                uid = str(uobj.get("id_str") or uobj.get("id") or "")
                if uid: users_index.setdefault(uid, uobj)

            r = from_tweet_obj(cand, users_index)
            if r: replies.append(r)
    return replies, users_index

# ---------- Ordering + depth ----------
def compute_depths(replies: List[Reply]) -> None:
    by_id = {r.id: r for r in replies}
    for r in replies:
        d, cur = 0, r.in_reply_to
        steps = 0
        while cur and cur in by_id and steps < 20:
            d += 1
            cur = by_id[cur].in_reply_to
            steps += 1
        r.depth = d
        r.parent_id = r.in_reply_to

def chronological(replies: List[Reply]) -> List[Reply]:
    # order strictly by created time (as posted)
    def key(r: Reply):
        try:
            return datetime.fromisoformat((r.created_iso or "").replace("Z","+00:00"))
        except Exception:
            return datetime(1970,1,1,tzinfo=timezone.utc)
    return sorted(replies, key=key)

# ---------- Render ----------
def render_reply(r: Reply) -> str:
    url = f"{BASE_X}/{esc(r.handle)}/status/{esc(r.id)}" if r.handle else f"{BASE_X}/i/web/status/{esc(r.id)}"
    when = fmt_when(r.created_iso)
    av = f'<div class="avatar-50">{f"<img src=\"{esc(r.avatar)}\" alt=\"\">" if r.avatar else ""}</div>'
    head = (
        '<div class="head">'
        f'  <span class="disp">{esc(r.name)}</span>'
        f'  <span class="handle"> @{esc(r.handle or "user")}</span>'
        f'  {"<span class=\"badge\"></span>" if r.verified else ""}'
        '</div>'
    )
    body = f'<div class="body">{r.html}</div>'
    embed = f'<div class="qt">{render_quote_embed(r.quoted_url)}</div>' if r.quoted_url else ""
    media = r.media_html
    cards = r.link_cards_html

    metrics = []
    if r.metrics.get("replies") is not None: metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M14 9V5l-7 7 7 7v-4h1c4 0 7 1 9 4-1-7-5-10-10-10h-1z"/></svg><span>{fmt_metric(r.metrics["replies"])}</span></span>')
    if r.metrics.get("reposts") is not None: metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M17 1l4 4-4 4V7H7a3 3 0 00-3 3v2H2V9a5 5 0 015-5h10V1zm-6 16H5l4-4v2h10a3 3 0 003-3v-2h2v3a5 5 0 01-5 5H11v2l-4-4 4-4v3z"/></svg><span>{fmt_metric(r.metrics["reposts"])}</span></span>')
    if r.metrics.get("likes")   is not None: metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M12 21s-7-4.4-9-8.6C1.1 9.6 3 7 5.9 7c1.9 0 3.1 1 4.1 2 1-1 2.2-2 4.1-2 2.9 0 4.8 2.6 2.9 5.4C19 16.6 12 21 12 21z"/></svg><span>{fmt_metric(r.metrics["likes"])}</span></span>')
    if r.metrics.get("quotes")  is not None: metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M6 2h12a1 1 0 011 1v19l-7-4-7 4V3a1 1 0 011-1z"/></svg><span>{fmt_metric(r.metrics["quotes"])}</span></span>')
    bar = (f'<div class="tweetbar">{"".join(metrics)}</div>' if metrics else "")
    linkx = f'<span class="linkx"><a href="{url}" target="_blank" rel="noopener">{esc(when or "Open on X")}</a></span>'

    attrs = {
        "class": "ss3k-reply",
        "data-id": r.id,
        "data-name": r.name,
        "data-handle": f"@{r.handle}" if r.handle else "",
        "data-verified": "true" if r.verified else "",
        "data-url": url,
        "data-ts": r.created_iso,
        "data-parent": r.parent_id or "",
        "data-depth": str(r.depth),
        "data-root": r.root_id or "",
        "data-replies": str(r.metrics.get("replies") or ""),
        "data-reposts": str(r.metrics.get("reposts") or ""),
        "data-likes":   str(r.metrics.get("likes") or ""),
        "data-quotes":  str(r.metrics.get("quotes") or ""),
    }
    attr_s = " ".join(f'{k}="{esc(v)}"' for k,v in attrs.items() if v)

    # Optional inline indent (keeps things readable even if CSS not yet added)
    indent_px = min(5, r.depth) * 16  # cap at depth 5
    style_indent = f' style="margin-left:{indent_px}px;"' if indent_px else ""

    return (
        f'<div {attr_s}{style_indent}>'
        f'  {av}'
        f'  <div>'
        f'    {head}'
        f'    {body}'
        f'    {embed}'
        f'    {media}'
        f'    {cards}'
        f'    {bar}'
        f'    {linkx}'
        f'  </div>'
        f'</div>'
    )

# ---------- Links sidebar ----------
def build_links_sidebar(all_replies: List[Reply]) -> str:
    doms: Dict[str,set] = defaultdict(set)
    for r in all_replies:
        ent = r.entities or {}
        for u in (ent.get("urls") or []):
            exp = u.get("unwound_url") or u.get("expanded_url") or u.get("url")
            if not exp: continue
            m = re.search(r"https?://([^/]+)/?", exp)
            dom = m.group(1) if m else "links"
            doms[dom].add(exp)
    lines = []
    for dom in sorted(doms):
        lines.append(f"<h4>{esc(dom)}</h4>")
        lines.append("<ul>")
        for u in sorted(doms[dom]):
            e = esc(u)
            lines.append(f'<li><a href="{e}" target="_blank" rel="noopener">{e}</a></li>')
        lines.append("</ul>")
    return "\n".join(lines)

# ---------- WP patch ----------
def wp_patch_if_possible(html_replies: str, html_links: str) -> None:
    if not (WP_BASE and WP_USER and WP_PW and WP_PID):
        return
    try:
        import base64
        body = {
            "post_id": int(WP_PID),
            "status": "complete",
            "progress": 100,
            "ss3k_replies_html": html_replies,
            "shared_links_html": html_links,
        }
        data = json.dumps(body).encode("utf-8")
        req = Request(f"{WP_BASE}/wp-json/ss3k/v1/patch-assets", data=data,
                      headers={"Content-Type":"application/json"})
        cred = (WP_USER + ":" + WP_PW).encode("utf-8")
        req.add_header("Authorization", "Basic " + base64.b64encode(cred).decode("ascii"))
        with urlopen(req, timeout=30) as r:
            _ = r.read()
        log(f"Patched WP post_id={WP_PID}")
    except Exception as e:
        log(f"WP patch failed: {e}")

# ---------- Main ----------
def main():
    try:
        replies, users = parse_jsonl(REPLIES_JSONL)

        root_sn, root_id = None, None
        if PURPLE:
            m = re.search(r"https?://(?:x|twitter)\.com/([^/]+)/status/(\d+)", PURPLE)
            if m:
                root_sn, root_id = m.group(1), m.group(2)

        if not replies and root_id and (AUTH.startswith("Bearer ") or (AUTH_COOKIE and CSRF)):
            tmap, umap = collect_via_api(root_sn or "", root_id)
            users.update(umap or {})
            for tid, t in (tmap or {}).items():
                conv = str(t.get("conversation_id_str") or t.get("conversation_id") or "")
                if root_id and conv != str(root_id): continue
                if str(t.get("id_str") or t.get("id")) == str(root_id): continue
                if t.get("retweeted_status_id") or t.get("retweeted_status_id_str"): continue
                r = from_tweet_obj(t, users)
                if r: replies.append(r)

        # Dedup + assign root
        uniq = {}
        for r in replies: uniq[r.id] = r
        replies = list(uniq.values())
        if root_id:
            for r in replies: r.root_id = root_id

        # Order by time; compute depths for optional indent/threading
        replies = chronological(replies)
        compute_depths(replies)

        # Emit HTML
        reply_blocks = [render_reply(r) for r in replies]
        html_replies = "\n".join(reply_blocks)
        with open(OUT_REPLIES, "w", encoding="utf-8") as f:
            f.write(html_replies)
        log(f"Wrote replies HTML: {OUT_REPLIES} ({len(replies)} items)")

        html_links = build_links_sidebar(replies)
        with open(OUT_LINKS, "w", encoding="utf-8") as f:
            f.write(html_links)
        log(f"Wrote links HTML: {OUT_LINKS}")

        try:
            print(html_replies)
        except Exception:
            pass

        wp_patch_if_possible(html_replies, html_links)

    except Exception as e:
        log(f"FATAL: {e}\n{traceback.format_exc()}")
        open(OUT_REPLIES, "w", encoding="utf-8").write(f"<!-- error: {esc(str(e))} -->\n")
        open(OUT_LINKS,   "w", encoding="utf-8").write("<!-- no links -->\n")

if __name__ == "__main__":
    main()
