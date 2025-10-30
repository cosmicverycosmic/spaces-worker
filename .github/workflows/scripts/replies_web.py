#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
replies_web.py — robust crawler+API replies builder (v3.1)

- Keeps replies; threads replies-to-replies; outputs nested HTML.
- Orders by posted time; preserves parent->children structure.
- Expands t.co; builds media blocks with *no crop*, height=400px, smart grids:
  n=1 (single), n=2 (side-by-side), n=3 (1 top, 2 below), n=4 (2×2).
- Robust OpenGraph/Twitter Card fetch (cached) for external links.
- Quote tweet embeds; metrics; timestamp link; sidebar links summary.
- WP patch optional.

Env:
  ARTDIR, BASE, PURPLE_TWEET_URL, REPLIES_JSONL
  TWITTER_AUTHORIZATION, TWITTER_AUTH_TOKEN, TWITTER_CSRF_TOKEN
  WP_BASE_URL/WP_URL, WP_USER, WP_APP_PASSWORD, WP_POST_ID
  OG_FETCH=1, OG_TIMEOUT=5, OG_MAXKB=128
"""

import os, re, json, html, time, traceback, ssl
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urljoin, urlparse
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

AUTH        = (os.environ.get("TWITTER_AUTHORIZATION", "") or "").strip()
AUTH_COOKIE = (os.environ.get("TWITTER_AUTH_TOKEN", "") or "").strip()
CSRF        = (os.environ.get("TWITTER_CSRF_TOKEN", "") or "").strip()

MAX_PAGES   = int(os.environ.get("REPLIES_MAX_PAGES", "40") or "40")
SLEEP_SEC   = float(os.environ.get("REPLIES_SLEEP", "0.7") or "0.7")
SAVE_JSON   = (os.environ.get("REPLIES_SAVE_JSON", "1") or "1").lower() not in ("0","false")

WP_BASE = (os.environ.get("WP_BASE_URL", "") or os.environ.get("WP_URL", "") or "").rstrip("/")
WP_USER = os.environ.get("WP_USER", "") or ""
WP_PW   = os.environ.get("WP_APP_PASSWORD", "") or ""
WP_PID  = (os.environ.get("WP_POST_ID", "") or os.environ.get("POST_ID", "") or "").strip()

# ---- OG fetch env
OG_FETCH   = (os.environ.get("OG_FETCH", "1") or "1").lower() not in ("0","false")
OG_TIMEOUT = float(os.environ.get("OG_TIMEOUT", "5.0") or "5.0")
OG_MAXKB   = int(os.environ.get("OG_MAXKB", "128") or "128")
OG_CACHE   = os.path.join(ARTDIR, f"{BASE}_ogcache.json")

# ---------- Constants ----------
BASE_X = "https://x.com"
CONVO_URL = f"{BASE_X}/i/api/2/timeline/conversation/{{tid}}.json"
SEARCH_URL = f"{BASE_X}/i/api/2/search/adaptive.json"

# ---------- FS helpers ----------
os.makedirs(ARTDIR, exist_ok=True)
os.makedirs(DBG_DIR, exist_ok=True)

def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    with open(LOG_PATH, "a", encoding="utf-8") as lf:
        lf.write(f"[{ts}Z] {msg}\n")

def save_debug_blob(kind: str, idx: int, raw: str) -> None:
    if not SAVE_JSON:
        return
    p = f"{DBG_PREFIX}_{kind}{idx:02d}.json"
    try:
        with open(p, "w", encoding="utf-8") as f:
            f.write(raw)
        log(f"Saved debug {kind} page {idx} to {p}")
    except Exception as e:
        log(f"Failed to save debug {kind} page {idx}: {e}")

# ---------- String & time utils ----------
EMOJI_RE = re.compile("["+
    "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF"+
    "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF"+
    "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF"+
    "]+", re.UNICODE)
ONLY_PUNCT_SPACE = re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")

def is_emoji_only(s: str) -> bool:
    if not s or not s.strip():
        return False
    t = ONLY_PUNCT_SPACE.sub("", s)
    t = EMOJI_RE.sub("", t)
    return len(t.strip()) == 0

def esc(s: Optional[str]) -> str:
    return html.escape(s or "", quote=True)

def iso_from_twitter(created_at: str) -> str:
    if not created_at:
        return ""
    try:
        dt = datetime.strptime(created_at, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc)
        return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00","Z")
    except Exception:
        return created_at

def fmt_when(iso: str) -> str:
    try:
        if not iso:
            return ""
        d = datetime.fromisoformat(iso.replace("Z","+00:00")).astimezone(timezone.utc)
        return d.strftime("%b %d, %Y, %H:%M UTC")
    except Exception:
        return iso

def fmt_metric(n: Optional[int]) -> str:
    if n is None:
        return ""
    try:
        n = int(n)
    except Exception:
        return ""
    if n < 1000:
        return str(n)
    if n < 1_000_000:
        return f"{round(n/100)/10}K"
    if n < 1_000_000_000:
        return f"{round(n/100_000)/10}M"
    return f"{round(n/100_000_000)/10}B"

# ---------- OG fetch/cache ----------
def _load_og_cache() -> dict:
    try:
        if os.path.isfile(OG_CACHE):
            with open(OG_CACHE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        log(f"OG cache load failed: {e}")
    return {}

def _save_og_cache(cache: dict) -> None:
    try:
        with open(OG_CACHE, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False)
    except Exception as e:
        log(f"OG cache save failed: {e}")

OG_CACHE_MAP = _load_og_cache()
META_RE = re.compile(
    rb'<meta\s+[^>]*?(?:property|name)\s*=\s*["\']([^"\']+)["\'][^>]*?content\s*=\s*["\']([^"\']+)["\'][^>]*?>',
    re.IGNORECASE
)

def _parse_og_from_html(head_bytes: bytes, base_url: str) -> dict:
    out = {"title": "", "description": "", "image": ""}
    try:
        for m in META_RE.finditer(head_bytes or b""):
            key = m.group(1).decode("utf-8", "ignore").strip().lower()
            val = m.group(2).decode("utf-8", "ignore").strip()
            if key in ("og:title", "twitter:title") and not out["title"]:
                out["title"] = val
            elif key in ("og:description", "twitter:description") and not out["description"]:
                out["description"] = val
            elif key in ("og:image", "twitter:image", "twitter:image:src") and not out["image"]:
                out["image"] = val
    except Exception:
        pass
    if out["image"]:
        out["image"] = urljoin(base_url, out["image"])
    return out

def _fetch_og(url: str) -> dict:
    try:
        key = re.sub(r"#.*$", "", url)
        if key in OG_CACHE_MAP:
            return OG_CACHE_MAP[key]
        if not OG_FETCH:
            return {}
        scheme = urlparse(url).scheme.lower()
        if scheme not in ("http", "https"):
            return {}
        ctx = ssl.create_default_context()
        req = Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.1",
            "Accept-Language": "en-US,en;q=0.9"
        })
        with urlopen(req, timeout=OG_TIMEOUT, context=ctx) as r:
            ctype = (r.headers.get("Content-Type") or "").lower()
            if "text/html" not in ctype:
                OG_CACHE_MAP[key] = {}
                return {}
            head = r.read(OG_MAXKB * 1024)
            res = _parse_og_from_html(head, r.geturl())
            cleaned = {
                "title": (res.get("title") or "").strip(),
                "description": (res.get("description") or "").strip(),
                "image": (res.get("image") or "").strip(),
            }
            OG_CACHE_MAP[key] = cleaned
            return cleaned
    except Exception as e:
        log(f"OG fetch failed for {url}: {e}")
        return {}

# ---------- Link expansion ----------
def expand_text_with_entities(text: str, entities: Dict[str, Any]) -> Tuple[str, List[Dict[str,Any]], List[Dict[str,Any]]]:
    s = esc(text or "")
    urls = list((entities or {}).get("urls") or [])
    media = list((entities or {}).get("media") or [])

    rep = []
    for u in urls:
        short = u.get("url") or ""
        exp = u.get("expanded_url") or short
        disp = u.get("display_url") or (exp.replace("https://",""))
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

def is_status_url(u: str) -> bool:
    return bool(re.search(r"https?://(x|twitter)\.com/[^/]+/status/\d+", u or ""))

# ---------- Media rendering (no crop, height=400, smart layouts) ----------
def best_mp4(variants: List[Dict[str,Any]]) -> Optional[str]:
    best = None
    best_br = -1
    for v in variants or []:
        if v.get("content_type") == "video/mp4" and v.get("url"):
            br = int(v.get("bitrate") or 0)
            if br > best_br:
                best_br = br; best = v["url"]
    return best

def render_media(media_list: List[Dict[str,Any]], extended: Dict[str,Any]) -> str:
    if not media_list and not extended:
        return ""
    ext_media = []
    if isinstance(extended, dict):
        ext_media = list((extended.get("media") or []))
    by_id = {}
    for m in media_list or []:
        mid = str(m.get("id_str") or m.get("id") or "")
        by_id[mid] = m
    if ext_media:
        for m in ext_media:
            mid = str(m.get("id_str") or m.get("id") or "")
            by_id[mid] = {**by_id.get(mid, {}), **m}

    items = []
    for m in by_id.values():
        t = (m.get("type") or "").lower()
        if t == "photo" and m.get("media_url_https"):
            items.append(f'<div class="cell"><img src="{esc(m["media_url_https"])}" alt=""></div>')
        elif t in ("video","animated_gif"):
            vi = (m.get("video_info") or {})
            src = best_mp4(vi.get("variants") or [])
            poster = m.get("media_url_https") or ""
            if src:
                items.append(
                    '<div class="cell"><video controls playsinline preload="metadata" %s>'
                    '  <source src="%s" type="video/mp4">'
                    '  Your browser does not support the video tag.'
                    '</video></div>' % (f'poster="{esc(poster)}"' if poster else "", esc(src))
                )

    if not items:
        return ""
    n = len(items)
    cls = "n1" if n == 1 else "n2" if n == 2 else "n3" if n == 3 else "n4" if n == 4 else "n4"
    # index classes for 3-layout
    items = [re.sub(r'^<div class="cell"', f'<div class="cell idx-{i+1}"', it) if n==3 else it for i,it in enumerate(items)]
    return f'<div class="ss3k-media {cls}">' + "".join(items[:4]) + "</div>"

# ---------- OG cards ----------
def render_link_card(u: Dict[str,Any]) -> str:
    exp = u.get("unwound_url") or u.get("expanded_url") or u.get("url") or ""
    ttl = u.get("title") or u.get("display_url") or exp
    desc = u.get("description") or ""
    thumb = None
    if isinstance(u.get("images"), list) and u["images"]:
        thumb = u["images"][0].get("url") or None

    # Fallback to OG fetch when missing
    if (not thumb or not ttl or ttl == exp):
        og = _fetch_og(exp)
        if og:
            ttl = og.get("title") or ttl
            if not desc:
                desc = og.get("description") or ""
            if not thumb:
                thumb = og.get("image") or None

    host = re.sub(r"^https?://", "", exp).split("/")[0]
    thumb_html = f'<img class="thumb" src="{esc(thumb)}" alt="">' if thumb else ""
    desc_html  = f'<div class="desc">{esc(desc)}</div>' if desc else ""

    return (
        '<div class="ss3k-card ext">'
        f'  <a class="wrap" href="{esc(exp)}" target="_blank" rel="noopener">'
        f'    {thumb_html}'
        f'    <div class="meta"><div class="ttl">{esc(ttl)}</div><div class="dom">{esc(host)}</div>{desc_html}</div>'
        '  </a>'
        '</div>'
    )

# ---------- API fallback ----------
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
        if not data:
            break
        parse_global_objects(data, tweets, users)
        nxt = find_bottom_cursor(data)
        if not nxt or nxt == cursor:
            break
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
            if not data:
                break
            parse_global_objects(data, tweets, users)
            nxt = find_bottom_cursor(data)
            if not nxt or nxt == cursor:
                break
            cursor = nxt
            time.sleep(SLEEP_SEC)
    return tweets, users

# ---------- Reply model ----------
class Reply:
    __slots__ = (
        "id","user_id","name","handle","verified","avatar",
        "text","html","created_iso","metrics","entities","extended_entities",
        "in_reply_to","quoted_url","parent_id","root_id","media_html","link_cards_html"
    )
    def __init__(self):
        self.id = ""; self.user_id = ""; self.name = "User"; self.handle = ""; self.verified=False
        self.avatar = ""; self.text=""; self.html=""; self.created_iso=""
        self.metrics = {"replies":None, "reposts":None, "likes":None, "quotes":None, "bookmarks":None, "views":None}
        self.entities = {}; self.extended_entities = {}
        self.in_reply_to = None; self.quoted_url = None; self.parent_id = None; self.root_id = None
        self.media_html = ""; self.link_cards_html = ""

def from_tweet_obj(t: Dict[str,Any], users: Dict[str,Any]) -> Optional[Reply]:
    tid = str(t.get("id_str") or t.get("id") or "").strip()
    if not tid:
        return None

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

    # Quote-tweet
    qt = None
    for uent in url_entities:
        ex = uent.get("expanded_url") or uent.get("unwound_url") or uent.get("url") or ""
        if is_status_url(ex):
            qt = ex; break
    r.quoted_url = qt

    r.media_html = render_media(media_entities, r.extended_entities)

    # External cards for non-status links
    cards = []
    seen = set()
    for uent in url_entities:
        ex = uent.get("unwound_url") or uent.get("expanded_url") or uent.get("url") or ""
        if not ex or is_status_url(ex):
            continue
        if ex in seen: continue
        seen.add(ex)
        cards.append(render_link_card(uent))
    r.link_cards_html = "".join(cards)

    r.text = text
    r.html = html_text

    ca = t.get("created_at") or ""
    r.created_iso = iso_from_twitter(ca) if ca else (t.get("created_at_iso") or t.get("created_at_utc") or "")
    pm = t.get("public_metrics") or {}
    r.metrics["replies"] = pm.get("reply_count") if pm else (t.get("reply_count"))
    r.metrics["reposts"] = pm.get("retweet_count") if pm else (t.get("retweet_count"))
    r.metrics["likes"]   = pm.get("like_count")    if pm else (t.get("favorite_count"))
    r.metrics["quotes"]  = pm.get("quote_count")    if pm else (t.get("quote_count"))
    r.metrics["bookmarks"] = t.get("bookmark_count")
    views = t.get("views") or t.get("ext_views") or {}
    if isinstance(views, dict):
        r.metrics["views"] = views.get("count")

    r.in_reply_to = str(t.get("in_reply_to_status_id_str") or t.get("in_reply_to_status_id") or "") or None
    for ref in (t.get("referenced_tweets") or []):
        if ref.get("type") == "replied_to" and ref.get("id"):
            r.in_reply_to = str(ref["id"]) or r.in_reply_to
        if ref.get("type") == "quoted" and ref.get("id"):
            if not r.quoted_url:
                r.quoted_url = f"https://x.com/i/web/status/{ref['id']}"

    return r

# ---------- Crawler JSONL ----------
def parse_jsonl(path: str) -> Tuple[List[Reply], Dict[str,Any]]:
    replies: List[Reply] = []
    users_index: Dict[str,Any] = {}
    if not path or not os.path.isfile(path):
        return replies, users_index

    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = (line or "").strip()
            if not line:
                continue
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

            if not isinstance(cand, dict):
                continue

            text = cand.get("full_text") or cand.get("text") or ""
            if text and is_emoji_only(text):
                continue

            uobj = cand.get("user")
            if isinstance(uobj, dict):
                uid = str(uobj.get("id_str") or uobj.get("id") or "")
                if uid:
                    users_index.setdefault(uid, uobj)

            r = from_tweet_obj(cand, users_index)
            if r:
                replies.append(r)

    return replies, users_index

# ---------- Build thread + HTML ----------
def build_thread(replies: List[Reply], root_id: Optional[str]) -> Tuple[List[Reply], Dict[str,List[Reply]]]:
    by_id = {r.id: r for r in replies if r.id}

    for r in replies:
        pid = r.in_reply_to
        if pid and pid in by_id:
            r.parent_id = pid
        r.root_id = root_id or r.root_id

    children: Dict[str,List[Reply]] = defaultdict(list)
    roots: List[Reply] = []
    for r in replies:
        if r.parent_id and r.parent_id in by_id:
            children[r.parent_id].append(r)
        else:
            roots.append(r)

    # Sort everything by created time (posted order)
    def by_time(x: Reply): return x.created_iso or ""
    roots.sort(key=by_time)
    for k in children:
        children[k].sort(key=by_time)

    # Flatten order: DFS but respecting time for each siblings set
    ordered: List[Reply] = []
    def visit(node: Reply):
        ordered.append(node)
        for ch in children.get(node.id, []):
            visit(ch)
    for rt in roots:
        visit(rt)

    return ordered, children

def render_quote_embed(u: str) -> str:
    safe = esc(u)
    return '<blockquote class="twitter-tweet" data-dnt="true"><a href="%s"></a></blockquote>' % safe

def render_reply_block(r: Reply, children: Dict[str,List[Reply]]) -> str:
    url = f"{BASE_X}/{esc(r.handle)}/status/{esc(r.id)}" if r.handle else f"{BASE_X}/i/web/status/{esc(r.id)}"
    when = fmt_when(r.created_iso)

    av = f'<div class="avatar-50">{f"<img src=\"{esc(r.avatar)}\" alt=\"\">" if r.avatar else ""}</div>'
    head = (
        '<div class="head">'
        f'  <span class="disp">{esc(r.name)}</span>'
        f'  <span class="handle"> @{esc(r.handle or "user")}</span>'
        f'  {"<span class=\\"badge\\"></span>" if r.verified else ""}'
        '</div>'
    )
    body = f'<div class="body">{r.html}</div>'
    embed = f'<div class="qt">{render_quote_embed(r.quoted_url)}</div>' if r.quoted_url else ""
    media = r.media_html
    cards = r.link_cards_html

    metrics = []
    if r.metrics.get("replies") is not None:  metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M14 9V5l-7 7 7 7v-4h1c4 0 7 1 9 4-1-7-5-10-10-10h-1z"/></svg><span>{fmt_metric(r.metrics["replies"])}</span></span>')
    if r.metrics.get("reposts") is not None:  metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M17 1l4 4-4 4V7H7a3 3 0 00-3 3v2H2V9a5 5 0 015-5h10V1zm-6 16H5l4-4v2h10a3 3 0 003-3v-2h2v3a5 5 0 01-5 5H11v2l-4-4 4-4v3z"/></svg><span>{fmt_metric(r.metrics["reposts"])}</span></span>')
    if r.metrics.get("likes")   is not None:  metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M12 21s-7-4.4-9-8.6C1.1 9.6 3 7 5.9 7c1.9 0 3.1 1 4.1 2 1-1 2.2-2 4.1-2 2.9 0 4.8 2.6 2.9 5.4C19 16.6 12 21 12 21z"/></svg><span>{fmt_metric(r.metrics["likes"])}</span></span>')
    if r.metrics.get("quotes")  is not None:  metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M6 2h12a1 1 0 011 1v19l-7-4-7 4V3a1 1 0 011-1z"/></svg><span>{fmt_metric(r.metrics["quotes"])}</span></span>')
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
        "data-root": r.root_id or "",
        "data-replies": str(r.metrics.get("replies") or ""),
        "data-reposts": str(r.metrics.get("reposts") or ""),
        "data-likes":   str(r.metrics.get("likes") or ""),
        "data-quotes":  str(r.metrics.get("quotes") or ""),
    }
    attr_s = " ".join(f'{k}="{esc(v)}"' for k,v in attrs.items() if v)

    # Render children (if any) nested
    kid_html = ""
    kids = children.get(r.id, [])
    if kids:
        kid_html = '<div class="ss3k-children">' + "".join(render_reply_block(k, children) for k in kids) + '</div>'

    return (
        f'<div {attr_s}>'
        f'  {av}'
        f'  <div>'
        f'    {head}'
        f'    {body}'
        f'    {embed}'
        f'    {media}'
        f'    {cards}'
        f'    <div class="tweetbar-wrap">{bar}</div>'
        f'    {linkx}'
        f'    {kid_html}'
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
                if root_id and conv != str(root_id):
                    continue
                if str(t.get("id_str") or t.get("id")) == str(root_id):
                    continue
                if t.get("retweeted_status_id") or t.get("retweeted_status_id_str"):
                    continue
                r = from_tweet_obj(t, users)
                if r:
                    replies.append(r)

        # Dedup
        uniq = {}
        for r in replies:
            uniq[r.id] = r
        replies = list(uniq.values())

        if root_id:
            for r in replies:
                r.root_id = root_id

        ordered, children = build_thread(replies, root_id)

        # Emit nested HTML: render only roots; children are included recursively
        roots_only = [r for r in ordered if not r.parent_id]
        html_replies = "".join(render_reply_block(r, children) for r in roots_only)
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

        _save_og_cache(OG_CACHE_MAP)
        wp_patch_if_possible(html_replies, html_links)

    except Exception as e:
        log(f"FATAL: {e}\n{traceback.format_exc()}")
        open(OUT_REPLIES, "w", encoding="utf-8").write(f"<!-- error: {esc(str(e))} -->\n")
        open(OUT_LINKS,   "w", encoding="utf-8").write("<!-- no links -->\n")

if __name__ == "__main__":
    main()
