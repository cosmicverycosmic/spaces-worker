#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
replies_web.py — replies + media + OG cards (full-text, multi-embed)  v3.3

Changes in this build:
- Full-text preservation during t.co expansion (no escape/index mismatch).
- Detects *all* tweet/status links → renders multiple <blockquote class="twitter-tweet">.
- OpenGraph fetch (title/description/image) with small cache and hard caps.
- Media block outputs an inline grid style with count-aware layout; each media is height 400px,
  variable width, object-fit: contain (no cropping).
- Chronological ordering while retaining data-parent for threading.

Env (same as before):
  ARTDIR, BASE, PURPLE_TWEET_URL, REPLIES_JSONL
  TWITTER_AUTHORIZATION, TWITTER_AUTH_TOKEN, TWITTER_CSRF_TOKEN (optional, API fallback)
  WP_BASE_URL, WP_USER, WP_APP_PASSWORD, WP_POST_ID (optional, patch)
"""

import os, re, json, html, time, traceback
from datetime import datetime, timezone
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlparse
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

os.makedirs(ARTDIR, exist_ok=True)
os.makedirs(DBG_DIR, exist_ok=True)

# X auth (optional — API fallback)
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

# ---------- X endpoints ----------
BASE_X = "https://x.com"
CONVO_URL = f"{BASE_X}/i/api/2/timeline/conversation/{{tid}}.json"
SEARCH_URL = f"{BASE_X}/i/api/2/search/adaptive.json"

# ---------- OG fetch limits ----------
MAX_OG_FETCH = int(os.environ.get("MAX_OG_FETCH", "60"))   # total per run cap
OG_TIMEOUT   = float(os.environ.get("OG_TIMEOUT", "8"))    # sec
OG_CACHE: Dict[str, Dict[str, str]] = {}
OG_FETCHED = 0

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

# ---------- utils ----------
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

def fmt_metric(n: Optional[int]) -> str:
    if n is None:
        return ""
    try:
        n = int(n)
    except Exception:
        return ""
    if n < 1000: return str(n)
    if n < 1_000_000: return f"{round(n/100)/10}K"
    if n < 1_000_000_000: return f"{round(n/100_000)/10}M"
    return f"{round(n/100_000_000)/10}B"

def host_of(u: str) -> str:
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return ""

# ---------- OG fetch ----------
META_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.I|re.S)
META_TAG_RE   = re.compile(
    r'<meta\s+(?:property|name)\s*=\s*"(og:[^"]+|twitter:[^"]+)"\s+content\s*=\s*"([^"]*)"', re.I)

def fetch_og(url: str) -> Dict[str, str]:
    """Small, cached OG fetcher (title/description/image)."""
    global OG_FETCHED
    if not url:
        return {}
    if url in OG_CACHE:
        return OG_CACHE[url]
    if OG_FETCHED >= MAX_OG_FETCH:
        return {}

    try:
        OG_FETCHED += 1
        # GET (HEAD often useless for HTML)
        req = Request(url, headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "text/html,application/xhtml+xml"
        })
        with urlopen(req, timeout=OG_TIMEOUT) as r:
            raw = r.read(512*1024)  # cap to 512KB
            text = raw.decode("utf-8", "ignore")
    except Exception as e:
        log(f"OG fetch fail {url}: {e}")
        OG_CACHE[url] = {}
        return OG_CACHE[url]

    og = {}
    # <meta property="og:..."> and twitter:...
    for prop, val in META_TAG_RE.findall(text):
        p = prop.lower()
        if p in ("og:title","twitter:title"):
            og.setdefault("title", val.strip())
        elif p in ("og:description","twitter:description","description"):
            og.setdefault("description", val.strip())
        elif p in ("og:image","twitter:image","og:image:url"):
            og.setdefault("image", val.strip())

    if "title" not in og:
        m = META_TITLE_RE.search(text)
        if m:
            og["title"] = re.sub(r"\s+", " ", m.group(1)).strip()

    OG_CACHE[url] = og
    return og

# ---------- text expansion (full-text, safe anchors) ----------
def is_status_url(u: str) -> bool:
    return bool(re.search(r"https?://(?:x|twitter)\.com/[^/]+/status/\d+", u or "", re.I))

def build_anchor(u: Dict[str,Any]) -> Tuple[str, bool]:
    short = u.get("url") or ""
    exp   = u.get("unwound_url") or u.get("expanded_url") or short
    disp  = u.get("display_url") or exp.replace("https://","").replace("http://","")
    return (f'<a href="{esc(exp)}" target="_blank" rel="noopener">{esc(disp)}</a>', is_status_url(exp))

def expand_text_full(text: str, entities: Dict[str,Any]) -> Tuple[str, List[str], List[str]]:
    """
    Returns HTML string where:
    - replacements using indices are applied on the *raw* text,
    - non-index fallbacks handled afterward,
    - media t.co placeholders removed,
    - mentions and hashtags linked (outside anchors).
    Also returns: list(status_urls), list(nonstatus_urls).
    """
    raw = text or ""
    urls = list((entities or {}).get("urls") or [])
    media = list((entities or {}).get("media") or [])
    media_short = {m.get("url") for m in media if m.get("url")}

    # Build replacement plan with indices
    repl = []
    status_urls, normal_urls = [], []
    for u in urls:
        start, end = None, None
        if isinstance(u.get("indices"), list) and len(u["indices"]) == 2:
            start, end = int(u["indices"][0]), int(u["indices"][1])
        anchor, is_status = build_anchor(u)
        if is_status: status_urls.append(u.get("unwound_url") or u.get("expanded_url") or u.get("url") or "")
        else:         normal_urls.append(u.get("unwound_url") or u.get("expanded_url") or u.get("url") or "")
        repl.append((start, end, u.get("url") or "", anchor))

    # Replace (right-to-left) on raw text, building an HTML stream (escaped chunks + raw anchors)
    parts: List[str] = []
    if any(r[0] is not None for r in repl):
        cur = 0
        for start, end, short, anchor in sorted([r for r in repl if r[0] is not None],
                                                key=lambda x: x[0], reverse=False):
            start = max(0, min(len(raw), start))
            end   = max(start, min(len(raw), end))
            # segment before
            seg = raw[cur:start]
            parts.append(esc(seg))
            # remove media short URLs entirely
            if short in media_short:
                parts.append("")  # skip
            else:
                parts.append(anchor)
            cur = end
        parts.append(esc(raw[cur:]))
        html_text = "".join(parts)
    else:
        html_text = esc(raw)

    # Non-index fallbacks: replace visible short URLs that remain
    for _, _, short, anchor in repl:
        if short and short not in media_short:
            html_text = html_text.replace(esc(short), anchor)

    # Mentions / hashtags (avoid inside anchors)
    def linkify(chunk: str) -> str:
        chunk = re.sub(r'(?<!["\w])@([A-Za-z0-9_]{1,15})',
                       r'<a href="https://x.com/\1" target="_blank" rel="noopener">@\1</a>', chunk)
        chunk = re.sub(r'(?<!["\w])#([A-Za-z0-9_]+)',
                       r'<a href="https://x.com/hashtag/\1" target="_blank" rel="noopener">#\1</a>', chunk)
        return chunk

    pieces = re.split(r'(<a\s+[^>]*>.*?</a>)', html_text, flags=re.I|re.S)
    for i in range(0, len(pieces), 2):  # only non-anchor segments
        pieces[i] = linkify(pieces[i])
    html_text = "".join(pieces)

    return html_text, status_urls, normal_urls

# ---------- media rendering (400px high, no crop; count-aware grid) ----------
def best_mp4(variants: List[Dict[str,Any]]) -> Optional[str]:
    best, best_br = None, -1
    for v in variants or []:
        if v.get("content_type") == "video/mp4" and v.get("url"):
            br = int(v.get("bitrate") or 0)
            if br > best_br:
                best_br, best = br, v["url"]
    return best

def render_media(media_list: List[Dict[str,Any]], extended: Dict[str,Any]) -> str:
    # Merge base + extended media
    items = []
    base = media_list or []
    extm = (extended or {}).get("media") or []

    by_id: Dict[str, Dict[str,Any]] = {}
    for m in base:
        by_id[str(m.get("id_str") or m.get("id") or "")] = m
    for m in extm:
        mid = str(m.get("id_str") or m.get("id") or "")
        by_id[mid] = {**by_id.get(mid, {}), **m}

    ordered = list(by_id.values())
    if not ordered:
        return ""

    # Build media tags (no crop, contain at 400px height)
    tags: List[str] = []
    for m in ordered:
        t = (m.get("type") or "").lower()
        if t == "photo" and m.get("media_url_https"):
            tags.append(
                f'<img class="ph" loading="lazy" src="{esc(m["media_url_https"])}" '
                f'style="height:400px;width:100%;object-fit:contain;background:#000;border:1px solid #e6eaf2;border-radius:10px;" alt="">'
            )
        elif t in ("video","animated_gif"):
            vi = (m.get("video_info") or {})
            src = best_mp4(vi.get("variants") or [])
            poster = m.get("media_url_https") or ""
            if src:
                tags.append(
                    '<video class="vid" controls playsinline preload="metadata" '
                    f'style="height:400px;width:100%;object-fit:contain;background:#000;border:1px solid #e6eaf2;border-radius:10px;" '
                    f'{"poster=\"%s\"" % esc(poster) if poster else ""}>'
                    f'<source src="{esc(src)}" type="video/mp4"></video>'
                )

    if not tags:
        return ""

    n = len(tags)
    # Container inline grid style according to count
    if n == 1:
        style = "display:grid;grid-template-columns:1fr;gap:8px;"
    elif n == 2:
        style = "display:grid;grid-template-columns:1fr 1fr;gap:8px;"
    else:
        # 3+: top wide then two; 4+ behaves as 2x2 and wraps
        style = "display:grid;grid-template-columns:1fr 1fr;gap:8px;"
    html_items = []
    if n == 3:
        html_items.append(f'<div style="grid-column:1/-1;">{tags[0]}</div>')
        html_items.append(f'<div>{tags[1]}</div>')
        html_items.append(f'<div>{tags[2]}</div>')
    else:
        html_items = [f'<div>{t}</div>' for t in tags]

    return f'<div class="ss3k-media" style="{style}">' + "".join(html_items) + "</div>"

# ---------- link cards (OpenGraph) ----------
def render_link_card(url_entity: Dict[str,Any]) -> str:
    exp = url_entity.get("unwound_url") or url_entity.get("expanded_url") or url_entity.get("url") or ""
    if not exp:
        return ""
    if is_status_url(exp):
        # handled as embedded tweet elsewhere
        return ""

    og = fetch_og(exp)
    title = og.get("title") or url_entity.get("title") or url_entity.get("display_url") or exp
    desc  = og.get("description") or ""
    img   = og.get("image")
    host  = host_of(exp) or ""

    thumb = f'<img class="thumb" src="{esc(img)}" alt="" style="width:160px;height:100%;object-fit:cover;display:block">' if img else ""
    meta  = (
        f'<div class="meta" style="padding:10px 12px;display:flex;flex-direction:column;justify-content:center;min-height:72px">'
        f'  <div class="ttl" style="font-weight:600;line-height:1.3;margin:0 0 4px 0;color:#0f172a">{esc(title)}</div>'
        f'  <div class="dom" style="font-size:12px;color:#6b7280">{esc(host)}</div>'
        f'  {f"<div class=\\"desc\\" style=\\"color:#475569;margin-top:4px;font-size:13px;\\">{esc(desc[:180])}{"…" if len(desc)>180 else ""}</div>" if desc else ""}'
        f'</div>'
    )
    return (
        '<div class="ss3k-card ext" style="margin-top:8px;border:1px solid #e6eaf2;'
        'border-radius:10px;background:#f6f8fc;overflow:hidden">'
        f'  <a class="wrap" href="{esc(exp)}" target="_blank" rel="noopener" '
        '     style="display:flex;align-items:stretch;gap:0;text-decoration:none;color:inherit">'
        f'    {thumb}'
        f'    {meta}'
        '  </a>'
        '</div>'
    )

# ---------- API fallback (unchanged essentials) ----------
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

# ---------- normalization ----------
class Reply:
    __slots__ = (
        "id","user_id","name","handle","verified","avatar",
        "text","html","created_iso","metrics","entities","extended_entities",
        "in_reply_to","parent_id","root_id","embeds_html","media_html","link_cards_html"
    )
    def __init__(self):
        self.id = ""; self.user_id = ""; self.name = "User"; self.handle=""; self.verified=False
        self.avatar=""; self.text=""; self.html=""; self.created_iso=""
        self.metrics={"replies":None,"reposts":None,"likes":None,"quotes":None,"bookmarks":None,"views":None}
        self.entities={}; self.extended_entities={}
        self.in_reply_to=None; self.parent_id=None; self.root_id=None
        self.embeds_html=""; self.media_html=""; self.link_cards_html=""

def from_tweet_obj(t: Dict[str,Any], users: Dict[str,Any]) -> Optional[Reply]:
    tid = str(t.get("id_str") or t.get("id") or "").strip()
    if not tid:
        return None
    r = Reply(); r.id = tid

    uid = str(t.get("user_id_str") or (t.get("user") or {}).get("id_str") or t.get("user_id") or "")
    u = users.get(uid) if uid and isinstance(users, dict) else (t.get("user") or {})
    r.user_id = uid or str(u.get("id_str") or u.get("id") or "")
    r.name = u.get("name") or (t.get("user") or {}).get("name") or "User"
    r.handle = u.get("screen_name") or (t.get("user") or {}).get("screen_name") or ""
    r.verified = bool(u.get("verified") or u.get("is_blue_verified") or u.get("ext_is_blue_verified"))
    r.avatar = (u.get("profile_image_url_https") or u.get("profile_image_url") or "").replace("_normal.", "_bigger.")

    r.entities = (t.get("entities") or {})
    r.extended_entities = (t.get("extended_entities") or {})

    text = t.get("full_text") or t.get("text") or ""
    html_text, status_urls, normal_urls = expand_text_full(text, r.entities)
    r.html = html_text
    r.text = text

    # Embeds for *every* status URL
    if status_urls:
        embeds = []
        for su in status_urls:
            safe = esc(su)
            embeds.append(f'<blockquote class="twitter-tweet" data-dnt="true"><a href="{safe}"></a></blockquote>')
        r.embeds_html = '<div class="qt">' + "".join(embeds) + "</div>"

    # Media
    r.media_html = render_media(list((r.entities or {}).get("media") or []), r.extended_entities)

    # External cards (non-status URLs)
    cards = []
    seen = set()
    for uobj in (r.entities or {}).get("urls") or []:
        exp = uobj.get("unwound_url") or uobj.get("expanded_url") or uobj.get("url") or ""
        if not exp or is_status_url(exp):  # statuses handled as embeds
            continue
        if exp in seen: continue
        seen.add(exp)
        cards.append(render_link_card(uobj))
    r.link_cards_html = "".join(cards)

    # Timestamps / metrics
    ca = t.get("created_at") or ""
    r.created_iso = iso_from_twitter(ca) if ca else (t.get("created_at_iso") or t.get("created_at_utc") or "")
    pm = t.get("public_metrics") or {}
    r.metrics["replies"] = pm.get("reply_count") if pm else t.get("reply_count")
    r.metrics["reposts"] = pm.get("retweet_count") if pm else t.get("retweet_count")
    r.metrics["likes"]   = pm.get("like_count") if pm else t.get("favorite_count")
    r.metrics["quotes"]  = pm.get("quote_count") if pm else t.get("quote_count")
    r.metrics["bookmarks"] = t.get("bookmark_count")

    views = t.get("views") or t.get("ext_views") or {}
    if isinstance(views, dict):
        r.metrics["views"] = views.get("count")

    # Threading
    r.in_reply_to = str(t.get("in_reply_to_status_id_str") or t.get("in_reply_to_status_id") or "") or None
    for ref in (t.get("referenced_tweets") or []):
        if ref.get("type") == "replied_to" and ref.get("id"):
            r.in_reply_to = str(ref["id"]) or r.in_reply_to

    return r

# ---------- ingest JSONL ----------
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

# ---------- ordering / HTML ----------
def render_reply(r: Reply) -> str:
    url = f"{BASE_X}/{esc(r.handle)}/status/{esc(r.id)}" if r.handle else f"{BASE_X}/i/web/status/{esc(r.id)}"
    when = r.created_iso
    # header
    av = f'<div class="avatar-50">{f"<img src=\"{esc(r.avatar)}\" alt=\"\">" if r.avatar else ""}</div>'
    head = (
        '<div class="head">'
        f'  <span class="disp">{esc(r.name)}</span>'
        f'  <span class="handle"> @{esc(r.handle or "user")}</span>'
        f'  {"<span class=\"badge\"></span>" if r.verified else ""}'
        '</div>'
    )

    body  = f'<div class="body">{r.html}</div>'
    embed = r.embeds_html
    media = r.media_html
    cards = r.link_cards_html

    metrics = []
    if r.metrics.get("replies") is not None:  metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M14 9V5l-7 7 7 7v-4h1c4 0 7 1 9 4-1-7-5-10-10-10h-1z"/></svg><span>{fmt_metric(r.metrics["replies"])}</span></span>')
    if r.metrics.get("reposts") is not None:  metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M17 1l4 4-4 4V7H7a3 3 0 00-3 3v2H2V9a5 5 0 015-5h10V1zm-6 16H5l4-4v2h10a3 3 0 003-3v-2h2v3a5 5 0 01-5 5H11v2l-4-4 4-4v3z"/></svg><span>{fmt_metric(r.metrics["reposts"])}</span></span>')
    if r.metrics.get("likes")   is not None:  metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M12 21s-7-4.4-9-8.6C1.1 9.6 3 7 5.9 7c1.9 0 3.1 1 4.1 2 1-1 2.2-2 4.1-2 2.9 0 4.8 2.6 2.9 5.4C19 16.6 12 21 12 21z"/></svg><span>{fmt_metric(r.metrics["likes"])}</span></span>')
    if r.metrics.get("quotes")  is not None:  metrics.append(f'<span class="metric"><svg viewBox="0 0 24 24"><path d="M6 2h12a1 1 0 011 1v19l-7-4-7 4V3a1 1 0 011-1z"/></svg><span>{fmt_metric(r.metrics["quotes"])}</span></span>')
    bar = (f'<div class="tweetbar">{"".join(metrics)}</div>' if metrics else "")
    linkx = f'<span class="linkx"><a href="{url}" target="_blank" rel="noopener">{esc(_fmt_when_local(when) or "Open on X")}</a></span>'

    attrs = {
        "class": "ss3k-reply",
        "data-id": r.id,
        "data-name": r.name,
        "data-handle": f"@{r.handle}" if r.handle else "",
        "data-verified": "true" if r.verified else "",
        "data-url": url,
        "data-ts": r.created_iso,
        "data-parent": r.in_reply_to or "",
        "data-root": r.root_id or "",
        "data-replies": str(r.metrics.get("replies") or ""),
        "data-reposts": str(r.metrics.get("reposts") or ""),
        "data-likes":   str(r.metrics.get("likes") or ""),
        "data-quotes":  str(r.metrics.get("quotes") or ""),
    }
    attr_s = " ".join(f'{k}="{esc(v)}"' for k,v in attrs.items() if v)

    return (
        f'<div {attr_s}>'
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

def _fmt_when_local(iso: str) -> str:
    try:
        if not iso: return ""
        d = datetime.fromisoformat(iso.replace("Z","+00:00"))
        return d.strftime("%b %d, %Y, %H:%M UTC")
    except Exception:
        return iso

# ---------- links sidebar (OG-titled list, no headers) ----------
def build_links_sidebar(all_replies: List[Reply]) -> str:
    dom2urls: Dict[str, List[str]] = defaultdict(list)
    for r in all_replies:
        for u in (r.entities or {}).get("urls") or []:
            exp = u.get("unwound_url") or u.get("expanded_url") or u.get("url")
            if not exp or is_status_url(exp):
                continue
            dom = host_of(exp) or "links"
            if exp not in dom2urls[dom]:
                dom2urls[dom].append(exp)

    out: List[str] = []
    out.append('<ul class="ss3k-links-list">')
    for dom in sorted(dom2urls.keys()):
        for exp in sorted(dom2urls[dom]):
            og = fetch_og(exp)
            ttl = og.get("title") or exp
            ttl = _truncate(_collapse_ws(ttl), 90)
            out.append(
                f'<li class="ss3k-link" data-dom="{esc(dom)}">'
                f'  <a href="{esc(exp)}" target="_blank" rel="noopener">{esc(ttl)}</a>'
                f'  <span class="dom" style="color:#6b7280;font-size:.9em;margin-left:8px">{esc(dom)}</span>'
                f'</li>'
            )
    out.append('</ul>')
    return "\n".join(out)

def _collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()

def _truncate(s: str, n: int) -> str:
    if len(s) <= n: return s
    cut = s[:n].rstrip()
    if " " in cut: cut = cut.rsplit(" ", 1)[0]
    return cut + "…"

# ---------- main ----------
def main():
    try:
        # 1) Prefer crawler JSONL
        replies, users = parse_jsonl(REPLIES_JSONL)

        # 2) Optional API fallback (when PURPLE + creds)
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
                r = from_tweet_obj(t, users); 
                if r: replies.append(r)

        # 3) Dedup + chronological order (preserve data-parent for UI threading)
        uniq = {r.id: r for r in replies}
        replies = sorted(uniq.values(), key=lambda x: x.created_iso or "")

        if root_id:
            for r in replies: r.root_id = root_id

        # 4) Emit HTML
        html_replies = "\n".join(render_reply(r) for r in replies)
        with open(OUT_REPLIES, "w", encoding="utf-8") as f:
            f.write(html_replies)
        log(f"Wrote replies HTML: {OUT_REPLIES} ({len(replies)} items)")

        html_links = build_links_sidebar(replies)
        with open(OUT_LINKS, "w", encoding="utf-8") as f:
            f.write(html_links)
        log(f"Wrote links HTML: {OUT_LINKS}")

        # stdout (debug)
        try: print(html_replies)
        except Exception: pass

        # 5) Optional: patch WP
        wp_patch_if_possible(html_replies, html_links)

    except Exception as e:
        log(f"FATAL: {e}\n{traceback.format_exc()}")
        open(OUT_REPLIES, "w", encoding="utf-8").write(f"<!-- error: {esc(str(e))} -->\n")
        open(OUT_LINKS,   "w", encoding="utf-8").write("<!-- no links -->\n")

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

if __name__ == "__main__":
    main()
