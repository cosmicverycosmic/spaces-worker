# file: .github/workflows/scripts/replies_web.py
#!/usr/bin/env python3
import os, re, json, html, time, traceback
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from collections import defaultdict

# ---------- ENV & paths ----------
ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
PURPLE = (os.environ.get("PURPLE_TWEET_URL","") or "").strip()

OUT_REPLIES = os.path.join(ARTDIR, f"{BASE}_replies.html")
OUT_LINKS   = os.path.join(ARTDIR, f"{BASE}_links.html")
LOG_PATH    = os.path.join(ARTDIR, f"{BASE}_replies.log")
DBG_DIR     = os.path.join(ARTDIR, "debug")
DBG_PREFIX  = os.path.join(DBG_DIR, f"{BASE}_replies_page")

AUTH        = (os.environ.get("TWITTER_AUTHORIZATION","") or "").strip()   # "Bearer …"
AUTH_COOKIE = (os.environ.get("TWITTER_AUTH_TOKEN","") or "").strip()      # auth_token cookie
CSRF        = (os.environ.get("TWITTER_CSRF_TOKEN","") or "").strip()      # ct0 cookie

MAX_PAGES   = int(os.environ.get("REPLIES_MAX_PAGES","40") or "40")
SLEEP_SEC   = float(os.environ.get("REPLIES_SLEEP","0.7") or "0.7")
SAVE_JSON   = (os.environ.get("REPLIES_SAVE_JSON","1") or "1") not in ("0","false","False")

BASE_X      = "https://x.com"
CONVO_URL   = f"{BASE_X}/i/api/2/timeline/conversation/{{tid}}.json"
SEARCH_URL  = f"{BASE_X}/i/api/2/search/adaptive.json"

# ---------- utils ----------
def ensure_dirs():
    os.makedirs(ARTDIR, exist_ok=True); os.makedirs(DBG_DIR, exist_ok=True)

def mask_token(s: str, keep=6):
    if not s: return ""
    s = str(s); return "*" * max(0, len(s)-keep) + s[-keep:]

def log(msg: str):
    ensure_dirs()
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    with open(LOG_PATH, "a", encoding="utf-8") as lf:
        lf.write(f"[{ts}Z] {msg}\n")

def write_empty(reason=""):
    ensure_dirs()
    open(OUT_REPLIES, "w", encoding="utf-8").write(f"<!-- no replies: {html.escape(reason)} -->\n")
    open(OUT_LINKS,   "w", encoding="utf-8").write(f"<!-- no links: {html.escape(reason)} -->\n")
    log(f"Wrote empty outputs: {reason}")

def safe_env_dump():
    lines = [
        f"ARTDIR={ARTDIR}", f"BASE={BASE}", f"PURPLE_TWEET_URL={(PURPLE or '')}",
        f"AUTH={mask_token(AUTH)}", f"AUTH_COOKIE={mask_token(AUTH_COOKIE)}", f"CSRF={mask_token(CSRF)}",
        f"MAX_PAGES={MAX_PAGES}", f"SLEEP_SEC={SLEEP_SEC}", f"SAVE_JSON={SAVE_JSON}",
    ]
    log("ENV:\n  " + "\n  ".join(lines))

def parse_purple(url):
    m = re.search(r"https?://(?:x|twitter)\.com/([^/]+)/status/(\d+)", url)
    return (m.group(1), m.group(2)) if m else (None, None)

def headers(screen_name, root_id):
    ck = f"auth_token={AUTH_COOKIE}; ct0={CSRF}" if (AUTH_COOKIE and CSRF) else ""
    hdr = {
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
        "Pragma": "no-cache", "Cache-Control": "no-cache",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": f"https://x.com/{screen_name}/status/{root_id}",
        "Origin":  "https://x.com",
    }
    if ck:
        hdr["Cookie"] = ck
        hdr["x-csrf-token"] = CSRF
    if AUTH.startswith("Bearer "):
        hdr["Authorization"] = AUTH
    return hdr

def save_debug_blob(kind, idx, raw):
    if not SAVE_JSON: return
    ensure_dirs()
    path = f"{DBG_PREFIX}_{kind}{idx:02d}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False))
        log(f"Saved debug {kind} page {idx} to {path}")
    except Exception as e:
        log(f"Failed to save debug {kind} page {idx}: {e}")

def ensure_inputs():
    ensure_dirs(); safe_env_dump()
    if not PURPLE:
        write_empty("No PURPLE_TWEET_URL provided")
        return None, None
    screen_name, root_id = parse_purple(PURPLE)
    if not (screen_name and root_id):
        write_empty("PURPLE_TWEET_URL did not match expected pattern")
        return None, None
    has_cookie = bool(AUTH_COOKIE and CSRF)
    has_bearer = bool(AUTH.startswith("Bearer "))
    if not (has_cookie or has_bearer):
        write_empty("Missing credentials: need auth_token+ct0 cookie or Bearer token")
        return None, None
    return screen_name, str(root_id)

# ---------- HTTP ----------
def fetch_json(url, hdrs, tag, attempt=1, backoff=2.0, timeout=30):
    try:
        req = Request(url, headers=hdrs)
        with urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "ignore")
            data = json.loads(raw) if raw.strip() else {}
            return data, raw, None
    except HTTPError as e:
        body = ""
        try: body = e.read().decode("utf-8","ignore")
        except Exception: pass
        log(f"{tag} HTTPError {e.code} url={url} body={body[:800]}")
        if e.code in (429, 403) and attempt <= 4:
            sleep_for = backoff ** attempt
            log(f"{tag} retry after {sleep_for:.1f}s (attempt {attempt}/4)")
            time.sleep(sleep_for); return fetch_json(url, hdrs, tag, attempt+1, backoff, timeout)
        return None, None, e
    except URLError as e:
        log(f"{tag} URLError {getattr(e,'reason',e)} url={url}")
        if attempt <= 4:
            sleep_for = backoff ** attempt
            log(f"{tag} retry after {sleep_for:.1f}s (attempt {attempt}/4)")
            time.sleep(sleep_for); return fetch_json(url, hdrs, tag, attempt+1, backoff, timeout)
        return None, None, e
    except Exception as e:
        log(f"{tag} EXC: {e}\n{traceback.format_exc()}")
        return None, None, e

# ---------- timeline cursor ----------
def find_bottom_cursor(data):
    def recurse(obj):
        if isinstance(obj, dict):
            if obj.get("cursorType") == "Bottom" and "value" in obj:
                return obj["value"]
            for v in obj.values():
                c = recurse(v)
                if c: return c
        elif isinstance(obj, list):
            for it in obj:
                c = recurse(it)
                if c: return c
        return None
    try:
        timeline = data.get("timeline") or {}
        instructions = timeline.get("instructions") or []
        for ins in instructions:
            entries = []
            if "addEntries" in ins and ins["addEntries"].get("entries"):
                entries.extend(ins["addEntries"]["entries"])
            if "replaceEntry" in ins and "entry" in ins["replaceEntry"]:
                entries.append(ins["replaceEntry"]["entry"])
            for e in entries:
                content = e.get("content") or {}
                cur = (((content.get("operation") or {}).get("cursor")) or {})
                if cur and cur.get("cursorType") == "Bottom" and cur.get("value"):
                    return cur["value"]
                item = content.get("itemContent") or {}
                cur = (item.get("value") or {})
                if isinstance(cur, dict) and cur.get("cursorType") == "Bottom" and cur.get("value"):
                    return cur["value"]
                cur = content.get("value") or {}
                if isinstance(cur, dict) and cur.get("cursorType") == "Bottom" and cur.get("value"):
                    return cur["value"]
    except Exception:
        pass
    return recurse(data)

# ---------- merge & extract ----------
def merge_objects(dst: dict, src: dict):
    for k, v in (src or {}).items():
        dst[k] = v

def extract_from_global_objects(data, agg_tweets, agg_users):
    g = (data.get("globalObjects") or {})
    merge_objects(agg_tweets, g.get("tweets") or {})
    merge_objects(agg_users,  g.get("users")  or {})

# ---------- collectors ----------
def collect_conversation(screen_name, root_id):
    tweets, users = {}, {}
    cursor, pages = None, 0
    while pages < MAX_PAGES:
        pages += 1
        params = {"count": 100, "tweet_mode": "extended"}
        if cursor: params["cursor"] = cursor
        url = CONVO_URL.format(tid=root_id) + "?" + urlencode(params)
        log(f"[CONVO] Fetch page {pages} cursor={cursor!r}")
        data, raw, err = fetch_json(url, headers(screen_name, root_id), tag="[CONVO]")
        if raw is not None: save_debug_blob("convo", pages, raw)
        if not data: break
        extract_from_global_objects(data, tweets, users)
        nxt = find_bottom_cursor(data)
        log(f"[CONVO] Bottom cursor: {nxt!r}")
        if not nxt or nxt == cursor: break
        cursor = nxt; time.sleep(SLEEP_SEC)
    log(f"[CONVO] pages={pages} tweets={len(tweets)} users={len(users)}")
    return tweets, users

def collect_search(screen_name, root_id):
    tweets, users = {}, {}
    cursor, pages = None, 0
    while pages < MAX_PAGES:
        pages += 1
        params = {
            "q": f"conversation_id:{root_id}",
            "count": 100,
            "tweet_search_mode": "live",
            "query_source": "typed_query",
            "tweet_mode": "extended",
            "pc": "ContextualServices",
            "spelling_corrections": "1",
            "include_quote_count": "true",
            "include_reply_count": "true",
            "ext": "mediaStats,highlightedLabel,hashtags,antispam_media_platform,voiceInfo,superFollowMetadata,unmentionInfo,editControl,emoji_reaction"
        }
        if cursor: params["cursor"] = cursor
        url = SEARCH_URL + "?" + urlencode(params)
        log(f"[SEARCH] Fetch page {pages} cursor={cursor!r}")
        data, raw, err = fetch_json(url, headers(screen_name, root_id), tag="[SEARCH]")
        if raw is not None: save_debug_blob("search", pages, raw)
        if not data: break
        extract_from_global_objects(data, tweets, users)
        nxt = find_bottom_cursor(data)
        log(f"[SEARCH] Bottom cursor: {nxt!r}")
        if not nxt or nxt == cursor: break
        cursor = nxt; time.sleep(SLEEP_SEC)
    log(f"[SEARCH] pages={pages} tweets={len(tweets)} users={len(users)}")
    return tweets, users

# ---------- builders (cards, media, embeds) ----------
def _collect_links(t: dict):
    out = []
    for u in ((t.get("entities") or {}).get("urls") or []):
        exp = (u.get("expanded_url") or u.get("url") or "").strip()
        if not exp: continue
        host = ""
        try: host = urlparse(exp).netloc.lower()
        except Exception: pass
        unw = u.get("unwound_url") or {}
        title = unw.get("title") or u.get("title") or ""
        desc  = unw.get("description") or u.get("description") or ""
        imgs  = u.get("images") or unw.get("images") or []
        thumb = ""
        if isinstance(imgs, list) and imgs:
            thumb = (imgs[0].get("url") or imgs[0].get("src") or "").strip()
        out.append({"url":exp, "domain":host, "title":title, "description":desc, "image":thumb})
    return out

def _collect_media(t: dict):
    out = []
    ee = t.get("extended_entities") or {}
    for m in (ee.get("media") or []):
        typ = m.get("type")
        alt = m.get("ext_alt_text") or ""
        base = (m.get("media_url_https") or m.get("media_url") or "").strip()
        if typ == "photo" and base:
            out.append({"type":"photo","src": base + "?name=large","alt": alt})
        elif typ in ("video","animated_gif"):
            info = m.get("video_info") or {}
            variants = [v for v in (info.get("variants") or []) if (isinstance(v, dict) and v.get("url") and v.get("content_type","").endswith("mp4"))]
            best = None
            for v in variants:
                if not best or int(v.get("bitrate") or 0) > int(best.get("bitrate") or 0):
                    best = v
            thumb = base + "?name=small" if base else ""
            if best:
                out.append({"type":"video","src": best["url"], "thumb": thumb})
            elif variants:
                out.append({"type":"video","src": variants[0]["url"], "thumb": thumb})
    return out

def _detect_embed(t: dict):
    for u in ((t.get("entities") or {}).get("urls") or []):
        ex = (u.get("expanded_url") or u.get("url") or "").strip()
        if not ex: continue
        try:
            host = urlparse(ex).netloc.lower()
            if any(k in host for k in ("youtube.com", "youtu.be", "vimeo.com", "rumble.com")):
                return ex
        except Exception:
            pass
    return ""

def _tstamp(tweet):
    try:
        import time as _t
        return _t.mktime(_t.strptime(tweet.get("created_at",""), "%a %b %d %H:%M:%S %z %Y"))
    except Exception:
        return 0

def build_outputs(replies, users, tweets_by_id):
    blocks = []
    for t in replies:
        uid = str(t.get("user_id_str") or t.get("user_id") or "")
        u = users.get(uid, {})
        name    = u.get("name") or "User"
        handle  = u.get("screen_name") or ""
        avatar  = (u.get("profile_image_url_https") or u.get("profile_image_url") or "").replace("_normal.","_bigger.")
        url     = f"https://x.com/{handle}/status/{t.get('id_str') or t.get('id')}"
        text    = t.get("full_text") or t.get("text") or ""
        created = t.get("created_at") or ""

        replies_ct   = t.get("reply_count")
        reposts_ct   = t.get("retweet_count")
        likes_ct     = t.get("favorite_count")
        views_ct     = t.get("ext_views") or t.get("view_count")
        bookmarks_ct = t.get("bookmark_count")

        quote_json = ""
        if t.get("is_quote_status"):
            qid = str(t.get("quoted_status_id_str") or t.get("quoted_status_id") or "")
            q   = tweets_by_id.get(qid) or (t.get("quoted_status") or {})
            if q:
                quser_id = str(q.get("user_id_str") or q.get("user_id") or "")
                qu       = users.get(quser_id, {})
                qhandle  = qu.get("screen_name") or ""
                qname    = qu.get("name") or ""
                qurl     = f"https://x.com/{qhandle}/status/{q.get('id_str') or q.get('id')}" if qhandle else ""
                quote = {
                    "text": q.get("full_text") or q.get("text") or "",
                    "url":  qurl,
                    "name": qname,
                    "handle": qhandle,
                    "verified": bool(qu.get("verified")),
                    "media": _collect_media(q),
                    "embed": _detect_embed(q),
                    "links": _collect_links(q)
                }
                quote_json = html.escape(json.dumps(quote, separators=(",",":")), quote=True)

        media_list = _collect_media(t)
        embed_url  = _detect_embed(t)

        attr = {
          "class": "ss3k-reply",
          "data-handle": "@"+handle if handle else "",
          "data-name": name,
          "data-verified": "true" if u.get("verified") else "false",
          "data-url": url,
          "data-ts": created,
          "data-replies": str(replies_ct or ""),
          "data-reposts": str(reposts_ct or ""),
          "data-likes": str(likes_ct or ""),
          "data-views": str(views_ct or ""),
          "data-bookmarks": str(bookmarks_ct or ""),
          "data-media": html.escape(json.dumps(media_list, separators=(",",":")), quote=True) if media_list else "",
          "data-embed": embed_url or "",
          "data-quote": quote_json
        }
        attr_s = " ".join(f'{k}="{v}"' for k,v in attr.items() if v)

        text_html = html.escape(text).replace("\n", "<br>")
        avatar_tag = f'<div class="avatar-50"><img src="{html.escape(avatar)}" alt=""></div>' if avatar else '<div class="avatar-50"></div>'
        who = html.escape(name) + (f' <span class="handle">@{html.escape(handle)}</span>' if handle else '')

        blocks.append(
          f'<div {attr_s}>'
          f'{avatar_tag}'
          f'<div><div class="head"><span class="disp">{who}</span></div>'
          f'<div class="body">{text_html}</div>'
          f'</div></div>'
        )

    open(OUT_REPLIES,"w", encoding="utf-8").write("\n".join(blocks))
    log(f"Wrote replies HTML: {OUT_REPLIES} ({len(blocks)} items)")

    doms = defaultdict(set)
    def add_urls_from(t):
        for u in ((t.get("entities") or {}).get("urls") or []):
            u2 = u.get("expanded_url") or u.get("url")
            if not u2: continue
            m = re.search(r"https?://([^/]+)/?", u2)
            dom = m.group(1) if m else "links"
            doms[dom].add(u2)
    for t in replies: add_urls_from(t)

    lines = []
    for dom in sorted(doms):
        lines.append(f"<h4>{html.escape(dom)}</h4>")
        lines.append("<ul>")
        for u in sorted(doms[dom]):
            e = html.escape(u)
            lines.append(f'<li><a href="{e}" target="_blank" rel="noopener">{e}</a></li>')
        lines.append("</ul>")
    open(OUT_LINKS,"w", encoding="utf-8").write("\n".join(lines))
    log(f"Wrote links HTML: {OUT_LINKS} (domains={len(doms)})")

# ---------- main ----------
def main():
    try:
        screen_name, root_id = ensure_inputs()
        if not (screen_name and root_id): return
        log(f"Begin collection for @{screen_name} status {root_id}")

        tweets, users = collect_conversation(screen_name, root_id)
        if not tweets:
            log("Conversation timeline empty; fallback to adaptive search.")
            tweets, users = collect_search(screen_name, root_id)

        replies = []
        for tid, t in (tweets or {}).items():
            conv = str(t.get("conversation_id_str") or t.get("conversation_id") or "")
            if conv != str(root_id): continue
            if str(t.get("id_str") or t.get("id")) == str(root_id): continue
            if t.get("retweeted_status_id") or t.get("retweeted_status_id_str"): continue
            replies.append(t)

        uniq = {}
        for t in replies:
            tid = str(t.get("id_str") or t.get("id") or "")
            if tid: uniq[tid] = t
        replies = list(uniq.values())
        replies.sort(key=_tstamp)

        log(f"Total replies in conversation: {len(replies)}")
        build_outputs(replies, users, tweets)

        if not replies:
            log("No replies found; wrote empty structures.")
    except Exception as e:
        log(f"FATAL: {e}\n{traceback.format_exc()}")
        write_empty(f"fatal error: {e}")

if __name__ == "__main__":
    main()
