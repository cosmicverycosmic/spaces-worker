# file: .github/workflows/scripts/replies_web.py
#!/usr/bin/env python3
import os, re, json, html, time, traceback, sys, math
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from collections import defaultdict

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
PURPLE = (os.environ.get("PURPLE_TWEET_URL","") or "").strip()

OUT_REPLIES = os.path.join(ARTDIR, f"{BASE}_replies.html")
OUT_LINKS   = os.path.join(ARTDIR, f"{BASE}_links.html")
LOG_PATH    = os.path.join(ARTDIR, f"{BASE}_replies.log")
DBG_DIR     = os.path.join(ARTDIR, "debug")
DBG_PREFIX  = os.path.join(DBG_DIR, f"{BASE}_replies_page")

AUTH        = (os.environ.get("TWITTER_AUTHORIZATION","") or "").strip()   # "Bearer ..."
AUTH_COOKIE = (os.environ.get("TWITTER_AUTH_TOKEN","") or "").strip()      # auth_token cookie
CSRF        = (os.environ.get("TWITTER_CSRF_TOKEN","") or "").strip()      # ct0 cookie

MAX_PAGES   = int(os.environ.get("REPLIES_MAX_PAGES","40") or "40")
SLEEP_SEC   = float(os.environ.get("REPLIES_SLEEP","0.7") or "0.7")
SAVE_JSON   = (os.environ.get("REPLIES_SAVE_JSON","1") or "1") not in ("0","false","False")

BASE_URL    = "https://twitter.com/i/api/2/search/adaptive.json"

def ensure_dirs():
    os.makedirs(ARTDIR, exist_ok=True)
    os.makedirs(DBG_DIR, exist_ok=True)

def mask_token(s: str, keep=6):
    if not s: return ""
    s = str(s)
    if len(s) <= keep: return "*" * len(s)
    return "*" * (len(s)-keep) + s[-keep:]

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
        f"ARTDIR={ARTDIR}",
        f"BASE={BASE}",
        f"PURPLE_TWEET_URL={(PURPLE or '')}",
        f"AUTH={mask_token(AUTH)}",
        f"AUTH_COOKIE={mask_token(AUTH_COOKIE)}",
        f"CSRF={mask_token(CSRF)}",
        f"MAX_PAGES={MAX_PAGES}",
        f"SLEEP_SEC={SLEEP_SEC}",
        f"SAVE_JSON={SAVE_JSON}",
    ]
    log("ENV:\n  " + "\n  ".join(lines))

def headers(screen_name, root_id):
    ck = f"auth_token={AUTH_COOKIE}; ct0={CSRF}"
    return {
        "Authorization": AUTH,
        "x-csrf-token": CSRF,
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://x.com/{screen_name}/status/{root_id}",
        "Cookie": ck,
    }

def build_params(root_id, cursor=None):
    q = f"conversation_id:{root_id}"
    params = {
        "q": q,
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
    if cursor:
        params["cursor"] = cursor
    return params

def fetch_page(screen_name, root_id, cursor=None, attempt=1, backoff=2.0):
    url = BASE_URL + "?" + urlencode(build_params(root_id, cursor))
    req = Request(url, headers=headers(screen_name, root_id))
    try:
        with urlopen(req, timeout=30) as r:
            raw = r.read().decode("utf-8", "ignore")
            data = json.loads(raw) if raw.strip() else {}
            return data, raw, None
    except HTTPError as e:
        msg = f"HTTPError {e.code} on cursor={cursor!r}"
        try:
            body = e.read().decode("utf-8","ignore")
        except Exception:
            body = ""
        log(msg + (f" body={body[:3000]}" if body else ""))
        if e.code in (429, 403) and attempt <= 4:
            sleep_for = backoff ** attempt
            log(f"Retrying after {sleep_for:.1f}s (attempt {attempt}/4)")
            time.sleep(sleep_for)
            return fetch_page(screen_name, root_id, cursor, attempt+1, backoff)
        return None, None, e
    except URLError as e:
        log(f"URLError {e.reason} on cursor={cursor!r}")
        if attempt <= 4:
            sleep_for = backoff ** attempt
            log(f"Retrying after {sleep_for:.1f}s (attempt {attempt}/4)")
            time.sleep(sleep_for)
            return fetch_page(screen_name, root_id, cursor, attempt+1, backoff)
        return None, None, e
    except Exception as e:
        log(f"Exception fetch_page: {e}\n{traceback.format_exc()}")
        return None, None, e

def save_debug_page(idx, raw):
    if not SAVE_JSON: return
    ensure_dirs()
    path = f"{DBG_PREFIX}{idx:02d}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False))
        log(f"Saved debug page {idx} to {path}")
    except Exception as e:
        log(f"Failed to save debug page {idx}: {e}")

def find_bottom_cursor(data):
    """Scan multiple shapes for a Bottom cursor."""
    # 1) Generic recursive
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

    # 2) Instruction/entry shapes often present in search timelines
    try:
        timeline = data.get("timeline") or {}
        instructions = timeline.get("instructions") or []
        # Newer shapes
        for ins in instructions:
            entries = []
            if "addEntries" in ins and ins["addEntries"].get("entries"):
                entries.extend(ins["addEntries"]["entries"])
            if "replaceEntry" in ins and "entry" in ins["replaceEntry"]:
                entries.append(ins["replaceEntry"]["entry"])
            for e in entries:
                content = e.get("content") or {}
                # content.operation.cursor
                cur = (((content.get("operation") or {}).get("cursor")) or {})
                if cur and cur.get("cursorType") == "Bottom" and cur.get("value"):
                    return cur["value"]
                # content.itemContent.value
                item = content.get("itemContent") or {}
                cur = (item.get("value") or {})
                if isinstance(cur, dict) and cur.get("cursorType") == "Bottom" and cur.get("value"):
                    return cur["value"]
                # legacy: content.value
                cur = content.get("value") or {}
                if isinstance(cur, dict) and cur.get("cursorType") == "Bottom" and cur.get("value"):
                    return cur["value"]
    except Exception as e:
        log(f"Cursor parse (instruction path) failed: {e}")

    # 3) Fallback recursive scan
    return recurse(data)

def merge_objects(dst: dict, src: dict):
    for k, v in (src or {}).items():
        dst[k] = v

def extract_objects(data, agg_tweets, agg_users):
    g = (data.get("globalObjects") or {})
    merge_objects(agg_tweets, g.get("tweets") or {})
    merge_objects(agg_users,  g.get("users")  or {})

def collect(screen_name, root_id):
    tweets, users = {}, {}
    cursor, pages = None, 0
    seen_pages = 0

    while pages < MAX_PAGES:
        pages += 1
        log(f"Fetching page {pages} cursor={cursor!r}")
        data, raw, err = fetch_page(screen_name, root_id, cursor)
        if raw is not None:
            save_debug_page(pages, raw)
        if not data:
            log(f"No data returned on page {pages}. Stopping.")
            break

        extract_objects(data, tweets, users)

        nxt = find_bottom_cursor(data)
        log(f"Parsed bottom cursor: {nxt!r}")
        if not nxt or nxt == cursor:
            log("No next cursor or same cursor encountered — pagination ends.")
            break
        cursor = nxt
        seen_pages += 1
        time.sleep(SLEEP_SEC)

    log(f"Collected pages: {seen_pages}, tweets: {len(tweets)}, users: {len(users)}")
    return tweets, users

def parse_purple(url):
    m = re.search(r"https?://(?:x|twitter)\.com/([^/]+)/status/(\d+)", url)
    if not m:
        return None, None
    return m.group(1), m.group(2)

def ensure_inputs():
    ensure_dirs()
    safe_env_dump()

    if not PURPLE:
        write_empty("No PURPLE_TWEET_URL provided")
        return None, None

    screen_name, root_id = parse_purple(PURPLE)
    if not (screen_name and root_id):
        write_empty("PURPLE_TWEET_URL did not match expected pattern")
        return None, None

    if not (AUTH.startswith("Bearer ") and AUTH_COOKIE and CSRF):
        write_empty("Missing or invalid AUTH/AUTH_COOKIE/CSRF")
        return None, None

    return screen_name, str(root_id)

def tstamp(tweet):
    try:
        import time as _t
        return _t.mktime(_t.strptime(tweet.get("created_at",""), "%a %b %d %H:%M:%S %z %Y"))
    except Exception:
        return 0

def build_outputs(replies, users):
    # Replies HTML
    blocks = []
    for t in replies:
        uid = str(t.get("user_id_str") or t.get("user_id") or "")
        u = users.get(uid, {})
        name = u.get("name") or "User"
        handle = u.get("screen_name") or ""
        avatar = (u.get("profile_image_url_https") or u.get("profile_image_url") or "").replace("_normal.","_bigger.")
        url = f"https://x.com/{handle}/status/{t.get('id_str') or t.get('id')}"
        text = html.escape(t.get("full_text") or t.get("text") or "")
        imgtag = f'<img class="ss3k-ravatar" src="{html.escape(avatar)}" alt="">' if avatar else '<div class="ss3k-ravatar" style="width:32px;height:32px;border-radius:50%;background:#eee"></div>'
        who = html.escape(f"{name} (@{handle})") if handle else html.escape(name)
        blocks.append(
            f'<div class="ss3k-reply"><a href="{url}" target="_blank" rel="noopener">{imgtag}</a>'
            f'<div class="ss3k-rcontent"><div class="ss3k-rname">{who}</div>'
            f'<div class="ss3k-rtext">{text}</div></div></div>'
        )
    open(OUT_REPLIES,"w", encoding="utf-8").write("\n".join(blocks))
    log(f"Wrote replies HTML: {OUT_REPLIES} ({len(blocks)} items)")

    # Links HTML grouped by domain
    doms = defaultdict(set)
    def add_urls_from(t):
        ent = t.get("entities") or {}
        for u in (ent.get("urls") or []):
            u2 = u.get("expanded_url") or u.get("url")
            if not u2: continue
            m = re.search(r"https?://([^/]+)/?", u2)
            dom = m.group(1) if m else "links"
            doms[dom].add(u2)

    for t in replies:
        add_urls_from(t)

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

def main():
    try:
        screen_name, root_id = ensure_inputs()
        if not (screen_name and root_id):
            return

        log(f"Begin collection for @{screen_name} status {root_id}")
        tweets, users = collect(screen_name, root_id)

        # Filter to the conversation thread only, exclude the root itself
        replies = []
        for tid, t in (tweets or {}).items():
            conv = str(t.get("conversation_id_str") or t.get("conversation_id") or "")
            if conv != str(root_id):
                continue
            if str(t.get("id_str") or t.get("id")) == str(root_id):
                continue
            # Skip pure retweets to cut noise
            if t.get("retweeted_status_id") or t.get("retweeted_status_id_str"):
                continue
            replies.append(t)

        # De-duplicate by tweet ID
        uniq = {}
        for t in replies:
            tid = str(t.get("id_str") or t.get("id") or "")
            uniq[tid] = t
        replies = list(uniq.values())

        replies.sort(key=tstamp)
        log(f"Total replies in conversation: {len(replies)}")

        build_outputs(replies, users)

        if not replies:
            log("No replies found; wrote empty structures with headers.")
    except Exception as e:
        log(f"FATAL: {e}\n{traceback.format_exc()}")
        write_empty(f"fatal error: {e}")

if __name__ == "__main__":
    main()
