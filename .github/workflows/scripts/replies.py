# file: .github/workflows/scripts/replies_web.py
#!/usr/bin/env python3
import os, re, json, html, time
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from collections import defaultdict

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
PURPLE = os.environ.get("PURPLE_TWEET_URL","").strip()

OUT_REPLIES = os.path.join(ARTDIR, f"{BASE}_replies.html")
OUT_LINKS   = os.path.join(ARTDIR, f"{BASE}_links.html")

AUTH = os.environ.get("TWITTER_AUTHORIZATION","").strip()  # must start with "Bearer "
AUTH_COOKIE = os.environ.get("TWITTER_AUTH_TOKEN","").strip()  # auth_token cookie
CSRF = os.environ.get("TWITTER_CSRF_TOKEN","").strip()         # ct0 cookie

def write_empty():
    open(OUT_REPLIES, "w").write("")
    open(OUT_LINKS, "w").write("")

m = re.search(r"https?://(?:x|twitter)\.com/([^/]+)/status/(\d+)", PURPLE)
if not m:
    write_empty(); raise SystemExit(0)

screen_name, root_id = m.group(1), m.group(2)
if not (AUTH.startswith("Bearer ") and AUTH_COOKIE and CSRF):
    write_empty(); raise SystemExit(0)

def headers():
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

BASE_URL = "https://twitter.com/i/api/2/search/adaptive.json"
Q = f"conversation_id:{root_id}"

def fetch_page(cursor=None, retries=2):
    params = {
        "q": Q,
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
    url = BASE_URL + "?" + urlencode(params)
    req = Request(url, headers=headers())
    try:
        with urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode("utf-8", "ignore"))
    except HTTPError as e:
        if e.code in (429, 403) and retries>0:
            time.sleep(3); return fetch_page(cursor, retries-1)
        return None
    except URLError:
        return None

def bottom_cursor(obj):
    if isinstance(obj, dict):
        if obj.get("cursorType") == "Bottom" and "value" in obj:
            return obj["value"]
        for v in obj.values():
            c = bottom_cursor(v)
            if c: return c
    elif isinstance(obj, list):
        for it in obj:
            c = bottom_cursor(it)
            if c: return c
    return None

def collect():
    tweets, users = {}, {}
    cursor, pages = None, 0
    while pages < 40:
        data = fetch_page(cursor)
        if not data: break
        pages += 1
        for k, v in (data.get("globalObjects", {}).get("tweets") or {}).items():
            tweets[k] = v
        for k, v in (data.get("globalObjects", {}).get("users") or {}).items():
            users[k] = v
        nxt = bottom_cursor(data.get("timeline") or data)
        if not nxt or nxt == cursor: break
        cursor = nxt
        time.sleep(0.6)
    return tweets, users

tweets, users = collect()

replies = []
for tid, t in tweets.items():
    if str(t.get("conversation_id_str") or t.get("conversation_id")) != str(root_id):
        continue
    if str(t.get("id_str") or t.get("id")) == str(root_id):
        continue
    replies.append(t)

def tstamp(t):
    try:
        import time as _t
        return _t.mktime(_t.strptime(t.get("created_at",""), "%a %b %d %H:%M:%S %z %Y"))
    except Exception:
        return 0
replies.sort(key=tstamp)

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
open(OUT_REPLIES,"w").write("\n".join(blocks))

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
open(OUT_LINKS,"w").write("\n".join(lines))
