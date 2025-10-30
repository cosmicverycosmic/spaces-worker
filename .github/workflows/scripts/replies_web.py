# file: .github/workflows/scripts/replies_web.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, json, html, time, traceback
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# ---------- ENV & paths ----------
ARTDIR = Path(os.environ.get("ARTDIR", "."))
BASE   = os.environ.get("BASE", "space")
PURPLE = (os.environ.get("PURPLE_TWEET_URL", "") or "").strip()

OUT_REPLIES = ARTDIR / f"{BASE}_replies.html"
OUT_LINKS   = ARTDIR / f"{BASE}_links.html"
LOG_PATH    = ARTDIR / f"{BASE}_replies.log"
DBG_DIR     = ARTDIR / "debug"
DBG_PREFIX  = DBG_DIR / f"{BASE}_replies_page"

# Auth (either cookie+ct0 or Bearer)
AUTH_BEARER = (os.environ.get("TWITTER_AUTHORIZATION") or "").strip()  # "Bearer XXX"
AUTH_COOKIE = (os.environ.get("TWITTER_AUTH_TOKEN") or "").strip()     # auth_token cookie
CSRF        = (os.environ.get("TWITTER_CSRF_TOKEN") or "").strip()     # ct0 cookie

# Optional start time to compute data-begin
START_ISO = (os.environ.get("START_ISO") or "").strip()

MAX_PAGES = int(os.environ.get("REPLIES_MAX_PAGES", "40") or "40")
SLEEP_SEC = float(os.environ.get("REPLIES_SLEEP", "0.7") or "0.7")
SAVE_JSON = (os.environ.get("REPLIES_SAVE_JSON", "1") or "1") not in ("0", "false", "False")

# Prefer x.com; keep both handy
BASE_X  = "https://x.com"
CONVO_URL  = f"{BASE_X}/i/api/2/timeline/conversation/{{tid}}.json"
SEARCH_URL = f"{BASE_X}/i/api/2/search/adaptive.json"

# ---------- small utils ----------
def ensure_dirs():
    ARTDIR.mkdir(parents=True, exist_ok=True)
    DBG_DIR.mkdir(parents=True, exist_ok=True)

def mask_token(s: str, keep=6) -> str:
    s = s or ""
    if len(s) <= keep: return s
    return s[:keep] + "…" + s[-keep:]

def log(msg: str):
    ensure_dirs()
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    with open(LOG_PATH, "a", encoding="utf-8") as lf:
        lf.write(f"[{ts}Z] {msg}\n")

def write_empty(reason=""):
    ensure_dirs()
    OUT_REPLIES.write_text(f"<!-- no replies: {html.escape(reason)} -->\n", encoding="utf-8")
    OUT_LINKS.write_text(f"<!-- no links: {html.escape(reason)} -->\n", encoding="utf-8")
    log(f"Wrote empty outputs: {reason}")
    print(OUT_REPLIES.read_text(encoding="utf-8"))

def safe_env_dump():
    lines = [
        f"ARTDIR={ARTDIR}", f"BASE={BASE}",
        f"PURPLE_TWEET_URL={(PURPLE or '')}",
        f"AUTH_BEARER={mask_token(AUTH_BEARER)}",
        f"AUTH_COOKIE={mask_token(AUTH_COOKIE)}",
        f"CSRF={mask_token(CSRF)}",
        f"MAX_PAGES={MAX_PAGES}",
        f"SAVE_JSON={SAVE_JSON}",
    ]
    log("ENV:\n  " + "\n  ".join(lines))

def headers(screen_name, root_id):
    ck = f"auth_token={AUTH_COOKIE}; ct0={CSRF}" if (AUTH_COOKIE and CSRF) else ""
    hdr = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "referer": f"{BASE_X}/{screen_name}/status/{root_id}",
        "user-agent": "Mozilla/5.0",
        "x-twitter-active-user": "yes",
        "x-twitter-client-language": "en",
    }
    if AUTH_BEARER.startswith("Bearer "):
        hdr["authorization"] = AUTH_BEARER
        hdr["x-twitter-auth-type"] = "OAuth2Session"
    if ck:
        hdr["cookie"] = ck
        hdr["x-csrf-token"] = CSRF
    return hdr

def save_debug_blob(kind, idx, raw):
    if not SAVE_JSON: return
    ensure_dirs()
    path = str(DBG_PREFIX) + f"_{kind}{idx:02d}.json"
    try:
        Path(path).write_text(raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False), encoding="utf-8")
        log(f"Saved debug {kind} page {idx} to {path}")
    except Exception as e:
        log(f"Failed to save debug {kind} page {idx}: {e}")

def parse_purple(url: str):
    m = re.search(r"https?://(?:x|twitter)\.com/([^/]+)/status/(\d+)", url)
    if not m: return None, None
    return m.group(1), m.group(2)

def load_start_epoch() -> float | None:
    if START_ISO:
        e = parse_time_any(START_ISO)
        if e: return e
    p = ARTDIR / f"{BASE}.start.txt"
    if p.exists():
        try: return parse_time_any(p.read_text(encoding="utf-8").strip())
        except Exception: pass
    return None

# ---------- HTTP with retry ----------
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
            log(f"{tag} backoff {sleep_for:.1f}s and retry (attempt {attempt+1})")
            time.sleep(sleep_for)
            return fetch_json(url, hdrs, tag, attempt+1, backoff, timeout)
        return None, body, e
    except URLError as e:
        log(f"{tag} URLError {e}")
        if attempt <= 3:
            time.sleep(1.5*attempt)
            return fetch_json(url, hdrs, tag, attempt+1, backoff, timeout)
        return None, None, e
    except Exception as e:
        log(f"{tag} EXC {e}")
        return None, None, e

# ---------- extraction helpers ----------
def merge_objects(dst: dict, src: dict):
    for k, v in (src or {}).items():
        dst[k] = v

def extract_from_global_objects(data, agg_tweets, agg_users):
    g = (data.get("globalObjects") or {})
    merge_objects(agg_tweets, g.get("tweets") or {})
    merge_objects(agg_users,  g.get("users")  or {})

def find_bottom_cursor(data) -> str | None:
    def rec(obj):
        if isinstance(obj, dict):
            if obj.get("cursorType") == "Bottom" and "value" in obj:
                return obj["value"]
            for v in obj.values():
                c = rec(v)
                if c: return c
        elif isinstance(obj, list):
            for v in obj:
                c = rec(v)
                if c: return c
        return None
    # Try common instruction shapes
    for path in [
        ("timeline", "instructions"),
        ("instructions",),
        ("data", "threaded_conversation_with_injections_v2", "instructions"),
    ]:
        node = data
        ok = True
        for k in path:
            node = node.get(k) if isinstance(node, dict) else None
            if node is None: ok = False; break
        if ok:
            c = rec(node)
            if c: return c
    return None

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
        data, raw, err = fetch_json(url, headers(screen_name, root_id), "[CONVO]")
        if raw is not None: save_debug_blob("convo", pages, raw)
        if not data:
            log(f"[CONVO] No data on page {pages}.")
            break
        extract_from_global_objects(data, tweets, users)
        nxt = find_bottom_cursor(data)
        if not nxt:
            log(f"[CONVO] No Bottom cursor; stop after {pages}")
            break
        cursor = nxt
        time.sleep(SLEEP_SEC)
    return tweets, users

def collect_search(screen_name, root_id):
    tweets, users = {}, {}
    cursor, pages = None, 0
    q = f"conversation_id:{root_id}"
    while pages < MAX_PAGES:
        pages += 1
        params = {"q": q, "count": 100, "tweet_mode": "extended"}
        if cursor: params["cursor"] = cursor
        url = SEARCH_URL + "?" + urlencode(params)
        log(f"[SEARCH] Fetch page {pages} cursor={cursor!r}")
        data, raw, err = fetch_json(url, headers(screen_name, root_id), "[SEARCH]")
        if raw is not None: save_debug_blob("search", pages, raw)
        if not data:
            log(f"[SEARCH] No data on page {pages}.")
            break
        extract_from_global_objects(data, tweets, users)
        nxt = find_bottom_cursor(data)
        if not nxt:
            log(f"[SEARCH] No Bottom cursor; stop after {pages}")
            break
        cursor = nxt
        time.sleep(SLEEP_SEC)
    return tweets, users

# ---------- formatting helpers ----------
EMOJI_RE = re.compile("[" +
    "\U0001F1E6-\U0001F1FF" "\U0001F300-\U0001F5FF" "\U0001F600-\U0001F64F" "\U0001F680-\U0001F6FF" +
    "\U0001F700-\U0001F77F" "\U0001F780-\U0001F7FF" "\U0001F800-\U0001F8FF" "\U0001F900-\U0001F9FF" +
    "\U0001FA00-\U0001FAFF" "\u2600-\u26FF" "\u2700-\u27BF" + "]+", re.UNICODE)
ONLY_PUNCT_SPACE = re.compile(r"^[\s\.,;:!?\-–—'\"“”‘’•·]+$")

def is_emoji_only(s: str) -> bool:
    if not s or not s.strip(): return False
    t = ONLY_PUNCT_SPACE.sub("", s)
    t = EMOJI_RE.sub("", t)
    return len(t.strip()) == 0

def esc(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def human_k(n) -> str:
    try: v = int(n or 0)
    except Exception: v = 0
    if v < 1000: return str(v)
    if v < 10000: return f"{v/1000:.1f}K".rstrip("0").rstrip(".") + "K"
    if v < 1_000_000: return f"{v//1000}K"
    if v < 10_000_000: return f"{v/1_000_000:.1f}M".rstrip("0").rstrip(".") + "M"
    return f"{v//1_000_000}M"

def parse_epoch_maybe(x):
    if x is None: return None
    try: v = float(x)
    except Exception: return None
    if v > 1e12: v /= 1000.0
    return v

def parse_time_any(created_at) -> float | None:
    if created_at is None: return None
    s = str(created_at).strip()
    if re.fullmatch(r"\d{10,13}", s): return parse_epoch_maybe(s)
    try:
        if s.endswith("Z"): dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        elif re.search(r"[+-]\d{2}:?\d{2}$", s):
            if re.search(r"[+-]\d{4}$", s):
                s = s[:-5] + s[-5:-3] + ":" + s[-3:]
            dt = datetime.fromisoformat(s)
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()
    except Exception:
        pass
    try:
        return datetime.strptime(s, "%a, %d %b %Y %H:%M:%S %z").timestamp()
    except Exception:
        return None

def fmt_local_ts(epoch: float | None):
    if not epoch: return ("", "")
    dt = datetime.utcfromtimestamp(epoch).replace(tzinfo=timezone.utc)
    disp = dt.strftime("%Y-%m-%d %H:%M UTC")
    return disp, dt.isoformat().replace("+00:00", "Z")

TCO_RE = re.compile(r"https?://t\.co/[A-Za-z0-9]+")
STATUS_RE = re.compile(r"https?://(?:x|twitter)\.com/[^/]+/status/\d+")

def expand_tco(text: str, entities: dict):
    s = text or ""
    links, removed_media_urls = [], []
    for m in (entities.get("media") or []):
        u = (m.get("url") or m.get("expanded_url"))
        if u: removed_media_urls.append(u)
    repls = []
    for u in (entities.get("urls") or []):
        tco = u.get("url") or ""
        if not tco: continue
        start, end = None, None
        if isinstance(u.get("indices"), list) and len(u["indices"]) == 2:
            start, end = int(u["indices"][0]), int(u["indices"][1])
        expanded = (u.get("unwound_url") or u.get("expanded_url") or u.get("display_url") or tco)
        links.append({
            "url": expanded, "title": u.get("title") or u.get("unwound_title"),
            "description": u.get("description") or u.get("unwound_description"),
            "images": u.get("images") or []
        })
        if start is not None: repls.append((start, end, expanded))
    if repls:
        repls.sort(key=lambda x: x[0], reverse=True)
        for st, en, rep in repls:
            if 0 <= st < en <= len(s): s = s[:st] + rep + s[en:]
    for mu in removed_media_urls: s = s.replace(mu, "")
    s = re.sub(r"\s+", " ", s).strip()
    return s, links

def collect_inline_media(extended_entities: dict):
    out = []
    if not isinstance(extended_entities, dict): return out
    for m in (extended_entities.get("media") or []):
        t = m.get("type")
        if t == "photo":
            src = m.get("media_url_https") or m.get("media_url")
            if src: out.append({"kind":"img","src":src})
        elif t in ("video","animated_gif"):
            best = None
            for v in (m.get("video_info", {}).get("variants") or []):
                if v.get("content_type") != "video/mp4": continue
                br = int(v.get("bitrate") or 0)
                if (best is None) or (br > best.get("bitrate",0)):
                    best = {"kind":"video","src":v.get("url"),"bitrate":br}
            if best and best.get("src"): out.append(best)
    return out

def make_status_url(handle, tid):
    h = (handle or "").lstrip("@"); i = (tid or "").strip()
    if not h or not i: return None
    return f"https://x.com/{h}/status/{i}"

def normalize(tweet_obj: dict, user_map: dict):
    if not isinstance(tweet_obj, dict): return None
    tid   = str(tweet_obj.get("id_str") or tweet_obj.get("id") or "")
    if not tid: return None
    full  = tweet_obj.get("full_text") or tweet_obj.get("text") or ""
    if not full.strip() or is_emoji_only(full): return None

    uid   = str(tweet_obj.get("user_id_str") or tweet_obj.get("user_id") or "")
    u     = user_map.get(uid, {}) if uid else {}
    name  = u.get("name") or "User"
    handle= u.get("screen_name") or ""
    avatar= (u.get("profile_image_url_https") or u.get("profile_image_url") or "")
    if avatar: avatar = avatar.replace("_normal.", "_bigger.")

    created= (tweet_obj.get("created_at") or tweet_obj.get("timestamp_ms"))
    epoch  = parse_time_any(created)

    leg    = tweet_obj
    ents   = leg.get("entities") or {}
    exts   = leg.get("extended_entities") or {}
    text_exp, link_cards = expand_tco(full, ents)
    media_items          = collect_inline_media(exts)

    pm = {
        "favorite_count": tweet_obj.get("favorite_count"),
        "retweet_count":  tweet_obj.get("retweet_count") or tweet_obj.get("repost_count"),
        "reply_count":    tweet_obj.get("reply_count"),
        "quote_count":    tweet_obj.get("quote_count"),
    }

    quote_url = None
    m = STATUS_RE.search(text_exp)
    if m: quote_url = m.group(0)

    return {
        "id": tid,
        "user_id": uid,
        "name": name,
        "handle": handle,
        "avatar": avatar,
        "text": text_exp,
        "created_raw": created,
        "created_epoch": epoch,
        "like_count": int(pm.get("favorite_count") or 0),
        "retweet_count": int(pm.get("retweet_count") or 0),
        "reply_count": int(pm.get("reply_count") or 0),
        "quote_count": int(pm.get("quote_count") or 0),
        "status_url": make_status_url(handle, tid),
        "link_cards": link_cards,
        "media": media_items,
        "in_reply_to": str(tweet_obj.get("in_reply_to_status_id_str") or tweet_obj.get("in_reply_to_status_id") or ""),
        "conversation_id": str(tweet_obj.get("conversation_id_str") or tweet_obj.get("conversation_id") or ""),
        "quote_url": quote_url,
    }

# ---------- HTML builders ----------
def render_link_cards(cards):
    parts, seen = [], set()
    for c in cards or []:
        u = (c.get("url") or "").strip()
        if not u or u in seen: continue
        seen.add(u)
        title = c.get("title") or ""
        desc  = c.get("description") or ""
        dom = ""
        try: dom = re.sub(r"^https?://(www\.)?([^/]+).*$", r"\2", u, flags=re.I)
        except Exception: pass
        img = None
        for im in (c.get("images") or []):
            if isinstance(im, dict) and im.get("url"): img = im["url"]; break
            if isinstance(im, str): img = im; break
        parts.append(
            f'<a class="ss3k-cardlink" href="{esc(u)}" target="_blank" rel="noopener">'
            f'  <div class="card">'
            f'    {"<img src=\"%s\" alt=\"\">" % esc(img) if img else ""}'
            f'    <div class="meta">'
            f'      <div class="t">{esc(title) if title else esc(dom or u)}</div>'
            f'      {("<div class=\"d\">%s</div>" % esc(desc)) if desc else ""}'
            f'      <div class="h">{esc(dom or "")}</div>'
            f'    </div>'
            f'  </div>'
            f'</a>'
        )
    return "".join(parts)

def render_media(ms):
    out = []
    for m in ms or []:
        if m.get("kind") == "img":
            out.append(f'<figure class="m"><img loading="lazy" src="{esc(m["src"])}" alt=""></figure>')
        elif m.get("kind") == "video" and m.get("src"):
            out.append(
                '<figure class="m"><video controls playsinline preload="metadata">'
                f'<source src="{esc(m["src"])}" type="video/mp4"></video></figure>')
    return "".join(out)

def render_reply_item(it, start_epoch):
    disp, iso = fmt_local_ts(it.get("created_epoch"))
    data_begin = ""
    if start_epoch and it.get("created_epoch"):
        rel = max(0.0, float(it["created_epoch"] - start_epoch))
        data_begin = f' data-begin="{rel:.3f}"'
    stats = (
        f'<div class="stats">'
        f'<span title="Replies">💬 {human_k(it.get("reply_count"))}</span>'
        f' <span title="Reposts">🔁 {human_k(it.get("retweet_count"))}</span>'
        f' <span title="Likes">❤️ {human_k(it.get("like_count"))}</span>'
        f'{(" <span title=\\"Quotes\\">🔗 " + human_k(it.get("quote_count")) + "</span>") if (it.get("quote_count") or 0)>0 else ""}'
        f'</div>'
    )
    quote = ""
    if it.get("quote_url"):
        q = it["quote_url"]
        quote = (
            '<blockquote class="twitter-tweet" data-dnt="true">'
            f'<a href="{esc(q)}"></a>'
            '</blockquote>'
            '<script async src="https://platform.twitter.com/widgets.js" charset="utf-8"></script>'
        )
    avatar = it.get("avatar") or ""
    handle = it.get("handle") or ""
    name   = it.get("name") or "User"
    status = it.get("status_url") or ""
    avatar_html = (
        f'<div class="ss3k-avatar">'
        f'  {("<img src=\\"%s\\" alt=\\"\\"/>" % esc(avatar)) if avatar else ("<span>" + esc((handle or "U")[:1].upper()) + "</span>")}'
        f'</div>'
    )
    body = (
        f'<div class="body">'
        f'  <div class="head"><span class="nm">{esc(name)}</span> <span class="hn">@{esc(handle)}</span></div>'
        f'  <div class="tx">{esc(it.get("text") or "")}</div>'
        f'  {render_media(it.get("media") or [])}'
        f'  {render_link_cards(it.get("link_cards") or [])}'
        f'  {quote}'
        f'  <div class="meta">'
        f'    <span class="when">{("<a href=\\"%s\\" target=\\"_blank\\" rel=\\"noopener\\">%s</a>" % (esc(status), esc(disp))) if status and disp else esc(disp)}</span>'
        f'    {stats}'
        f'  </div>'
        f'</div>'
    )
    return (
        f'<div class="ss3k-reply" data-id="{esc(it.get("id") or "")}" data-parent="{esc(it.get("in_reply_to") or "")}" '
        f'data-conv="{esc(it.get("conversation_id") or "")}" data-ts="{esc(it.get("created_raw") or "")}"{data_begin}>'
        f'{avatar_html}{body}</div>'
    )

CSS_INLINE = """
<style>
#ss3k-replies.ss3k-replies-list{border:1px solid var(--line,#e6eaf2);border-radius:10px;overflow:hidden;background:var(--card,#fff)}
@media(prefers-color-scheme:dark){
  #ss3k-replies.ss3k-replies-list{border-color:rgba(255,255,255,.1);background:rgba(255,255,255,.03)}
}
#ss3k-replies .ss3k-reply{display:grid;grid-template-columns:50px minmax(0,1fr);gap:12px;padding:12px 12px 14px;border-bottom:1px solid var(--line,#e6eaf2)}
#ss3k-replies .ss3k-reply:nth-child(odd){background:var(--soft,#f6f8fc)}
@media(prefers-color-scheme:dark){
  #ss3k-replies .ss3k-reply:nth-child(odd){background:rgba(255,255,255,.04)}
}
#ss3k-replies .ss3k-reply:last-child{border-bottom:none}
#ss3k-replies .ss3k-reply.highlight{background:var(--highlight,#fef3c7)}
#ss3k-replies .ss3k-avatar{width:50px;height:50px;border-radius:50%;overflow:hidden;background:#e5e7eb;display:flex;align-items:center;justify-content:center;font-weight:700;color:#6b7280}
#ss3k-replies .ss3k-avatar img{width:100%;height:100%;object-fit:cover;display:block}
#ss3k-replies .body .head{font-weight:700;display:flex;gap:.5ch;align-items:baseline}
#ss3k-replies .body .hn{color:#6b7280;font-weight:500}
#ss3k-replies .body .tx{margin:.25rem 0 .4rem;line-height:1.45;word-wrap:anywhere}
#ss3k-replies figure.m{margin:.25rem 0}
#ss3k-replies figure.m img, #ss3k-replies figure.m video{max-width:100%;height:auto;border-radius:10px;border:1px solid var(--line,#e6eaf2)}
#ss3k-replies .meta{display:flex;align-items:center;justify-content:space-between;font-size:.9em;color:#6b7280;margin-top:.35rem}
#ss3k-replies .stats span{margin-left:.8ch}
#ss3k-replies .ss3k-cardlink{display:block;text-decoration:none;color:inherit;margin:.35rem 0}
#ss3k-replies .ss3k-cardlink .card{display:grid;grid-template-columns:96px minmax(0,1fr);gap:10px;border:1px solid var(--line,#e6eaf2);border-radius:10px;padding:8px;background:var(--card,#fff)}
#ss3k-replies .ss3k-cardlink .card img{width:96px;height:72px;object-fit:cover;border-radius:8px}
#ss3k-replies .ss3k-cardlink .card .t{font-weight:700;margin-bottom:2px}
#ss3k-replies .ss3k-cardlink .card .d{font-size:.9em;color:#6b7280}
#ss3k-replies .ss3k-cardlink .card .h{font-size:.85em;color:#9aa3b2;margin-top:4px}
</style>
"""

# ---------- build+write ----------
def build_outputs(replies, users):
    start_epoch = load_start_epoch()
    rows = [render_reply_item(r, start_epoch) for r in replies]
    replies_html = CSS_INLINE + '<section class="ss3k-replies-list" id="ss3k-replies">\n' + "\n".join(rows) + "\n</section>\n"
    OUT_REPLIES.write_text(replies_html, encoding="utf-8")
    print(replies_html)

    # links rollup
    link_cards_flat = []
    for r in replies:
        for c in (r.get("link_cards") or []):
            if isinstance(c, dict) and c.get("url"):
                link_cards_flat.append(c)
    seen, items = set(), []
    for c in link_cards_flat:
        u = c.get("url")
        if not u or u in seen: continue
        seen.add(u)
        items.append(f'<li class="link">{render_link_cards([c])}</li>')
    OUT_LINKS.write_text('<ul class="ss3k-links">\n' + "\n".join(items) + "\n</ul>\n" if items else "<!-- no links -->", encoding="utf-8")

def main():
    try:
        ensure_dirs()
        safe_env_dump()

        if not PURPLE:
            write_empty("No PURPLE_TWEET_URL provided")
            return
        screen_name, root_id = parse_purple(PURPLE)
        if not (screen_name and root_id):
            write_empty("PURPLE_TWEET_URL did not match expected pattern")
            return

        if not (AUTH_BEARER.startswith("Bearer ") or (AUTH_COOKIE and CSRF)):
            write_empty("Missing credentials: need TWITTER_AUTHORIZATION or TWITTER_AUTH_TOKEN+TWITTER_CSRF_TOKEN")
            return

        # Collect
        tw_convo, users_convo = collect_conversation(screen_name, root_id)
        tw_search, users_search = ({}, {})
        if not tw_convo:
            log("Conversation endpoint empty; trying search fallback…")
            tw_search, users_search = collect_search(screen_name, root_id)

        tweets = {**tw_convo, **tw_search}
        users  = {**users_convo, **users_search}
        log(f"Collected tweets={len(tweets)} users={len(users)}")

        if not tweets:
            OUT_REPLIES.write_text(
                f'<div class="ss3k-replies"><p><a href="{html.escape(PURPLE)}" target="_blank" rel="noopener">Open conversation on X</a></p></div>',
                encoding="utf-8"
            )
            OUT_LINKS.write_text("<!-- no links: replies empty -->", encoding="utf-8")
            print(OUT_REPLIES.read_text(encoding="utf-8"))
            return

        # Normalize, dedupe, filter out the root and empty/emoji rows
        norm = []
        for tid, tobj in tweets.items():
            if str(tid) == str(root_id):  # skip the root
                continue
            n = normalize(tobj, users)
            if n: norm.append(n)

        # de-dupe by id, prefer latest metrics if seen twice
        uniq = {}
        for n in norm:
            uniq[n["id"]] = n
        replies = list(uniq.values())
        replies.sort(key=lambda x: (x.get("created_epoch") or 0, x.get("id") or ""))

        log(f"Total replies after normalize: {len(replies)}")
        build_outputs(replies, users)

        if not replies:
            log("No replies found; wrote empty structures with headers.")
    except Exception as e:
        log(f"FATAL: {e}\n{traceback.format_exc()}")
        write_empty(f"fatal error: {e}")

if __name__ == "__main__":
    main()
