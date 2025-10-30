# file: .github/workflows/scripts/replies_web.py
#!/usr/bin/env python3
import os, re, sys, json, html, time, traceback, glob
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from collections import defaultdict

# =========================
# ENV & OUTPUT PATHS
# =========================
ARTDIR = os.environ.get("ARTDIR", ".")
BASE   = os.environ.get("BASE", "space")

# Primary crawler input (JSONL). If not set, we try to discover it.
CRAWLER_INPUT = os.environ.get("CRAWLER_INPUT", "").strip()

# (Optional) Known root post to help detect thread root
PURPLE = (os.environ.get("PURPLE_TWEET_URL", "") or "").strip()

# Where we write artifacts
OUT_REPLIES = os.path.join(ARTDIR, f"{BASE}_replies.html")
OUT_LINKS   = os.path.join(ARTDIR, f"{BASE}_links.html")
LOG_PATH    = os.path.join(ARTDIR, f"{BASE}_replies.log")
DBG_DIR     = os.path.join(ARTDIR, "debug")

# WordPress patch endpoint (optional)
WP_BASE      = (os.environ.get("WP_BASE_URL") or os.environ.get("WP_SITE") or "").rstrip("/")
WP_ENDPOINT  = (os.environ.get("WP_PATCH_ENDPOINT") or "/wp-json/ss3k/v1/patch-assets").strip() or "/wp-json/ss3k/v1/patch-assets"
WP_AUTH      = (os.environ.get("WP_AUTH") or "").strip()        # e.g., "Bearer <token>" OR "Basic <...>"
WP_POST_ID   = (os.environ.get("POST_ID") or os.environ.get("WP_POST_ID") or "").strip()

SAVE_JSON_DEBUG = (os.environ.get("REPLIES_SAVE_JSON","0") not in ("0","false","False"))

# =========================
# LOGGING
# =========================
def ensure_dirs():
    os.makedirs(ARTDIR, exist_ok=True)
    os.makedirs(DBG_DIR, exist_ok=True)

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

# =========================
# INPUT DISCOVERY
# =========================
def parse_purple(url):
    m = re.search(r"https?://(?:x|twitter)\.com/([^/]+)/status/(\d+)", url or "")
    if not m: return None, None
    return m.group(1), m.group(2)

def discover_inputs():
    """Find a reasonable JSONL produced by the crawler."""
    if CRAWLER_INPUT and os.path.exists(CRAWLER_INPUT):
        return CRAWLER_INPUT
    # Heuristics: prefer newest / largest matching files
    candidates = []
    pats = [
        os.path.join(ARTDIR, f"{BASE}*.jsonl"),
        os.path.join(ARTDIR, "debug", f"{BASE}*.jsonl"),
        os.path.join(ARTDIR, f"{BASE}_*.log"),
        os.path.join(ARTDIR, "debug", f"{BASE}_*.log"),
    ]
    for p in pats:
        candidates.extend(glob.glob(p))
    if not candidates:
        return ""
    # Pick the most recent by mtime, tie-break by size
    candidates.sort(key=lambda p: (os.path.getmtime(p), os.path.getsize(p)), reverse=True)
    return candidates[0]

# =========================
# SAFE HTML HELPERS
# =========================
def esc(s):
    return html.escape("" if s is None else str(s), quote=True)

def safe_newlines_to_br(s):
    return esc(s).replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")

def kfmt(n):
    try:
        n = int(n)
        if n < 1000: return str(n)
        if n < 1_000_000: return f"{n/1000:.1f}K".rstrip("0").rstrip(".")+"K"
        if n < 1_000_000_000: return f"{n/1_000_000:.1f}M".rstrip("0").rstrip(".")+"M"
        return f"{n/1_000_000_000:.1f}B".rstrip("0").rstrip(".")+"B"
    except Exception:
        return str(n)

def parse_time_any(s):
    """Return (epoch, iso) from various Twitter formats (v1 string, v2 ISO, int epoch)."""
    if s is None: return 0, ""
    if isinstance(s, (int,float)):   # epoch seconds
        ep = int(s)
        try:
            iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ep))
        except Exception:
            iso = ""
        return ep, iso
    st = str(s).strip()
    # v2 ISO "2021-11-29T16:39:57.000Z"
    try:
        if st.endswith("Z") and "T" in st:
            # Drop fractional seconds for strptime if needed
            main = st.split(".")[0] + "Z" if "." in st else st
            ep = int(time.mktime(time.strptime(main, "%Y-%m-%dT%H:%M:%SZ")))
            return ep, st
    except Exception:
        pass
    # v1 "Mon Nov 29 16:39:57 +0000 2021"
    try:
        ep = int(time.mktime(time.strptime(st, "%a %b %d %H:%M:%S %z %Y")))
        # convert to Z ISO
        iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ep))
        return ep, iso
    except Exception:
        return 0, ""

# =========================
# NORMALIZATION LAYER
# Accepts: v1, v2, legacy/globalObjects, or nested line containers
# =========================
def is_reaction_line(rec):
    """Skip non-tweet records (emoji/VTT/metadata)."""
    if not isinstance(rec, dict): return True
    # obvious flags
    if rec.get("kind") in ("emoji","reaction","cue","metadata"): return True
    if "emoji" in rec and "e" in rec: return True
    # some crawlers tag VTT/emoji with   {"type":"vtt","track":"metadata",...}
    t = rec.get("type")
    if t in ("emoji","vtt","metadata","captionCue","emote"): return True
    return False

def merge_dict(dst, src):
    for k,v in (src or {}).items():
        dst[k] = v

def normalize_from_v1(t):
    """Classic REST v1.1 tweet object."""
    u = t.get("user") or {}
    e = t.get("entities") or {}
    ee= t.get("extended_entities") or {}
    pm = {
        "reply_count": t.get("reply_count"),
        "retweet_count": t.get("retweet_count"),
        "favorite_count": t.get("favorite_count"),
        "quote_count": t.get("quote_count"),
        "bookmark_count": t.get("bookmark_count"),
        "view_count": t.get("views",{}).get("count") if isinstance(t.get("views"), dict) else t.get("view_count")
    }
    return {
        "id": str(t.get("id_str") or t.get("id") or ""),
        "conversation_id": str(t.get("conversation_id_str") or t.get("conversation_id") or ""),
        "created_at": t.get("created_at"),
        "text": t.get("full_text") or t.get("text") or "",
        "user": {
            "id": str(u.get("id_str") or u.get("id") or ""),
            "name": u.get("name"),
            "screen_name": u.get("screen_name"),
            "profile_image_url": (u.get("profile_image_url_https") or u.get("profile_image_url") or ""),
            "verified": bool(u.get("verified")),
        },
        "entities": e,
        "extended_entities": ee,
        "public_metrics": pm,
        "in_reply_to_status_id": str(t.get("in_reply_to_status_id_str") or t.get("in_reply_to_status_id") or "") or "",
        "is_quote_status": bool(t.get("is_quote_status")),
        "quoted_status_id": str(t.get("quoted_status_id_str") or t.get("quoted_status_id") or "") or "",
        "quoted_status": t.get("quoted_status") or {},
        "retweeted_status_id": str(t.get("retweeted_status_id_str") or t.get("retweeted_status_id") or "") or "",
        "urls": (e.get("urls") or []),
        "_raw": t,
    }

def normalize_from_v2(t, includes):
    """Twitter API v2 format: 'data' tweet + 'includes'."""
    pm = (t.get("public_metrics") or {})
    author_id = str(t.get("author_id") or "")
    u = {}
    for cand in (includes.get("users") or []):
        if str(cand.get("id")) == author_id:
            u = cand or {}
            break
    # attachments -> media
    ee = {}
    media_list = []
    media_keys = set((t.get("attachments") or {}).get("media_keys") or [])
    if media_keys and (includes.get("media")):
        mk = {m.get("media_key"): m for m in includes["media"]}
        for k in media_keys:
            m = mk.get(k)
            if not m: continue
            typ = m.get("type")
            if typ == "photo":
                media_list.append({
                    "id_str": str(m.get("media_key") or ""),
                    "media_url_https": m.get("url"),
                    "type": "photo"
                })
            elif typ in ("video","animated_gif"):
                variants = []
                for v in (m.get("variants") or []):
                    ct = v.get("content_type","")
                    if ct.endswith("mp4") and v.get("url"):
                        variants.append({"bitrate": v.get("bit_rate") or v.get("bitrate"),
                                         "content_type": ct, "url": v["url"]})
                media_list.append({
                    "id_str": str(m.get("media_key") or ""),
                    "type": typ,
                    "video_info": {"variants": variants}
                })
    if media_list:
        ee = {"media": media_list}

    # referenced_tweets for replies/quotes/retweets
    in_reply_to = ""
    quoted_id   = ""
    retweeted_id= ""
    for ref in (t.get("referenced_tweets") or []):
        typ = ref.get("type"); rid = str(ref.get("id") or "")
        if typ == "replied_to": in_reply_to = rid
        elif typ == "quoted":   quoted_id   = rid
        elif typ == "retweeted":retweeted_id= rid

    # entities/urls may carry unwound info in v2
    entities = t.get("entities") or {}

    # user card
    user_card = {
        "id": author_id,
        "name": u.get("name"),
        "screen_name": u.get("username"),
        "profile_image_url": u.get("profile_image_url") or "",
        "verified": bool(u.get("verified") or u.get("verified_type")),
    }

    return {
        "id": str(t.get("id") or ""),
        "conversation_id": str(t.get("conversation_id") or ""),
        "created_at": t.get("created_at"),
        "text": t.get("text") or "",
        "user": user_card,
        "entities": entities,
        "extended_entities": ee,
        "public_metrics": {
            "reply_count": pm.get("reply_count"),
            "retweet_count": pm.get("retweet_count"),
            "favorite_count": pm.get("like_count"),
            "quote_count": pm.get("quote_count"),
            "bookmark_count": pm.get("bookmark_count"),
            "view_count": pm.get("impression_count"),
        },
        "in_reply_to_status_id": in_reply_to,
        "is_quote_status": bool(quoted_id),
        "quoted_status_id": quoted_id,
        "quoted_status": {},  # will fill from includes if present
        "retweeted_status_id": retweeted_id,
        "urls": (entities.get("urls") or []),
        "_raw": t,
    }

def normalize_global_objects(line):
    """legacy 'globalObjects': merge into tweet/user maps."""
    tweets = (line.get("globalObjects") or {}).get("tweets") or {}
    users  = (line.get("globalObjects") or {}).get("users")  or {}
    return tweets, users

def normalize_line(line, agg_tweets, agg_users):
    """
    Accept multiple shapes:
      - v1 tweet at top-level
      - v2 container: {"data": {...}, "includes": {...}}
      - legacy: {"globalObjects": {...}}
      - wrapper: {"tweet": {...}} or {"message": {...}}
    """
    if not isinstance(line, dict):
        return

    # legacy/globalObjects (mass merge, not a single node)
    if "globalObjects" in line:
        tw, us = normalize_global_objects(line)
        for k,v in tw.items():
            agg_tweets[str(k)] = normalize_from_v1(v)
        for k,u in us.items():
            uid = str(u.get("id_str") or u.get("id") or k)
            # minimal user cache — front-end uses what's embedded per-tweet
            agg_users[uid] = {
                "id": uid,
                "name": u.get("name"),
                "screen_name": u.get("screen_name"),
                "profile_image_url": (u.get("profile_image_url_https") or u.get("profile_image_url") or ""),
                "verified": bool(u.get("verified")),
            }
        return

    # v2 shape
    if "data" in line and isinstance(line["data"], dict):
        try:
            norm = normalize_from_v2(line["data"], line.get("includes") or {})
            agg_tweets[norm["id"]] = norm
        except Exception:
            pass
        return

    # nested tweet/message
    for key in ("tweet","message","note","status"):
        if key in line and isinstance(line[key], dict):
            t = line[key]
            # detect v1 vs v2 by keys
            if "created_at" in t and ("full_text" in t or "text" in t):
                agg_tweets[str(t.get("id_str") or t.get("id"))] = normalize_from_v1(t)
            elif "id" in t and "text" in t:
                agg_tweets[str(t.get("id"))] = normalize_from_v2(t, line.get("includes") or {})
            return

    # plain v1 tweet object
    if "created_at" in line and ("full_text" in line or "text" in line):
        agg_tweets[str(line.get("id_str") or line.get("id"))] = normalize_from_v1(line)
        return

    # plain v2 tweet object
    if "id" in line and "text" in line and "author_id" in line:
        agg_tweets[str(line.get("id"))] = normalize_from_v2(line, line.get("includes") or {})
        return

# =========================
# TEXT EXPANSION (t.co)
# =========================
def expand_tco(text, entities):
    """
    Replace t.co URLs in text with expanded URLs; strip media t.co placeholders.
    Use indices when present; fallback to regex on remaining t.co tokens.
    """
    s = text or ""
    urls = list((entities or {}).get("urls") or [])
    media_urls = set()
    for m in (entities or {}).get("media", []) or []:
        if m.get("url"): media_urls.add(m["url"])

    # With indices: build slices safely
    repl = []
    for u in urls:
        tco = u.get("url")
        if not tco: continue
        # skip media placeholders (they'll render as media tiles)
        if tco in media_urls: 
            repl.append((u.get("indices",[0,0])[0], u.get("indices",[0,0])[1], ""))  # remove
            continue
        expanded = u.get("expanded_url") or u.get("unwound_url") or tco
        disp     = u.get("display_url") or expanded
        a = f'<a href="{esc(expanded)}" target="_blank" rel="noopener">{esc(disp)}</a>'
        i0, i1 = (u.get("indices") or [None,None])[:2]
        if isinstance(i0, int) and isinstance(i1, int) and 0 <= i0 <= i1 <= len(s):
            repl.append((i0, i1, a))

    # Apply index-based replacements right-to-left
    repl.sort(key=lambda x: x[0], reverse=True)
    for i0, i1, a in repl:
        s = s[:i0] + a + s[i1:]

    # Fallback: any leftover raw t.co
    def repl_tco(m):
        u = m.group(0)
        return f'<a href="{esc(u)}" target="_blank" rel="noopener">{esc(u)}</a>'
    s = re.sub(r'https?://t\.co/\w+', repl_tco, s)

    return s

# =========================
# MEDIA / LINKS / QUOTES
# =========================
def collect_link_cards(entities):
    out = []
    for u in (entities or {}).get("urls", []) or []:
        exp = (u.get("expanded_url") or u.get("unwound_url") or u.get("url") or "").strip()
        if not exp: continue
        try:
            host = urlparse(exp).netloc.lower()
        except Exception:
            host = ""
        # skip x.com status links here (handled as quote embed)
        if "x.com" in host or "twitter.com" in host:
            continue

        unw = u.get("unwound_url") if isinstance(u.get("unwound_url"), dict) else {}
        images = u.get("images") or (unw.get("images") if isinstance(unw, dict) else None) or []
        thumb = ""
        if isinstance(images, list) and images:
            thumb = images[0].get("url") or images[0].get("src") or ""

        out.append({
            "url": exp,
            "domain": host or (urlparse(exp).netloc if "://" in exp else ""),
            "title": (unw.get("title") if isinstance(unw, dict) else None)
                        or u.get("title") or "",
            "description": (unw.get("description") if isinstance(unw, dict) else None)
                        or u.get("description") or "",
            "image": thumb
        })
    return out

def collect_media(extended_entities):
    """Return normalized media tiles suitable for HTML rendering."""
    out = []
    ee = extended_entities or {}
    for m in (ee.get("media") or []):
        typ = m.get("type")
        base = (m.get("media_url_https") or m.get("media_url") or "").strip()
        if typ == "photo" and base:
            out.append({"type":"photo", "src": base + ("?name=large" if "?" not in base else ""), "alt": m.get("ext_alt_text") or ""})
        elif typ in ("video","animated_gif"):
            info = m.get("video_info") or {}
            variants = []
            for v in (info.get("variants") or []):
                ct = v.get("content_type","")
                if ct.endswith("mp4") and v.get("url"):
                    variants.append((int(v.get("bitrate") or 0), v["url"]))
            if variants:
                variants.sort(key=lambda x: x[0], reverse=True)
                best = variants[0][1]
                thumb = base + "?name=small" if base else ""
                out.append({"type":"video","src":best,"thumb":thumb})
    return out

def detect_quote_url(text, entities, quoted_status_id):
    # Prefer explicit v1 flags
    if quoted_status_id:
        # try to build canonical url if user handle is known in text entities
        for u in (entities or {}).get("urls", []) or []:
            exp = (u.get("expanded_url") or u.get("url") or "")
            if "/status/" in exp:
                return exp
        # fallback: leave blank; frontend still has cards
        return ""
    # otherwise scan links for x.com status
    for u in (entities or {}).get("urls", []) or []:
        exp = (u.get("expanded_url") or u.get("url") or "")
        if re.search(r'https?://(?:x|twitter)\.com/[^/]+/status/\d+', exp):
            return exp
    # fallback: regex in raw text (just in case)
    m = re.search(r'https?://(?:x|twitter)\.com/[^/]+/status/\d+', text or "")
    return m.group(0) if m else ""

# =========================
# THREAD & ROOT
# =========================
def pick_root_id(tweets_by_id, purple_url):
    if purple_url:
        _, rid = parse_purple(purple_url)
        if rid and rid in tweets_by_id:
            return rid
    # try: tweet whose id == conversation_id
    for tid, t in tweets_by_id.items():
        if t.get("conversation_id") and str(tid) == str(t.get("conversation_id")):
            return tid
    # try: earliest with no parent
    candidates = [t for t in tweets_by_id.values() if not t.get("in_reply_to_status_id")]
    if candidates:
        candidates.sort(key=lambda x: parse_time_any(x.get("created_at"))[0] or 0)
        return candidates[0].get("id")
    # fallback: earliest tweet
    all_list = list(tweets_by_id.values())
    if not all_list: return ""
    all_list.sort(key=lambda x: parse_time_any(x.get("created_at"))[0] or 0)
    return all_list[0].get("id")

# =========================
# HTML BUILD
# =========================
def build_reply_html(t, users_cache):
    u = t.get("user") or {}
    name   = u.get("name") or "User"
    handle = u.get("screen_name") or ""
    avatar = (u.get("profile_image_url") or "").replace("_normal", "_bigger")
    verified = bool(u.get("verified"))

    tid     = t.get("id") or ""
    url     = f"https://x.com/{handle}/status/{tid}" if handle and tid else ""
    ep, iso = parse_time_any(t.get("created_at"))

    # Text with t.co expansion
    text_html = expand_tco(t.get("text") or "", t.get("entities") or {})

    # Metrics
    pm = t.get("public_metrics") or {}
    replies_ct = pm.get("reply_count")
    reposts_ct = pm.get("retweet_count")
    likes_ct   = pm.get("favorite_count")
    quotes_ct  = pm.get("quote_count")
    views_ct   = pm.get("view_count")
    marks_ct   = pm.get("bookmark_count")

    # Media, link cards, quote embeds
    media_tiles = collect_media(t.get("extended_entities") or {})
    link_cards  = collect_link_cards(t.get("entities") or {})
    quote_url   = detect_quote_url(t.get("text"), t.get("entities"), t.get("quoted_status_id"))

    # data-* attributes for JS sync & UI
    attrs = {
        "class": "ss3k-reply",
        "data-id": esc(tid),
        "data-handle": esc("@"+handle) if handle else "",
        "data-name": esc(name),
        "data-url": esc(url),
        "data-ts": esc(iso),
        "data-epoch": str(ep or ""),
        "data-parent": esc(t.get("in_reply_to_status_id") or ""),
        "data-conv": esc(t.get("conversation_id") or ""),
        "data-replies": str(replies_ct or ""),
        "data-reposts": str(reposts_ct or ""),
        "data-likes": str(likes_ct or ""),
        "data-quotes": str(quotes_ct or ""),
        "data-views": str(views_ct or ""),
        "data-bookmarks": str(marks_ct or ""),
    }
    attr_s = " ".join(f'{k}="{v}"' for k,v in attrs.items() if v)

    # avatar
    if avatar:
        avatar_tag = f'<div class="avatar-50"><img src="{esc(avatar)}" alt=""></div>'
    else:
        avatar_tag = '<div class="avatar-50"></div>'

    # verified badge (simple dot)
    vbadge = ' <span class="badge" title="Verified"></span>' if verified else ''

    # head (name + handle)
    who = f'{esc(name)} <span class="handle">@{esc(handle)}</span>{vbadge}' if handle else esc(name)

    # metrics bar
    def metric(icon, val):
        if not val and val != 0: return ""
        return f'<span class="metric" data-m="{icon}"><span class="ic {icon}"></span><span>{kfmt(val)}</span></span>'

    tweetbar = (
        '<div class="tweetbar">' +
        metric("reply", replies_ct) +
        metric("repost", reposts_ct) +
        metric("like", likes_ct) +
        (metric("bookmark", marks_ct) if marks_ct else "") +
        (metric("views", views_ct) if views_ct else "") +
        (metric("quote", quotes_ct) if quotes_ct else "") +
        '</div>'
    )

    # date link (bottom-right)
    when_label = ""
    if ep:
        try:
            when_label = time.strftime("%Y-%b-%d %H:%M", time.localtime(ep))
        except Exception:
            when_label = iso or "Open on X"
    linkx = f'<span class="linkx"><a target="_blank" rel="noopener" href="{esc(url)}">{esc(when_label or "Open on X")}</a></span>'

    # media grid
    media_html = ""
    if media_tiles:
        cols = min(len(media_tiles), 4)
        buf = [f'<div class="tweet-media cols-{cols}">']
        for m in media_tiles:
            if m["type"] == "photo":
                buf.append(f'<a class="tile" href="{esc(m["src"])}" target="_blank" rel="noopener"><img src="{esc(m["src"])}" alt="{esc(m.get("alt",""))}"></a>')
            elif m["type"] == "video":
                v = f'<div class="tile tweet-video"><video controls playsinline preload="metadata"{(" poster="+esc(m["thumb"])) if m.get("thumb") else ""}><source src="{esc(m["src"])}" type="video/mp4"></video></div>'
                buf.append(v)
        buf.append('</div>')
        media_html = "".join(buf)

    # quote tweet embed (official blockquote)
    quote_html = ""
    if quote_url:
        quote_html = f'<blockquote class="twitter-tweet"><a href="{esc(quote_url)}"></a></blockquote>'

    # external link cards
    cards_html = ""
    if link_cards:
        tmp = []
        for lc in link_cards:
            thumb = f'<div class="thumb"><img src="{esc(lc["image"])}" alt=""></div>' if lc.get("image") else '<div class="thumb"></div>'
            meta  = (
                f'<div class="meta">'
                f'<div class="domain">{esc(lc.get("domain",""))}</div>'
                f'<div class="title">{esc(lc.get("title") or lc["url"])}</div>' +
                (f'<div class="desc">{esc(lc.get("description",""))}</div>' if lc.get("description") else "") +
                f'</div>'
            )
            tmp.append(f'<a href="{esc(lc["url"])}" target="_blank" rel="noopener"><div class="link-card">{thumb}{meta}</div></a>')
        cards_html = "".join(tmp)

    body_html = f'<div class="body">{text_html}</div>' + (media_html or "") + (quote_html or "") + (cards_html or "")

    return (
        f'<div {attr_s}>'
        f'{avatar_tag}'
        f'<div><div class="head"><span class="disp">{who}</span></div>'
        f'{body_html}'
        f'{tweetbar}'
        f'{linkx}'
        f'</div></div>'
    )

# =========================
# LINKS OUTPUT (grouped by domain)
# =========================
def extract_all_urls(t):
    out = set()
    for u in (t.get("entities") or {}).get("urls", []) or []:
        exp = (u.get("expanded_url") or u.get("unwound_url") or u.get("url") or "").strip()
        if exp: out.add(exp)
    return out

def build_links_html(replies):
    doms = defaultdict(set)
    for t in replies:
        for u in extract_all_urls(t):
            try:
                dom = urlparse(u).netloc or "links"
            except Exception:
                dom = "links"
            doms[dom].add(u)
    lines = []
    for dom in sorted(doms):
        lines.append(f"<h4>{esc(dom)}</h4>")
        lines.append("<ul>")
        for u in sorted(doms[dom]):
            e = esc(u)
            lines.append(f'<li><a href="{e}" target="_blank" rel="noopener">{e}</a></li>')
        lines.append("</ul>")
    return "\n".join(lines)

# =========================
# WORDPRESS PATCH (optional)
# =========================
def wp_patch(html_str):
    if not (WP_BASE and WP_POST_ID and (WP_AUTH)):
        return False, "missing creds"
    url = f"{WP_BASE}{WP_ENDPOINT}"
    payload = {
        "post_id": WP_POST_ID,
        "fields": {
            "ss3k_replies_html": html_str
        }
    }
    data = json.dumps(payload).encode("utf-8")
    hdr = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "ss3k-replies-bot/1.0",
        "Authorization": WP_AUTH
    }
    try:
        req = Request(url, data=data, headers=hdr, method="POST")
        with urlopen(req, timeout=45) as r:
            body = r.read().decode("utf-8","ignore")
            if r.status >= 200 and r.status < 300:
                log(f"Patched WP replies HTML for post_id={WP_POST_ID}")
                if SAVE_JSON_DEBUG:
                    with open(os.path.join(DBG_DIR, f"{BASE}_patch_response.json"), "w", encoding="utf-8") as f:
                        f.write(body)
                return True, "ok"
            return False, f"http {r.status} body={body[:200]}"
    except Exception as e:
        log(f"WP PATCH error: {e}")
        return False, str(e)

# =========================
# MAIN
# =========================
def main():
    try:
        ensure_dirs()

        src = discover_inputs()
        if not src:
            write_empty("no crawler JSONL discovered")
            print("")  # no stdout content
            return

        log(f"Using crawler input: {src}")

        tweets_by_id = {}   # id -> normalized tweet
        users_cache  = {}   # optional user info if we see globalObjects

        cnt_total, cnt_used = 0, 0

        with open(src, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                cnt_total += 1
                try:
                    rec = json.loads(line)
                except Exception:
                    # Sometimes the file may contain "raw:" prefixes; best effort parse
                    try:
                        jpos = line.find("{")
                        if jpos >= 0:
                            rec = json.loads(line[jpos:])
                        else:
                            continue
                    except Exception:
                        continue

                if is_reaction_line(rec):
                    continue

                normalize_line(rec, tweets_by_id, users_cache)

        log(f"Parsed lines: total={cnt_total} tweets={len(tweets_by_id)} users_cache={len(users_cache)}")

        if not tweets_by_id:
            write_empty("no tweets parsed from crawler")
            print("")  # stdout
            return

        # Root detection & thread filter
        root_id = pick_root_id(tweets_by_id, PURPLE)
        log(f"Root detection: root_id={root_id}")

        # Exclude root itself from "replies" but keep in thread context
        replies = []
        for tid, t in tweets_by_id.items():
            # remove retweets / pure RT nodes
            if t.get("retweeted_status_id"):
                continue
            # keep same conversation
            conv = str(t.get("conversation_id") or "")
            if root_id and conv and conv != str(tweets_by_id.get(root_id,{}).get("conversation_id") or conv):
                # If we know root, require same conversation id
                continue
            if str(tid) == str(root_id):
                continue
            # only real tweets/messages
            replies.append(t)

        # Dedup by id
        uniq = {}
        for t in replies:
            tid = t.get("id")
            if tid: uniq[tid] = t
        replies = list(uniq.values())
        replies.sort(key=lambda x: parse_time_any(x.get("created_at"))[0] or 0)

        log(f"Total replies retained: {len(replies)}")

        # Build replies HTML
        blocks = []
        for t in replies:
            try:
                blocks.append(build_reply_html(t, users_cache))
                cnt_used += 1
            except Exception as e:
                log(f"build_html error for id={t.get('id')}: {e}")

        replies_html = "\n".join(blocks)
        links_html   = build_links_html(replies)

        open(OUT_REPLIES, "w", encoding="utf-8").write(replies_html)
        open(OUT_LINKS,   "w", encoding="utf-8").write(links_html)
        log(f"Wrote: replies={OUT_REPLIES} ({cnt_used}) links={OUT_LINKS}")

        # Also print to stdout for no-creds workflow
        try:
            sys.stdout.write(replies_html)
            sys.stdout.flush()
        except Exception:
            pass

        # Optionally patch WordPress
        if WP_BASE and WP_AUTH and WP_POST_ID:
            ok, msg = wp_patch(replies_html)
            log(f"WP patch result: ok={ok} msg={msg}")

        # Optional: save combined JSON for debugging
        if SAVE_JSON_DEBUG:
            dbg_path = os.path.join(DBG_DIR, f"{BASE}_replies_parsed.json")
            with open(dbg_path, "w", encoding="utf-8") as fdbg:
                json.dump({"root_id": root_id, "tweets": list(replies)}, fdbg, ensure_ascii=False)
            log(f"Saved debug JSON: {dbg_path}")

    except Exception as e:
        log(f"FATAL: {e}\n{traceback.format_exc()}")
        write_empty(f"fatal: {e}")
        try:
            print("")  # no stdout
        except Exception:
            pass

if __name__ == "__main__":
    main()
