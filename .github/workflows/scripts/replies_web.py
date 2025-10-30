#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
replies_web.py — resilient replies/links builder for Space Worker

- Discovers crawler outputs automatically (JSONL or debug page JSON/HTML).
- Ignores emoji/VTT rows; keeps only real tweet/reply/message nodes.
- Handles v1/v2/legacy shapes (ids, user, text, created_at, public_metrics, entities, extended_entities).
- Expands t.co, removes media placeholders, builds content cards, embeds quote tweets.
- Emits clean, discussion-board HTML (50x50 avatars, alternating rows) with data attributes for frontend sync.
- Prints HTML to stdout (no-creds friendly) and also writes:
    {ARTDIR}/{BASE}_replies.html
    {ARTDIR}/{BASE}_links.html
    {ARTDIR}/{BASE}_replies.log
- If WP creds exist, POSTs to /wp-json/ss3k/v1/patch-assets (field: ss3k_replies_html, shared_links_html).
"""

import os, re, sys, json, html, math, time, traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ----------------- Env -----------------
ARTDIR = Path(os.environ.get("ARTDIR", "."))
BASE   = os.environ.get("BASE", "space")
PURPLE = os.environ.get("PURPLE_TWEET_URL", "").strip()

WP_BASE_URL     = (os.environ.get("WP_BASE_URL") or "").strip()
WP_USER         = (os.environ.get("WP_USER") or "").strip()
WP_APP_PASSWORD = (os.environ.get("WP_APP_PASSWORD") or "").strip()

# Optionally injected by other steps
START_ISO = (os.environ.get("START_ISO") or "").strip()

# Explicit inputs (if the crawler wrote them)
REPLIES_JSONL = (os.environ.get("REPLIES_JSONL") or
                 os.environ.get("CRAWLER_REPLIES_JSONL") or "").strip()

# WORKDIR for recursive discovery fallback
WORKDIR = Path(os.environ.get("WORKDIR") or ARTDIR)

# Files we will write
OUT_REPLIES = ARTDIR / f"{BASE}_replies.html"
OUT_LINKS   = ARTDIR / f"{BASE}_links.html"
LOG_PATH    = ARTDIR / f"{BASE}_replies.log"
DBG_DIR     = ARTDIR / "debug"

# --------------- Logging ---------------
def log(msg: str):
    ts = time.strftime("[%Y-%m-%d %H:%M:%SZ]", time.gmtime())
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(f"{ts} {msg}\n")
    except Exception:
        pass

def die_empty(reason: str):
    log(f"Wrote empty outputs: {reason}")
    OUT_REPLIES.write_text("<!-- no replies: {} -->".format(reason), encoding="utf-8")
    OUT_LINKS.write_text("<!-- no links: {} -->".format(reason), encoding="utf-8")
    print(OUT_REPLIES.read_text(encoding="utf-8"))
    sys.exit(0)

# --------------- Utils -----------------
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

def human_k(n: Optional[int]) -> str:
    try:
        v = int(n or 0)
    except Exception:
        v = 0
    if v < 1000: return str(v)
    if v < 10000: return f"{v/1000:.1f}K".rstrip("0").rstrip(".") + "K"
    if v < 1_000_000: return f"{v//1000}K"
    if v < 10_000_000: return f"{v/1_000_000:.1f}M".rstrip("0").rstrip(".") + "M"
    return f"{v//1_000_000}M"

def parse_epoch_maybe(x: Any) -> Optional[float]:
    if x is None: return None
    try:
        v = float(x)
    except Exception:
        return None
    if v > 1e12: v = v / 1000.0
    return v

def parse_time_any(created_at: Any) -> Optional[float]:
    if created_at is None: return None
    s = str(created_at).strip()
    if re.fullmatch(r"\d{10,13}", s):
        return parse_epoch_maybe(s)
    try:
        if s.endswith("Z"): dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        elif re.search(r"[+-]\d{2}:?\d{2}$", s):
            if re.search(r"[+-]\d{4}$", s):
                s = s[:-5] + s[-5:-3] + ":" + s[-3:]
            dt = datetime.fromisoformat(s)
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).timestamp()
    except Exception:
        pass
    try:
        return datetime.strptime(s, "%a, %d %b %Y %H:%M:%S %z").timestamp()
    except Exception:
        return None

def load_start_epoch() -> Optional[float]:
    if START_ISO:
        e = parse_time_any(START_ISO)
        if e: return e
    p = ARTDIR / f"{BASE}.start.txt"
    if p.exists():
        try:
            txt = p.read_text(encoding="utf-8").strip()
            return parse_time_any(txt)
        except Exception:
            return None
    return None

def make_status_url(author_handle: str, tweet_id: str) -> Optional[str]:
    h = (author_handle or "").lstrip("@")
    i = (tweet_id or "").strip()
    if not h or not i: return None
    return f"https://x.com/{h}/status/{i}"

TCO_RE = re.compile(r"https?://t\.co/[A-Za-z0-9]+")
STATUS_RE = re.compile(r"https?://(?:x|twitter)\.com/[^/]+/status/\d+")
HTTP_RE = re.compile(r"https?://[^\s<]+", re.I)

# ----------------- Discovery -----------------
def find_jsonl_candidates() -> List[Path]:
    cands: List[Path] = []
    names = [
        f"{BASE}_replies.jsonl", f"{BASE}-replies.jsonl", f"{BASE}.replies.jsonl",
        f"{BASE}_conversation.jsonl", f"{BASE}_thread.jsonl",
    ]
    for nm in names:
        p = ARTDIR / nm
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            cands.append(p)
    for pat in [f"{BASE}*replies*.jsonl", "*replies*.jsonl", "*conversation*.jsonl", "*thread*.jsonl"]:
        for p in ARTDIR.glob(pat):
            if p.is_file() and p.stat().st_size > 0:
                cands.append(p)
    for p in WORKDIR.rglob("*replies*.jsonl"):
        if p.is_file() and p.stat().st_size > 0:
            cands.append(p)
    seen = set(); out=[]
    for p in cands:
        rp = p.resolve()
        if rp in seen: continue
        seen.add(rp); out.append(p)
    return out

def find_jsonl() -> Optional[Path]:
    if REPLIES_JSONL:
        p = Path(REPLIES_JSONL)
        if p.exists() and p.is_file() and p.stat().st_size > 0: return p
    cands = find_jsonl_candidates()
    return cands[0] if cands else None

def iter_debug_pages():
    pages = []
    if DBG_DIR.exists():
        pages.extend(sorted(DBG_DIR.glob(f"{BASE}_replies_page*.json")))
        pages.extend(sorted(DBG_DIR.glob(f"{BASE}_page*.json")))
        pages.extend(sorted(DBG_DIR.glob("*.json")))
        pages.extend(sorted(DBG_DIR.glob(f"{BASE}_replies_page*.html")))
        pages.extend(sorted(DBG_DIR.glob("*.html")))

    searched = [str(p) for p in pages]
    if searched: log("Debug pages considered: " + ", ".join(searched[:10]) + (" ..." if len(searched)>10 else ""))

    for p in pages:
        txt = p.read_text(encoding="utf-8", errors="ignore")
        try:
            if p.suffix.lower() == ".json":
                doc = json.loads(txt)
                yield from _walk_any(doc)
            else:
                blobs = re.findall(r"(<script[^>]*>)([\s\S]{100,}?)(</script>)", txt, re.I)
                for _, body, _ in blobs:
                    body = body.strip()
                    try:
                        j = json.loads(body)
                        yield from _walk_any(j)
                        continue
                    except Exception:
                        pass
                    m = re.search(r"=\s*({[\s\S]+?})\s*;?\s*$", body)
                    if m:
                        try:
                            j = json.loads(m.group(1))
                            yield from _walk_any(j); continue
                        except Exception:
                            pass
        except Exception:
            continue

def _walk_any(doc):
    stack = [doc]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if "entries" in cur and isinstance(cur["entries"], list):
                for e in cur["entries"]:
                    yield e
            if "tweet_results" in cur and isinstance(cur["tweet_results"], dict):
                yield cur
            for v in cur.values():
                if isinstance(v, (dict, list)): stack.append(v)
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)): stack.append(v)

# ----------------- Extraction -----------------
def first(*vals):
    for v in vals:
        if v not in (None, "", [], {}): return v
    return None

def take_user(u: Dict[str, Any]) -> Tuple[str, str, str]:
    if not isinstance(u, dict): u = {}
    name = first(u.get("name"), u.get("legacy", {}).get("name"), u.get("display_name"), "User")
    sn   = first(u.get("screen_name"), u.get("legacy", {}).get("screen_name"), u.get("username"), "")
    av   = first(u.get("profile_image_url_https"),
                 u.get("profile_image_url"),
                 u.get("legacy", {}).get("profile_image_url_https"),
                 "")
    return str(name or "User"), str(sn or ""), str(av or "")

def unwrap_entities(obj: Dict[str, Any]) -> Dict[str, Any]:
    ent = first(obj.get("entities"), obj.get("legacy", {}).get("entities"), {}) or {}
    ext = first(obj.get("extended_entities"), obj.get("legacy", {}).get("extended_entities"), {}) or {}
    return {"entities": ent, "extended_entities": ext}

def expand_tco(text: str, entities: Dict[str, Any]) -> Tuple[str, List[Dict[str, Any]], List[str]]:
    s = text or ""
    links: List[Dict[str, Any]] = []
    removed_media_urls: List[str] = []
    for m in (entities.get("media") or []):
        u = first(m.get("url"), m.get("expanded_url"))
        if u: removed_media_urls.append(u)
    repls: List[Tuple[int, int, str]] = []
    for u in (entities.get("urls") or []):
        tco = u.get("url") or ""
        if not tco: continue
        start, end = None, None
        if isinstance(u.get("indices"), list) and len(u["indices"]) == 2:
            start, end = int(u["indices"][0]), int(u["indices"][1])
        expanded = first(u.get("unwound_url"), u.get("expanded_url"), u.get("display_url"), tco)
        if not expanded: expanded = tco
        card = {
            "url": expanded,
            "title": first(u.get("title"), u.get("unwound_title")),
            "description": first(u.get("description"), u.get("unwound_description")),
            "images": first(u.get("images"), []),
        }
        links.append(card)
        if start is not None:
            repls.append((start, end, expanded))
    if repls:
        repls.sort(key=lambda x: x[0], reverse=True)
        for st, en, rep in repls:
            if 0 <= st < en <= len(s):
                s = s[:st] + rep + s[en:]
    s = TCO_RE.sub(lambda m: m.group(0), s)
    for mu in removed_media_urls:
        s = s.replace(mu, "")
    s = re.sub(r"\s+", " ", s).strip()
    return s, links, removed_media_urls

def collect_inline_media(extended_entities: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
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

def pick_id(d: Dict[str, Any]) -> Optional[str]:
    return str(first(d.get("rest_id"), d.get("id_str"), d.get("id"), None) or "" ) or None

def is_valid_text(s: str) -> bool:
    if not s or not s.strip(): return False
    if is_emoji_only(s): return False
    return True

def walk_debug_entry(e: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(e, dict): return None
    cur = e
    keys = [
        ("content","itemContent","tweet_results","result"),
        ("item","itemContent","tweet_results","result"),
        ("itemContent","tweet_results","result"),
        ("content","tweetResult","result"),
    ]
    for path in keys:
        node = cur
        ok = True
        for k in path:
            if isinstance(node, dict) and k in node:
                node = node[k]
            else:
                ok = False; break
        if ok and isinstance(node, dict):
            return node
    if "tweet_results" in e and isinstance(e["tweet_results"], dict):
        return e
    return None

def normalize_tweet(t: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(t, dict): return None
    core = t
    leg  = first(core.get("legacy"), core.get("tweet"), {})
    user = first(core.get("core", {}).get("user_results", {}).get("result"),
                 core.get("author"), core.get("user"), {})
    name, screen_name, avatar = take_user(user)

    tid = pick_id(core) or pick_id(leg or {}) or None
    text = first(leg.get("full_text") if isinstance(leg, dict) else None,
                 core.get("text"),
                 core.get("note_tweet", {}).get("note_tweet_results", {}).get("result", {}).get("text"))
    if not is_valid_text(text): return None

    ents = unwrap_entities(leg if isinstance(leg, dict) else core)
    created = first(leg.get("created_at") if isinstance(leg, dict) else None,
                    core.get("created_at"),
                    core.get("legacy", {}).get("created_at"),
                    core.get("timestamp_ms"),
                    core.get("createdAt"),
                    core.get("created_at_secs"))
    created_epoch = parse_time_any(created)

    pm = first(core.get("legacy", {}),
               core.get("metrics"),
               core.get("public_metrics"),
               {})
    fav = first(pm.get("favorite_count"), pm.get("like_count"), 0)
    rt  = first(pm.get("retweet_count"), pm.get("repost_count"), pm.get("repost_count_result"), 0)
    rep = first(pm.get("reply_count"), 0)
    quo = first(pm.get("quote_count"), 0)

    in_reply_to = str(first(leg.get("in_reply_to_status_id_str") if isinstance(leg, dict) else None,
                            core.get("in_reply_to_status_id_str"), core.get("in_reply_to_status_id")) or "")

    text_expanded, link_cards, _media_tcos = expand_tco(text, ents["entities"])
    media_items = collect_inline_media(ents["extended_entities"])

    quote_url = None
    m = STATUS_RE.search(text_expanded or "")
    if m: quote_url = m.group(0)

    return {
        "id": tid,
        "conv_id": str(first(core.get("conversation_id_str"), core.get("conversation_id"), "")),
        "parent_id": in_reply_to or "",
        "text": text_expanded,
        "raw_text": text,
        "name": name,
        "handle": screen_name,
        "avatar": avatar,
        "created_epoch": created_epoch,
        "created_raw": created,
        "like_count": int(fav or 0),
        "retweet_count": int(rt or 0),
        "reply_count": int(rep or 0),
        "quote_count": int(quo or 0),
        "status_url": make_status_url(screen_name, tid) if tid else None,
        "link_cards": link_cards,
        "media": media_items,
        "quote_url": quote_url,
    }

def load_from_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = line.strip()
            if not line: continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if isinstance(obj, dict) and isinstance(obj.get("payload"), str):
                try:
                    pl = json.loads(obj["payload"])
                    if isinstance(pl, dict) and isinstance(pl.get("body"), str):
                        obj = json.loads(pl["body"])
                except Exception:
                    pass
            cand = obj
            if isinstance(cand, dict) and "result" in cand:
                cand = cand["result"]
            if isinstance(cand, dict) and "tweet_results" in cand:
                cand = cand.get("tweet_results", {}).get("result", cand)
            norm = normalize_tweet(cand) if isinstance(cand, dict) else None
            if norm: out.append(norm)
    return out

def load_from_debug() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for e in iter_debug_pages():
        t = walk_debug_entry(e)
        if not t: continue
        norm = normalize_tweet(t)
        if norm: out.append(norm)
    return out

# ----------------- Build HTML -----------------
def fmt_local_ts(epoch: Optional[float]) -> Tuple[str, str]:
    if not epoch: return ("", "")
    dt = datetime.utcfromtimestamp(epoch).replace(tzinfo=timezone.utc)
    disp = dt.strftime("%Y-%m-%d %H:%M UTC")
    iso  = dt.isoformat().replace("+00:00", "Z")
    return disp, iso

def render_link_cards(cards: List[Dict[str, Any]]) -> str:
    parts = []
    seen = set()
    for c in cards or []:
        u = (c.get("url") or "").strip()
        if not u or u in seen: continue
        seen.add(u)
        title = c.get("title") or ""
        desc  = c.get("description") or ""
        dom   = ""
        try:
            dom = re.sub(r"^https?://(www\.)?([^/]+).*$", r"\2", u, flags=re.I)
        except Exception:
            pass
        img  = None
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

def render_media(ms: List[Dict[str, Any]]) -> str:
    out = []
    for m in ms or []:
        if m.get("kind") == "img":
            out.append(f'<figure class="m"><img loading="lazy" src="{esc(m["src"])}" alt=""></figure>')
        elif m.get("kind") == "video" and m.get("src"):
            out.append(
                '<figure class="m"><video controls playsinline preload="metadata">'
                f'<source src="{esc(m["src"])}" type="video/mp4"></video></figure>')
    return "".join(out)

def render_reply_item(it: Dict[str, Any], start_epoch: Optional[float]) -> str:
    disp, iso = fmt_local_ts(it.get("created_epoch"))
    data_begin = ""
    if start_epoch and it.get("created_epoch"):
        rel = max(0.0, float(it["created_epoch"] - start_epoch))
        data_begin = f' data-begin="{rel:.3f}"'
    av = it.get("avatar") or ""
    handle = it.get("handle") or ""
    name   = it.get("name") or ""
    status = it.get("status_url") or ""
    stats = (
        f'<div class="stats"><span title="Replies">💬 {human_k(it.get("reply_count"))}</span>'
        f' <span title="Reposts">🔁 {human_k(it.get("retweet_count"))}</span>'
        f' <span title="Likes">❤️ {human_k(it.get("like_count"))}</span>'
        f'{(" <span title=\"Quotes\">🔗 " + human_k(it.get("quote_count")) + "</span>") if (it.get("quote_count") or 0)>0 else ""}</div>'
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
    body = (
        f'<div class="body">'
        f'  <div class="head"><span class="nm">{esc(name)}</span> <span class="hn">@{esc(handle)}</span></div>'
        f'  <div class="tx">{esc(it.get("text") or "")}</div>'
        f'  {render_media(it.get("media") or [])}'
        f'  {render_link_cards(it.get("link_cards") or [])}'
        f'  {quote}'
        f'  <div class="meta">'
        f'    <span class="when">{("<a href=\"%s\" target=\"_blank\" rel=\"noopener\">%s</a>" % (esc(status), esc(disp))) if status and disp else esc(disp)}</span>'
        f'    {stats}'
        f'  </div>'
        f'</div>'
    )
    avatar_html = (
        f'<div class="ss3k-avatar">'
        f'  {("<img src=\"%s\" alt=\"\">" % esc(av)) if av else ("<span>" + esc((handle or "U")[:1].upper()) + "</span>")}'
        f'</div>'
    )
    return (
        f'<div class="ss3k-reply" data-id="{esc(it.get("id") or "")}" data-parent="{esc(it.get("parent_id") or "")}" '
        f'data-conv="{esc(it.get("conv_id") or "")}" data-ts="{esc(it.get("created_raw") or "")}"{data_begin}>'
        f'{avatar_html}{body}</div>'
    )

# ----------------- MAIN -----------------
def main():
    ARTDIR.mkdir(parents=True, exist_ok=True)
    start_epoch = load_start_epoch()

    log("ENV:")
    for k in ("ARTDIR","BASE","PURPLE","WP_BASE_URL","WP_USER"):
        log(f"  {k}={globals().get(k)}")

    tweets: List[Dict[str, Any]] = []
    searched_files: List[str] = []

    src = None
    if REPLIES_JSONL:
        p = Path(REPLIES_JSONL)
        if p.exists() and p.is_file() and p.stat().st_size > 0:
            src = p
    if not src:
        cands = find_jsonl_candidates()
        searched_files.extend([str(c) for c in cands])
        src = cands[0] if cands else None

    if src:
        log(f"Using JSONL: {src}")
        tweets = load_from_jsonl(src)
    else:
        log("No JSONL candidate found.")

    if not tweets:
        log("No replies from JSONL; trying debug pages fallback…")
        tweets = load_from_debug()

    if not tweets:
        if PURPLE:
            log("No replies parsed; writing Purple-pill only placeholder.")
            if searched_files:
                log("Searched JSONL candidates: " + ", ".join(searched_files[:12]) + (" ..." if len(searched_files)>12 else ""))
            OUT_REPLIES.write_text(
                f'<div class="ss3k-replies"><p><a href="{html.escape(PURPLE)}" target="_blank" rel="noopener">Open conversation on X</a></p></div>',
                encoding="utf-8"
            )
            OUT_LINKS.write_text("<!-- no links: replies empty -->", encoding="utf-8")
            print(OUT_REPLIES.read_text(encoding="utf-8"))
            return
        die_empty("no crawler JSONL or debug pages discovered")

    tweets = [t for t in tweets if t.get("created_epoch")]
    tweets.sort(key=lambda x: (x.get("created_epoch") or 0, x.get("id") or ""))

    log(f"Collected replies: {len(tweets)}")

    rows = [render_reply_item(t, start_epoch) for t in tweets]
    replies_html = (
        CSS_INLINE +
        '<section class="ss3k-replies-list" id="ss3k-replies">\n' +
        "\n".join(rows) + "\n</section>\n"
    )
    OUT_REPLIES.write_text(replies_html, encoding="utf-8")
    print(replies_html)

    link_cards_flat: List[Dict[str, Any]] = []
    for t in tweets:
        for c in (t.get("link_cards") or []):
            if isinstance(c, dict) and c.get("url"):
                link_cards_flat.append(c)

    seen = set()
    link_items = []
    for c in link_cards_flat:
        u = c.get("url")
        if not u or u in seen: continue
        seen.add(u)
        link_items.append(
            f'<li class="link">{render_link_cards([c])}</li>'
        )

    if link_items:
        OUT_LINKS.write_text(
            '<ul class="ss3k-links">\n' + "\n".join(link_items) + "\n</ul>\n",
            encoding="utf-8"
        )
    else:
        OUT_LINKS.write_text("<!-- no links: none extracted from replies -->", encoding="utf-8")

    pid = (os.environ.get("POST_ID") or os.environ.get("WP_POST_ID") or "").strip()
    if pid and WP_BASE_URL and WP_USER and WP_APP_PASSWORD:
        try:
            import requests
            body = {
                "post_id": int(pid),
                "status": "complete",
                "progress": 100,
                "ss3k_replies_html": OUT_REPLIES.read_text(encoding="utf-8"),
                "shared_links_html": OUT_LINKS.read_text(encoding="utf-8"),
            }
            url = WP_BASE_URL.rstrip("/") + "/wp-json/ss3k/v1/patch-assets"
            r = requests.post(url, json=body, auth=(WP_USER, WP_APP_PASSWORD), timeout=20)
            log(f"WP patch status={r.status_code}")
        except Exception as e:
            log(f"WP patch failed: {e}")

# -------- scoped CSS (discussion board, dark-mode friendly) --------
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

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        log("ERROR: " + "".join(traceback.format_exception(e)))
        die_empty("exception during replies build")
