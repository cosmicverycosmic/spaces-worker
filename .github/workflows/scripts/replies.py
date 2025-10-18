#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
replies.py — Build threaded, time-sorted replies HTML + a grouped links list
Inputs (env):
  ARTDIR               - output directory (default ".")
  BASE                 - base filename (default "space")
  PURPLE_TWEET_URL     - root tweet URL to thread (optional)
  TWITTER_AUTHORIZATION (optional) - "Bearer xxxxx"
  TWITTER_AUTH_TOKEN     (optional) - cookie auth_token value
  TWITTER_CSRF_TOKEN     (optional) - cookie ct0 value

Outputs:
  {BASE}_replies.html   - threaded conversation
  {BASE}_links.html     - links grouped by domain with friendly names

Notes:
- Prefers cookie auth (auth_token + ct0). Falls back to guest token with Bearer.
- Uses /2/timeline/conversation/<id>.json, reads v1-style entities under globalObjects.
- Robust parent stitching: if a tweet replies to a missing parent, it attaches under the nearest known ancestor; if none found, under root.
- Expands t.co links, linkifies URLs, @mentions, and #hashtags. Preserves newlines as <br>.
- Collects external links (expanded_url/unwound_url/media) across the conversation and renders a grouped list.
"""

import os
import re
import sys
import json
import html
import time
from collections import defaultdict

try:
    import tldextract  # optional; installed in workflow
except Exception:
    tldextract = None


# ---------- Config / I/O ----------

ARTDIR = os.environ.get("ARTDIR", ".")
BASE = os.environ.get("BASE", "space")
PURPLE = (os.environ.get("PURPLE_TWEET_URL", "") or "").strip()

REPLIES_OUT = os.path.join(ARTDIR, f"{BASE}_replies.html")
LINKS_OUT = os.path.join(ARTDIR, f"{BASE}_links.html")


def write_empty():
    os.makedirs(ARTDIR, exist_ok=True)
    open(REPLIES_OUT, "w", encoding="utf-8").write("")
    open(LINKS_OUT, "w", encoding="utf-8").write("")


def esc(x: str) -> str:
    return html.escape(x or "")


# ---------- HTTP helpers (urllib only) ----------

def http_json(url, method="GET", headers=None, data=None, timeout=30):
    import urllib.request
    req = urllib.request.Request(url=url, method=method)
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    body = data.encode("utf-8") if isinstance(data, str) else data
    try:
        with urllib.request.urlopen(req, data=body, timeout=timeout) as r:
            txt = r.read().decode("utf-8", "ignore")
            return json.loads(txt)
    except Exception:
        return None


def get_guest_token(bearer: str):
    if not bearer:
        return None
    headers = {
        "authorization": bearer,
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0",
    }
    d = http_json(
        "https://api.twitter.com/1.1/guest/activate.json",
        method="POST",
        headers=headers,
        data="{}",
    )
    return (d or {}).get("guest_token")


def fetch_conversation(tid_str: str):
    """
    Return (tweets, users) dicts from Twitter web API, or None on failure.
    Chooses cookie auth if available, else bearer+guest.
    """
    bearer = (os.environ.get("TWITTER_AUTHORIZATION", "") or "").strip()
    at = (os.environ.get("TWITTER_AUTH_TOKEN", "") or "").strip()
    ct0 = (os.environ.get("TWITTER_CSRF_TOKEN", "") or "").strip()

    headers = {
        "user-agent": "Mozilla/5.0",
        "accept": "application/json, text/plain, */*",
    }

    # Prefer cookie-based (more reliable)
    if at and ct0:
        if bearer:
            headers["authorization"] = bearer
        headers["x-csrf-token"] = ct0
        headers["cookie"] = f"auth_token={at}; ct0={ct0}"
    elif bearer:
        gt = get_guest_token(bearer)
        if not gt:
            return None
        headers["authorization"] = bearer
        headers["x-guest-token"] = gt
    else:
        return None

    url = f"https://api.twitter.com/2/timeline/conversation/{tid_str}.json?tweet_mode=extended&include_ext_alt_text=true"
    data = http_json(url, headers=headers)
    if not data:
        return None

    tweets = (data.get("globalObjects", {}) or {}).get("tweets", {}) or {}
    users = (data.get("globalObjects", {}) or {}).get("users", {}) or {}
    return (tweets, users) if tweets else None


# ---------- Text expansion / linkification ----------

HTTP_RE = re.compile(r"(https?://[^\s<>'\")]+)")
MENTION_RE = re.compile(r"(?<!\w)@([A-Za-z0-9_]{1,15})")
HASH_RE = re.compile(r"(?<!\w)#([A-Za-z0-9_]{2,60})")


def expand_text_with_entities(text: str, entities: dict) -> str:
    """
    1) Replace t.co with expanded/unwound URLs.
    2) Escape HTML.
    3) Linkify URLs, @mentions, #hashtags.
    4) Preserve newlines with <br>.
    """
    text = text or ""
    entities = entities or {}

    # Step 1: Replace t.co short links with expanded or unwound URLs
    urls = entities.get("urls") or []
    # Replace longer short URLs first to avoid partial collisions
    for u in sorted(urls, key=lambda x: len(x.get("url", "")), reverse=True):
        short = u.get("url") or ""
        expanded = u.get("expanded_url") or u.get("unwound_url") or short
        if short and expanded and short in text:
            text = text.replace(short, expanded)

    # Step 2: Escape
    out = esc(text)

    # Step 3a: Linkify URLs
    def repl_url(m):
        u = m.group(1)
        return f'<a href="{esc(u)}" target="_blank" rel="noopener">{esc(u)}</a>'

    out = HTTP_RE.sub(repl_url, out)

    # Step 3b: Linkify @mentions (avoid replacing inside URLs by running after URL linkification)
    def repl_mention(m):
        handle = m.group(1)
        return f'<a href="https://x.com/{esc(handle)}" target="_blank" rel="noopener">@{esc(handle)}</a>'

    out = MENTION_RE.sub(repl_mention, out)

    # Step 3c: Linkify #hashtags
    def repl_hash(m):
        tag = m.group(1)
        return f'<a href="https://x.com/hashtag/{esc(tag)}" target="_blank" rel="noopener">#{esc(tag)}</a>'

    out = HASH_RE.sub(repl_hash, out)

    # Step 4: Preserve line breaks
    out = out.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")
    return out


# ---------- Link bucket / pretty names ----------

def add_link(bucket: dict, url: str):
    if not url:
        return
    try:
        # rudimentary sanity
        if not url.startswith("http"):
            return
        dom = url
        if tldextract:
            try:
                ext = tldextract.extract(url)
                dom = ".".join([p for p in [ext.domain, ext.suffix] if p])
            except Exception:
                pass
        bucket[dom].add(url)
    except Exception:
        pass


def pretty_link_title(url: str) -> str:
    """Return a short friendly label for a URL."""
    try:
        m = re.match(r"https?://([^/]+)(/[^?#]*)?", url)
        host = m.group(1) if m else ""
        path = (m.group(2) or "/").strip("/")

        h = host.lower()
        if "youtube." in h or "youtu.be" in h:
            return "YouTube"
        if "substack" in h:
            return "Substack"
        if "x.com" in h or "twitter.com" in h:
            return "Tweet"
        if "chbmp" in h:
            return "CHBMP"
        if "who.int" in h:
            return "WHO"
        if "pubmed" in h or "nih.gov" in h:
            return "PubMed"
        if "github.com" in h:
            return "GitHub"

        # derive from path
        parts = [p for p in re.split(r"[-_/]+", path) if p and len(p) > 2][:3]
        parts = [re.sub(r"[^A-Za-z0-9]+", "", p).title() for p in parts][:3]
        if parts:
            return " ".join(parts[:3])

        # fallback to brand from host
        brand = host.split(".")
        brand = brand[-2] if len(brand) > 1 else brand[0]
        return brand.title()
    except Exception:
        return "Link"


# ---------- Conversation building ----------

def parse_time(ts):
    try:
        return time.mktime(time.strptime(ts, "%a %b %d %H:%M:%S %z %Y"))
    except Exception:
        return 0.0


def collect_links_from_tweet(tw: dict, links_by_domain: dict):
    ent = tw.get("entities") or {}
    for u in (ent.get("urls") or []):
        add_link(links_by_domain, u.get("expanded_url") or u.get("unwound_url") or u.get("url"))

    # pull media expanded links if present
    ext_ent = tw.get("extended_entities") or {}
    for m in (ext_ent.get("media") or []):
        add_link(links_by_domain, m.get("expanded_url") or m.get("media_url_https") or m.get("media_url"))

    # quoted tweet permalinks
    q = tw.get("quoted_status_permalink") or {}
    add_link(links_by_domain, q.get("expanded") or q.get("url"))


def build_tree(tweets: dict, users: dict, root_id: str):
    nodes = {}
    children = defaultdict(list)
    links_by_domain = defaultdict(set)

    # Keep only tweets in this conversation_id
    for tid, tw in tweets.items():
        conv = str(tw.get("conversation_id_str") or tw.get("conversation_id") or "")
        if conv == root_id:
            nodes[tid] = tw

    # Ensure root is present if available
    if root_id in tweets and root_id not in nodes:
        nodes[root_id] = tweets[root_id]

    # Collect links
    for tid, tw in nodes.items():
        collect_links_from_tweet(tw, links_by_domain)

    # Parent stitching: attach child under nearest ancestor (walk up) or root
    for tid, tw in nodes.items():
        parent = tw.get("in_reply_to_status_id_str") or ""
        cur = parent
        attached = False
        while cur:
            if cur in nodes:
                children[cur].append(tid)
                attached = True
                break
            # Walk up if we can
            nxt = tweets.get(cur, {})
            cur = nxt.get("in_reply_to_status_id_str") or ""
        if not attached and tid != root_id:
            # if parent chain missing, drop under root
            children[root_id].append(tid)

    # Sort children by created_at asc
    for p in list(children.keys()):
        children[p].sort(key=lambda k: parse_time(nodes[k].get("created_at", "")))

    return nodes, children, links_by_domain


# ---------- HTML rendering ----------

CSS = """
<style>
.ss3k-reply{display:flex;gap:10px;padding:10px 8px;border-bottom:1px solid #eee}
.ss3k-ravatar{width:32px;height:32px;border-radius:50%}
.ss3k-rname{font:600 14px/1.2 system-ui,-apple-system,Segoe UI,Roboto,Arial}
.ss3k-rhandle,.ss3k-rtime{font:12px/1.2 system-ui,-apple-system,Segoe UI,Roboto,Arial;color:#64748b;text-decoration:none}
.ss3k-rcontent{flex:1}
.ss3k-rtext{margin-top:4px;font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Arial}
details.ss3k-thread{margin:6px 0 8px 42px}
details.ss3k-thread > summary{cursor:pointer;color:#2563eb}
.ss3k-links h4{margin:14px 0 6px 0;font:600 14px/1.2 system-ui}
.ss3k-links ul{margin:0 0 10px 16px;padding:0}
.ss3k-links li{margin:2px 0}
</style>
""".strip()


def render_thread(nodes: dict, children: dict, users: dict, root_id: str) -> str:
    def expanded_text(tw: dict) -> str:
        return expand_text_with_entities(
            tw.get("full_text") or tw.get("text") or "",
            tw.get("entities") or {},
        )

    def render_item(tid: str, depth: int = 0) -> str:
        t = nodes[tid]
        uid = str(t.get("user_id_str") or "")
        u = users.get(uid, {})

        name = u.get("name") or "User"
        handle = u.get("screen_name") or ""
        avatar = (u.get("profile_image_url_https") or u.get("profile_image_url") or "").replace(
            "_normal.", "_bigger."
        )
        text_html = expanded_text(t)
        url = f"https://x.com/{handle}/status/{tid}"

        ts = t.get("created_at", "")
        try:
            ts_short = time.strftime("%Y-%m-%d %H:%M:%SZ", time.strptime(ts, "%a %b %d %H:%M:%S %z %Y"))
        except Exception:
            ts_short = ts

        kids = children.get(tid, [])

        head = (
            f'<div class="ss3k-reply" style="margin-left:{depth*16}px">'
            f'  <a href="https://x.com/{esc(handle)}" target="_blank" rel="noopener">'
            f'    <img class="ss3k-ravatar" src="{esc(avatar)}" alt=""></a>'
            f'  <div class="ss3k-rcontent">'
            f'    <div class="ss3k-rname">'
            f'      <a href="https://x.com/{esc(handle)}" target="_blank" rel="noopener">{esc(name)}</a>'
            f'      <span class="ss3k-rhandle">@{esc(handle)}</span>'
            f'      · <a class="ss3k-rtime" href="{esc(url)}" target="_blank" rel="noopener">{esc(ts_short)}</a>'
            f'    </div>'
            f'    <div class="ss3k-rtext">{text_html}</div>'
            f'  </div>'
            f'</div>'
        )

        if not kids:
            return head

        body = "".join(render_item(k, depth + 1) for k in kids)
        return (
            head
            + f'<details class="ss3k-thread" style="margin-left:{depth*16}px">'
            + f"<summary>Show {len(kids)} repl{'y' if len(kids)==1 else 'ies'}</summary>"
            + body
            + "</details>"
        )

    roots = children.get(root_id, [])
    roots.sort(key=lambda k: parse_time(nodes[k].get("created_at", "")))
    return CSS + "".join(render_item(tid, 0) for tid in roots)


def render_links_list(links_by_domain: dict) -> str:
    lines = ["<div class='ss3k-links'>"]
    for dom in sorted(links_by_domain):
        group = sorted(links_by_domain[dom])
        if not group:
            continue
        lines.append(f"<h4>{esc(dom)}</h4>")
        lines.append("<ul>")
        for u in group:
            nm = pretty_link_title(u)
            lines.append(f'<li><a href="{esc(u)}" target="_blank" rel="noopener">{esc(nm)}</a></li>')
        lines.append("</ul>")
    lines.append("</div>")
    return "\n".join(lines)


# ---------- Main ----------

def main():
    if not PURPLE:
        write_empty()
        return

    m = re.search(r"/status/(\d+)", PURPLE)
    if not m:
        write_empty()
        return

    tid_str = m.group(1)
    out = fetch_conversation(tid_str)
    if out is None:
        write_empty()
        return

    tweets, users = out
    nodes, children, links_by_domain = build_tree(tweets, users, tid_str)
    if not nodes:
        write_empty()
        return

    os.makedirs(ARTDIR, exist_ok=True)
    open(REPLIES_OUT, "w", encoding="utf-8").write(render_thread(nodes, children, users, tid_str))
    open(LINKS_OUT, "w", encoding="utf-8").write(render_links_list(links_by_domain))


if __name__ == "__main__":
    main()
