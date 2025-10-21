#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
.github/workflows/scripts/replies.py

Scrape replies + shared links for a given “Purple Pill” tweet and emit two HTML
artifacts into $ARTDIR:

  - $BASE_replies.html  (threaded replies)
  - $BASE_links.html    (unique URLs shared across the thread)

Design goals:
  • Graceful no-op when inputs/creds are missing (exit 0; do not fail the job).
  • Prefer python-twitter (v1.1 search; 7-day limit) if TW_API_* keys are set.
  • Flat, dependency-light. No web scraping of x.com HTML (it’s JS-rendered).

ENV expected (workflow sets these):
  PURPLE_TWEET_URL   e.g. https://x.com/username/status/1234567890
  WORKDIR            work area (default: ./work)
  ARTDIR             artifacts out dir (default: ./out)
  BASE               prefix for artifact filenames (default uses UTC date)

Optional creds (python-twitter):
  TW_API_CONSUMER_KEY
  TW_API_CONSUMER_SECRET
  TW_API_ACCESS_TOKEN
  TW_API_ACCESS_TOKEN_SECRET
"""

import os
import re
import sys
import html
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# env / paths
# ---------------------------------------------------------------------------
WORKDIR = os.environ.get("WORKDIR", os.path.join(os.getcwd(), "work"))
ARTDIR  = os.environ.get("ARTDIR",  os.path.join(os.getcwd(), "out"))
BASE    = os.environ.get("BASE",    f"space-{datetime.now(timezone.utc):%m-%d-%Y}-unknown")
PURPLE  = (os.environ.get("PURPLE_TWEET_URL") or "").strip()

TW_CK = os.environ.get("TW_API_CONSUMER_KEY", "")
TW_CS = os.environ.get("TW_API_CONSUMER_SECRET", "")
TW_AT = os.environ.get("TW_API_ACCESS_TOKEN", "")
TW_AS = os.environ.get("TW_API_ACCESS_TOKEN_SECRET", "")

os.makedirs(WORKDIR, exist_ok=True)
os.makedirs(ARTDIR,  exist_ok=True)

REPLIES_PATH = os.path.join(ARTDIR, f"{BASE}_replies.html")
LINKS_PATH   = os.path.join(ARTDIR, f"{BASE}_links.html")


def log(msg: str) -> None:
    print(msg, flush=True)


# ---------------------------------------------------------------------------
# utilities
# ---------------------------------------------------------------------------
def parse_tweet_url(u: str):
    """
    Accepts https://x.com/<user>/status/<id> or https://twitter.com/<user>/status/<id>
    Returns (screen_name, tweet_id:int) or (None, None) if not parseable.
    """
    if not u:
        return None, None
    m = re.search(r"https?://(?:x|twitter)\.com/([^/]+)/status/(\d+)", u)
    if not m:
        return None, None
    return m.group(1), int(m.group(2))


def safe_created_at(s: str) -> str:
    """
    python-twitter created_at looks like: 'Mon Oct 21 15:21:14 +0000 2025'
    Make a compact ISO-like string in UTC; fall back to the original on parse error.
    """
    if not s:
        return ""
    try:
        # Try with timezone first
        dt = datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        try:
            # Fallback without %z (just in case)
            dt = datetime.strptime(s, "%a %b %d %H:%M:%S %Y")
            return dt.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        except Exception:
            return s


def extract_urls(text: str):
    if not text:
        return []
    # Rough URL matcher; good enough for artifacting.
    return re.findall(r"https?://[^\s)>\]]+", text)


# ---------------------------------------------------------------------------
# python-twitter path (v1.1 search)
# ---------------------------------------------------------------------------
def collect_replies_with_python_twitter(root_sn: str, root_id: int):
    """
    Strategy:
      1) Search for tweets "to:root_sn" since root_id, paging until exhausted.
      2) Build a map: parent_id -> [child_tweet, ...]
      3) DFS from root_id to produce a threaded list of replies (with depth).
    Returns:
      replies: list of dicts {id, user, screen_name, text, created_at, url, depth}
      links:   list of (url, context_snippet)
    """
    try:
        import twitter  # pip install python-twitter
    except Exception:
        log("[replies] python-twitter not installed; skipping.")
        return [], []

    if not (TW_CK and TW_CS and TW_AT and TW_AS):
        log("[replies] No TW_API_* keys configured; skipping python-twitter mode.")
        return [], []

    api = twitter.Api(
        consumer_key=TW_CK,
        consumer_secret=TW_CS,
        access_token_key=TW_AT,
        access_token_secret=TW_AS,
        sleep_on_rate_limit=True,
    )

    term = f"to:{root_sn}"
    log(f"[replies] v1.1 search term={term} since_id={root_id}")

    # 1) fetch all candidate tweets
    parent_map = {}  # pid -> [status, ...]
    seen_ids = set()
    max_id = None
    total = 0

    while True:
        try:
            batch = api.GetSearch(term=term, since_id=root_id, max_id=max_id, count=100)
        except Exception as e:
            log(f"[replies] twitter API error, stopping: {e}")
            break

        if not batch:
            break

        for st in batch:
            try:
                d = st.AsDict()
                tid = int(d.get("id"))
                if tid in seen_ids:
                    continue
                seen_ids.add(tid)

                pid = d.get("in_reply_to_status_id")
                if pid is None:
                    continue
                try:
                    pid = int(pid)
                except Exception:
                    continue

                parent_map.setdefault(pid, []).append(st)
                total += 1
            except Exception:
                continue

        # prepare next page
        try:
            max_id = min(int(s.id) for s in batch) - 1
        except Exception:
            break

        # stop if we’re clearly done
        if len(batch) < 100:
            break

        # be polite
        time.sleep(0.5)

    log(f"[replies] gathered {total} candidate tweets; building thread…")

    # 2) DFS to build threaded order
    replies = []
    links_seen = {}
    links = []

    def dfs(parent_id: int, depth: int = 0):
        children = parent_map.get(parent_id, [])
        # chronological order:
        try:
            children.sort(key=lambda s: datetime.strptime(s.created_at, "%a %b %d %H:%M:%S %z %Y"))
        except Exception:
            # fallback: ids ascending
            children.sort(key=lambda s: int(getattr(s, "id", 0)))

        for st in children:
            d = st.AsDict()
            rid = int(d.get("id"))
            u = d.get("user") or {}
            sn = (u.get("screen_name") or "").strip()
            name = (u.get("name") or "").strip()
            txt = d.get("text") or ""
            cat = safe_created_at(d.get("created_at") or "")
            url = f"https://x.com/{sn}/status/{rid}"

            replies.append({
                "id": rid,
                "user": name,
                "screen_name": sn,
                "text": txt,
                "created_at": cat,
                "url": url,
                "depth": depth,
            })

            # gather links
            for uurl in extract_urls(txt):
                if uurl not in links_seen:
                    links_seen[uurl] = txt
                    links.append((uurl, txt))

            # recurse into deeper branches
            dfs(rid, depth + 1)

    dfs(root_id, 0)
    log(f"[replies] threaded replies: {len(replies)}; unique links: {len(links)}")
    return replies, links


# ---------------------------------------------------------------------------
# artifact writers
# ---------------------------------------------------------------------------
def write_replies_html(path: str, replies):
    if not replies:
        return

    head = """<!doctype html>
<meta charset="utf-8">
<title>Replies</title>
<style>
body{font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;padding:14px}
ul{list-style: none;padding-left:0}
li{margin:.5em 0}
.meta{color:#555;font-size:.9em;margin-bottom:2px}
.bubble{background:#fff;border:1px solid #e6e8ef;border-radius:8px;padding:8px 10px;box-shadow:0 1px 3px rgba(0,0,0,.04)}
.indent-0{margin-left:0}
.indent-1{margin-left:18px}
.indent-2{margin-left:36px}
.indent-3{margin-left:54px}
.indent-4{margin-left:72px}
</style>
<ul>"""
    parts = [head]

    for r in replies:
        indent_class = f"indent-{min(4, int(r.get('depth') or 0))}"
        name = html.escape(r.get("user") or "")
        sn = html.escape(r.get("screen_name") or "")
        created = html.escape(r.get("created_at") or "")
        url = html.escape(r.get("url") or "")
        txt = html.escape(r.get("text") or "")

        parts.append(
            f"<li class='{indent_class}'>"
            f"  <div class='meta'><strong>{name}</strong> (@{sn}) — {created} — "
            f"    <a href='{url}' target='_blank' rel='noopener'>open</a></div>"
            f"  <div class='bubble'>{txt}</div>"
            f"</li>"
        )

    parts.append("</ul>")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))
    log(f"[replies] wrote {path}")


def write_links_html(path: str, links):
    if not links:
        return

    head = """<!doctype html>
<meta charset="utf-8">
<title>Links</title>
<style>
body{font-family:system-ui,Segoe UI,Roboto,Arial,sans-serif;padding:14px}
li{margin:.5em 0}
.ctx{color:#555;font-size:.9em;margin-top:2px}
</style>
<ul>"""
    parts = [head]

    for url, ctx in links:
        parts.append(
            f"<li><a href='{html.escape(url)}' target='_blank' rel='noopener'>"
            f"{html.escape(url)}</a>"
            f"<div class='ctx'>{html.escape(ctx)}</div></li>"
        )

    parts.append("</ul>")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))
    log(f"[replies] wrote {path}")


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> int:
    if not PURPLE:
        log("[replies] No PURPLE_TWEET_URL provided; skipping (exit 0).")
        return 0

    screen_name, tweet_id = parse_tweet_url(PURPLE)
    if not tweet_id:
        log(f"[replies] Could not parse tweet id from URL: {PURPLE}; skipping (exit 0).")
        return 0

    # Try python-twitter path if creds exist
    replies, links = [], []
    if TW_CK and TW_CS and TW_AT and TW_AS:
        replies, links = collect_replies_with_python_twitter(screen_name or "", tweet_id)
    else:
        log("[replies] No TW_API_* keys set; replies scrape skipped (exit 0).")
        return 0

    if replies:
        write_replies_html(REPLIES_PATH, replies)
    if links:
        write_links_html(LINKS_PATH, links)

    log(f"[replies] done. replies={len(replies)} links={len(links)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
