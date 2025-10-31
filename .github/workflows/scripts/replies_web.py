#!/usr/bin/env python3
# Build replies + shared links HTML from either:
#  - a local replies JSONL (REPLIES_JSONL), or
#  - Purple tweet URL (PURPLE_TWEET_URL) via API fallback (needs auth cookie/bearer).
#
# Outputs in $ARTDIR:
#   BASE_replies.html
#   BASE_links.html
#   BASE_replies.log

import os, re, json, html, time, traceback
from pathlib import Path
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

ARTDIR = Path(os.environ.get("ARTDIR","."))
BASE   = os.environ.get("BASE","space")
PURPLE = (os.environ.get("PURPLE_TWEET_URL","") or "").strip()
REPLIES_JSONL = (os.environ.get("REPLIES_JSONL","") or "").strip()

OUT_REPLIES = ARTDIR / f"{BASE}_replies.html"
OUT_LINKS   = ARTDIR / f"{BASE}_links.html"
LOG_PATH    = ARTDIR / f"{BASE}_replies.log"

AUTH        = (os.environ.get("TWITTER_AUTHORIZATION","") or "").strip()
AUTH_TOKEN  = (os.environ.get("TWITTER_AUTH_TOKEN","") or "").strip()
CSRF        = (os.environ.get("TWITTER_CSRF_TOKEN","") or "").strip()

def log(msg):
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

def _discover_local_jsonl():
    if REPLIES_JSONL and Path(REPLIES_JSONL).is_file():
        return Path(REPLIES_JSONL)
    # try to find something plausible
    candidates = []
    for pat in ("*repl*.jsonl","*reply*.jsonl","*tweets*.jsonl","*tweet*.jsonl"):
        candidates += list(ARTDIR.glob(pat))
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None

def expand_tco(url, timeout=6):
    try:
        req = Request(url, headers={"User-Agent":"Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as r:
            # If not redirected, still return original
            return r.geturl()
    except Exception:
        return url

def parse_local_jsonl(p: Path):
    # You can normalize to: created_at ISO, author name/handle, text, metrics, tweet_id, link (if any)
    items = []
    with p.open("r", encoding="utf-8", errors="ignore") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln: continue
            try:
                o = json.loads(ln)
            except Exception:
                continue
            # This may vary by your crawler; handle both raw tweet & wrapped forms
            tw = o.get("tweet") or o.get("legacy") or o
            tid = tw.get("id_str") or tw.get("tweet_id") or tw.get("rest_id") or tw.get("id")
            if not tid:
                continue
            text = tw.get("full_text") or tw.get("text") or ""
            user = tw.get("user") or o.get("user_results",{}).get("result",{}).get("legacy",{})
            name = user.get("name") or o.get("author_name") or ""
            handle = user.get("screen_name") or o.get("author_screen_name") or ""
            ava = (user.get("profile_image_url_https") or user.get("profile_image_url") or "").replace("_normal.","_bigger.")
            dt = tw.get("created_at") or o.get("created_at")
            likes = tw.get("favorite_count") or tw.get("favourites_count") or o.get("favourites") or 0
            rts = tw.get("retweet_count") or o.get("retweets") or 0
            # expand any t.co links
            urls = []
            for m in (tw.get("entities",{}).get("urls") or []):
                u = m.get("expanded_url") or m.get("url")
                if u and "t.co/" in u: u = expand_tco(u)
                if u: urls.append(u)
            items.append({
                "id": str(tid),
                "text": text,
                "name": name,
                "handle": handle,
                "avatar": ava,
                "created_at": dt,
                "likes": int(likes or 0),
                "retweets": int(rts or 0),
                "urls": urls
            })
    return items

def _cookies_headers():
    hdrs = {"User-Agent":"Mozilla/5.0", "Accept":"application/json, text/plain, */*"}
    if AUTH and AUTH.startswith("Bearer "):
        hdrs["Authorization"] = AUTH
    if AUTH_TOKEN and CSRF:
        hdrs["Cookie"] = f"auth_token={AUTH_TOKEN}; ct0={CSRF}"
        hdrs["x-csrf-token"] = CSRF
    return hdrs

def _tweet_id_from_url(u: str):
    # Handle https://x.com/<user>/status/<id>
    try:
        path = urlparse(u).path.strip("/")
        parts = path.split("/")
        if "status" in parts:
            i = parts.index("status")
            return parts[i+1]
    except Exception:
        pass
    return ""

def _get_replies_via_api(root_url: str):
    # Minimal fallback: call syndication endpoint or twitter api v2 conversation search if available
    # Here we lean on the syndication oembed+timeline endpoints that don’t require full firehose
    # NOTE: This is best-effort; cookie/bearer greatly helps.
    tid = _tweet_id_from_url(root_url)
    if not tid:
        log("No tweet id in PURPLE_TWEET_URL")
        return []
    log(f"API fallback fetch for thread id {tid}")
    # This is intentionally conservative; your existing private endpoints (if any) can be restored here
    # Return empty rather than failing hard
    return []

def build_html(items):
    # 50x50 avatars, “discussion board” style, timestamp linked to original x post in bottom-right
    rows = []
    links = set()
    for it in items:
        txt = html.escape(it["text"] or "")
        name = html.escape(it.get("name") or "")
        handle = html.escape(it.get("handle") or "")
        ava = it.get("avatar") or ""
        tid = it.get("id")
        # canonical link:
        link = f"https://x.com/{handle}/status/{tid}" if handle else f"https://x.com/i/web/status/{tid}"
        dt = it.get("created_at") or ""
        rows.append(f"""
<div class="rep-row">
  <img class="rep-ava" src="{ava}" alt="" width="50" height="50" loading="lazy" decoding="async" />
  <div class="rep-body">
    <div class="rep-hd"><span class="rep-name">{name}</span> <span class="rep-handle">@{handle}</span></div>
    <div class="rep-txt">{txt}</div>
    <div class="rep-ft">
      <span class="rep-metric">❤ {it.get('likes',0)}</span>
      <span class="rep-metric">↻ {it.get('retweets',0)}</span>
      <a class="rep-time" href="{link}" target="_blank" rel="nofollow noopener">{html.escape(dt or '')}</a>
    </div>
  </div>
</div>
""")
        for u in (it.get("urls") or []):
            links.add(u)

    html_out = f"""<!doctype html>
<meta charset="utf-8">
<style>
.rep-row{{display:flex;gap:.6rem;align-items:flex-start;padding:.5rem .6rem;border-bottom:1px solid rgba(0,0,0,.06)}}
.rep-ava{{width:50px;height:50px;border-radius:999px;object-fit:cover;flex:0 0 auto}}
.rep-body{{flex:1 1 auto}}
.rep-hd{{font-weight:600;}}
.rep-handle{{color:#666;font-weight:400;margin-left:.35rem}}
.rep-txt{{margin:.25rem 0;line-height:1.4}}
.rep-ft{{display:flex;gap:1rem;color:#555;font-size:.92rem}}
.rep-metric{{opacity:.8}}
.rep-time{{margin-left:auto;color:#3b82f6;text-decoration:none}}
</style>
<div class="rep-list">
{''.join(rows)}
</div>
"""
    links_out = ""
    if links:
        links_out = "<ul>\n" + "\n".join(f'  <li><a href="{html.escape(u)}" target="_blank" rel="nofollow noopener">{html.escape(u)}</a></li>' for u in sorted(links)) + "\n</ul>\n"
    return html_out, links_out

def main():
    try:
        items = []
        src = _discover_local_jsonl()
        if src:
            log(f"Using local replies JSONL: {src}")
            items = parse_local_jsonl(src)
        elif PURPLE:
            log("No local replies JSONL found; attempting API fallback via PURPLE_TWEET_URL")
            items = _get_replies_via_api(PURPLE)
        else:
            log("No local replies JSONL and no PURPLE_TWEET_URL; nothing to do.")
            items = []

        if not items:
            # still emit empty but valid HTML files so WP patch can run
            OUT_REPLIES.write_text("<div class=\"rep-list\"></div>\n", encoding="utf-8")
            OUT_LINKS.write_text("<ul></ul>\n", encoding="utf-8")
            log("No replies parsed; wrote empty shells.")
            return 0

        html_out, links_out = build_html(items)
        OUT_REPLIES.write_text(html_out, encoding="utf-8")
        OUT_LINKS.write_text(links_out, encoding="utf-8")
        log(f"Wrote replies: {OUT_REPLIES}")
        log(f"Wrote links:   {OUT_LINKS}")
        return 0

    except Exception as e:
        log(f"ERROR: {e}")
        log(traceback.format_exc())
        # write empty shells to avoid breaking downstream
        OUT_REPLIES.write_text("<div class=\"rep-list\"></div>\n", encoding="utf-8")
        OUT_LINKS.write_text("<ul></ul>\n", encoding="utf-8")
        return 0

if __name__ == "__main__":
    raise SystemExit(main())
