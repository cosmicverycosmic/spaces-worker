#!/usr/bin/env python3
"""
Adapted from Giovanni Merlos Mellini's twitter-scraper example.

This script is tailored for the CHBMP Space Worker "replies_only" mode.
It expects environment variables (supplied by the GitHub workflow):

Required for v1.1 API (python-twitter):
  TW_API_CONSUMER_KEY
  TW_API_CONSUMER_SECRET
  TW_API_ACCESS_TOKEN
  TW_API_ACCESS_TOKEN_SECRET

Inputs / context:
  PURPLE_TWEET_URL   - canonical tweet URL announcing the Space
  SPACE_ID           - optional; used as fallback (searches i/spaces/<id>)
  ARTDIR             - output directory (defaults to ./out)
  BASE               - base filename prefix (defaults to "thread")
  LOCAL_TIMEZONE     - optional tz string for display; default "UTC"

Outputs (placed in ARTDIR):
  <BASE>_replies.html
  <BASE>_links.html
"""

import os
import re
import sys
import html
import json
import time
from datetime import datetime, timezone
from collections import defaultdict, deque

try:
    import tldextract
except Exception:
    tldextract = None

try:
    import twitter  # python-twitter
except ImportError as e:
    print(f"[replies.py] python-twitter not installed: {e}", file=sys.stderr)
    sys.exit(0)


# ----------------------------
# Utilities
# ----------------------------

def esc(s: str) -> str:
    return html.escape(s or "", quote=True)


def getenv(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return v if v is not None else default


def parse_tweet_url(u: str):
    """
    Accepts twitter.com or x.com status URLs. Returns (screen_name, tweet_id) or (None, None).
    """
    if not u:
        return None, None
    u = u.strip()
    # Normalize mobile links, remove query/fragment
    u = re.sub(r'^https?://(mobile\.)?(twitter|x)\.com/', 'https://twitter.com/', u)
    u = u.split('?')[0].split('#')[0]
    m = re.search(r'https?://twitter\.com/([A-Za-z0-9_]+)/status/(\d+)$', u)
    if m:
        return m.group(1), m.group(2)
    return None, None


def build_api_from_env():
    ck  = getenv("TW_API_CONSUMER_KEY")
    cs  = getenv("TW_API_CONSUMER_SECRET")
    at  = getenv("TW_API_ACCESS_TOKEN")
    ats = getenv("TW_API_ACCESS_TOKEN_SECRET")
    missing = [k for k,v in {
        "TW_API_CONSUMER_KEY": ck, "TW_API_CONSUMER_SECRET": cs,
        "TW_API_ACCESS_TOKEN": at, "TW_API_ACCESS_TOKEN_SECRET": ats
    }.items() if not v]
    if missing:
        print(f"[replies.py] Missing Twitter v1.1 credentials: {', '.join(missing)}", file=sys.stderr)
        return None
    return twitter.Api(
        consumer_key=ck,
        consumer_secret=cs,
        access_token_key=at,
        access_token_secret=ats,
        sleep_on_rate_limit=True,
        tweet_mode='extended'  # ensure full_text
    )


def iso_utc(ts: int | float) -> str:
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat(timespec="seconds").replace("+00:00","Z")
    except Exception:
        return ""


def expand_urls_in_text(status) -> str:
    """
    Prefer full_text when available. Replace t.co with expanded_url for readability.
    """
    txt = getattr(status, "full_text", None) or getattr(status, "text", "") or ""
    try:
        urls = status.urls or []
        for u in urls:
            short = u.url or ""
            longu = u.expanded_url or short
            if short and longu and short in txt:
                txt = txt.replace(short, longu)
    except Exception:
        pass
    return txt


def collect_links_from_status(status, bucket: dict):
    """
    bucket: dict[domain] -> list[urls]
    """
    try:
        urls = status.urls or []
        for u in urls:
            href = (u.expanded_url or u.url or "").strip()
            if not href:
                continue
            if tldextract:
                dom = tldextract.extract(href)
                host = dom.registered_domain or href
            else:
                # naive fallback
                m = re.match(r'https?://([^/]+)', href)
                host = m.group(1) if m else href
            if href not in bucket[host]:
                bucket[host].append(href)
    except Exception:
        pass


# ----------------------------
# Core scraping
# ----------------------------

def fetch_status(api, tweet_id: str):
    try:
        return api.GetStatus(status_id=int(tweet_id), include_entities=True)
    except twitter.error.TwitterError as e:
        print(f"[replies.py] GetStatus failed for {tweet_id}: {e}", file=sys.stderr)
        return None


def search_root_by_space_id(api, space_id: str):
    """
    Best-effort fallback: find a tweet that links to i/spaces/<space_id>.
    Limited to last 7 days by API.
    """
    if not space_id:
        return None
    term = f"i/spaces/{space_id}"
    print(f"[replies.py] PURPLE_TWEET_URL not provided; searching for '{term}' ...", file=sys.stderr)
    try:
        # Pull a few pages and pick the oldest (first announcement) or newest; take newest by default.
        max_id = None
        candidates = []
        for _ in range(5):
            batch = api.GetSearch(term=term, count=100, include_entities=True, max_id=max_id, result_type="recent")
            if not batch:
                break
            candidates.extend(batch)
            max_id = min(t.id for t in batch) - 1
        if not candidates:
            print("[replies.py] No tweets referencing the Space ID were found in the last 7 days.", file=sys.stderr)
            return None
        tgt = sorted(candidates, key=lambda t: t.created_at_in_seconds or 0)[-1]
        return tgt
    except twitter.error.TwitterError as e:
        print(f"[replies.py] Search by space id failed: {e}", file=sys.stderr)
        return None


def get_direct_replies(api, tweet):
    """
    Return list of statuses that directly reply to `tweet` (in_reply_to_status_id == tweet.id).
    Uses search term 'to:<screen_name>' and filters by parent id.
    """
    parent_id = tweet.id
    screen = tweet.user.screen_name if tweet.user else None
    if not screen:
        return []

    results = []
    max_id = None
    pages = 0
    while True:
        pages += 1
        try:
            batch = api.GetSearch(
                term=f"to:{screen}",
                since_id=parent_id,
                max_id=max_id,
                count=100,
                include_entities=True,
                result_type="recent",
            )
        except twitter.error.TwitterError as e:
            # back off and continue; don't fail the entire run
            print(f"[replies.py] Search error (page {pages}), backing off: {e}", file=sys.stderr)
            time.sleep(10)
            continue

        if not batch:
            break

        for st in batch:
            # Only keep messages that are direct replies to parent
            try:
                if int(st.in_reply_to_status_id or 0) == int(parent_id):
                    results.append(st)
            except Exception:
                pass

        max_id = min(t.id for t in batch) - 1
        if len(batch) < 100 or pages >= 25:
            break

    # Sort chronologically
    results.sort(key=lambda s: s.created_at_in_seconds or 0)
    return results


def build_thread_tree(api, root):
    """
    BFS over replies to collect a full reply tree (limited by 7-day search & rate limits).
    Returns a dict: id -> {"tweet": status, "children": [nodes...]}
    """
    nodes = {}
    def wrap(st):
        return {"tweet": st, "children": []}

    nodes[root.id] = wrap(root)
    q = deque([root])
    visited = set([root.id])

    while q:
        current = q.popleft()
        replies = get_direct_replies(api, current)
        for r in replies:
            if r.id not in nodes:
                nodes[r.id] = wrap(r)
            # attach to parent
            nodes[current.id]["children"].append(nodes[r.id])
            if r.id not in visited:
                visited.add(r.id)
                q.append(r)

    # Sort children by time
    for n in nodes.values():
        n["children"].sort(key=lambda ch: ch["tweet"].created_at_in_seconds or 0)
    return nodes


# ----------------------------
# Rendering
# ----------------------------

CSS = """<style>
.ss3k-replies{font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
.ss3k-reply-card{border:1px solid #e2e8f0;border-radius:12px;padding:12px;margin:10px 0;background:#fff;box-shadow:0 1px 1px rgba(0,0,0,.03)}
.ss3k-reply-head{display:flex;align-items:center;gap:8px}
.ss3k-reply-head img.avatar{width:28px;height:28px;border-radius:50%}
.ss3k-reply-meta{color:#64748b;font-size:12px;margin-top:4px}
.ss3k-reply-content{margin-top:8px;white-space:pre-wrap;word-break:break-word}
.ss3k-link-cards{margin-top:8px;display:flex;flex-wrap:wrap;gap:6px}
.ss3k-link-card{border:1px solid #cbd5e1;padding:4px 8px;border-radius:8px;background:#f8fafc;font-size:12px}
.ss3k-children{margin-left:16px;display:none}
.ss3k-toggle{margin-top:6px;font-size:12px;color:#2563eb;background:none;border:none;padding:0;cursor:pointer}
.reply-media{max-width:100%;border-radius:10px;margin-top:8px}
</style>
<script>
function ss3kToggleReplies(id,btn){
  const el=document.getElementById(id); if(!el) return;
  const open=(el.style.display==='block'); el.style.display=open?'none':'block';
  if(btn){ btn.textContent=open?(btn.dataset.label||'Show replies'):(btn.dataset.hide||'Hide replies'); }
}
</script>"""

def avatar_url(user) -> str:
    if not user:
        return ""
    return getattr(user, "profile_image_url_https", None) or getattr(user, "profile_image_url", "") or (f"https://unavatar.io/x/{esc(user.screen_name)}" if getattr(user, "screen_name", None) else "")


def link_chips(status):
    chips = []
    seen = set()
    try:
        for u in (status.urls or []):
            href = (u.expanded_url or u.url or "").strip()
            if not href or href in seen:
                continue
            seen.add(href)
            if tldextract:
                dom = tldextract.extract(href)
                host = dom.registered_domain or href
            else:
                m = re.match(r'https?://([^/]+)', href)
                host = m.group(1) if m else href
            chips.append(f'<a class="ss3k-link-card" href="{esc(href)}" target="_blank" rel="noopener">{esc(host)}</a>')
    except Exception:
        pass
    if chips:
        return '\n        <div class="ss3k-link-cards">\n          ' + "\n          ".join(chips) + "\n        </div>"
    return ""


def media_block(status):
    parts = []
    try:
        for m in getattr(status, "media", []) or []:
            if getattr(m, "type", None) == "photo" and getattr(m, "media_url_https", None):
                parts.append(f'<img class="reply-media" src="{esc(m.media_url_https)}" alt="">')
    except Exception:
        pass
    return ("\n        " + "\n        ".join(parts)) if parts else ""


def render_node(node, level=0):
    t = node["tweet"]
    sn = t.user.screen_name if t.user else "user"
    name = t.user.name if t.user else sn
    prof = f"https://x.com/{esc(sn)}"
    tw   = f"https://x.com/{esc(sn)}/status/{t.id}"
    dt   = t.created_at_in_seconds or 0
    iso  = iso_utc(dt)
    av   = avatar_url(t.user)
    txt  = esc(expand_urls_in_text(t))
    links = link_chips(t)
    media = media_block(t)

    children_html = ""
    if node["children"]:
        cid = f"children-{t.id}"
        count = len(node["children"])
        btn_label = f"Show {count} repl" + ("y" if count == 1 else "ies")
        children_html = (
            f'\n      <button class="ss3k-toggle" data-label="{esc(btn_label)}" data-hide="Hide replies" '
            f'onclick="ss3kToggleReplies(\'{cid}\', this)">{esc(btn_label)}</button>\n'
            f'      <div class="ss3k-children" id="{cid}">\n' +
            "\n".join(render_node(c, level + 1) for c in node["children"]) +
            "\n      </div>"
        )

    return f"""
      <div class="ss3k-reply-card" id="{t.id}" data-level="{level}">
        <div class="ss3k-reply-head">
          <a href="{prof}" target="_blank" rel="noopener">
            <img class="avatar" src="{esc(av)}" alt="">
          </a>
          <div>
            <div>
              <a href="{prof}" target="_blank" rel="noopener"><strong>{esc(name)}</strong></a>
              <span style="color:#64748b">@{esc(sn)}</span>
            </div>
            <div class="ss3k-reply-meta">
              <a href="{tw}" target="_blank" rel="noopener"><time datetime="{iso}">{iso.replace('T',' ')} UTC</time></a>
            </div>
          </div>
        </div>
        <div class="ss3k-reply-content">{txt}</div>{media}{links}{children_html}
      </div>""".rstrip()


def render_thread_html(root_node):
    html_top = CSS + "\n<div class=\"ss3k-replies\">"
    html_bottom = "\n</div>\n"
    return html_top + "\n" + render_node(root_node) + "\n" + html_bottom


def render_links_html(domain_links: dict):
    parts = []
    for dom in sorted(domain_links):
        links = "\n".join(f'  <li><a href="{esc(u)}" target="_blank" rel="noopener">{esc(u)}</a></li>' for u in domain_links[dom])
        parts.append(f"<h4>{esc(dom)}</h4>\n<ul>\n{links}\n</ul>")
    return "\n\n".join(parts)


# ----------------------------
# Main
# ----------------------------

def main():
    artdir = getenv("ARTDIR", os.path.join(os.getcwd(), "out"))
    base   = getenv("BASE", "thread")
    os.makedirs(artdir, exist_ok=True)

    purple_url = getenv("PURPLE_TWEET_URL", "").strip()
    space_id   = getenv("SPACE_ID", "").strip()

    api = build_api_from_env()
    if api is None:
        # Graceful no-op so the workflow can continue and skip WP patch
        sys.exit(0)

    root_status = None
    root_screen = None
    root_id     = None

    # Prefer explicit tweet URL
    if purple_url:
        root_screen, root_id = parse_tweet_url(purple_url)
        if not (root_screen and root_id):
            print(f"[replies.py] PURPLE_TWEET_URL looks invalid: {purple_url}", file=sys.stderr)
        else:
            root_status = fetch_status(api, root_id)

    # Fallback: try to find a tweet that references the Space ID
    if root_status is None and space_id:
        root_status = search_root_by_space_id(api, space_id)
        if root_status:
            root_screen = root_status.user.screen_name if root_status.user else root_screen
            root_id     = str(root_status.id)

    if root_status is None:
        print("[replies.py] No root tweet could be determined; nothing to do.", file=sys.stderr)
        sys.exit(0)

    # Build tree
    nodes = build_thread_tree(api, root_status)
    root_node = nodes[root_status.id]

    # Collect link domains
    domain_links: dict[str, list[str]] = defaultdict(list)
    for n in nodes.values():
        collect_links_from_status(n["tweet"], domain_links)

    # Render outputs
    replies_html = render_thread_html(root_node)
    links_html   = render_links_html(domain_links) if any(domain_links.values()) else ""

    out_replies = os.path.join(artdir, f"{base}_replies.html")
    out_links   = os.path.join(artdir, f"{base}_links.html")

    with open(out_replies, "w", encoding="utf-8") as f:
        f.write(replies_html)
    if links_html:
        with open(out_links, "w", encoding="utf-8") as f:
            f.write(links_html)

    print(f"[replies.py] Wrote: {out_replies}")
    if links_html:
        print(f"[replies.py] Wrote: {out_links}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
