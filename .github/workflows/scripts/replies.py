#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
replies.py — Build nested replies HTML for one or more root tweets.

Usage (single URL):
  python3 replies.py --url "https://twitter.com/<SCREEN_NAME>/status/<ID>"

Usage (file, one URL per line like twitter-scraper.py):
  python3 replies.py --file tweet.list
  python3 replies.py --file tweet.list --short   # CSV to stdout for diffing

Env (picked up automatically if flags not passed):
  ARTDIR, BASE
  PURPLE_URL or TWEET_URL
  TW_API_CONSUMER_KEY, TW_API_CONSUMER_SECRET, TW_API_ACCESS_TOKEN, TW_API_ACCESS_TOKEN_SECRET

Outputs:
  {ARTDIR}/{BASE}_replies.html
  {ARTDIR}/{BASE}_links.html
  (stdout CSV if --short)
"""

import os, re, sys, json, html, argparse, traceback
from collections import defaultdict, deque
from typing import List, Tuple, Dict

# ---- 3rd party (install via pip): python-twitter, tldextract ----
try:
    import twitter  # python-twitter (v1.1)
    import tldextract
except Exception:
    print("Missing deps: install with `pip install python-twitter tldextract`", file=sys.stderr)
    sys.exit(0)

# ------------ Config / defaults ------------
ARTDIR = os.environ.get("ARTDIR", ".")
BASE   = os.environ.get("BASE", "space")

CK  = os.environ.get("TW_API_CONSUMER_KEY") or ""
CS  = os.environ.get("TW_API_CONSUMER_SECRET") or ""
AT  = os.environ.get("TW_API_ACCESS_TOKEN") or ""
ATS = os.environ.get("TW_API_ACCESS_TOKEN_SECRET") or ""

COUNT_PER_PAGE     = 100
MAX_PAGES_SEARCH   = 25
DEPTH_LIMIT        = 4
MAX_TWEETS_TOTAL   = 8000  # global cap across roots (defensive)

# ------------ Helpers ------------
def esc(s: str) -> str:
    return html.escape(s or "", quote=True)

TWEET_URL_RE = re.compile(
    r"^https?://(?:x|twitter)\.com/([A-Za-z0-9_]+)/status/([0-9]+)",
    re.IGNORECASE
)

def parse_tweet_url(url: str) -> Tuple[str, int]:
    m = TWEET_URL_RE.search(url.strip())
    if not m:
        raise ValueError(f"Invalid tweet URL: {url}")
    screen_name, sid = m.group(1), int(m.group(2))
    return screen_name, sid

def create_api() -> twitter.Api:
    if not (CK and CS and AT and ATS):
        print("Replies disabled — missing Twitter credentials.", file=sys.stderr)
        sys.exit(0)
    return twitter.Api(
        consumer_key=CK, consumer_secret=CS,
        access_token_key=AT, access_token_secret=ATS,
        sleep_on_rate_limit=True, tweet_mode="extended"
    )

def get_status_by_id(api: twitter.Api, sid: int) -> twitter.Status:
    try:
        return api.GetStatus(status_id=sid, include_my_retweet=False, trim_user=False)
    except Exception as e:
        print(f"GetStatus({sid}) error: {e}", file=sys.stderr)
        return None

def do_search(api: twitter.Api, term: str, since_id: int = None,
              max_pages: int = 5, cap: int = 2000) -> List[twitter.Status]:
    out, max_id, pages = [], None, 0
    while pages < max_pages and len(out) < cap:
        try:
            batch = api.GetSearch(
                term=term, count=COUNT_PER_PAGE, include_entities=True,
                max_id=max_id, since_id=since_id, result_type="recent"
            )
        except Exception as e:
            print(f"Search error for '{term}': {e}", file=sys.stderr)
            break
        if not batch:
            break
        out.extend(batch)
        max_id = min(t.id for t in batch) - 1
        pages += 1
    return out

def avatar_for(user) -> str:
    if not user: return ""
    return getattr(user, "profile_image_url_https", None) \
        or getattr(user, "profile_image_url", None) \
        or f"https://unavatar.io/x/{esc(getattr(user, 'screen_name', '') or '')}"

def expand_text(status: twitter.Status) -> str:
    txt = getattr(status, "full_text", None) or getattr(status, "text", "") or ""
    for u in (getattr(status, "urls", None) or []):
        try:
            if u.expanded_url and u.url:
                txt = txt.replace(u.url, u.expanded_url)
        except Exception:
            pass
    return esc(txt)

def media_block(status: twitter.Status) -> str:
    media = getattr(status, "media", None) or []
    parts = []
    for m in media:
        try:
            if getattr(m, "type", None) == "photo" and getattr(m, "media_url_https", None):
                parts.append(f'<img class="reply-media" src="{esc(m.media_url_https)}" alt="">')
        except Exception:
            pass
    return ("\n        " + "\n        ".join(parts)) if parts else ""

def link_chips(status: twitter.Status) -> str:
    chips, seen = [], set()
    for u in (getattr(status, "urls", None) or []):
        try:
            href = u.expanded_url or u.url
            if not href or href in seen: 
                continue
            seen.add(href)
            dom = tldextract.extract(href)
            host = dom.registered_domain or href
            chips.append(f'<a class="ss3k-link-card" href="{esc(href)}" target="_blank" rel="noopener">{esc(host)}</a>')
        except Exception:
            pass
    return ('\n        <div class="ss3k-link-cards">' + "\n          " + "\n          ".join(chips) + "\n        </div>") if chips else ""

def collect_links(status: twitter.Status, bucket: Dict[str, List[str]]):
    for u in (getattr(status, "urls", None) or []):
        try:
            href = u.expanded_url or u.url
            if not href: 
                continue
            dom = tldextract.extract(href)
            host = dom.registered_domain or href
            if href not in bucket[host]:
                bucket[host].append(href)
        except Exception:
            pass

def ts_iso(status: twitter.Status) -> str:
    try:
        t = status.created_at_in_seconds or 0
        from datetime import datetime, timezone
        return datetime.fromtimestamp(t, tz=timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return ""

# ------------ Core: build a tree for one root tweet ------------
def build_thread_for_root(api: twitter.Api, root: twitter.Status,
                          depth_limit: int = DEPTH_LIMIT) -> Tuple[Dict[int, dict], Dict[str, List[str]]]:
    """
    Build a reply tree for 'root' using BFS over 'to:@user filter:replies' searches with since_id=root.id.
    Attach only tweets whose in_reply_to_status_id is already a known node (keeps us inside the thread).
    """
    node: Dict[int, dict] = {}  # id -> {"tweet": status, "children": []}
    def make_node(t): return {"tweet": t, "children": []}
    node[root.id] = make_node(root)

    known_ids = set([root.id])
    queue = deque()

    root_user = getattr(root, "user", None)
    root_sn = (getattr(root_user, "screen_name", None) or "").lower()
    if root_sn:
        queue.append((root_sn, 0))

    total_collected = 1
    domain_links = defaultdict(list)
    collect_links(root, domain_links)

    processed_keys = set()  # (screen_name, depth)

    while queue and total_collected < MAX_TWEETS_TOTAL:
        sn, depth = queue.popleft()
        if depth > depth_limit or not sn:
            continue
        key = (sn, depth)
        if key in processed_keys:
            continue
        processed_keys.add(key)

        term = f"to:{sn} filter:replies"
        replies = do_search(api, term, since_id=root.id, max_pages=MAX_PAGES_SEARCH, cap=3000)

        attached = 0
        for t in replies:
            pid = getattr(t, "in_reply_to_status_id", None)
            if not pid or pid not in node:
                continue  # not attached to something we already have
            if t.id in node:
                continue
            node[t.id] = make_node(t)
            node[pid]["children"].append(node[t.id])
            known_ids.add(t.id)
            attached += 1
            total_collected += 1
            collect_links(t, domain_links)

        # sort children by time
        for n in node.values():
            if n["children"]:
                n["children"].sort(key=lambda x: x["tweet"].created_at_in_seconds or 0)

        # enqueue next authors
        if attached and depth + 1 <= depth_limit:
            new_users = set()
            for nid, n in node.items():
                t = n["tweet"]
                pid = getattr(t, "in_reply_to_status_id", None)
                if pid and pid in node:
                    u = getattr(t, "user", None)
                    sn2 = (getattr(u, "screen_name", None) or "").lower()
                    if sn2:
                        new_users.add(sn2)
            for sn2 in new_users:
                queue.append((sn2, depth + 1))

    return node, domain_links

# ------------ Rendering ------------
def render_tree(node_map: Dict[int, dict], root_id: int) -> str:
    def render(n, level=1):
        t = n["tweet"]
        user = getattr(t, "user", None)
        sn = getattr(user, "screen_name", None) or "user"
        name = getattr(user, "name", None) or sn
        prof = f"https://x.com/{esc(sn)}"
        tw   = f"https://x.com/{esc(sn)}/status/{t.id}"
        iso  = ts_iso(t)
        av   = avatar_for(user)
        content = expand_text(t)
        links   = link_chips(t)
        media   = media_block(t)

        child = ""
        if n["children"]:
            cid = f"children-{t.id}"
            btn = f"Show {len(n['children'])} reply" + ("" if len(n["children"]) == 1 else "ies")
            child = (
                f'\n      <button class="ss3k-toggle" data-label="{esc(btn)}" data-hide="Hide replies" '
                f'onclick="ss3kToggleReplies(\'{cid}\', this)">{esc(btn)}</button>\n'
                f'      <div class="ss3k-children" id="{cid}">\n'
                + "\n".join(render(c, level+1) for c in n["children"]) +
                "\n      </div>"
            )

        return f'''
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
              <a href="{tw}" target="_blank" rel="noopener">
                <time datetime="{iso}">{esc(iso.replace('T',' '))} UTC</time>
              </a>
            </div>
          </div>
        </div>
        <div class="ss3k-reply-content">{content}</div>{media}{links}{child}
      </div>'''.rstrip()

    html_top = '''<style>
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
    </script>
    <div class="ss3k-replies">'''
    html_bottom = "\n</div>\n"
    return html_top + "\n" + render(node_map[root_id]) + "\n" + html_bottom

def write_links_html(domain_links: Dict[str, List[str]], out_path: str):
    parts = []
    for dom in sorted(domain_links):
        links = "\n".join(
            f'  <li><a href="{esc(u)}" target="_blank" rel="noopener">{esc(u)}</a></li>'
            for u in domain_links[dom]
        )
        parts.append(f"<h4>{esc(dom)}</h4>\n<ul>\n{links}\n</ul>")
    open(out_path, "w", encoding="utf-8").write("\n\n".join(parts))

# ------------ Short (CSV) output ------------
def print_csv(node_map: Dict[int, dict], root_id: int):
    """
    Print CSV: date,reply_url,parent_url (breadth-first).
    """
    from collections import deque
    print("date,reply,parent_thread")
    q = deque([node_map[root_id]])
    while q:
        n = q.popleft()
        t = n["tweet"]
        iso = ts_iso(t).replace("T", " ")
        user = getattr(t, "user", None)
        sn = getattr(user, "screen_name", None) or "user"
        url = f"https://twitter.com/{sn}/status/{t.id}"
        # parent:
        pid = getattr(t, "in_reply_to_status_id", None)
        purl = ""
        if pid and pid in node_map:
            p = node_map[pid]["tweet"]
            psn = getattr(getattr(p, "user", None), "screen_name", None) or "user"
            purl = f"https://twitter.com/{psn}/status/{p.id}"
        print(f"{iso},{url},{purl}")
        for c in n["children"]:
            q.append(c)

# ------------ Main ------------
def main():
    parser = argparse.ArgumentParser(description="Build nested replies from a Purple Pill tweet URL (or list).")
    parser.add_argument("-u","--url", help="Single tweet URL (https://twitter.com/<screen_name>/status/<id>)")
    parser.add_argument("-f","--file", help="File with tweet URLs (one per line)")
    parser.add_argument("-s","--short", action="store_true", help="CSV to stdout (date,reply,parent)")
    args = parser.parse_args()

    # Resolve inputs
    urls = []
    if args.url:
        urls = [args.url.strip()]
    elif args.file:
        with open(args.file, "r", encoding="utf-8", errors="ignore") as fh:
            urls = [ln.strip() for ln in fh if ln.strip()]
    else:
        # env fallbacks
        env_url = os.environ.get("PURPLE_URL") or os.environ.get("TWEET_URL") or ""
        if env_url.strip():
            urls = [env_url.strip()]
        else:
            # also support default tweet.list in ARTDIR if present
            cand = os.path.join(ARTDIR, "tweet.list")
            if os.path.exists(cand):
                with open(cand, "r", encoding="utf-8", errors="ignore") as fh:
                    urls = [ln.strip() for ln in fh if ln.strip()]

    if not urls:
        print("No tweet URL(s) provided. Use --url, --file, PURPLE_URL/TWEET_URL env, or ARTDIR/tweet.list.", file=sys.stderr)
        sys.exit(0)

    api = create_api()
    os.makedirs(ARTDIR, exist_ok=True)

    # Aggregate outputs across all roots
    all_html_blocks = []
    all_links = defaultdict(list)
    total_nodes = 0

    for u in urls:
        try:
            sn, sid = parse_tweet_url(u)
        except Exception as e:
            print(e, file=sys.stderr)
            continue

        root = get_status_by_id(api, sid)
        if not root:
            continue

        try:
            node_map, links = build_thread_for_root(api, root, depth_limit=DEPTH_LIMIT)
        except Exception as e:
            print(f"Error building thread for {u}: {e}", file=sys.stderr)
            traceback.print_exc()
            continue

        total_nodes += len(node_map)

        # Merge links
        for dom, hrefs in links.items():
            for h in hrefs:
                if h not in all_links[dom]:
                    all_links[dom].append(h)

        # Render per-root block (keeps tree separated if multiple roots)
        all_html_blocks.append(render_tree(node_map, root.id))

        # Optional CSV
        if args.short:
            print_csv(node_map, root.id)

    # Write combined HTML files
    OUT_REPLIES = os.path.join(ARTDIR, f"{BASE}_replies.html")
    OUT_LINKS   = os.path.join(ARTDIR, f"{BASE}_links.html")

    if all_html_blocks:
        # Wrap multiple blocks
        page = "\n<hr>\n".join(all_html_blocks)
        open(OUT_REPLIES, "w", encoding="utf-8").write(page)
    else:
        # still write an empty shell to avoid 404 in downstream steps
        open(OUT_REPLIES, "w", encoding="utf-8").write("<div class=\"ss3k-replies\"></div>\n")

    write_links_html(all_links, OUT_LINKS)

    print(f"Wrote replies HTML → {OUT_REPLIES}")
    print(f"Wrote links HTML   → {OUT_LINKS}")
    print(f"Tweets captured    → {total_nodes}")

if __name__ == "__main__":
    main()
