#!/usr/bin/env python3
# Scrapes tweet replies for the Space "purple pill" tweet (best-effort).
# Inputs (env):
#   ARTDIR, BASE, SPACE_ID
#   TW_API_CONSUMER_KEY, TW_API_CONSUMER_SECRET, TW_API_ACCESS_TOKEN, TW_API_ACCESS_TOKEN_SECRET
# Optional:
#   reads ARTDIR/_as_line.json to infer creator username (for root tweet selection).

import os, json, html, sys
from collections import defaultdict

try:
    import tldextract
except Exception:
    print("tldextract missing", file=sys.stderr)
    sys.exit(0)

try:
    import twitter  # python-twitter
except Exception:
    print("python-twitter missing", file=sys.stderr)
    sys.exit(0)

out_dir = os.environ.get("ARTDIR") or ""
base    = os.environ.get("BASE") or ""
space_id = (os.environ.get("SPACE_ID") or "").strip()

if not (out_dir and base and space_id):
    # Nothing to do
    sys.exit(0)

os.makedirs(out_dir, exist_ok=True)

# Try to infer creator username from crawler metadata line
creator = None
as_json = os.path.join(out_dir, "_as_line.json")
if os.path.exists(as_json):
    try:
        data = json.load(open(as_json, "r", encoding="utf-8", errors="ignore"))
        a = (data.get("audioSpace") or {})
        creator = a.get("username") or a.get("creator_results", {}).get("result", {}).get("legacy", {}).get("screen_name")
    except Exception:
        creator = None

ck  = os.environ.get("TW_API_CONSUMER_KEY") or ""
cs  = os.environ.get("TW_API_CONSUMER_SECRET") or ""
at  = os.environ.get("TW_API_ACCESS_TOKEN") or ""
ats = os.environ.get("TW_API_ACCESS_TOKEN_SECRET") or ""

if not (ck and cs and at and ats):
    # Credentials not present; exit quietly
    sys.exit(0)

api = twitter.Api(
    consumer_key=ck,
    consumer_secret=cs,
    access_token_key=at,
    access_token_secret=ats,
    sleep_on_rate_limit=True,
    tweet_mode='extended'
)

def do_search(q, pages=5):
    """Search helper with pagination (100 per page)."""
    res = []
    max_id = None
    for _ in range(pages):
        batch = api.GetSearch(term=q, count=100, include_entities=True, max_id=max_id, result_type='recent')
        if not batch:
            break
        res.extend(batch)
        max_id = min(t.id for t in batch) - 1
    return res

terms = [f"i/spaces/{space_id}", f"https://twitter.com/i/spaces/{space_id}"]
candidates = do_search(terms[0])
if creator:
    candidates += do_search(f'from:{creator} "{terms[0]}"')

def has_space_link(t):
    txt = (getattr(t, "full_text", None) or t.text or "")
    if space_id in txt:
        return True
    for u in (t.urls or []):
        expanded = (u.expanded_url or u.url) or ""
        if space_id in expanded:
            return True
    return False

cands = [t for t in candidates if has_space_link(t)]
if not cands:
    # No root candidates found
    sys.exit(0)

# Choose a "root" tweet — prefer creator's earliest, else earliest overall
root = None
if creator:
    for t in sorted(cands, key=lambda x: x.created_at_in_seconds or 0):
        if t.user and (t.user.screen_name or "").lower() == creator.lower():
            root = t
            break
if root is None:
    root = sorted(cands, key=lambda x: x.created_at_in_seconds or 0)[0]

# Fetch replies (best-effort) using a "to:user since_id:root"
q = f"to:{root.user.screen_name} since_id:{root.id}"
all_replies = []
max_id = None
for _ in range(25):  # many pages; rate limited by python-twitter
    batch = api.GetSearch(term=q, count=100, include_entities=True, max_id=max_id, result_type='recent')
    if not batch:
        break
    all_replies.extend(batch)
    max_id = min(t.id for t in batch) - 1

# Build a tree (flat + children list)
node = {}
def make_node(t): return {"tweet": t, "children": []}
node[root.id] = make_node(root)
for t in all_replies:
    node[t.id] = make_node(t)
for t in all_replies:
    pid = t.in_reply_to_status_id
    if pid in node:
        node[pid]["children"].append(node[t.id])
for n in node.values():
    n["children"].sort(key=lambda x: x["tweet"].created_at_in_seconds or 0)

def esc(s): return html.escape(s or "", True)

def avatar(user):
    if not user:
        return ""
    return (user.profile_image_url_https or
            user.profile_image_url or
            f"https://unavatar.io/x/{esc(user.screen_name)}")

def link_chips(t):
    chips = []
    seen = set()
    for u in (t.urls or []):
        href = u.expanded_url or u.url
        if not href or href in seen:
            continue
        seen.add(href)
        host = tldextract.extract(href).registered_domain or href
        chips.append(f'<a class="ss3k-link-card" href="{esc(href)}" target="_blank" rel="noopener">{esc(host)}</a>')
    if chips:
        return '\n        <div class="ss3k-link-cards">\n          ' + "\n          ".join(chips) + "\n        </div>"
    return ""

def media_block(t):
    media = getattr(t, "media", None) or []
    parts = []
    for m in media:
        if getattr(m, "type", None) == "photo" and getattr(m, "media_url_https", None):
            parts.append(f'<img class="reply-media" src="{esc(m.media_url_https)}" alt="">')
    return ("\n        " + "\n        ".join(parts)) if parts else ""

# Collect shared links (for sidebar/links artifact)
domain_links = defaultdict(list)
def collect_links(t):
    for u in (t.urls or []):
        href = u.expanded_url or u.url
        if not href:
            continue
        host = tldextract.extract(href).registered_domain or href
        if href not in domain_links[host]:
            domain_links[host].append(href)

for t in [root] + all_replies:
    collect_links(t)

def render(n, level=1):
    t = n["tweet"]
    sn = t.user.screen_name if t.user else "user"
    name = t.user.name if t.user else sn
    prof = f"https://x.com/{esc(sn)}"
    tw   = f"https://x.com/{esc(sn)}/status/{t.id}"
    dt   = t.created_at_in_seconds or 0
    iso  = datetime_from_ts(dt)
    av   = avatar(t.user)

    # Expand t.co in text
    txt = (getattr(t, "full_text", None) or t.text or "")
    for u in (t.urls or []):
        if u.expanded_url and u.url:
            txt = txt.replace(u.url, u.expanded_url)
    content = esc(txt)
    links = link_chips(t)
    media = media_block(t)

    child = ""
    if n["children"]:
        cid = f"children-{t.id}"
        count = len(n["children"])
        btn = f"Show {count} repl{'y' if count == 1 else 'ies'}"
        child = (
            f'\n      <button class="ss3k-toggle" data-label="{esc(btn)}" data-hide="Hide replies" onclick="ss3kToggleReplies(\'{cid}\', this)">{esc(btn)}</button>\n'
            f'      <div class="ss3k-children" id="{cid}">\n' +
            "\n".join(render(c, level + 1) for c in n["children"]) +
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
              <a href="{tw}" target="_blank" rel="noopener"><time datetime="{iso}">{iso.replace('T',' ')} UTC</time></a>
            </div>
          </div>
        </div>
        <div class="ss3k-reply-content">{content}</div>{media}{links}{child}
      </div>'''.rstrip()

from datetime import datetime, timezone
def datetime_from_ts(ts):
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")

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

replies_html = html_top + "\n" + render(node[root.id]) + "\n" + html_bottom

# Write replies HTML artifact
open(os.path.join(out_dir, f"{base}_replies.html"), "w", encoding="utf-8").write(replies_html)

# Write grouped links artifact
parts = []
for dom in sorted(domain_links):
    links = "\n".join(
        f'  <li><a href="{html.escape(u, True)}" target="_blank" rel="noopener">{html.escape(u, True)}</a></li>'
        for u in domain_links[dom]
    )
    parts.append(f"<h4>{html.escape(dom, True)}</h4>\n<ul>\n{links}\n</ul>")
links_html = "\n\n".join(parts)
open(os.path.join(out_dir, f"{base}_links.html"), "w", encoding="utf-8").write(links_html)
