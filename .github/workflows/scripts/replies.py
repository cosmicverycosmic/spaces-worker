#!/usr/bin/env python3
import os, re, json, html, time
from collections import defaultdict
import requests, tldextract

ARTDIR = os.environ.get("ARTDIR", ".")
BASE   = os.environ.get("BASE", "space")
PURPLE = os.environ.get("PURPLE_TWEET_URL", "").strip()
SPACE_ID = os.environ.get("SPACE_ID", "").strip()

# Optional v1.1 keys (if present we'll use python-twitter; otherwise cookie/bearer fallback)
TW_API_CONSUMER_KEY        = os.environ.get("TW_API_CONSUMER_KEY", "")
TW_API_CONSUMER_SECRET     = os.environ.get("TW_API_CONSUMER_SECRET", "")
TW_API_ACCESS_TOKEN        = os.environ.get("TW_API_ACCESS_TOKEN", "")
TW_API_ACCESS_TOKEN_SECRET = os.environ.get("TW_API_ACCESS_TOKEN_SECRET", "")

AUTH = os.environ.get("TWITTER_AUTHORIZATION", "")
AUTH_TOKEN = os.environ.get("TWITTER_AUTH_TOKEN", "")
CSRF = os.environ.get("TWITTER_CSRF_TOKEN", "")

def esc(s): return html.escape(s or "", quote=True)

def ensure_dirs():
    os.makedirs(ARTDIR, exist_ok=True)

def write_empty_and_exit():
    open(os.path.join(ARTDIR, f"{BASE}_replies.html"), "w", encoding="utf-8").write("")
    open(os.path.join(ARTDIR, f"{BASE}_links.html"),   "w", encoding="utf-8").write("")
    raise SystemExit(0)

def extract_tweet_id(url):
    m = re.search(r"/status/(\d+)", url)
    return m.group(1) if m else ""

def fetch_syndication(id_or_url):
    url = "https://cdn.syndication.twimg.com/tweet-result"
    params = {"id": id_or_url} if id_or_url.isdigit() else {"url": id_or_url}
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.ok: return r.json()
    except Exception:
        pass
    return {}

def cookie_headers():
    return {
        "authorization": AUTH,
        "x-twitter-active-user": "yes",
        "x-twitter-auth-type": "OAuth2Session",
        "x-csrf-token": CSRF,
        "user-agent": "Mozilla/5.0",
        "accept": "application/json"
    }, {"auth_token": AUTH_TOKEN, "ct0": CSRF}

def call_search_adaptive(q, cursor=None):
    headers, cookies = cookie_headers()
    params = {
        "q": q,
        "count": "100",
        "tweet_mode": "extended",
        "query_source": "typed_query",
        "pc": "1",
        "spelling_corrections": "1",
    }
    if cursor: params["cursor"] = cursor
    url = "https://api.twitter.com/2/search/adaptive.json"
    r = requests.get(url, headers=headers, cookies=cookies, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def collect_conversation_with_cookie(root_id, screen_name):
    tweets, users = {}, {}
    cursor = None
    pages = 0
    # Query focuses on this conversation; adding `to:screen_name` narrows noise
    q = f"conversation_id:{root_id}" + (f" to:{screen_name}" if screen_name else "")
    while pages < 12:
        data = call_search_adaptive(q, cursor)
        pages += 1
        go = data.get("globalObjects") or {}
        tweets.update(go.get("tweets") or {})
        users.update(go.get("users") or {})
        # find next cursor (Bottom)
        cursor = None
        tl = data.get("timeline") or {}
        for instr in tl.get("instructions", []):
            ae = instr.get("addEntries", {})
            if ae:
                for e in ae.get("entries", []):
                    cur = e.get("content", {}).get("operation", {}).get("cursor", {})
                    if cur.get("cursorType") == "Bottom":
                        cursor = cur.get("value")
            re_ = instr.get("replaceEntry", {})
            if re_:
                e = re_.get("entry", {})
                cur = e.get("content", {}).get("operation", {}).get("cursor", {})
                if cur.get("cursorType") == "Bottom":
                    cursor = cur.get("value")
        if not cursor:
            break
        time.sleep(0.7)
    return tweets, users

def build_and_write_html(root_id, tweets, users):
    # Only tweets in this conversation (skip the root in the list)
    conv = {tid:t for tid,t in tweets.items() if t.get("conversation_id_str")==root_id and tid != root_id}

    # parent -> children list
    by_parent = defaultdict(list)
    for tid, t in conv.items():
        pid = t.get("in_reply_to_status_id_str") or root_id
        by_parent[pid].append(tid)
    for lst in by_parent.values():
        lst.sort(key=lambda tid: tweets[tid].get("id_str"))

    def user_of(t):
        uid = t.get("user_id_str") or ""
        return users.get(uid) or {}

    def links_and_media(t):
        chips=[]; seen=set()
        ents = (t.get("entities") or {})
        for u in ents.get("urls", []):
            href = u.get("expanded_url") or u.get("url") or ""
            if not href or href in seen: continue
            seen.add(href)
            dom = tldextract.extract(href)
            host = dom.registered_domain or href
            chips.append(f'<a class="ss3k-link-card" href="{esc(href)}" target="_blank" rel="noopener">{esc(host)}</a>')
        media_html=""
        ents2=(t.get("extended_entities") or {})
        for m in ents2.get("media", []):
            if m.get("type")=="photo" and m.get("media_url_https"):
                media_html += f'\n        <img class="reply-media" src="{esc(m["media_url_https"])}" alt="">'
        return (('\n        <div class="ss3k-link-cards">'+"\n          "+"\n          ".join(chips)+"\n        </div>") if chips else ""), media_html

    def render(pid):
        out=[]
        for tid in by_parent.get(pid, []):
            t = tweets[tid]
            u = user_of(t)
            sn = u.get("screen_name") or ""
            nm = u.get("name") or sn
            prof = f"https://x.com/{esc(sn)}" if sn else "#"
            twurl = f"https://x.com/{esc(sn)}/status/{tid}" if sn else f"https://x.com/i/status/{tid}"
            av = u.get("profile_image_url_https") or (f"https://unavatar.io/x/{esc(sn)}" if sn else "")
            txt = t.get("full_text") or t.get("text") or ""
            ents = (t.get("entities") or {})
            for uo in ents.get("urls", []):
                if uo.get("url") and uo.get("expanded_url"):
                    txt = txt.replace(uo["url"], uo["expanded_url"])
            links, media = links_and_media(t)
            created = t.get("created_at") or ""
            card = f'''
      <div class="ss3k-reply-card" id="{tid}">
        <div class="ss3k-reply-head">
          <a href="{prof}" target="_blank" rel="noopener"><img class="avatar" src="{esc(av)}" alt=""></a>
          <div>
            <div><a href="{prof}" target="_blank" rel="noopener"><strong>{esc(nm)}</strong></a> <span style="color:#64748b">@{esc(sn)}</span></div>
            <div class="ss3k-reply-meta"><a href="{twurl}" target="_blank" rel="noopener">{esc(created)}</a></div>
          </div>
        </div>
        <div class="ss3k-reply-content">{esc(txt)}</div>{media}{links}
      </div>'''.rstrip()
            out.append(card)
            out.extend(render(tid))
        return out

    style = '''<style>
.ss3k-replies{font:14px/1.45 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
.ss3k-reply-card{border:1px solid #e2e8f0;border-radius:12px;padding:12px;margin:10px 0;background:#fff;box-shadow:0 1px 1px rgba(0,0,0,.03)}
.ss3k-reply-head{display:flex;align-items:center;gap:8px}
.ss3k-reply-head img.avatar{width:28px;height:28px;border-radius:50%}
.ss3k-reply-meta{color:#64748b;font-size:12px;margin-top:4px}
.ss3k-reply-content{margin-top:8px;white-space:pre-wrap;word-break:break-word}
.ss3k-link-cards{margin-top:8px;display:flex;flex-wrap:wrap;gap:6px}
.ss3k-link-card{border:1px solid #cbd5e1;padding:4px 8px;border-radius:8px;background:#f8fafc;font-size:12px}
.reply-media{max-width:100%;border-radius:10px;margin-top:8px}
</style>
<div class="ss3k-replies">
'''
    root_link = f"https://x.com/i/status/{root_id}"
    html_out = style + f'<div class="ss3k-reply-meta">Thread: <a href="{esc(root_link)}" target="_blank" rel="noopener">{esc(root_link)}</a></div>\n'
    html_out += "\n".join(render(root_id)) + "\n</div>\n"
    open(os.path.join(ARTDIR, f"{BASE}_replies.html"), "w", encoding="utf-8").write(html_out)

    # Shared links by domain
    domain_links=defaultdict(list)
    for t in conv.values():
        for uo in (t.get("entities") or {}).get("urls", []):
            href = uo.get("expanded_url") or uo.get("url") or ""
            if not href: continue
            dom = tldextract.extract(href)
            host = dom.registered_domain or href
            if href not in domain_links[host]:
                domain_links[host].append(href)
    parts=[]
    for dom, urls in sorted(domain_links.items()):
        parts.append("<h4>"+esc(dom)+"</h4>\n<ul>\n" + "\n".join(f'  <li><a href="{esc(u)}" target="_blank" rel="noopener">{esc(u)}</a></li>' for u in urls) + "\n</ul>")
    open(os.path.join(ARTDIR, f"{BASE}_links.html"), "w", encoding="utf-8").write("\n\n".join(parts))

def main():
    ensure_dirs()

    # We need at least a purple tweet URL (best) or a space id (not implemented for root discovery here).
    root_id = extract_tweet_id(PURPLE) if PURPLE else ""
    if not root_id:
        write_empty_and_exit()

    # Try v1.1 first if keys exist
    if all([TW_API_CONSUMER_KEY, TW_API_CONSUMER_SECRET, TW_API_ACCESS_TOKEN, TW_API_ACCESS_TOKEN_SECRET]):
        # Defer to the previous v1.1 implementation if you still keep it around,
        # otherwise just fall through to cookie scraping for simplicity.
        pass  # we’ll use cookie flow below for consistency
    # Cookie/bearer fallback
    if not (AUTH.startswith("Bearer ") and AUTH_TOKEN and CSRF):
        write_empty_and_exit()

    # Resolve screen_name cheaply via syndication (unauthenticated)
    screen_name = ""
    syn = fetch_syndication(root_id) or fetch_syndication(PURPLE)
    if syn:
        screen_name = (syn.get("user") or {}).get("screen_name") or ""

    tweets, users = collect_conversation_with_cookie(root_id, screen_name)
    if not tweets:
        write_empty_and_exit()

    build_and_write_html(root_id, tweets, users)

if __name__ == "__main__":
    main()
