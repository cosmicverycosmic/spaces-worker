#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
replies.py
----------
Fetches and renders tweet replies for a given "purple pill" tweet (root of convo).

ENV:
  ARTDIR             - output directory (default ".")
  BASE               - base filename (default "space")
  PURPLE_TWEET_URL   - e.g., https://x.com/user/status/1234567890123456789
  TWITTER_AUTHORIZATION - optional "Bearer ..." (guest activation path)
  TWITTER_AUTH_TOKEN    - optional cookie "auth_token"
  TWITTER_CSRF_TOKEN    - optional cookie "ct0"

Outputs:
  {BASE}_replies.html
  {BASE}_links.html
"""

import os, re, sys, json, html, time
from collections import defaultdict
from typing import Dict, Any, Tuple

try:
    import tldextract  # installed by workflow; optional
except Exception:
    tldextract = None

ARTDIR = os.environ.get("ARTDIR",".")
BASE   = os.environ.get("BASE","space")
PURPLE = (os.environ.get("PURPLE_TWEET_URL","") or "").strip()

REPLIES_OUT = os.path.join(ARTDIR, f"{BASE}_replies.html")
LINKS_OUT   = os.path.join(ARTDIR, f"{BASE}_links.html")

def write_empty():
    open(REPLIES_OUT,"w",encoding="utf-8").write("")
    open(LINKS_OUT,"w",encoding="utf-8").write("")

def esc(x): return html.escape(x or "")

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

def get_guest_token(bearer):
    if not bearer: return None
    headers = {
        "authorization": bearer,
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0"
    }
    d = http_json("https://api.twitter.com/1.1/guest/activate.json", method="POST", headers=headers, data="{}")
    return (d or {}).get("guest_token")

def expand_text_with_entities(text, entities):
    text = text or ""
    if not entities:
        return esc(text)
    urls = entities.get("urls") or []
    out = text
    # replace t.co with expanded URLs
    for u in sorted(urls, key=lambda x: len(x.get("url","")), reverse=True):
        short = u.get("url") or ""
        expanded = u.get("expanded_url") or u.get("unwound_url") or short
        if short and short in out:
            out = out.replace(short, expanded)
    out = re.sub(r'(https?://\S+)', r'<a href="\1" target="_blank" rel="noopener">\1</a>', esc(out))
    return out

def add_link(bucket, url):
    if not url: return
    dom = url
    if tldextract:
        try:
            ext = tldextract.extract(url)
            dom = ".".join([p for p in [ext.domain, ext.suffix] if p])
        except Exception:
            pass
    bucket[dom].add(url)

def name_for_url(url):
    try:
        m = re.match(r"https?://([^/]+)(/[^?#]*)?", url)
        host = m.group(1) if m else ""
        path = (m.group(2) or "/").strip("/")
        host_core = host.split(":")[0].lower()
        if "youtube." in host_core or "youtu.be" in host_core: return "YouTube"
        if "substack" in host_core: return "Substack"
        if "x.com" in host_core or "twitter.com" in host_core: return "Tweet"
        if "chbmp" in host_core: return "CHBMP"
        if "who.int" in host_core: return "WHO"
        if "nih.gov" in host_core or "pubmed" in host_core: return "PubMed"
        parts = [p for p in re.split(r"[-_/]+", path) if p and len(p) > 2][:3]
        parts = [re.sub(r'[^A-Za-z0-9]+','',p).title() for p in parts][:3]
        if parts: return " ".join(parts)
        brand = host_core.split(".")
        brand = brand[-2] if len(brand) > 1 else brand[0]
        return brand.title()
    except Exception:
        return "Link"

def try_web_conversation(tid_str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    bearer = (os.environ.get("TWITTER_AUTHORIZATION","") or "").strip()
    at     = (os.environ.get("TWITTER_AUTH_TOKEN","") or "").strip()
    ct0    = (os.environ.get("TWITTER_CSRF_TOKEN","") or "").strip()
    headers = {"user-agent":"Mozilla/5.0","accept":"application/json, text/plain, */*"}
    if at and ct0:
        # Cookie path (more reliable)
        if bearer:
            headers.update({"authorization": bearer})
        headers.update({"x-csrf-token": ct0, "cookie": f"auth_token={at}; ct0={ct0}"})
    elif bearer:
        gt = get_guest_token(bearer)
        if not gt: return {}, {}
        headers.update({"authorization": bearer, "x-guest-token": gt})
    else:
        return {}, {}

    url = f"https://api.twitter.com/2/timeline/conversation/{tid_str}.json?tweet_mode=extended&include_ext_alt_text=true"
    data = http_json(url, headers=headers)
    if not data:
        return {}, {}
    tweets = (data.get("globalObjects", {}) or {}).get("tweets", {}) or {}
    users  = (data.get("globalObjects", {}) or {}).get("users", {}) or {}
    return tweets, users

def parse_time(ts):
    try:
        return time.mktime(time.strptime(ts, "%a %b %d %H:%M:%S %z %Y"))
    except Exception:
        return 0.0

def build_tree(tweets, users, root_id):
    nodes={}
    children=defaultdict(list)
    links_by_domain=defaultdict(set)

    for tid, tw in tweets.items():
        conv = str(tw.get("conversation_id_str") or tw.get("conversation_id") or "")
        if conv != root_id: 
            continue
        nodes[tid] = tw
        ent = tw.get("entities") or {}
        for u in (ent.get("urls") or []):
            add_link(links_by_domain, u.get("expanded_url") or u.get("unwound_url") or u.get("url"))

    for tid, tw in nodes.items():
        parent = tw.get("in_reply_to_status_id_str") or ""
        if parent and parent in nodes:
            children[parent].append(tid)
        elif parent == root_id:
            children[root_id].append(tid)

    for p in children:
        children[p].sort(key=lambda k: parse_time(nodes[k].get("created_at","")))

    return nodes, children, links_by_domain

def render_thread(nodes, children, users, root_id):
    def render_item(tid, depth=0):
        t = nodes[tid]
        uid = str(t.get("user_id_str") or "")
        u = users.get(uid, {})
        name   = u.get("name") or "User"
        handle = u.get("screen_name") or ""
        avatar = (u.get("profile_image_url_https") or u.get("profile_image_url") or "").replace("_normal.","_bigger.")
        text   = expand_text_with_entities(t.get("full_text") or t.get("text") or "", t.get("entities") or {})
        url    = f"https://x.com/{handle}/status/{tid}"
        ts     = t.get("created_at","")
        ts_short = ts
        try:
            ts_short = time.strftime("%Y-%m-%d %H:%M:%SZ", time.strptime(ts, "%a %b %d %H:%M:%S %z %Y"))
        except Exception:
            pass
        kids   = children.get(tid, [])

        head = (
          f'<div class="ss3k-reply" style="margin-left:{depth*16}px">'
          f'  <a href="https://x.com/{handle}" target="_blank" rel="noopener"><img class="ss3k-ravatar" src="{esc(avatar)}" alt=""></a>'
          f'  <div class="ss3k-rcontent">'
          f'    <div class="ss3k-rname"><a href="https://x.com/{handle}" target="_blank" rel="noopener">{esc(name)}</a>'
          f'      <span class="ss3k-rhandle">@{esc(handle)}</span>'
          f'      · <a href="{url}" target="_blank" rel="noopener" class="ss3k-rtime">{esc(ts_short)}</a>'
          f'    </div>'
          f'    <div class="ss3k-rtext">{text}</div>'
          f'  </div>'
          f'</div>'
        )

        if not kids:
            return head
        body = "".join(render_item(k, depth+1) for k in kids)
        return head + f'<details class="ss3k-thread" style="margin-left:{depth*16}px"><summary>Show {len(kids)} repl{"y" if len(kids)==1 else "ies"}</summary>{body}</details>'

    roots = children.get(root_id, [])
    roots.sort(key=lambda k: parse_time(nodes[k].get("created_at","")))

    css = """
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
    """
    return css + "".join(render_item(tid,0) for tid in roots)

def main():
    if not PURPLE:
        write_empty(); return
    m = re.search(r"/status/(\d+)", PURPLE)
    if not m:
        write_empty(); return
    tid_str = m.group(1)

    tweets, users = try_web_conversation(tid_str)
    if not tweets:
        write_empty(); return

    nodes, children, links_by_domain = build_tree(tweets, users, tid_str)
    if not nodes:
        write_empty(); return

    open(REPLIES_OUT,"w",encoding="utf-8").write(render_thread(nodes, children, users, tid_str))

    # link summary
    lines=["<div class='ss3k-links'>"]
    for dom in sorted(links_by_domain):
        lines.append(f"<h4>{esc(dom)}</h4>")
        lines.append("<ul>")
        for u in sorted(links_by_domain[dom]):
            nm = name_for_url(u)
            eu = esc(u)
            lines.append(f'<li><a href="{eu}" target="_blank" rel="noopener">{esc(nm)}</a></li>')
        lines.append("</ul>")
    lines.append("</div>")
    open(LINKS_OUT,"w",encoding="utf-8").write("\n".join(lines))

if __name__ == "__main__":
    main()
