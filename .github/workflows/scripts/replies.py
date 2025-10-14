# .github/workflows/scripts/replies.py
import os, re, sys, json, html
from collections import defaultdict

# Optional deps that are already in the workflow:
try:
    import tldextract
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

def add_link(links_by_domain, url):
    if not url: return
    dom = url
    if tldextract:
        ext = tldextract.extract(url)
        dom = ".".join([p for p in [ext.domain, ext.suffix] if p])
    links_by_domain[dom].add(url)

def render_replies(replies):
    parts = []
    for r in replies:
        name = r.get("name") or "User"
        handle = r.get("handle")
        display = f"{name} (@{handle})" if handle else name
        avatar = r.get("avatar") or ""
        text = r.get("text") or ""

        parts.append(
            f'<div class="ss3k-reply">'
            f'  <img class="ss3k-ravatar" src="{esc(avatar)}" alt="">'
            f'  <div class="ss3k-rcontent">'
            f'    <div class="ss3k-rname">{esc(display)}</div>'
            f'    <div class="ss3k-rtext">{text}</div>'
            f'  </div>'
            f'</div>'
        )
    return "\n".join(parts)

def render_links(links_by_domain):
    out = []
    for dom in sorted(links_by_domain.keys()):
        out.append(f"<h4>{esc(dom)}</h4>")
        out.append("<ul>")
        for u in sorted(links_by_domain[dom]):
            e = esc(u)
            out.append(f'<li><a href="{e}" target="_blank" rel="noopener">{e}</a></li>')
        out.append("</ul>")
    return "\n".join(out)

def expand_text_with_entities(text, entities):
    """Replace t.co URLs with expanded_url, linkify; preserve original if missing."""
    text = text or ""
    if not entities: 
        return esc(text)
    urls = entities.get("urls") or []
    out = text
    # Replace longer first to avoid overlaps
    urls_sorted = sorted(urls, key=lambda u: len(u.get("url","")), reverse=True)
    for u in urls_sorted:
        short = u.get("url") or ""
        expanded = u.get("expanded_url") or u.get("unwound_url") or short
        if short and short in out:
            out = out.replace(short, expanded)
    # Minimal linkify for any remaining http(s)://… strings
    out = re.sub(r'(https?://\S+)', r'<a href="\1" target="_blank" rel="noopener">\1</a>', out)
    return out

# ---------- Path A: Twitter API v1.1 if keys provided ----------
def try_v1(screen_name, tid_int):
    ck  = os.environ.get("TW_API_CONSUMER_KEY","")
    cs  = os.environ.get("TW_API_CONSUMER_SECRET","")
    at  = os.environ.get("TW_API_ACCESS_TOKEN","")
    ats = os.environ.get("TW_API_ACCESS_TOKEN_SECRET","")
    if not all([ck,cs,at,ats]):
        return None

    try:
        import twitter
    except Exception:
        return None

    try:
        api = twitter.Api(
            consumer_key=ck, consumer_secret=cs,
            access_token_key=at, access_token_secret=ats,
            tweet_mode="extended", sleep_on_rate_limit=True
        )
        root = api.GetStatus(status_id=tid_int, include_my_retweet=False)
    except Exception:
        return None

    replies = []
    links_by_domain = defaultdict(set)

    # include links from root tweet
    try:
        for u in (getattr(root, "urls", None) or []):
            add_link(links_by_domain, getattr(u, "expanded_url", None) or getattr(u, "url", None))
    except Exception:
        pass

    # search replies to root
    try:
        rs = api.GetSearch(term=f"to:{screen_name}", since_id=tid_int, count=100, result_type="recent", include_entities=True)
        for t in rs:
            if getattr(t, "in_reply_to_status_id", None) == tid_int:
                txt = getattr(t, "full_text", None) or getattr(t, "text", "") or ""
                # linkify using entities if possible
                ent = getattr(t, "urls", None)
                if ent:
                    # python-twitter gives a list of objects; map to dicts with .expanded_url / .url.
                    for u in ent:
                        add_link(links_by_domain, getattr(u, "expanded_url", None) or getattr(u, "url", None))
                # fallback plain linkify
                txt_html = re.sub(r'(https?://\S+)', r'<a href="\1" target="_blank" rel="noopener">\1</a>', esc(txt))
                u = getattr(t, "user", None)
                replies.append({
                    "name": getattr(u, "name", "User") if u else "User",
                    "handle": getattr(u, "screen_name", None) if u else None,
                    "avatar": (getattr(u, "profile_image_url_https", "") if u else ""),
                    "text": txt_html
                })
    except Exception:
        pass

    return {"replies": replies, "links": links_by_domain}

# ---------- Path B: Web conversation API with cookie/bearer ----------
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

def try_web_conversation(tid_str):
    bearer = (os.environ.get("TWITTER_AUTHORIZATION","") or "").strip()
    at     = (os.environ.get("TWITTER_AUTH_TOKEN","") or "").strip()
    ct0    = (os.environ.get("TWITTER_CSRF_TOKEN","") or "").strip()

    if not (bearer or (at and ct0)):
        return None

    headers = {
        "user-agent": "Mozilla/5.0",
        "accept": "application/json, text/plain, */*",
    }

    # Prefer cookie+csrf if present (more reliable)
    if at and ct0:
        headers.update({
            "authorization": bearer or "Bearer",
            "x-csrf-token": ct0,
            "cookie": f"auth_token={at}; ct0={ct0}",
        })
    elif bearer:
        gt = get_guest_token(bearer)
        if not gt: 
            return None
        headers.update({
            "authorization": bearer,
            "x-guest-token": gt,
        })

    # v2 timeline conversation (legacy JSON shape with globalObjects)
    url = f"https://api.twitter.com/2/timeline/conversation/{tid_str}.json?tweet_mode=extended&include_ext_alt_text=true"
    data = http_json(url, headers=headers)
    if not data:
        return None

    tweets = (data.get("globalObjects", {}) or {}).get("tweets", {}) or {}
    users  = (data.get("globalObjects", {}) or {}).get("users", {}) or {}
    if not tweets:
        return None

    replies = []
    links_by_domain = defaultdict(set)

    # collect replies to root
    for tw in tweets.values():
        if tw.get("in_reply_to_status_id_str") == tid_str:
            uid = tw.get("user_id_str")
            u = users.get(uid, {}) if uid else {}
            name   = u.get("name") or "User"
            handle = u.get("screen_name")
            avatar = u.get("profile_image_url_https") or u.get("profile_image_url") or ""
            text   = tw.get("full_text") or tw.get("text") or ""

            ent = tw.get("entities") or {}
            txt_html = expand_text_with_entities(text, ent)

            # stash link targets
            for uu in (ent.get("urls") or []):
                add_link(links_by_domain, uu.get("expanded_url") or uu.get("unwound_url") or uu.get("url"))

            replies.append({
                "name": name,
                "handle": handle,
                "avatar": avatar,
                "text": txt_html
            })

    # include root tweet links
    root = tweets.get(tid_str)
    if root:
        ent = root.get("entities") or {}
        for uu in (ent.get("urls") or []):
            add_link(links_by_domain, uu.get("expanded_url") or uu.get("unwound_url") or uu.get("url"))

    return {"replies": replies, "links": links_by_domain}

def main():
    if not PURPLE:
        write_empty(); return

    m = re.search(r"/([^/]+)/status/(\d+)", PURPLE)
    if not m:
        write_empty(); return

    screen_name = m.group(1)
    tid_str     = m.group(2)
    tid_int     = int(tid_str)

    # Try v1 first, fallback to web timeline
    payload = try_v1(screen_name, tid_int)
    if payload is None:
        payload = try_web_conversation(tid_str)

    if payload is None:
        # no creds / no data
        write_empty()
        return

    replies = payload.get("replies") or []
    links_by_domain = payload.get("links") or defaultdict(set)

    open(REPLIES_OUT, "w", encoding="utf-8").write(render_replies(replies))
    open(LINKS_OUT, "w", encoding="utf-8").write(render_links(links_by_domain))

if __name__ == "__main__":
    main()
