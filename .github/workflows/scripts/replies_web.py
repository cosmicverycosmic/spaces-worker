#!/usr/bin/env python3
# .github/workflows/scripts/replies_web.py
import os, json, html, re, sys
from datetime import datetime, timezone

PATH = os.environ.get("REPLIES_JSONL") or ""
post_id = int(os.environ.get("WP_POST_ID") or "0")
if not PATH or not os.path.isfile(PATH) or post_id <= 0:
    print("No replies JSONL or post_id; nothing to do.")
    sys.exit(0)

def first(*xs):
    for x in xs:
        if x not in (None, "", []):
            return x
    return None

def fmt_int(n):
    try:
        n = int(n)
    except Exception:
        return "0"
    if n >= 1_000_000:
        s = f"{n/1_000_000:.1f}".rstrip("0").rstrip(".")
        return f"{s}M"
    if n >= 1_000:
        s = f"{n/1_000:.1f}".rstrip("0").rstrip(".")
        return f"{s}K"
    return str(n)

def parse_created(s):
    """
    Try to turn 'created_at' into 'YYYY-MM-DD HH:MM' local time (or return original on failure).
    Handles common Twitter formats and ISO-ish strings.
    """
    if not s or not isinstance(s, str):
        return ""
    s = s.strip()
    fmts = [
        "%a %b %d %H:%M:%S %z %Y",  # Wed Oct 30 12:34:56 +0000 2025
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            if dt.tzinfo is None:
                # Assume UTC if naive
                dt = dt.replace(tzinfo=timezone.utc)
            # Format without timezone; runner localtime is fine for display
            return dt.astimezone().strftime("%Y-%m-%d %H:%M")
        except Exception:
            continue
    # If it's epoch-ish
    if re.fullmatch(r"\d{10}", s):
        try:
            dt = datetime.fromtimestamp(int(s), tz=timezone.utc).astimezone()
            return dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            pass
    return s  # fallback to raw

def collect_url_entities(o):
    """
    Return list of url entities with: url (short), expanded_url, display_url, indices (tuple or None)
    """
    entities = first(
        o.get("entities"),
        (o.get("legacy") or {}).get("entities") if isinstance(o.get("legacy"), dict) else None,
        {},
    )
    urls = []
    if isinstance(entities, dict):
        for u in entities.get("urls", []) or []:
            if not isinstance(u, dict): 
                continue
            url = u.get("url") or ""
            expanded = first(u.get("unwound_url"), u.get("expanded_url"), url)
            display = u.get("display_url") or expanded
            idx = tuple(u.get("indices", [])) if isinstance(u.get("indices"), list) else None
            urls.append({"short": url, "expanded": expanded, "display": display, "indices": idx})
    return urls, entities

def expand_tco_to_anchors(text, url_entities, media_entities=None):
    """
    Expand t.co links in tweet text to clickable anchors.
    Uses indices if present; falls back to simple replacement if not.
    Removes t.co placeholders for media entities.
    """
    if text is None:
        return ""
    raw = str(text)

    # Media t.co placeholders to strip
    media_short = set()
    if isinstance(media_entities, dict):
        for m in media_entities.get("media", []) or []:
            if isinstance(m, dict) and m.get("url"):
                media_short.add(m["url"])

    # If we have indices, build piecewise
    pieces = []
    last = 0
    # Filter url_entities that actually exist in text
    ents = []
    for u in (url_entities or []):
        short = u.get("short") or ""
        if not short:
            continue
        idx = u.get("indices")
        # If indices missing, try to find; else keep None
        if not idx:
            m = re.search(re.escape(short), raw)
            if m:
                idx = (m.start(), m.end())
        if idx:
            ents.append((idx[0], idx[1], u))

    if ents:
        # Sort by start ascending
        ents.sort(key=lambda x: x[0])
        for start, end, u in ents:
            start = max(0, min(start, len(raw)))
            end = max(start, min(end, len(raw)))
            # Append escaped text before the URL
            pieces.append(html.escape(raw[last:start]))
            short = u.get("short") or ""
            if short in media_short:
                # strip media link placeholder
                anchor = ""
            else:
                expanded = u.get("expanded") or short
                display = u.get("display") or expanded
                anchor = f'<a href="{html.escape(expanded)}" target="_blank" rel="noopener">{html.escape(display)}</a>'
            pieces.append(anchor)
            last = end
        pieces.append(html.escape(raw[last:]))
        out = "".join(pieces)
    else:
        # No indices — naive fallback: replace any https://t.co/xxxxx we have mappings for
        out = html.escape(raw)
        for u in (url_entities or []):
            short = u.get("short") or ""
            if not short:
                continue
            expanded = u.get("expanded") or short
            display = u.get("display") or expanded
            # Replace the escaped short-form
            esc_short = html.escape(short)
            anchor = f'<a href="{html.escape(expanded)}" target="_blank" rel="noopener">{html.escape(display)}</a>'
            out = out.replace(esc_short, anchor)
        # Strip media placeholders if any remain
        for short in media_short:
            out = out.replace(html.escape(short), "")

    # Normalize whitespace and linebreaks
    out = re.sub(r"\s{2,}", " ", out).strip()
    out = out.replace("\n", "<br>")
    return out

items = []
with open(PATH, "r", encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if not line:
            continue
        try:
            o = json.loads(line)
        except Exception:
            continue

        # Heuristics: keep tweet/reply/message nodes; drop reaction/emoji nodes.
        t = (o.get("type") or o.get("kind") or "").lower()
        user = first(o.get("user"), o.get("author"), o.get("speaker"), {})
        if isinstance(user, str):
            user = {"screen_name": user}

        screen_name = first(
            user.get("screen_name") if isinstance(user, dict) else None,
            user.get("username") if isinstance(user, dict) else None,
        )
        name = (user.get("name") if isinstance(user, dict) else None) or screen_name or "user"

        text = first(o.get("full_text"), o.get("text"), o.get("body"), o.get("content"))
        tweet_id = first(o.get("tweet_id"), o.get("id_str"), o.get("id"))

        # Stats can be v1 style or v2 public_metrics
        pm = o.get("public_metrics") or {}
        likes = first(o.get("favorite_count"), pm.get("like_count"), 0) or 0
        retweets = first(o.get("retweet_count"), pm.get("retweet_count"), pm.get("repost_count"), 0) or 0
        replies = first(o.get("reply_count"), pm.get("reply_count"), 0) or 0
        quotes = first(o.get("quote_count"), pm.get("quote_count"), 0) or 0

        created_at = first(o.get("created_at"), o.get("time"), o.get("createdAt"), "")

        if (t in {"tweet", "reply", "note", "message"}) or (tweet_id and text):
            # URLs/entities for t.co expansion
            url_entities, ent = collect_url_entities(o)
            txt_html = expand_tco_to_anchors(text, url_entities, ent)

            url = None
            if screen_name and tweet_id:
                url = f"https://x.com/{screen_name}/status/{tweet_id}"
            elif tweet_id:
                url = f"https://x.com/i/web/status/{tweet_id}"

            parent = first(o.get("in_reply_to_status_id_str"), o.get("in_reply_to"), "")

            items.append({
                "id": str(tweet_id or ""),
                "parent": str(parent or ""),
                "name": name,
                "screen_name": screen_name or "",
                "url": url,
                "html": txt_html,
                "ts": parse_created(created_at),
                "likes": int(likes) if str(likes).isdigit() else likes,
                "retweets": int(retweets) if str(retweets).isdigit() else retweets,
                "replies": int(replies) if str(replies).isdigit() else replies,
                "quotes": int(quotes) if str(quotes).isdigit() else quotes,
            })

# Build a simple nested thread (one level; robust to missing parents).
by_id = {i["id"]: i for i in items if i["id"]}
children = {}
for it in items:
    p = it.get("parent") or ""
    children.setdefault(p, []).append(it)

def render_list(nodes, depth=0):
    if not nodes:
        return ""
    out = ["<ul>"]
    for n in nodes:
        who = html.escape(n["name"])
        handle = ("@" + n["screen_name"]) if n["screen_name"] else ""
        meta_left = f'{who} {html.escape(handle)}'.strip()

        # right-side meta: date (linked) + stats
        right_bits = []
        if n.get("url"):
            # date link
            date_lbl = n["ts"] or "open"
            right_bits.append(f'<a class="ss3k-reply-link" href="{html.escape(n["url"])}" target="_blank" rel="noopener">{html.escape(date_lbl)}</a>')
        # stats
        right_bits.append(f'💬 <b>{fmt_int(n.get("replies",0))}</b>')
        right_bits.append(f'🔁 <b>{fmt_int(n.get("retweets",0))}</b>')
        right_bits.append(f'❤️ <b>{fmt_int(n.get("likes",0))}</b>')

        meta = (
            '<div class="ss3k-reply-meta">'
            f'<span class="left">{meta_left}</span>'
            f'<span class="right">{" &nbsp;•&nbsp; ".join(right_bits)}</span>'
            '</div>'
        )
        body = f'<div class="ss3k-reply-body">{n["html"]}</div>'

        out.append("<li>" + meta + body)
        out.append(render_list(children.get(n.get("id",""), []), depth+1))
        out.append("</li>")
    out.append("</ul>")
    return "".join(out)

# Roots: items with no parent (or explicit ''), keep original order
roots = [it for it in items if not it.get("parent")]

html_out = (
    '<div class="ss3k-replies-web">'
    '<style>'
    '.ss3k-replies-web ul{list-style:disc;margin:0 0 0 1.1em;padding:0}'
    '.ss3k-replies-web li{margin:0 0 .8em .1em}'
    '.ss3k-reply-meta{display:flex;justify-content:space-between;gap:10px;align-items:baseline;font-size:14px}'
    '.ss3k-reply-meta .left{font-weight:600}'
    '.ss3k-reply-meta .right{font-size:12px;opacity:.9}'
    '.ss3k-reply-link{color:#59a7ff;text-decoration:none}'
    '.ss3k-reply-body{margin:.25em 0 .6em;white-space:pre-wrap;word-break:break-word}'
    'a{color:#59a7ff}'
    '</style>'
    + (render_list(roots) if roots else '<ul><li>No replies found.</li></ul>')
    + '</div>'
)

# Post to WP /patch-assets
base = os.environ.get("WP_BASE_URL","").rstrip("/")
user = os.environ.get("WP_USER","")
pwd  = os.environ.get("WP_APP_PASSWORD","")
if base and user and pwd:
    import urllib.request, base64
    data = json.dumps({"post_id": post_id, "ss3k_replies_html": html_out}).encode("utf-8")
    req = urllib.request.Request(
        f"{base}/wp-json/ss3k/v1/patch-assets",
        data=data,
        headers={"Content-Type":"application/json"},
        method="POST"
    )
    auth = (user + ":" + pwd).encode("utf-8")
    req.add_header("Authorization", "Basic " + base64.b64encode(auth).decode("ascii"))
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            print("PATCH replies:", r.status)
    except Exception as e:
        print("PATCH replies failed:", e)
else:
    print("WP creds missing; wrote HTML to stdout.\n")
    print(html_out)
