#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build neat HTML for replies and external links for a Space.
- Restores stats (likes, reposts/RTs, replies) and per-reply date/time (linked to original X post).
- Renders uploaded media (photos, mp4/gifs) inline.
- Creates simple content cards for external links; nests quoted tweets as cards.
- Ignores non-reply records in JSONL (e.g., emoji/emote/transcript); writes emotes to BASE_emotes.vtt.
- Tries multiple sensible input filenames so it “just works” with the crawler output you have.

ENV:
  ARTDIR              default "."
  BASE                default "space"
  PURPLE_TWEET_URL    optional; used to feature a “purple pill” link on links page
"""

import os, re, json, html, time, traceback, sys
from urllib.parse import urlparse

# --------- ENV & Paths ----------
ARTDIR = os.environ.get("ARTDIR", ".")
BASE   = os.environ.get("BASE", "space")
PURPLE = (os.environ.get("PURPLE_TWEET_URL", "") or "").strip()

OUT_REPLIES = os.path.join(ARTDIR, f"{BASE}_replies.html")
OUT_LINKS   = os.path.join(ARTDIR, f"{BASE}_links.html")
OUT_EMOTES  = os.path.join(ARTDIR, f"{BASE}_emotes.vtt")
LOG_PATH    = os.path.join(ARTDIR, f"{BASE}_replies.log")

# --------- Utility ----------
def log(msg: str) -> None:
    os.makedirs(os.path.dirname(LOG_PATH) or ".", exist_ok=True)
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")

def read_text(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_text(path, text):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def find_first_existing(candidates):
    for p in candidates:
        if p and os.path.isfile(p):
            return p
    return None

def ts_to_local_str(ts_iso_or_ms):
    """
    Accepts:
      - ISO strings (2025-10-29T12:34:56.000Z or similar)
      - unix ms (int) or seconds (int/float)
    Returns: 'YYYY-MM-DD HH:MM' in localtime.
    """
    try:
        if isinstance(ts_iso_or_ms, (int, float)):
            # could be ms or sec — treat > 10^12 as ms
            sec = ts_iso_or_ms / 1000.0 if ts_iso_or_ms > 10**12 else float(ts_iso_or_ms)
            t = time.localtime(sec)
            return time.strftime("%Y-%m-%d %H:%M", t)
        if isinstance(ts_iso_or_ms, str):
            s = ts_iso_or_ms.strip()
            # Extract epoch?
            m = re.match(r"^\d{10}(?:\.\d+)?$", s)
            if m:
                t = time.localtime(float(s))
                return time.strftime("%Y-%m-%d %H:%M", t)
            # Fallback: parse ISO-ish by removing non-digits and colon/space basics
            # Many crawler outputs are Zulu; we won't shift TZ precisely here — localtime simplicity.
            # Try strptime with common shapes:
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ",
                        "%Y-%m-%dT%H:%M:%SZ",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%d %H:%M"):
                try:
                    t = time.strptime(s.replace("Z",""), fmt.replace("Z",""))
                    # time.strptime returns naive struct_time; display as-is
                    return time.strftime("%Y-%m-%d %H:%M", t)
                except Exception:
                    pass
    except Exception:
        pass
    return ""

def fmt_int(n):
    try:
        n = int(n)
    except Exception:
        return "0"
    # 1.2K, 3.4M style
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M".rstrip("0").rstrip(".")
    if n >= 1_000:
        return f"{n/1_000:.1f}K".rstrip("0").rstrip(".")
    return str(n)

def escape_text(s):
    return html.escape(s or "")

def domain_of(url):
    try:
        return urlparse(url).netloc
    except Exception:
        return ""

def coalesce(*vals, default=None):
    for v in vals:
        if v is not None:
            return v
    return default

# --------- Input discovery ----------
CANDIDATE_JSONL = [
    os.path.join(ARTDIR, f"{BASE}_replies.jsonl"),
    os.path.join(ARTDIR, f"{BASE}.replies.jsonl"),
    os.path.join(ARTDIR, f"{BASE}_crawler.jsonl"),
    os.path.join(ARTDIR, f"{BASE}_all.jsonl"),
]
CANDIDATE_JSON = [
    os.path.join(ARTDIR, f"{BASE}_replies.json"),
    os.path.join(ARTDIR, f"{BASE}.replies.json"),
]

def load_records():
    path = find_first_existing(CANDIDATE_JSONL) or find_first_existing(CANDIDATE_JSON)
    if not path:
        log("No replies JSON(L) found.")
        return [], None
    log(f"Using input: {path}")
    records = []
    if path.endswith(".jsonl"):
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    records.append(obj)
                except Exception as e:
                    log(f"Bad JSONL line skipped: {e}")
    else:
        try:
            data = json.loads(read_text(path))
            # accept either list or object with 'replies'
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict) and "replies" in data and isinstance(data["replies"], list):
                records = data["replies"]
            else:
                # Some crawlers dump under 'items'
                records = data.get("items", [])
        except Exception as e:
            log(f"Failed reading JSON file: {e}")
    return records, path

# --------- Record normalization ----------
def is_emote_record(r):
    # Typical shapes:
    # { "type":"emote", "start":0.02, "end":0.82, "user":"abc", "emotes":["💯","😂"] }
    # { "kind":"emoji", ... }
    t = (r.get("type") or r.get("kind") or "").lower()
    return t in {"emote", "emoji", "emotes", "emoji_vtt", "vtt_emoji"}

def is_reply_like(r):
    """
    Accept only tweet/reply/quote/retweet items — skip transcript chunks and emotes.
    We detect by presence of typical tweet keys OR explicit type.
    """
    t = (r.get("type") or r.get("kind") or "").lower()
    # known reply-like labels
    if t in {"tweet", "reply", "quote", "retweet", "status"}:
        return True
    # Otherwise infer by fields:
    tweetish_keys = {"id", "id_str", "text", "full_text", "created_at", "user"}
    return any(k in r for k in tweetish_keys) and isinstance(r.get("user", {}), dict)

def normalize_user(u):
    if not isinstance(u, dict):
        return {"name":"", "screen_name":"", "avatar":""}
    return {
        "name": coalesce(u.get("name"), u.get("full_name"), ""),
        "screen_name": coalesce(u.get("screen_name"), u.get("username"), u.get("handle"), ""),
        "avatar": coalesce(u.get("profile_image_url_https"), u.get("profile_image_url"), u.get("avatar"), ""),
    }

def normalize_media(mobj):
    out = []
    if not mobj:
        return out
    # extended_entities.media or entities.media or 'media' array flattened
    media_list = []
    if isinstance(mobj, dict):
        if "media" in mobj and isinstance(mobj["media"], list):
            media_list = mobj["media"]
    elif isinstance(mobj, list):
        media_list = mobj
    for m in media_list:
        if not isinstance(m, dict):
            continue
        mtype = m.get("type") or ""
        # photo
        if mtype == "photo":
            url = m.get("media_url_https") or m.get("media_url") or m.get("url")
            if url:
                out.append({"type":"photo", "url":url})
        elif mtype in ("video", "animated_gif"):
            # select a variant mp4 if available
            v = m.get("video_info", {}).get("variants", [])
            mp4s = [x for x in v if "mp4" in (x.get("content_type") or "")]
            mp4s.sort(key=lambda x: x.get("bitrate", 0), reverse=True)
            url = mp4s[0]["url"] if mp4s else None
            thumb = m.get("media_url_https") or m.get("media_url")
            if url:
                out.append({"type":"video", "url":url, "poster":thumb})
    return out

def normalize_urls(entities):
    urls = []
    if not isinstance(entities, dict):
        return urls
    for u in entities.get("urls", []):
        expanded = coalesce(u.get("unwound_url"), u.get("expanded_url"), u.get("url"))
        display  = u.get("display_url") or expanded
        if expanded:
            urls.append({"expanded": expanded, "display": display})
    return urls

def normalize_tweet(r):
    # Base
    tid = coalesce(r.get("id_str"), r.get("id"))
    text = coalesce(r.get("full_text"), r.get("text"), "")
    created = coalesce(r.get("created_at"), r.get("createdAt"), r.get("time"))
    stats = r.get("public_metrics") or {}
    likes   = coalesce(r.get("favorite_count"), stats.get("like_count"), 0)
    retw    = coalesce(r.get("retweet_count"), stats.get("retweet_count"), stats.get("repost_count"), 0)
    replies = coalesce(r.get("reply_count"),  stats.get("reply_count"), 0)
    quotes  = coalesce(r.get("quote_count"),  stats.get("quote_count"), 0)

    user = normalize_user(r.get("user") or r.get("author") or {})
    entities = r.get("entities") or {}
    ext_entities = r.get("extended_entities") or {}
    media = normalize_media(ext_entities) or normalize_media(entities) or normalize_media(r.get("media"))
    urls = normalize_urls(entities)

    # URL to original post if we can make it
    url = ""
    if tid and user.get("screen_name"):
        url = f"https://x.com/{user['screen_name']}/status/{tid}"

    # quoted
    quoted_raw = r.get("quoted_status") or r.get("quoted") or {}
    quoted = None
    if isinstance(quoted_raw, dict) and quoted_raw:
        quoted = normalize_tweet(quoted_raw)

    return {
        "id": str(tid) if tid is not None else "",
        "url": url,
        "text": text,
        "created_str": ts_to_local_str(created) or "",
        "user": user,
        "likes": int(likes) if str(likes).isdigit() or isinstance(likes, int) else 0,
        "retweets": int(retw) if str(retw).isdigit() or isinstance(retw, int) else 0,
        "replies": int(replies) if str(replies).isdigit() or isinstance(replies, int) else 0,
        "quotes": int(quotes) if str(quotes).isdigit() or isinstance(quotes, int) else 0,
        "media": media,
        "urls": urls,
        "quoted": quoted,
    }

def collect_emotes(records):
    """Return list of (start_sec, end_sec, speaker_id, emote_string) and write WebVTT."""
    emotes = []
    for r in records:
        if not is_emote_record(r):
            continue
        start = coalesce(r.get("start"), r.get("begin"), 0)
        end   = coalesce(r.get("end"), r.get("finish"), max(float(start)+0.8, 0.8))
        speaker = coalesce(r.get("speaker"), r.get("user"), r.get("uid"), "")
        # emotes could be list or string
        e = r.get("emotes") or r.get("emoji") or r.get("data")
        if isinstance(e, list):
            emstr = " ".join(map(str, e))
        else:
            emstr = str(e or "").strip()
        if emstr:
            emotes.append((float(start), float(end), str(speaker), emstr))
    # Write VTT
    if emotes:
        lines = ["WEBVTT", ""]
        def srt_time(secs):
            h = int(secs // 3600); m = int((secs % 3600) // 60); s = secs % 60
            return f"{h:02d}:{m:02d}:{s:06.3f}".replace(".", ",").replace(",", ".")
        for i, (a,b,sp,txt) in enumerate(sorted(emotes, key=lambda x:x[0]), start=1):
            lines.append(str(i))
            lines.append(f"{srt_time(a)} --> {srt_time(b)}")
            lines.append(f"<v {html.escape(sp)}> {txt}")
            lines.append("")
        write_text(OUT_EMOTES, "\n".join(lines).strip()+"\n")
        log(f"Wrote emotes VTT with {len(emotes)} cues: {OUT_EMOTES}")
    else:
        # If previously existed, leave it; otherwise, create a minimal file so frontend can attempt loading.
        if not os.path.exists(OUT_EMOTES):
            write_text(OUT_EMOTES, "WEBVTT\n\n")
    return emotes

# --------- HTML builders ----------
CSS = """
<style>
:root{--bg:#0b0d10;--fg:#e8eef6;--muted:#8fa1b3;--card:#12161b;--alt:#0e1217;--line:#1e2630;--accent:#1da1f2;--good:#23d160;}
*{box-sizing:border-box}
body{margin:0;padding:16px;color:var(--fg);background:transparent;font:14px/1.4 system-ui,Segoe UI,Roboto,Helvetica,Arial}
.replies{display:flex;flex-direction:column;gap:10px}
.reply{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px;display:grid;grid-template-columns:50px 1fr;gap:10px}
.reply.alt{background:var(--alt)}
.ava{width:50px;height:50px;border-radius:50%;overflow:hidden;background:#222}
.ava img{width:100%;height:100%;object-fit:cover;display:block}
.hdr{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.hdr .name{font-weight:600}
.hdr .handle{color:var(--muted)}
.txt{white-space:pre-wrap;word-break:break-word;margin-top:6px}
.media{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.media img{max-width:100%;height:auto;border-radius:10px;border:1px solid var(--line)}
.media video{max-width:100%;height:auto;border-radius:10px;border:1px solid var(--line)}
.card{border:1px solid var(--line);background:#0c1116;border-radius:10px;padding:8px;margin-top:8px}
.card .url{font-size:12px;color:var(--muted);margin-bottom:4px}
.card .title{font-weight:600;margin-bottom:4px}
.card .desc{color:var(--muted)}
.card .yt{position:relative;padding-top:56.25%;border-radius:10px;overflow:hidden;border:1px solid var(--line);margin-top:6px}
.card .yt iframe{position:absolute;inset:0;width:100%;height:100%;border:0}
.quote{border-left:3px solid var(--line);padding-left:8px;margin-top:8px}
.meta{display:flex;gap:12px;align-items:center;justify-content:flex-end;margin-top:10px;color:var(--muted);font-size:12px}
.meta a{color:var(--muted);text-decoration:none}
.meta .dot{opacity:.6}
.tag{font-size:12px;color:#fff;background:var(--accent);padding:2px 6px;border-radius:999px}
.stat{display:inline-flex;gap:6px;align-items:center}
.stat b{color:#fff}
.links{display:flex;flex-direction:column;gap:10px}
.link{border:1px solid var(--line);background:var(--card);border-radius:12px;padding:10px}
.link .top{display:flex;justify-content:space-between;gap:8px}
.link a{color:#9bd1ff;text-decoration:none;word-break:break-all}
</style>
"""

def render_youtube_embed(url):
    """
    Return iframe embed div if url is YouTube.
    """
    try:
        u = urlparse(url)
        host = (u.netloc or "").lower()
        if "youtube.com" in host:
            # look for v=
            q = dict([p.split("=",1) for p in (u.query or "").split("&") if "=" in p])
            vid = q.get("v")
        elif "youtu.be" in host:
            vid = u.path.strip("/").split("/")[0]
        else:
            vid = None
        if vid:
            return f'<div class="yt"><iframe src="https://www.youtube.com/embed/{html.escape(vid)}" allowfullscreen></iframe></div>'
    except Exception:
        pass
    return ""

def render_urls_cards(urls):
    out = []
    for u in urls:
        expanded = u.get("expanded") or ""
        if not expanded:
            continue
        dom = domain_of(expanded)
        yt = render_youtube_embed(expanded)
        title = expanded  # Without fetching OG, we keep it simple
        card = [
            '<div class="card">',
            f'<div class="url">{html.escape(dom)}</div>',
            f'<div class="title"><a href="{html.escape(expanded)}" target="_blank" rel="noopener noreferrer">{html.escape(title)}</a></div>',
        ]
        if yt:
            card.append(yt)
        card.append('</div>')
        out.append("\n".join(card))
    return "\n".join(out)

def render_media(media):
    if not media:
        return ""
    items = []
    for m in media:
        if m.get("type") == "photo":
            items.append(f'<img loading="lazy" src="{html.escape(m["url"])}" alt="image"/>')
        elif m.get("type") == "video":
            poster = f' poster="{html.escape(m.get("poster",""))}"' if m.get("poster") else ""
            items.append(f'<video controls preload="metadata"{poster}><source src="{html.escape(m["url"])}" type="video/mp4"></video>')
    return f'<div class="media">{"".join(items)}</div>'

def render_quote(q):
    if not q:
        return ""
    # shallow card for quoted tweet
    hdr = f'<div class="hdr"><span class="name">{html.escape(q["user"]["name"])}</span><span class="handle">@{html.escape(q["user"]["screen_name"])}</span></div>'
    txt = f'<div class="txt">{escape_text(q["text"])}</div>' if q["text"] else ""
    med = render_media(q.get("media"))
    urls = render_urls_cards(q.get("urls", []))
    meta = []
    if q.get("url"):
        meta.append(f'<a href="{html.escape(q["url"])}" target="_blank" rel="noopener noreferrer">{html.escape(q.get("created_str",""))}</a>')
    st = []
    if q.get("replies",0) or q.get("retweets",0) or q.get("likes",0):
        st.append(f'<span class="stat">💬 <b>{fmt_int(q["replies"])}</b></span>')
        st.append(f'<span class="stat">🔁 <b>{fmt_int(q["retweets"])}</b></span>')
        st.append(f'<span class="stat">❤️ <b>{fmt_int(q["likes"])}</b></span>')
    meta_html = f'<div class="meta">{"<span class=dot>•</span>".join(meta+st)}</div>' if (meta or st) else ""
    return f'<div class="quote card">{hdr}{txt}{med}{urls}{meta_html}</div>'

def render_reply_item(t, alt=False):
    ava = f'<div class="ava"><img src="{html.escape(t["user"]["avatar"])}" alt="avatar"/></div>' if t["user"]["avatar"] else '<div class="ava"></div>'
    hdr = f'<div class="hdr"><span class="name">{html.escape(t["user"]["name"])}</span><span class="handle">@{html.escape(t["user"]["screen_name"])}</span></div>'
    txt = f'<div class="txt">{escape_text(t["text"])}</div>' if t["text"] else ""
    med = render_media(t.get("media"))
    urls = render_urls_cards(t.get("urls", []))
    quote = render_quote(t.get("quoted"))

    # meta: date (link to original), replies/retweets/likes
    left = []
    if t.get("url"):
        left.append(f'<a href="{html.escape(t["url"])}" target="_blank" rel="noopener noreferrer">{html.escape(t.get("created_str",""))}</a>')
    stats = [
        f'<span class="stat">💬 <b>{fmt_int(t["replies"])}</b></span>',
        f'<span class="stat">🔁 <b>{fmt_int(t["retweets"])}</b></span>',
        f'<span class="stat">❤️ <b>{fmt_int(t["likes"])}</b></span>',
    ]
    meta = f'<div class="meta">{"<span class=dot>•</span>".join(left + stats)}</div>'

    klass = "reply alt" if alt else "reply"
    return f'<div class="{klass}">{ava}<div>{hdr}{txt}{med}{urls}{quote}{meta}</div></div>'

def build_replies_html(tweets):
    rows = []
    for i, t in enumerate(tweets):
        rows.append(render_reply_item(t, alt=bool(i%2)))
    doc = f"""<!doctype html>
<html><head><meta charset="utf-8">{CSS}</head>
<body>
<div class="replies">
{''.join(rows) if rows else '<div class="reply"><div class="ava"></div><div>No replies found.</div></div>'}
</div>
</body></html>"""
    return doc

def build_links_html(tweets):
    seen = set()
    items = []
    if PURPLE:
        items.append(f'<div class="link"><div class="top"><span class="tag">Purple</span><span>{time.strftime("%Y-%m-%d %H:%M")}</span></div><a href="{html.escape(PURPLE)}" target="_blank" rel="noopener noreferrer">{html.escape(PURPLE)}</a></div>')
    for t in tweets:
        for u in (t.get("urls") or []):
            expanded = u.get("expanded")
            if not expanded or expanded in seen:
                continue
            seen.add(expanded)
            items.append(f'<div class="link"><div class="top"><span>{html.escape(domain_of(expanded))}</span><span>{html.escape(t.get("created_str",""))}</span></div><a href="{html.escape(expanded)}" target="_blank" rel="noopener noreferrer">{html.escape(expanded)}</a></div>')
        # also collect from quoted
        q = t.get("quoted")
        if q:
            for u in (q.get("urls") or []):
                expanded = u.get("expanded")
                if not expanded or expanded in seen:
                    continue
                seen.add(expanded)
                items.append(f'<div class="link"><div class="top"><span>{html.escape(domain_of(expanded))}</span><span>{html.escape(q.get("created_str",""))}</span></div><a href="{html.escape(expanded)}" target="_blank" rel="noopener noreferrer">{html.escape(expanded)}</a></div>')
    doc = f"""<!doctype html>
<html><head><meta charset="utf-8">{CSS}</head>
<body>
<div class="links">
{''.join(items) if items else '<div class="link">No external links found.</div>'}
</div>
</body></html>"""
    return doc

# --------- Main ----------
def main():
    try:
        records, src = load_records()
        if not records:
            write_text(OUT_REPLIES, build_replies_html([]))
            write_text(OUT_LINKS,   build_links_html([]))
            log("No records; wrote placeholder HTMLs.")
            print("No replies JSON found; wrote placeholders.")
            return

        # Split: emotes vs replies
        emote_count = len([r for r in records if is_emote_record(r)])
        collect_emotes(records)  # writes VTT (noop if none)

        raw_replies = [r for r in records if is_reply_like(r)]
        tweets = [normalize_tweet(r) for r in raw_replies]

        # Basic sort by created time if present (most likely ascending)
        def parse_sort_key(t):
            # rough: convert created_str back to epoch-ish for stable sort
            s = t.get("created_str","")
            try:
                tt = time.strptime(s, "%Y-%m-%d %H:%M")
                return time.mktime(tt)
            except Exception:
                return 0.0
        tweets.sort(key=parse_sort_key)

        write_text(OUT_REPLIES, build_replies_html(tweets))
        write_text(OUT_LINKS,   build_links_html(tweets))

        log(f"Done. replies={len(tweets)}, emotes={emote_count}, src={src}")
        print(f"Wrote {OUT_REPLIES} and {OUT_LINKS}. Replies: {len(tweets)}. Emote cues: {emote_count}.")
    except Exception as e:
        tb = traceback.format_exc()
        log(f"FATAL: {e}\n{tb}")
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
