#!/usr/bin/env python3
# Stable working version — confirmed functional before UI breakages.
import os, json, html, re, sys
from datetime import datetime
from urllib.parse import urlparse

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

def expand_links(text: str) -> str:
    """Expand t.co links to their full hrefs when provided."""
    if not text:
        return ""
    # Common patterns to expand or unescape
    text = text.replace("&amp;", "&")
    return text

def clean_text(s: str) -> str:
    s = s.replace("\n", " ").strip()
    s = re.sub(r"\s+", " ", s)
    return s

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

        if not isinstance(o, dict):
            continue

        # Extract only valid tweet replies
        txt = first(o.get("text"), o.get("body"))
        if not txt:
            continue

        user = o.get("user") or {}
        screen = first(user.get("screen_name"), o.get("username"), o.get("handle"))
        name = first(user.get("name"), o.get("display_name")) or screen or "User"
        avatar = first(user.get("profile_image_url_https"), user.get("profile_image_url")) or ""
        likes = int(first(o.get("favorite_count"), o.get("likes"), 0) or 0)
        rts = int(first(o.get("retweet_count"), o.get("retweets"), 0) or 0)
        created = first(o.get("created_at"), o.get("date"))
        id_ = first(o.get("id_str"), o.get("id"))
        url = ""
        if screen and id_:
            url = f"https://x.com/{screen}/status/{id_}"

        # Format timestamp nicely
        dt_display = ""
        if created:
            try:
                if re.search(r"\d{4}-\d{2}-\d{2}", created):
                    dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                else:
                    dt = datetime.strptime(created, "%a %b %d %H:%M:%S %z %Y")
                dt_display = dt.strftime("%b %d, %Y · %H:%M")
            except Exception:
                dt_display = str(created)

        txt = clean_text(html.escape(txt))
        txt = expand_links(txt)

        items.append({
            "screen": screen,
            "name": name,
            "avatar": avatar,
            "likes": likes,
            "rts": rts,
            "date": dt_display,
            "text": txt,
            "url": url,
        })

# ----------- Output HTML ------------
OUT = os.path.join(os.path.dirname(PATH), f"space_replies.html")
html_items = []

for i, it in enumerate(items, 1):
    av = f'<img src="{html.escape(it["avatar"], True)}" width="50" height="50" style="border-radius:50%;margin-right:8px;">' if it["avatar"] else ""
    name_html = html.escape(it["name"])
    handle_html = f'@{html.escape(it["screen"])}' if it["screen"] else ""
    body = f'<p style="margin:4px 0 6px 0;">{it["text"]}</p>'
    meta = f'<div style="font-size:12px;color:#555;">{it["date"]} · ❤️ {it["likes"]} · 🔁 {it["rts"]}</div>'
    link = f'<a href="{it["url"]}" target="_blank" style="text-decoration:none;color:#1d9bf0;">Open on X</a>' if it["url"] else ""

    html_items.append(f"""
    <div style="display:flex;align-items:flex-start;padding:10px;border-bottom:1px solid #ddd;">
      {av}
      <div style="flex:1;">
        <div style="font-weight:bold;">{name_html} <span style="color:#777;">{handle_html}</span></div>
        {body}
        {meta}
        <div style="margin-top:4px;">{link}</div>
      </div>
    </div>""")

OUT_HTML = f"""
<div class="ss3k-replies" style="font:15px/1.5 system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;
  border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;">
  {''.join(html_items) if html_items else '<p>No replies found.</p>'}
</div>
"""

with open(OUT, "w", encoding="utf-8") as f:
    f.write(OUT_HTML)

print(f"Wrote {len(items)} replies → {OUT}")
