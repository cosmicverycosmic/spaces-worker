#!/usr/bin/env python3
import os, re, json, html
from datetime import datetime, timezone

CC = os.environ.get("CC_JSONL", "").strip()
ARTDIR = os.environ.get("ARTDIR", ".").strip()
BASE = os.environ.get("BASE", "space").strip()
SHIFT = float(os.environ.get("SHIFT_SECS", "0") or "0")  # positive = shift forward
PARA_GAP = float(os.environ.get("PARA_GAP_SECS", "8") or "8")
EMOJI_DUR = float(os.environ.get("EMOJI_DURATION_SECS", "1.2") or "1.2")

os.makedirs(ARTDIR, exist_ok=True)

def p(s): print(s, flush=True)

def parse_iso(s):
    if not s: return None
    s = s.strip()
    if s.endswith("Z"): s = s[:-1] + "+00:00"
    if re.search(r"\+\d{4}$", s):  # +0000 → +00:00
        s = s[:-5] + s[-5:-2] + ":" + s[-2:]
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def fmt_ts(sec):
    if sec < 0: sec = 0.0
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"

def load_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                rec = json.loads(line)
                pld = json.loads(rec.get("payload") or "{}")
                body = json.loads(pld.get("body") or "{}")
                yield body
            except Exception:
                continue

if not CC or not os.path.isfile(CC):
    # create empty artifacts
    open(os.path.join(ARTDIR, f"{BASE}.vtt"), "w", encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(ARTDIR, f"{BASE}_emoji.vtt"), "w", encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(ARTDIR, f"{BASE}_transcript.html"), "w", encoding="utf-8").write("")
    p("No CC.jsonl found; wrote empty artifacts.")
    raise SystemExit(0)

recs = list(load_jsonl(CC))
caps = [r for r in recs if r.get("type") == 45 and (r.get("final") in (True, None))]
emjs = [r for r in recs if r.get("type") == 2]

# Establish time zero from the earliest caption
caps_timeful = [r for r in caps if parse_iso(r.get("programDateTime"))]
if not caps_timeful:
    open(os.path.join(ARTDIR, f"{BASE}.vtt"), "w", encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(ARTDIR, f"{BASE}_emoji.vtt"), "w", encoding="utf-8").write("WEBVTT\n\n")
    open(os.path.join(ARTDIR, f"{BASE}_transcript.html"), "w", encoding="utf-8").write("")
    p("No timeful captions; wrote empty artifacts.")
    raise SystemExit(0)

caps_sorted = sorted(caps_timeful, key=lambda r: parse_iso(r["programDateTime"]))
t0 = parse_iso(caps_sorted[0]["programDateTime"])

def rel_s(dt): return (dt - t0).total_seconds() - SHIFT

# --- Captions VTT ---
out = ["WEBVTT", ""]
for i, r in enumerate(caps_sorted):
    t = parse_iso(r["programDateTime"])
    if not t: continue
    start = rel_s(t)
    # end = min(start + 3.0, next_start - 0.1)
    if i + 1 < len(caps_sorted):
        t2 = parse_iso(caps_sorted[i+1]["programDateTime"])
        next_start = rel_s(t2) if t2 else (start + 2.0)
    else:
        next_start = start + 3.0
    end = max(start + 0.6, min(start + 3.0, next_start - 0.1))
    who = r.get("username") or r.get("displayName") or r.get("user_id") or "unknown"
    text = re.sub(r"\s+", " ", (r.get("body") or "").strip())
    out.append(str(i+1))
    out.append(f"{fmt_ts(start)} --> {fmt_ts(end)}")
    out.append(f"<v {who}>{text}")
    out.append("")
open(os.path.join(ARTDIR, f"{BASE}.vtt"), "w", encoding="utf-8").write("\n".join(out))

# --- Emoji VTT ---
emj_timeful = [r for r in emjs if parse_iso(r.get("programDateTime"))]
emj_sorted = sorted(emj_timeful, key=lambda r: parse_iso(r["programDateTime"]))
out = ["WEBVTT", ""]
for i, r in enumerate(emj_sorted):
    t = parse_iso(r["programDateTime"])
    start = rel_s(t)
    end = start + EMOJI_DUR
    who = r.get("username") or r.get("displayName") or r.get("remoteID") or "anon"
    em = (r.get("body") or "").strip()
    out.append(str(i+1))
    out.append(f"{fmt_ts(start)} --> {fmt_ts(end)}")
    out.append(f"<v {who}> {em}")
    out.append("")
open(os.path.join(ARTDIR, f"{BASE}_emoji.vtt"), "w", encoding="utf-8").write("\n".join(out))

# --- Paragraph Transcript (HTML) ---
paras = []
cur_s, cur_text, cur_start, last_t = None, [], None, None

def flush():
    global paras, cur_s, cur_text, cur_start
    if cur_text:
        text = re.sub(r"\s+", " ", " ".join(cur_text).strip())
        who = html.escape(cur_s or "Unknown")
        start_attr = f' data-start="{cur_start:.3f}"' if cur_start is not None else ""
        paras.append(f'<p{start_attr}><strong>{who}:</strong> {html.escape(text)}</p>')
    cur_s, cur_text, cur_start = None, [], None

for r in caps_sorted:
    t = rel_s(parse_iso(r["programDateTime"]))
    who = r.get("username") or r.get("displayName") or "Unknown"
    text = (r.get("body") or "").strip()
    if not text: continue
    if cur_s is None:
        cur_s, cur_text, cur_start = who, [text], t
    else:
        if who != cur_s or (last_t is not None and (t - last_t) > PARA_GAP):
            flush()
            cur_s, cur_text, cur_start = who, [text], t
        else:
            cur_text.append(text)
    last_t = t
flush()

html_out = "<div>\n" + "\n".join(paras) + "\n</div>"
open(os.path.join(ARTDIR, f"{BASE}_transcript.html"), "w", encoding="utf-8").write(html_out)

p("Wrote VTT (speech), VTT (emoji), and HTML transcript.")
