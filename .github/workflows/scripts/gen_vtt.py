#!/usr/bin/env python3
# Builds 3 artifacts from a Space CC JSONL:
#  1) BASE.vtt               (spoken text only; emoji stripped)
#  2) BASE_emoji.vtt         (emoji-only cues; JSON payload per cue with name/avatar/handle-or-id)
#  3) BASE_transcript.html   (clean transcript markup; no timestamps; no emoji)
#
# Inputs (env):
#   ARTDIR        - output dir
#   BASE          - base filename (without extension)
#   CC_JSONL      - path to crawler-generated CC JSONL
#   SHIFT_SECS    - head trim/shift seconds to align with AUDIO_IN (float, optional)
#
# Behavior when no captions exist: creates a minimal transcript with a "No captions" note
# and still emits BASE_emoji.vtt (if any reactions exist).

import os, sys, json, re, html
from pathlib import Path
from datetime import datetime

ARTDIR = Path(os.environ.get("ARTDIR","."))
BASE   = os.environ.get("BASE","space")
CC     = os.environ.get("CC_JSONL","")
SHIFT  = float(os.environ.get("SHIFT_SECS","0") or "0")
TRIM   = float(os.environ.get("TRIM_LEAD","0") or "0")
TOTAL_SHIFT = SHIFT + TRIM

OUT_VTT        = ARTDIR / f"{BASE}.vtt"
OUT_EMOJI_VTT  = ARTDIR / f"{BASE}_emoji.vtt"
OUT_HTML       = ARTDIR / f"{BASE}_transcript.html"
OUT_SPEECH_JSON = ARTDIR / f"{BASE}_speech.json"
OUT_REACT_JSON  = ARTDIR / f"{BASE}_reactions.json"

ARTDIR.mkdir(parents=True, exist_ok=True)

EMOJI_RE = re.compile(
    r"[\U0001F300-\U0001F5FF"  # symbols & pictographs
    r"\U0001F600-\U0001F64F"   # emoticons
    r"\U0001F680-\U0001F6FF"   # transport & map symbols
    r"\U0001F700-\U0001F77F"   # alchemical symbols
    r"\U0001F780-\U0001F7FF"   # geometric shapes extended
    r"\U0001F800-\U0001F8FF"   # supplemental arrows-C
    r"\U0001F900-\U0001F9FF"   # supplemental symbols and pictographs
    r"\U0001FA00-\U0001FA6F"   # chess symbols etc
    r"\U0001FA70-\U0001FAFF"   # symbols & pictographs extended-A
    r"\U00002700-\U000027BF"   # dingbats
    r"\U00002600-\U000026FF"   # miscellaneous symbols
    r"\U00002B00-\U00002BFF"   # misc symbols & arrows
    r"]+", flags=re.UNICODE
)

def strip_emojis(s: str) -> str:
    return EMOJI_RE.sub("", s or "")

def only_emoji(s: str) -> bool:
    if not s: return False
    # Remove variation selectors / ZWJ / skin-tone modifiers
    t = re.sub(r"[\u200d\ufe0f\U0001F3FB-\U0001F3FF]", "", s)
    return strip_emojis(t) == ""

def ts_norm(x):
    try:
        v = float(x)
        v = max(0.0, v - TOTAL_SHIFT)
        return v
    except Exception:
        return None

def parse_jsonl_line(line: str):
    # Crawler lines are wrapper JSON with "payload" that itself contains JSON whose "body" is also JSON.
    try:
        outer = json.loads(line)
    except Exception:
        return None, None
    payload = outer.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = None
    if not isinstance(payload, dict):
        return outer, None
    body = payload.get("body")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except Exception:
            body = None
    return payload, body

def iter_rows(path: Path):
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln: continue
            pay, body = parse_jsonl_line(ln)
            if body is None: 
                # registration/join/other events we don't need
                continue
            yield pay, body

def extract_caption(body):
    # Returns (start_s, end_s, text, speaker_name, handle, avatar) or None
    # Many crawlers put spoken captions as type 45-ish entries; but sometimes it's text without explicit type.
    # We'll use content-based filtering: non-empty, not emoji-only, has some letters.
    txt = body.get("text") or body.get("caption") or body.get("body") or ""
    txt = str(txt)
    if not txt: 
        return None
    if only_emoji(txt):  # this is a reaction, not speech
        return None
    # Require at least a letter/number to avoid stray symbols
    if not re.search(r"[A-Za-z0-9]", txt):
        return None
    # time bounds
    # Try typical fields: start/end in seconds; else timestamp in ms/µs
    start = body.get("start") or body.get("startSec") or body.get("timestamp") or body.get("ts") or None
    end   = body.get("end")   or body.get("endSec")   or body.get("timestampEnd") or None
    # Convert ms-like integers
    def to_sec(v):
        if v is None: return None
        try:
            vv = float(v)
            # Heuristic: > 10^6 is probably ms or µs
            if vv > 3_600_000: vv = vv / 1000.0
            return vv
        except Exception:
            return None
    start_s = ts_norm(to_sec(start))
    end_s   = ts_norm(to_sec(end)) if end is not None else None
    if start_s is None:
        return None
    if end_s is None:
        end_s = start_s + 1.8  # conservative default window
    # speaker
    speaker_name = body.get("speakerName") or body.get("name") or ""
    handle = body.get("speakerHandle") or body.get("handle") or ""
    avatar = body.get("avatar") or ""
    return (start_s, end_s, txt, speaker_name, handle, avatar)

def extract_reaction(pay, body):
    # Returns (t_s, emoji_str, user_handle_or_id, display_name, avatar_url) or None
    txt = body.get("text") or body.get("body") or ""
    if not txt or not only_emoji(txt):
        return None
    # pick a representative emoji (could also emit one cue per codepoint if needed)
    emj = txt
    # sender info from payload
    sender = pay.get("sender") or {}
    disp = sender.get("displayName") or body.get("displayName") or ""
    avatar = sender.get("profile_image_url_https") or sender.get("profile_image_url") or ""
    handle_or_id = sender.get("screen_name") or sender.get("twitter_id") or ""
    # time
    t = body.get("start") or body.get("startSec") or body.get("timestamp") or body.get("ts") or pay.get("timestamp") or None
    def to_sec(v):
        if v is None: return None
        try:
            vv = float(v)
            if vv > 3_600_000: vv = vv / 1000.0
            return vv
        except Exception:
            return None
    t_s = ts_norm(to_sec(t))
    if t_s is None:
        return None
    return (t_s, emj, handle_or_id, disp, avatar)

def vtt_time(s):
    # s in seconds -> "HH:MM:SS.mmm"
    if s < 0: s = 0
    hrs = int(s//3600)
    s -= hrs*3600
    mins = int(s//60)
    sec = s - mins*60
    return f"{hrs:02d}:{mins:02d}:{sec:06.3f}"

def write_vtt(path: Path, cues):
    with path.open("w", encoding="utf-8") as fh:
        fh.write("WEBVTT\n\n")
        for (st, en, text) in cues:
            fh.write(f"{vtt_time(st)} --> {vtt_time(en)}\n{text}\n\n")

def main():
    if not CC or not Path(CC).is_file():
        print("No CC JSONL provided; nothing to do.")
        return 0

    speech = []
    reacts = []

    for pay, body in iter_rows(Path(CC)):
        cap = extract_caption(body)
        if cap:
            speech.append(cap)
            continue
        r = extract_reaction(pay, body)
        if r:
            reacts.append(r)

    # Sort
    speech.sort(key=lambda x: x[0])
    reacts.sort(key=lambda x: x[0])

    # Build speech VTT cues (emoji stripped)
    vtt_cues = []
    speech_json = []
    for (st, en, txt, name, handle, avatar) in speech:
        clean = strip_emojis(txt).strip()
        if not clean:
            continue
        vtt_cues.append((st, en, clean))
        speech_json.append({
            "t0": round(st,3),
            "t1": round(en,3),
            "text": clean,
            "name": name,
            "handle": handle,
            "avatar": avatar
        })

    # Build emoji VTT cues (JSON payload per cue)
    emoji_cues = []
    react_json = []
    for (t_s, emj, h, disp, ava) in reacts:
        payload = {
            "t": round(t_s,3),
            "e": emj,
            "user": str(h or ""),
            "name": disp or "",
            "avatar": ava or ""
        }
        text = json.dumps(payload, ensure_ascii=False)
        emoji_cues.append((t_s, t_s + 0.8, text))
        react_json.append(payload)

    # Write artifacts
    if vtt_cues:
        write_vtt(OUT_VTT, vtt_cues)
    else:
        # create an empty WEBVTT so the player doesn't 404
        OUT_VTT.write_text("WEBVTT\n\n", encoding="utf-8")

    if emoji_cues:
        write_vtt(OUT_EMOJI_VTT, emoji_cues)
    else:
        OUT_EMOJI_VTT.write_text("WEBVTT\n\n", encoding="utf-8")

    # JSON sidecars (optional debug)
    OUT_SPEECH_JSON.write_text(json.dumps(speech_json, ensure_ascii=False, indent=2), encoding="utf-8")
    OUT_REACT_JSON.write_text(json.dumps(react_json, ensure_ascii=False, indent=2), encoding="utf-8")

    # Minimal transcript HTML (no timestamps; no emoji)
    if speech_json:
        rows = []
        for seg in speech_json:
            name = html.escape(seg.get("name") or "").strip()
            handle = html.escape(seg.get("handle") or "").strip()
            avatar = html.escape(seg.get("avatar") or "").strip()
            txt = html.escape(seg["text"])
            # 50x50 avatars per your spec
            head = f'''
            <div class="ss3k-row" data-t0="{seg["t0"]}" data-t1="{seg["t1"]}">
              <img class="ss3k-ava" src="{avatar}" alt="" width="50" height="50" loading="lazy" decoding="async" />
              <div class="ss3k-bubble">
                <div class="ss3k-name">{name or handle}</div>
                <div class="ss3k-text">{txt}</div>
              </div>
            </div>
            '''
            rows.append(head)
        html_out = f"""<!doctype html>
<meta charset="utf-8">
<style>
  .ss3k-row{{display:flex;gap:.6rem;align-items:flex-start;padding:.35rem .5rem;border-bottom:1px solid rgba(0,0,0,.05);}}
  .ss3k-ava{{border-radius:999px;flex:0 0 auto;width:50px;height:50px;object-fit:cover;}}
  .ss3k-bubble{{flex:1 1 auto;}}
  .ss3k-name{{font-weight:600;margin-bottom:.1rem;}}
  .ss3k-text{{line-height:1.35}}
</style>
<div class="ss3k-transcript">
{''.join(rows)}
</div>
"""
        OUT_HTML.write_text(html_out, encoding="utf-8")
    else:
        OUT_HTML.write_text("""<!doctype html>
<meta charset="utf-8">
<style>
  .ss3k-note{padding:.75rem;border:1px dashed #bbb;border-radius:.5rem;background:#fafafa}
</style>
<div class="ss3k-note">No captions were available for this Space. Emoji reactions are provided via the emoji VTT.</div>
""", encoding="utf-8")

    print(f"Wrote: {OUT_VTT}, {OUT_EMOJI_VTT}, {OUT_HTML}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
