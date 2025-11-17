#!/usr/bin/env python3
import os, sys, json, re, math
from datetime import datetime, timezone

CC_JSONL   = os.environ.get("CC_JSONL", "").strip()
ARTDIR     = os.environ.get("ARTDIR", ".").strip()
BASE       = os.environ.get("BASE", "space").strip()
SHIFT_SECS = float(os.environ.get("SHIFT_SECS", "0") or 0)

OUT_VTT        = os.path.join(ARTDIR, f"{BASE}.vtt")
OUT_EMOJI_VTT  = os.path.join(ARTDIR, f"{BASE}_emoji.vtt")
OUT_HTML       = os.path.join(ARTDIR, f"{BASE}_transcript.html")

FALLBACK_TXT   = None
if CC_JSONL:
    head = os.path.dirname(CC_JSONL)
    # The crawler often writes a sidecar .txt
    cand = re.sub(r"\.jsonl$", ".txt", CC_JSONL, flags=re.I)
    FALLBACK_TXT = cand if os.path.isfile(cand) else None

def to_ms(ts):
    """
    Convert a timestamp-ish value into milliseconds from some origin.
    Handles:
      - absolute epoch seconds or ms
      - offsets (seconds or ms)
      - ISO8601 strings
    Returns (ms_value, is_absolute)
      is_absolute=True if it looked like an epoch (needs normalization to t0)
    """
    if ts is None:
        return None, False

    # Strings that might be ISO or numeric
    if isinstance(ts, str):
        s = ts.strip()
        # ISO?
        if re.search(r"\d{4}-\d{2}-\d{2}T", s):
            try:
                # allow Z or offset
                dt = datetime.fromisoformat(s.replace("Z","+00:00"))
                return int(dt.timestamp()*1000), True
            except Exception:
                pass
        # numeric-in-string
        if re.fullmatch(r"-?\d+(\.\d+)?", s):
            try:
                val = float(s)
                # Heuristic for units
                if val > 1e12:    # ms epoch
                    return int(val), True
                if val > 1e9:     # sec epoch
                    return int(val*1000), True
                if val > 1e6:     # probably ms offset
                    return int(val), False
                # treat as seconds offset
                return int(val*1000), False
            except Exception:
                return None, False
        return None, False

    if isinstance(ts, (int, float)):
        val = float(ts)
        if val > 1e12:  # ms epoch
            return int(val), True
        if val > 1e9:   # sec epoch
            return int(val*1000), True
        if val > 1e6:   # ms offset
            return int(val), False
        return int(val*1000), False

    return None, False

def first_nonempty(*vals):
    for v in vals:
        if v is None: 
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        return v
    return None

def deep_get(obj, keys):
    cur = obj
    for k in keys:
        if cur is None:
            return None
        if isinstance(cur, dict) and k in cur:
            cur = cur[k]
        else:
            return None
    return cur

def find_text(o):
    """
    Try a bunch of plausible places for caption text.
    """
    # common spots
    candidates = [
        deep_get(o, ["text"]),
        deep_get(o, ["caption"]),
        deep_get(o, ["payloadText"]),
        deep_get(o, ["body","text"]),
        deep_get(o, ["message","text"]),
        deep_get(o, ["payload","text"]),
        deep_get(o, ["payload","body","text"]),
        deep_get(o, ["data","text"]),
        deep_get(o, ["value","text"]),
    ]
    # arrays of fragments
    arr = first_nonempty(
        deep_get(o, ["payload","segments"]),
        deep_get(o, ["body","segments"]),
        deep_get(o, ["segments"]),
    )
    if isinstance(arr, list):
        s = " ".join([str(x.get("text","")).strip() for x in arr if isinstance(x, dict)])
        candidates.append(s)

    # some crawlers wrap in {"type": "...", "value": "..."}
    tval = deep_get(o, ["value"])
    if isinstance(tval, str):
        candidates.append(tval)

    txt = first_nonempty(*candidates)
    if isinstance(txt, str):
        txt = txt.strip()
    return txt or None

def find_emoji_only(txt):
    if not txt: return False
    # consider "emoji-only" if it's made only of emoji/pictographs/whitespace
    # loose heuristic
    stripped = re.sub(r"\s+", "", txt)
    if not stripped:
        return False
    # if it contains letters/digits/punct beyond common emoji punctuation, treat as not emoji-only
    if re.search(r"[A-Za-z0-9]", stripped):
        return False
    # lots of reactions are hearts/claps/etc
    return True

def find_time_ms(o):
    """
    Heuristically find a timestamp field.
    Return (ms, absolute)
    """
    # likely keys
    possibilities = [
        deep_get(o, ["timestamp"]), deep_get(o, ["ts"]),
        deep_get(o, ["time"]), deep_get(o, ["time_ms"]),
        deep_get(o, ["start"]), deep_get(o, ["start_ms"]),
        deep_get(o, ["offset"]), deep_get(o, ["offset_ms"]),
        deep_get(o, ["created_at"]), deep_get(o, ["createdAt"]),
        deep_get(o, ["programDateTime"]),
        deep_get(o, ["payload","timestamp"]), deep_get(o, ["payload","ts"]),
        deep_get(o, ["body","timestamp"]), deep_get(o, ["body","ts"]),
    ]
    for p in possibilities:
        v = p
        if v is None: 
            continue
        ms, is_abs = to_ms(v)
        if ms is not None:
            return ms, is_abs
    return None, False

def ms_to_vtt(t_ms):
    t = max(0.0, t_ms / 1000.0)
    h = int(t // 3600); t -= h*3600
    m = int(t // 60);   t -= m*60
    s = int(t)
    ms = int(round((t - s)*1000))
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"

def cue_block(start_ms, end_ms, text):
    return f"{ms_to_vtt(start_ms)} --> {ms_to_vtt(end_ms)}\n{text}\n\n"

def html_escape(s):
    return (s.replace("&","&amp;")
             .replace("<","&lt;")
             .replace(">","&gt;"))

def write_if_changed(path, content):
    try:
        prev = ""
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                prev = f.read()
        if prev != content:
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
    except Exception as e:
        print(f"[gen_vtt] write failed for {path}: {e}", file=sys.stderr)

def parse_jsonl(path):
    if not path or not os.path.isfile(path): 
        return []
    items = []
    unk_samples = []
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line: 
                continue
            try:
                o = json.loads(line)
            except Exception:
                continue

            txt = find_text(o)
            t_ms, is_abs = find_time_ms(o)

            # Some events carry the useful stuff under nested "data" or "message"
            if txt is None:
                for key in ("data","message","payload","body","value"):
                    if isinstance(o.get(key), dict):
                        txt = find_text(o[key])
                        if txt: 
                            if t_ms is None:
                                t_ms, is_abs = find_time_ms(o[key])
                            break

            if txt is None and t_ms is None:
                # record strange items occasionally for debugging
                if len(unk_samples) < 8:
                    unk_samples.append(o)
                continue

            items.append({
                "txt": txt or "",
                "t_ms": t_ms,         # may be None for some weird items
                "is_abs": is_abs,
                "raw": o
            })
    # Optional: dump curious shapes for debugging
    if unk_samples:
        try:
            dbg = os.path.join(ARTDIR, f"{BASE}_cc_unknown.json")
            with open(dbg, "w", encoding="utf-8") as df:
                json.dump(unk_samples, df, ensure_ascii=False, indent=2)
        except Exception:
            pass
    return items

def fallback_from_txt(path):
    """
    If JSONL produced no cues, build coarse cues from the crawler's CC.txt.
    We accept:
      - lines like: 00:12:34.567  some text
      - or plain lines; we space them by 3s.
    """
    if not path or not os.path.isfile(path): 
        return []
    out = []
    base_ms = 0
    step = 3000
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            m = re.match(r"^(\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?)\s+(.*)$", s)
            if m:
                ts, txt = m.group(1), m.group(2).strip()
                # parse HH:MM:SS.mmm
                hh, mm, rest = ts.split(":")
                ss, ms = (rest.split(".")+["0"])[:2]
                t_ms = (int(hh)*3600 + int(mm)*60 + int(ss)) * 1000 + int(ms.ljust(3,"0")[:3])
                out.append({"txt": txt, "t_ms": t_ms, "is_abs": False})
            else:
                out.append({"txt": s, "t_ms": base_ms, "is_abs": False})
                base_ms += step
    return out

def main():
    events = parse_jsonl(CC_JSONL)
    # Separate into text vs emoji
    text_ev, emoji_ev = [], []

    # Determine t0 for absolute-time events
    abs_times = [e["t_ms"] for e in events if e["t_ms"] is not None and e["is_abs"]]
    t0 = min(abs_times) if abs_times else None

    for e in events:
        t_ms = e["t_ms"]
        txt  = (e["txt"] or "").strip()

        # no timestamp found? skip (we need ordering)
        if t_ms is None:
            continue

        # normalize absolute → offset from first seen
        if e["is_abs"] and t0 is not None:
            t_ms = t_ms - t0

        # apply SHIFT_SECS (crawler provides trim lead, etc.)
        t_ms = int(t_ms - SHIFT_SECS*1000)

        if t_ms < 0:
            # keep but clamp to zero to avoid negative cues
            t_ms = 0

        # classify
        if find_emoji_only(txt):
            emoji_ev.append((t_ms, txt))
        else:
            if txt:
                text_ev.append((t_ms, txt))

    # If we got nothing textual, try fallback from CC.txt
    if not text_ev and FALLBACK_TXT:
        fb = fallback_from_txt(FALLBACK_TXT)
        for e in fb:
            t_ms = int(e["t_ms"] - SHIFT_SECS*1000)
            if t_ms < 0: t_ms = 0
            txt  = e["txt"].strip()
            if find_emoji_only(txt):
                emoji_ev.append((t_ms, txt))
            elif txt:
                text_ev.append((t_ms, txt))

    text_ev.sort(key=lambda x: x[0])
    emoji_ev.sort(key=lambda x: x[0])

    # Build VTTs
    vtt = ["WEBVTT\n"]
    if text_ev:
        # durations: until next cue - 0.3s, min 1.2s, max 8s
        for i, (t, txt) in enumerate(text_ev):
            t_next = text_ev[i+1][0] if i+1 < len(text_ev) else t + 4000
            dur = max(1200, min(8000, t_next - t - 300))
            vtt.append(cue_block(t, t+dur, txt))

    write_if_changed(OUT_VTT, "".join(vtt))

    # Emoji VTT (lightweight): put reactions as short pops
    evtt = ["WEBVTT\n"]
    if emoji_ev:
        for (t, txt) in emoji_ev:
            evtt.append(cue_block(t, t+1500, txt))
    write_if_changed(OUT_EMOJI_VTT, "".join(evtt))

    # Basic HTML transcript
    html = [
        "<!doctype html><meta charset='utf-8'>",
        "<style>body{font:16px/1.4 system-ui,sans-serif;margin:1rem} .row{margin:.25rem 0} .t{color:#666;margin-right:.5rem;font-variant-numeric:tabular-nums} .e{opacity:.7}</style>",
        "<h1>Transcript</h1>"
    ]
    if not text_ev and not emoji_ev:
        html.append("<p><em>No captions found.</em></p>")
    else:
        for (t, txt) in text_ev:
            html.append(f"<div class='row'><span class='t'>{html_escape(ms_to_vtt(t))}</span>{html_escape(txt)}</div>")
        if emoji_ev:
            html.append("<h2>Reactions</h2>")
            for (t, txt) in emoji_ev:
                html.append(f"<div class='row e'><span class='t'>{html_escape(ms_to_vtt(t))}</span>{html_escape(txt)}</div>")

    write_if_changed(OUT_HTML, "".join(html))

    # stderr summary
    print(f"[gen_vtt] text_cues={len(text_ev)} emoji_cues={len(emoji_ev)} shift={SHIFT_SECS}s", file=sys.stderr)
    print(f"[gen_vtt] wrote: {OUT_VTT}, {OUT_EMOJI_VTT}, {OUT_HTML}", file=sys.stderr)

if __name__ == "__main__":
    os.makedirs(ARTDIR, exist_ok=True)
    main()
