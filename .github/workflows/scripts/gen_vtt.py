#!/usr/bin/env python3
import os, sys, json, math, re
from collections import defaultdict, Counter
from datetime import datetime, timezone

ARTDIR = os.environ.get("ARTDIR", os.getcwd())
BASE   = os.environ.get("BASE", "space")
CC_JSONL = os.environ.get("CC_JSONL")
SHIFT_SECS = float(os.environ.get("SHIFT_SECS", "0") or "0")

out_vtt_path       = os.path.join(ARTDIR, f"{BASE}.vtt")
out_emoji_vtt_path = os.path.join(ARTDIR, f"{BASE}_emoji.vtt")
out_html_path      = os.path.join(ARTDIR, f"{BASE}_transcript.html")

def fmt_time(sec: float) -> str:
    if sec < 0: sec = 0.0
    ms = int(round(sec * 1000.0))
    h = ms // 3600000; ms %= 3600000
    m = ms // 60000;   ms %= 60000
    s = ms // 1000;    ms %= 1000
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"

def load_as_line_base_ms() -> int | None:
    cand = os.path.join(ARTDIR, "_as_line.json")
    if not os.path.isfile(cand):
        return None
    try:
        with open(cand, "r", encoding="utf-8") as f:
            data = json.load(f)
        a = data.get("audioSpace", data) or {}
        meta = a.get("metadata", {})
        candidates = [
            meta.get("started_at"),
            meta.get("created_at"),
            meta.get("start"),
            data.get("started_at"),
            data.get("created_at"),
            data.get("start"),
        ]
        for v in candidates:
            if isinstance(v, (int, float)) and 9_000_000_000_000 > v > 1_000_000_000:
                return int(v)
    except Exception:
        pass
    return None

def parse_jsonl_events(path: str):
    """Yield normalized events from CC-like JSONL captured by twspace-crawler."""
    if not path or not os.path.isfile(path):
        raise FileNotFoundError(f"CC JSONL not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            try:
                j = json.loads(line)
                p = j.get("payload")
                if isinstance(p, str):
                    p = json.loads(p)
                body = p.get("body")
                if isinstance(body, str):
                    body = json.loads(body)
            except Exception:
                continue
            yield {
                "etype": body.get("type"),
                "display": body.get("displayName") or body.get("name") or "",
                "ts_ms": body.get("timestamp") if isinstance(body.get("timestamp"), int) else None,
                "body": body.get("body"),
            }

def is_emoji(s: str) -> bool:
    if not isinstance(s, str) or not s:
        return False
    if len(s) <= 3 and not re.search(r"[A-Za-z0-9]", s):
        return True
    return False

def build_emoji_vtt(events: list[dict], base_ms: int | None) -> tuple[str,int]:
    MIN_EPOCH_MS = 978307200000  # 2001-01-01 UTC
    emoji_events = [e for e in events if e["etype"] == 2 and isinstance(e["body"], str) and is_emoji(e["body"]) and isinstance(e["ts_ms"], int)]
    if not emoji_events:
        return "WEBVTT\n\nNOTE No emoji events found\n", 0

    plaus = [e for e in emoji_events if e["ts_ms"] >= (base_ms or MIN_EPOCH_MS) - 86_400_000]
    if not plaus:
        plaus = emoji_events

    if base_ms is None:
        base_ms = min(e["ts_ms"] for e in plaus)
        use_shift = True
    else:
        use_shift = False

    rows = []
    for e in plaus:
        rel = (e["ts_ms"] - base_ms) / 1000.0
        if use_shift and SHIFT_SECS:
            rel = rel - float(SHIFT_SECS)
        if rel < -5:
            continue
        rel = max(0.0, rel)
        rows.append((rel, e["body"], e.get("display","")))
    if not rows:
        return "WEBVTT\n\nNOTE No emoji events usable after filtering\n", 0

    bucket = defaultdict(list)
    bucket_size = 0.5
    for t, emo, disp in rows:
        key = math.floor(t / bucket_size) * bucket_size
        bucket[key].append((t, emo, disp))

    lines = ["WEBVTT", ""]
    cue_count = 0
    for start in sorted(bucket.keys()):
        items = bucket[start]
        counts = Counter([emo for _, emo, _ in items])
        text = " ".join((emo * min(c, 6)) if len(emo) == 1 else (emo + f"×{c}" if c>1 else emo) for emo, c in counts.items())
        if not text:
            continue
        cue_count += 1
        cue_start = start
        cue_end = start + 1.2
        lines.append(f"{fmt_time(cue_start)} --> {fmt_time(cue_end)}")
        lines.append(text)
        lines.append("")
    return "\n".join(lines) + ("\n" if not lines[-1].endswith("\n") else ""), cue_count

def main():
    if not CC_JSONL or not os.path.isfile(CC_JSONL):
        print(f"[gen_vtt] No CC_JSONL found at {CC_JSONL!r}; nothing to do.", file=sys.stderr)
        for p in (out_vtt_path, out_emoji_vtt_path, out_html_path):
            try:
                with open(p, "w", encoding="utf-8") as f:
                    f.write("")
            except Exception:
                pass
        return 0

    events = list(parse_jsonl_events(CC_JSONL))
    base_ms = load_as_line_base_ms()

    emoji_vtt, emoji_count = build_emoji_vtt(events, base_ms)
    with open(out_emoji_vtt_path, "w", encoding="utf-8") as f:
        f.write(emoji_vtt)

    text_vtt = "WEBVTT\n\nNOTE No text captions were present in the provided CC JSONL.\n"
    with open(out_vtt_path, "w", encoding="utf-8") as f:
        f.write(text_vtt)

    emoji_counts = Counter()
    for e in events:
        if e["etype"] == 2 and isinstance(e["body"], str) and is_emoji(e["body"]):
            emoji_counts[e["body"]] += 1

    try:
        first_ts = min([e["ts_ms"] for e in events if e["ts_ms"]], default=None)
        last_ts  = max([e["ts_ms"] for e in events if e["ts_ms"]], default=None)
        def tsfmt(ms):
            if not isinstance(ms, int): return "n/a"
            return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat()
        window = f"{tsfmt(first_ts)} → {tsfmt(last_ts)}"
    except Exception:
        window = "n/a"

    html = ["<!DOCTYPE html><meta charset='utf-8'><title>Transcript</title>",
            "<style>body{font-family:system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;line-height:1.4;margin:0;padding:1rem;}",
            "h1{font-size:1.2rem;margin:.5rem 0;} .muted{opacity:.7;font-size:.9rem;} .chip{display:inline-block;margin:.15rem .3rem;padding:.2rem .5rem;border-radius:.75rem;background:#f1f1f1;}",
            "code{background:#f7f7f7;padding:.1rem .25rem;border-radius:.25rem;}</style>",
            f"<h1>{BASE} — Transcript</h1>",
            "<p class='muted'>No text captions were present in the source JSONL; generated emoji VTT only.</p>",
            f"<p class='muted'><b>Time window:</b> {window}</p>",
            "<h2>Emoji summary</h2>",
            "<p>Top reactions:</p><p>"]
    for emo, c in emoji_counts.most_common(20):
        display = (emo * min(c, 6)) if len(emo) == 1 else emo
        html.append(f"<span class='chip'>{display} <small>×{c}</small></span>")
    html.append("</p>")
    with open(out_html_path, "w", encoding="utf-8") as f:
        f.write("".join(html))

    print(f"[gen_vtt] Wrote: {out_emoji_vtt_path} ({emoji_count} cues), {out_vtt_path}, {out_html_path}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
