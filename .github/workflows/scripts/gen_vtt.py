#!/usr/bin/env python3
"""
gen_vtt.py — Parse X/Twitter Spaces CC.jsonl into WebVTT + HTML transcript.

Env:
  CC_JSONL, ARTDIR, BASE, SHIFT_SECS (float, default 0)
  JOIN_GAP_SECS (float, default 1.2), PAD_SECS (float, default 0.08)

Outputs:
  {ARTDIR}/{BASE}.vtt
  {ARTDIR}/{BASE}_emoji.vtt  (if any emoji found)
  {ARTDIR}/{BASE}_transcript.html
"""
from __future__ import annotations
import os, json, re, html
from datetime import datetime, timezone

def _parse_iso(s: str | None):
    if not s: return None
    try:
        if s.endswith('Z'): s = s[:-1] + '+00:00'
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _decode(line: str):
    """Return (top, payload, body) where body is dict or None."""
    try:
        top = json.loads(line)
    except Exception:
        return None, None, None
    payload = top.get("payload")
    if isinstance(payload, str):
        try: payload = json.loads(payload)
        except Exception: payload = None
    if isinstance(payload, dict):
        body = payload.get("body")
        if isinstance(body, str):
            try: body = json.loads(body)
            except Exception: body = None
    else:
        body = None
    return top, payload, body

def _speaker(username: str | None, display: str | None) -> str:
    return "@"+username if username else (display or "Speaker")

def _norm_text(t: str) -> str:
    t = t.replace('\r', ' ').replace('\n', ' ').strip()
    t = re.sub(r'\s+', ' ', t)
    t = re.sub(r'\s*-\s*$', '', t)
    return t

def _fmt_ts(sec: float) -> str:
    if sec < 0: sec = 0.0
    ms = int(round(sec*1000))
    h = ms // 3_600_000; ms -= h*3_600_000
    m = ms // 60_000;     ms -= m*60_000
    s = ms // 1_000;      ms -= s*1_000
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"

def parse_captions(jsonl_path: str, shift_secs: float=0.0, join_gap: float=1.2, pad: float=0.08):
    events, reactions = [], []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for _, line in enumerate(f):
            top, payload, body = _decode(line)
            if not isinstance(body, dict): continue
            ttype = body.get("type")

            if ttype == 45:  # speech-to-text (final)
                txt = _norm_text(str(body.get("body","")))
                if not txt: continue
                dt = _parse_iso(body.get("programDateTime"))
                if dt is None:
                    ts = top.get("timestamp")
                    if isinstance(ts, (int, float)):
                        try: dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
                        except Exception: dt = None
                if dt is None: continue
                events.append({
                    "dt": dt,
                    "text": txt,
                    "user": _speaker(body.get("username"), body.get("displayName"))
                })

            elif ttype == 2:  # reactions/emoji
                emo = str(body.get("body","")).strip()
                if not emo: continue
                dt = _parse_iso(body.get("programDateTime"))
                if dt is None:
                    ts = top.get("timestamp")
                    if isinstance(ts, (int, float)):
                        try: dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
                        except Exception: dt = None
                if dt:
                    reactions.append({
                        "dt": dt,
                        "text": f"{emo} {_speaker(body.get('username'), body.get('displayName'))}"
                    })

    if not events:
        return {"base_dt": None, "cues": [], "emoji": []}

    events.sort(key=lambda e: e["dt"])
    base = events[0]["dt"]

    cues, cur = [], None
    for e in events:
        t = (e["dt"] - base).total_seconds() - float(shift_secs)
        if t < 0: t = 0.0
        if cur is None:
            cur = {"start": t, "end": None, "user": e["user"], "text": e["text"]}
            continue

        if e["user"] == cur["user"] and (cur["end"] is None or (t - cur["end"]) <= join_gap):
            cur["end"] = t
            if cur["text"] and not cur["text"].endswith(('-', '—')): cur["text"] += " "
            cur["text"] += e["text"]
        else:
            if cur["end"] is None or cur["end"] < cur["start"] + 0.4:
                wc = max(1, len(cur["text"].split()))
                cur["end"] = cur["start"] + min(6.0, max(1.6, 0.35*wc))
            cur["end"] = max(cur["end"], cur["start"] + 0.24)
            cur["end"] = min(cur["end"], t - pad) if t > cur["start"] else cur["end"]
            cues.append(cur)
            cur = {"start": t, "end": None, "user": e["user"], "text": e["text"]}

    if cur is not None:
        if cur["end"] is None or cur["end"] < cur["start"] + 0.4:
            wc = max(1, len(cur["text"].split()))
            cur["end"] = cur["start"] + min(6.0, max(1.6, 0.35*wc))
        cur["end"] = max(cur["end"], cur["start"] + 0.24)
        cues.append(cur)

    emoji_cues = []
    if reactions:
        reactions.sort(key=lambda r: r["dt"])
        for r in reactions:
            t = (r["dt"] - base).total_seconds() - float(shift_secs)
            if t < 0: t = 0.0
            emoji_cues.append({"start": t, "end": t + 0.8, "text": r["text"]})

    return {"base_dt": base, "cues": cues, "emoji": emoji_cues}

def write_vtt(cues, out_path):
    lines = ["WEBVTT", ""]
    for c in cues:
        lines.append(f"{_fmt_ts(c['start'])} --> {_fmt_ts(c['end'])}")
        speaker = c.get("user") or "Speaker"
        text = re.sub(r'\s{2,}', ' ', c["text"]).strip()
        lines.append(f"<v {speaker}>{html.escape(text)}")
        lines.append("")
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))

def write_transcript_html(cues, out_path):
    parts = []
    parts.append('<!doctype html><meta charset="utf-8"><style>body{font-family:system-ui,Segoe UI,Arial,sans-serif;line-height:1.45;padding:1rem} .t{cursor:pointer} .speaker{color:#333;font-weight:600} .time{color:#777;font-size:.85em;margin-left:.25rem} .seg{margin:.25rem 0}</style><section class="transcript">')
    for c in cues:
        spk = html.escape(c.get("user") or "Speaker")
        start = c["start"]
        parts.append(f'<p class="seg"><span class="speaker">{spk}</span><span class="time" data-start="{start:.3f}">[{_fmt_ts(start)}]</span> <span class="t" data-start="{start:.3f}">{html.escape(c["text"])}</span></p>')
    parts.append("</section>")
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(parts))

def main():
    cc = os.environ.get("CC_JSONL") or ""
    art = os.environ.get("ARTDIR") or "."
    base = os.environ.get("BASE") or "space"
    shift = float(os.environ.get("SHIFT_SECS") or 0.0)
    join_gap = float(os.environ.get("JOIN_GAP_SECS") or 1.2)
    pad = float(os.environ.get("PAD_SECS") or 0.08)

    if not os.path.isfile(cc):
        raise SystemExit(f"CC_JSONL not found: {cc}")

    os.makedirs(art, exist_ok=True)

    parsed = parse_captions(cc, shift_secs=shift, join_gap=join_gap, pad=pad)
    cues, emoji = parsed["cues"], parsed["emoji"]

    write_vtt(cues, os.path.join(art, f"{base}.vtt"))
    if emoji:
        write_vtt(emoji, os.path.join(art, f"{base}_emoji.vtt"))
    write_transcript_html(cues, os.path.join(art, f"{base}_transcript.html"))

    print(f"[gen_vtt] wrote {len(cues)} cues → {os.path.join(art, base+'.vtt')}")
    if emoji:
        print(f"[gen_vtt] wrote {len(emoji)} emoji cues → {os.path.join(art, base+'_emoji.vtt')}")
    print(f"[gen_vtt] wrote transcript html → {os.path.join(art, base+'_transcript.html')}")

if __name__ == "__main__":
    main()
