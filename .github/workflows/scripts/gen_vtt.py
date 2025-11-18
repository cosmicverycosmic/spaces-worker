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
from typing import Any, Tuple


def _parse_iso(s: str | None):
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _decode(line: str) -> Tuple[Any, Any, Any]:
    """
    Best-effort decode for twspace-crawler CC.jsonl lines.

    Historically:
      {"payload": "{\"body\":\"{...caption json...}\"}", "timestamp": ...}

    Newer versions may put fields directly under `body` or at the top level.
    We try, in order:
      payload.body (decoded)
      top.body (decoded)
      top itself
    and only return a dict for `body`.
    """
    try:
        top = json.loads(line)
    except Exception:
        return None, None, None
    if not isinstance(top, dict):
        return None, None, None

    payload = top.get("payload")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = None

    body = None

    # 1) payload.body or payload itself
    if isinstance(payload, dict):
        candidate = payload.get("body", payload)
        if isinstance(candidate, str):
            try:
                candidate = json.loads(candidate)
            except Exception:
                candidate = None
        if isinstance(candidate, dict):
            body = candidate

    # 2) top.body
    if body is None:
        candidate = top.get("body")
        if isinstance(candidate, str):
            try:
                candidate = json.loads(candidate)
            except Exception:
                candidate = None
        if isinstance(candidate, dict):
            body = candidate

    # 3) fall back to top itself
    if body is None and isinstance(top, dict):
        body = top

    if not isinstance(body, dict):
        body = None

    return top, payload, body


def _speaker(username: str | None, display: str | None) -> str:
    if username:
        username = username.lstrip("@")
        return "@" + username
    return display or "Speaker"


def _norm_text(t: str) -> str:
    t = t.replace("\r", " ").replace("\n", " ").strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s*-\s*$", "", t)
    return t


def _fmt_ts(sec: float) -> str:
    if sec < 0:
        sec = 0.0
    ms = int(round(sec * 1000))
    h = ms // 3_600_000
    ms -= h * 3_600_000
    m = ms // 60_000
    ms -= m * 60_000
    s = ms // 1_000
    ms -= s * 1_000
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"


def _extract_text_and_user(body: dict) -> Tuple[str | None, str]:
    """
    Pull a plausible caption/reaction text and speaker handle from a body dict.
    We don't rely on numeric type codes any more; we just look for texty fields.
    """
    text = ""

    # Common places text shows up
    for key in ("body", "text", "caption", "captionText", "message", "content"):
        v = body.get(key)
        if isinstance(v, str):
            if v.strip():
                text = v
                break
        elif isinstance(v, dict):
            tv = v.get("text")
            if isinstance(tv, str) and tv.strip():
                text = tv
                break

    text = _norm_text(text) if text else ""

    username = (
        body.get("username")
        or body.get("user_name")
        or body.get("screen_name")
        or body.get("handle")
    )
    display = (
        body.get("displayName")
        or body.get("display_name")
        or body.get("name")
    )
    speaker = _speaker(username, display)

    return (text or None), speaker


def parse_captions(
    jsonl_path: str,
    shift_secs: float = 0.0,
    join_gap: float = 1.2,
    pad: float = 0.08,
):
    """
    Parse a CC.jsonl from twspace-crawler into:
      - merged speech cues
      - short-lived emoji/reaction cues

    Strategy:
      * Iterate every JSON line and dig out a dict-ish "body".
      * Extract text + user, skipping lines without any text.
      * If the text has alphabetic characters, treat as speech.
        Otherwise treat as an emoji / reaction event.
      * Timestamps primarily come from body.programDateTime, with fallback
        to top-level `timestamp` (ms).
    """
    events, reactions = [], []

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for _, line in enumerate(f):
            line = line.strip()
            if not line:
                continue

            top, payload, body = _decode(line)
            if not isinstance(body, dict):
                continue

            # Timestamp
            dt = _parse_iso(body.get("programDateTime"))
            if dt is None and isinstance(top, dict):
                ts = top.get("timestamp")
                if isinstance(ts, (int, float)):
                    try:
                        dt = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc)
                    except Exception:
                        dt = None
            if dt is None:
                continue

            text, user = _extract_text_and_user(body)
            if not text:
                continue

            # Distinguish "speech" from emoji-ish reactions:
            has_alpha = any(ch.isalpha() for ch in text)
            if has_alpha:
                events.append(
                    {
                        "dt": dt,
                        "text": text,
                        "user": user,
                    }
                )
            else:
                reactions.append(
                    {
                        "dt": dt,
                        "text": f"{text} {user}".strip(),
                    }
                )

    if not events:
        return {"base_dt": None, "cues": [], "emoji": []}

    events.sort(key=lambda e: e["dt"])
    base = events[0]["dt"]

    cues = []
    cur = None

    for e in events:
        t = (e["dt"] - base).total_seconds() - float(shift_secs)
        if t < 0:
            t = 0.0

        if cur is None:
            cur = {
                "start": t,
                "end": None,
                "user": e["user"],
                "text": e["text"],
            }
            continue

        # Same speaker and close in time → merge into same cue
        if e["user"] == cur["user"] and (
            cur["end"] is None or (t - cur["end"]) <= join_gap
        ):
            cur["end"] = t
            if cur["text"] and not cur["text"].endswith(("-", "—")):
                cur["text"] += " "
            cur["text"] += e["text"]
        else:
            # Finalize previous cue
            if cur["end"] is None or cur["end"] < cur["start"] + 0.4:
                wc = max(1, len(cur["text"].split()))
                cur["end"] = cur["start"] + min(6.0, max(1.6, 0.35 * wc))
            cur["end"] = max(cur["end"], cur["start"] + 0.24)
            if t > cur["start"]:
                cur["end"] = min(cur["end"], t - pad)
            cues.append(cur)

            # Start new cue
            cur = {
                "start": t,
                "end": None,
                "user": e["user"],
                "text": e["text"],
            }

    if cur is not None:
        if cur["end"] is None or cur["end"] < cur["start"] + 0.4:
            wc = max(1, len(cur["text"].split()))
            cur["end"] = cur["start"] + min(6.0, max(1.6, 0.35 * wc))
        cur["end"] = max(cur["end"], cur["start"] + 0.24)
        cues.append(cur)

    emoji_cues = []
    if reactions:
        reactions.sort(key=lambda r: r["dt"])
        for r in reactions:
            t = (r["dt"] - base).total_seconds() - float(shift_secs)
            if t < 0:
                t = 0.0
            emoji_cues.append(
                {
                    "start": t,
                    "end": t + 0.8,
                    "text": r["text"],
                }
            )

    return {"base_dt": base, "cues": cues, "emoji": emoji_cues}


def write_vtt(cues, out_path):
    lines = ["WEBVTT", ""]
    for c in cues:
        lines.append(f"{_fmt_ts(c['start'])} --> {_fmt_ts(c['end'])}")
        speaker = c.get("user") or "Speaker"
        text = re.sub(r"\s{2,}", " ", c["text"]).strip()
        lines.append(f"<v {speaker}>{html.escape(text)}")
        lines.append("")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def write_transcript_html(cues, out_path):
    parts = []
    parts.append(
        "<!doctype html><meta charset=\"utf-8\">"
        "<style>"
        "body{font-family:system-ui,Segoe UI,Arial,sans-serif;line-height:1.45;padding:1rem}"
        ".t{cursor:pointer}"
        ".speaker{color:#333;font-weight:600}"
        ".time{color:#777;font-size:.85em;margin-left:.25rem}"
        ".seg{margin:.25rem 0}"
        "</style>"
        "<section class=\"transcript\">"
    )
    for c in cues:
        spk = html.escape(c.get("user") or "Speaker")
        start = c["start"]
        parts.append(
            f'<p class="seg">'
            f'<span class="speaker">{spk}</span>'
            f'<span class="time" data-start="{start:.3f}">[{_fmt_ts(start)}]</span> '
            f'<span class="t" data-start="{start:.3f}">{html.escape(c["text"])}</span>'
            f"</p>"
        )
    parts.append("</section>")
    with open(out_path, "w", encoding="utf-8") as f:
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

    vtt_path = os.path.join(art, f"{base}.vtt")
    write_vtt(cues, vtt_path)
    if emoji:
        emoji_vtt_path = os.path.join(art, f"{base}_emoji.vtt")
        write_vtt(emoji, emoji_vtt_path)
    html_path = os.path.join(art, f"{base}_transcript.html")
    write_transcript_html(cues, html_path)

    print(f"[gen_vtt] parsed {len(cues)} cues and {len(emoji)} emoji events")
    print(f"[gen_vtt] wrote VTT → {vtt_path}")
    if emoji:
        print(f"[gen_vtt] wrote emoji VTT → {emoji_vtt_path}")
    print(f"[gen_vtt] wrote transcript html → {html_path}")


if __name__ == "__main__":
    main()
