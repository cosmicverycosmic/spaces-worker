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

import os
import json
import re
import html
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    """Parse an ISO8601-ish string (possibly ending in Z) into an aware datetime."""
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _ts_to_dt(ts: Any) -> Optional[datetime]:
    """Best-effort conversion of various timestamp representations to UTC datetime."""
    if ts is None:
        return None
    try:
        if isinstance(ts, str):
            ts = float(ts)
        if not isinstance(ts, (int, float)):
            return None
        # Heuristic: Twitter-style milliseconds since epoch are large.
        if ts > 1e11:
            ts = ts / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None


def _extract_dt(top: Dict[str, Any], body: Dict[str, Any]) -> Optional[datetime]:
    """Try a few different fields for the event datetime."""
    # Prefer explicit ISO-ish fields.
    for key in ("programDateTime", "program_date_time", "timestampISO", "timestamp_iso"):
        dt = _parse_iso(body.get(key) or top.get(key))
        if dt is not None:
            return dt

    # Fallback to millisecond / second timestamps.
    for key in (
        "timestamp",
        "timestampMs",
        "timestamp_ms",
        "timeMillis",
        "time_millis",
        "ts",
    ):
        dt = _ts_to_dt(body.get(key))
        if dt is not None:
            return dt
        dt = _ts_to_dt(top.get(key))
        if dt is not None:
            return dt

    return None


def _decode(line: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Decode a single JSONL line.

    Returns (top, payload, body), where body is a dict or None.
    Handles a few possible nesting variants:
      - {"payload": "{\"body\":\"{...}\", ...}"}
      - {"payload": {"body": "{...}"}}
      - {"body": "{...}"}
      - {"body": {...}}
    """
    try:
        top = json.loads(line)
    except Exception:
        return None, None, None

    payload: Any = top.get("payload")
    body: Any = None

    # payload may itself be a JSON string.
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = None

    if isinstance(payload, dict):
        body = payload.get("body")

    # Some dumps have the body directly on the top-level object.
    if body is None and "body" in top:
        body = top.get("body")

    # body might itself still be a JSON string.
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except Exception:
            body = None

    if not isinstance(body, dict):
        body = None

    return top, payload if isinstance(payload, dict) else None, body


def _speaker(username: Optional[str], display: Optional[str]) -> str:
    """Normalise a display name for the transcript."""
    username = (username or "").strip()
    display = (display or "").strip()
    if username:
        if not username.startswith("@"):
            return "@" + username
        return username
    if display:
        return display
    return "Speaker"


def _norm_text(t: str) -> str:
    """Collapse whitespace and strip trailing hyphens."""
    t = t.replace("\r", " ").replace("\n", " ").strip()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"\s*-\s*$", "", t)
    return t


def _fmt_ts(sec: float) -> str:
    """Format seconds as WebVTT timestamp hh:mm:ss.mmm."""
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


def parse_captions(
    jsonl_path: str,
    shift_secs: float = 0.0,
    join_gap: float = 1.2,
    pad: float = 0.08,
) -> Dict[str, Any]:
    """
    Parse a CC.jsonl file into caption cues + emoji cues.

    Returns:
      {
        "base_dt": datetime | None,
        "cues": [{"start","end","user","text"}, ...],
        "emoji": [{"start","end","text"}, ...],
      }
    """
    events: List[Dict[str, Any]] = []
    reactions: List[Dict[str, Any]] = []

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for _, line in enumerate(f):
            line = line.strip()
            if not line:
                continue

            top, payload, body = _decode(line)
            if not isinstance(body, dict):
                continue

            ttype = body.get("type")
            # Type sometimes comes as a string.
            if isinstance(ttype, str):
                try:
                    ttype = int(ttype)
                except Exception:
                    # Non-numeric type; keep as-is for potential future use.
                    pass

            dt = _extract_dt(top or {}, body)
            if dt is None:
                # As a very last resort, try the original "timestamp" logic from top-level.
                ts = (top or {}).get("timestamp")
                dt = _ts_to_dt(ts)

            if ttype == 45:  # STT final caption
                # Caption text can live under a few different keys.
                raw = (
                    body.get("body")
                    or body.get("sentence")
                    or body.get("text")
                    or body.get("caption")
                )
                # Some variants wrap the actual text again.
                if isinstance(raw, dict) and "body" in raw:
                    raw = raw.get("body")
                txt = _norm_text(str(raw or ""))
                if not txt or dt is None:
                    continue

                events.append(
                    {
                        "dt": dt,
                        "text": txt,
                        "user": _speaker(
                            body.get("username"), body.get("displayName")
                        ),
                    }
                )

            elif ttype == 2:  # reactions / emoji
                raw_emo = (
                    body.get("body")
                    or body.get("emoji")
                    or body.get("reaction")
                    or body.get("text")
                )
                emo = str(raw_emo or "").strip()
                if not emo or dt is None:
                    continue
                reactions.append(
                    {
                        "dt": dt,
                        "text": f"{emo} {_speaker(body.get('username'), body.get('displayName'))}",
                    }
                )

            # Ignore everything else for now.

    if not events:
        return {"base_dt": None, "cues": [], "emoji": []}

    # Sort by datetime.
    events.sort(key=lambda e: e["dt"])
    base = events[0]["dt"]

    cues: List[Dict[str, Any]] = []
    cur: Optional[Dict[str, Any]] = None

    for e in events:
        t = (e["dt"] - base).total_seconds() - float(shift_secs)
        if t < 0:
            t = 0.0

        if cur is None:
            cur = {
                "start": t,
                "end": None,   # Finalised at close.
                "last": t,     # Last word arrival time for join-gap logic.
                "user": e["user"],
                "text": e["text"],
            }
            continue

        same_user = (e["user"] == cur["user"])
        gap = t - float(cur.get("last", cur["start"]))
        if same_user and gap <= join_gap:
            # Extend the current cue.
            cur["last"] = t
            if cur["text"] and not cur["text"].endswith(("-", "—")):
                cur["text"] += " "
            cur["text"] += e["text"]
        else:
            # Close out the current cue before starting a new one.
            end = cur["end"] if cur["end"] is not None else float(cur.get("last", cur["start"]))
            if end < cur["start"] + 0.4:
                wc = max(1, len(cur["text"].split()))
                end = cur["start"] + min(6.0, max(1.6, 0.35 * wc))
            end = max(end, cur["start"] + 0.24)
            if t > end:
                end = min(end, t - pad)
            cur["end"] = end

            cues.append(
                {
                    "start": cur["start"],
                    "end": cur["end"],
                    "user": cur["user"],
                    "text": cur["text"],
                }
            )

            # Start a new cue.
            cur = {
                "start": t,
                "end": None,
                "last": t,
                "user": e["user"],
                "text": e["text"],
            }

    # Flush the final cue.
    if cur is not None:
        end = cur["end"] if cur["end"] is not None else float(cur.get("last", cur["start"]))
        if end < cur["start"] + 0.4:
            wc = max(1, len(cur["text"].split()))
            end = cur["start"] + min(6.0, max(1.6, 0.35 * wc))
        end = max(end, cur["start"] + 0.24)
        cur["end"] = end

        cues.append(
            {
                "start": cur["start"],
                "end": cur["end"],
                "user": cur["user"],
                "text": cur["text"],
            }
        )

    # Emoji cues: short blips at their timestamps.
    emoji_cues: List[Dict[str, Any]] = []
    if reactions:
        reactions.sort(key=lambda r: r["dt"])
        for r in reactions:
            t = (r["dt"] - base).total_seconds() - float(shift_secs)
            if t < 0:
                t = 0.0
            emoji_cues.append({"start": t, "end": t + 0.8, "text": r["text"]})

    return {"base_dt": base, "cues": cues, "emoji": emoji_cues}


def write_vtt(cues: List[Dict[str, Any]], out_path: str) -> None:
    """Write a list of cues to a WebVTT file."""
    lines: List[str] = ["WEBVTT", ""]
    for c in cues:
        lines.append(f"{_fmt_ts(c['start'])} --> {_fmt_ts(c['end'])}")
        speaker = c.get("user") or "Speaker"
        text = re.sub(r"\s{2,}", " ", str(c.get("text", ""))).strip()
        lines.append(f"<v {speaker}>{html.escape(text)}")
        lines.append("")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def write_transcript_html(cues: List[Dict[str, Any]], out_path: str) -> None:
    """Write a simple clickable HTML transcript with data-start attributes."""
    parts: List[str] = []
    parts.append(
        '<!doctype html><meta charset="utf-8"><style>'
        "body{font-family:system-ui,Segoe UI,Arial,sans-serif;line-height:1.45;padding:1rem}"
        ".t{cursor:pointer}"
        ".speaker{color:#333;font-weight:600}"
        ".time{color:#777;font-size:.85em;margin-left:.25rem}"
        ".seg{margin:.25rem 0}"
        "</style><section class=\"transcript\">"
    )
    for c in cues:
        spk = html.escape(str(c.get("user") or "Speaker"))
        start = float(c.get("start", 0.0))
        text = html.escape(str(c.get("text", "")))
        parts.append(
            f'<p class="seg"><span class="speaker">{spk}</span>'
            f'<span class="time" data-start="{start:.3f}">[{_fmt_ts(start)}]</span> '
            f'<span class="t" data-start="{start:.3f}">{text}</span></p>'
        )
    parts.append("</section>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(parts))


def main() -> None:
    cc = os.environ.get("CC_JSONL") or ""
    art = os.environ.get("ARTDIR") or "."
    base = os.environ.get("BASE") or "space"

    try:
        shift = float(os.environ.get("SHIFT_SECS") or 0.0)
    except Exception:
        shift = 0.0

    try:
        join_gap = float(os.environ.get("JOIN_GAP_SECS") or 1.2)
    except Exception:
        join_gap = 1.2

    try:
        pad = float(os.environ.get("PAD_SECS") or 0.08)
    except Exception:
        pad = 0.08

    if not os.path.isfile(cc):
        raise SystemExit(f"CC_JSONL not found: {cc}")

    os.makedirs(art, exist_ok=True)

    parsed = parse_captions(cc, shift_secs=shift, join_gap=join_gap, pad=pad)
    cues = parsed.get("cues", []) or []
    emoji = parsed.get("emoji", []) or []

    vtt_path = os.path.join(art, f"{base}.vtt")
    write_vtt(cues, vtt_path)

    if emoji:
        emoji_path = os.path.join(art, f"{base}_emoji.vtt")
        write_vtt(emoji, emoji_path)
    else:
        emoji_path = None

    html_path = os.path.join(art, f"{base}_transcript.html")
    write_transcript_html(cues, html_path)

    print(f"[gen_vtt] wrote {len(cues)} cues → {vtt_path}")
    if emoji:
        print(f"[gen_vtt] wrote {len(emoji)} emoji cues → {emoji_path}")
    print(f"[gen_vtt] wrote transcript html → {html_path}")


if __name__ == "__main__":
    main()
