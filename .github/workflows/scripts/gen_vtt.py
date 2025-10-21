import os, json, html
from datetime import timedelta

CC_JSONL = os.environ.get("CC_JSONL", "")
ARTDIR   = os.environ.get("ARTDIR", ".")
BASE     = os.environ.get("BASE", "space")

def ts(sec: float) -> str:
    td = timedelta(seconds=float(sec))
    s  = str(td)
    if '.' not in s:
        s += '.000000'
    h, m, rest = s.split(':')
    sec, ms = rest.split('.')
    return f"{int(h):02d}:{int(m):02d}:{int(sec):02d}.{ms[:3]}"

segs = []
if not CC_JSONL or not os.path.exists(CC_JSONL):
    raise SystemExit(0)

with open(CC_JSONL, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            j = json.loads(line)
        except Exception:
            continue
        # common twspace-crawler caption keys
        start = j.get('start') or (j.get('start_ms', 0) or 0) / 1000.0
        end   = j.get('end')   or (j.get('end_ms',   0) or 0) / 1000.0
        text  = j.get('text')  or j.get('content') or ""
        spk   = j.get('speaker') or j.get('user') or j.get('handle') or ""
        if start is None or end is None or not text:
            continue
        try:
            start = float(start)
            end   = float(end)
        except Exception:
            continue
        segs.append((start, end, str(text), str(spk)))

segs.sort(key=lambda x: x[0])

# write VTT
os.makedirs(ARTDIR, exist_ok=True)
vtt_path = os.path.join(ARTDIR, f"{BASE}.vtt")
with open(vtt_path, 'w', encoding='utf-8') as v:
    v.write("WEBVTT\n\n")
    for i, (s, e, t, _) in enumerate(segs, 1):
        v.write(f"{i}\n{ts(s)} --> {ts(e)}\n{t}\n\n")

# write simple syncable transcript
html_path = os.path.join(ARTDIR, f"{BASE}_transcript.html")
with open(html_path, 'w', encoding='utf-8') as h:
    for s, e, t, spk in segs:
        if spk:
            meta = f'<div class="ss3k-meta">{html.escape(spk)} {ts(s)}–{ts(e)}</div>'
        else:
            meta = f'<div class="ss3k-meta">{ts(s)}–{ts(e)}</div>'
        h.write(
            f'<div class="ss3k-seg" data-start="{s:.3f}" data-end="{e:.3f}">'
            f'{meta}<div class="txt">{html.escape(t)}</div></div>\n'
        )
