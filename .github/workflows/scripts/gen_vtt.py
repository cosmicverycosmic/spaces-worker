import sys
import json
import os
import argparse
from datetime import timedelta

def sec_to_time(s):
    s = float(s)
    ms = int((s % 1) * 1000)
    td = timedelta(seconds=int(s))
    return f"{str(td):0>8}.{ms:03d}"

def main():
    parser = argparse.ArgumentParser(description="Generate VTT and HTML transcript from CC JSONL")
    parser.add_argument('jsonl', help="Path to CC.jsonl")
    parser.add_argument('--shift', default=0, type=float, help="Timestamp shift in seconds")
    parser.add_argument('--outdir', default='.', help="Output directory")
    parser.add_argument('--base', default='space', help="Base filename")
    args = parser.parse_args()

    shift = args.shift
    vtt_path = os.path.join(args.outdir, f"{args.base}.vtt")
    html_path = os.path.join(args.outdir, f"{args.base}_transcript.html")

    with open(args.jsonl, 'r') as f, open(vtt_path, 'w') as vtt, open(html_path, 'w') as html:
        vtt.write("WEBVTT\n\n")
        html.write('<div class="ss3k-transcript-segments">\n')
        prev_end = 0
        for line in f:
            try:
                seg = json.loads(line)
            except json.JSONDecodeError:
                continue
            start = seg.get('start', prev_end) + shift
            end = seg.get('end', start + 1) + shift  # Fallback duration if missing
            text = seg.get('text', '').strip()
            if not text: continue
            vtt.write(f"{sec_to_time(start)} --> {sec_to_time(end)}\n{text}\n\n")
            html.write(f'<div class="ss3k-seg" data-start="{start:.3f}" data-end="{end:.3f}">\n')
            html.write(f'  <div class="ss3k-meta">{sec_to_time(start)}</div>\n')
            html.write(f'  <div class="txt">{text}</div>\n')
            html.write('</div>\n')
            prev_end = end
        html.write('</div>\n')

if __name__ == "__main__":
    main()
