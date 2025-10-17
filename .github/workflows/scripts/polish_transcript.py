import sys
import os
import json
from bs4 import BeautifulSoup

def main():
    if len(sys.argv) < 2:
        print("Usage: polish_transcript.py input_html [reactions_json] [output_html]")
        sys.exit(1)
    in_path = sys.argv[1]
    reactions_path = sys.argv[2] if len(sys.argv) > 2 else ''
    out_path = sys.argv[3] if len(sys.argv) > 3 else in_path.replace('.html', '_polished.html')

    with open(in_path, 'r') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')

    # Light polish: trim extra spaces, no arbitrary word-to-emoji
    for txt in soup.find_all(class_='txt'):
        text = txt.text.strip()
        txt.string = ' '.join(text.split())

    # Integrate reactions if provided
    if reactions_path and os.path.isfile(reactions_path):
        with open(reactions_path, 'r') as rf:
            reactions = json.load(rf)
        # Assume reactions = [{"time": sec_float, "emoji": "😊", "count": int}, ...]
        segs = soup.find_all(class_='ss3k-seg')
        for r in reactions:
            t = r.get('time', 0)
            emoji = r.get('emoji', '')
            count = r.get('count', 1)
            if not emoji: continue
            # Find closest seg where start <= t < end
            closest = None
            min_diff = float('inf')
            for seg in segs:
                start = float(seg.get('data-start', 0))
                end = float(seg.get('data-end', float('inf')))
                if start <= t < end:
                    closest = seg
                    break
                diff = abs(start - t)
                if diff < min_diff:
                    min_diff = diff
                    closest = seg
            if closest:
                existing = closest.get('data-reactions', '[]')
                try:
                    reacts = json.loads(existing)
                except json.JSONDecodeError:
                    reacts = []
                reacts.append({"emoji": emoji, "count": count})
                closest['data-reactions'] = json.dumps(reacts)

    with open(out_path, 'w') as f:
        f.write(str(soup))

if __name__ == "__main__":
    main()
