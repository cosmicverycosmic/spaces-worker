import sys
import os
import re
from bs4 import BeautifulSoup

def main():
    if len(sys.argv) < 2:
        print("Usage: polish_transcript.py input_html [output_html]")
        sys.exit(1)
    in_path = sys.argv[1]
    out_path = sys.argv[2] if len(sys.argv) > 2 else in_path.replace('.html', '_polished.html')

    with open(in_path, 'r') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')

    # Example polish: trim extra spaces, add emojis if in text (placeholder; customize for reactions.json)
    for txt in soup.find_all(class_='txt'):
        text = txt.text.strip()
        # Placeholder emoji addition (e.g., from reactions; load if needed)
        if 'happy' in text.lower(): text += ' 😊'
        txt.string = re.sub(r'\s+', ' ', text)

    with open(out_path, 'w') as f:
        f.write(str(soup))

if __name__ == "__main__":
    main()
