import os, json, requests, re, html
from pathlib import Path
from bs4 import BeautifulSoup
from collections import Counter
import tldextract
from keybert import KeyBERT

ARTDIR = Path(os.environ.get("ARTDIR","."))
BASE   = os.environ.get("BASE","space")
PURPLE = os.environ.get("PURPLE_TWEET_URL","").strip()
BEARER = os.environ.get("TWITTER_AUTHORIZATION","").strip()
AUTH_TOKEN = os.environ.get("TWITTER_AUTH_TOKEN","").strip()
CSRF_TOKEN = os.environ.get("TWITTER_CSRF_TOKEN","").strip()
FETCH_TITLES = (os.environ.get("LINK_LABEL_FETCH_TITLES","true").lower() == "true")
FETCH_LIMIT  = int(os.environ.get("LINK_LABEL_FETCH_LIMIT","18") or "18")
TIMEOUT      = int(os.environ.get("LINK_LABEL_TIMEOUT_SEC","4") or "4")
LINK_LABEL_AI = os.environ.get("LINK_LABEL_AI","keybert")
LINK_LABEL_MODEL = os.environ.get("LINK_LABEL_MODEL","sentence-transformers/all-MiniLM-L6-v2")
KEYBERT_TOPN = int(os.environ.get("KEYBERT_TOPN","8") or "8")
KEYBERT_NGRAM_MIN = int(os.environ.get("KEYBERT_NGRAM_MIN","1") or "1")
KEYBERT_NGRAM_MAX = int(os.environ.get("KEYBERT_NGRAM_MAX","3") or "3")
KEYBERT_USE_MMR = (os.environ.get("KEYBERT_USE_MMR","true").lower() == "true")
KEYBERT_DIVERSITY = float(os.environ.get("KEYBERT_DIVERSITY","0.6") or "0.6")

if not PURPLE:
    (ARTDIR / f"{BASE}_replies.html").write_text("", encoding="utf-8")
    (ARTDIR / f"{BASE}_links.html").write_text("", encoding="utf-8")
    (ARTDIR / f"{BASE}_reactions.json").write_text("[]", encoding="utf-8")
    raise SystemExit(0)

m = re.search(r'/status/(\d+)', PURPLE)
if not m:
    html = f'<div class="ss3k-replies"><p><a href="{html.escape(PURPLE)}" target="_blank" rel="noopener">Open conversation on X (purple pill)</a></p></div>'
    (ARTDIR / f"{BASE}_replies.html").write_text(html, encoding="utf-8")
    raise SystemExit(0)

post_id = m.group(1)

tweets = []
users = {}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}
cookies = {}
if AUTH_TOKEN and CSRF_TOKEN:
    cookies = {'auth_token': AUTH_TOKEN, 'ct0': CSRF_TOKEN}
    headers['x-csrf-token'] = CSRF_TOKEN
if BEARER:
    headers["Authorization"] = BEARER

# Get main tweet
main_url = f"https://api.x.com/2/tweets?ids={post_id}&tweet.fields=author_id,created_at,text,in_reply_to_status_id,entities&expansions=author_id,in_reply_to_status_id&user.fields=name,username"
r = requests.get(main_url, headers=headers, cookies=cookies, timeout=TIMEOUT)
if r.status_code == 200:
    data = r.json()
    if 'data' in data:
        tweets.extend(data['data'])
    if 'includes' in data and 'users' in data['includes']:
        for u in data['includes']['users']:
            users[u['id']] = u

# Get replies
params = {
    "query": f"conversation_id:{post_id}",
    "tweet.fields": "author_id,created_at,text,in_reply_to_status_id,entities",
    "expansions": "author_id,in_reply_to_status_id",
    "user.fields": "name,username",
    "max_results": "100"
}
url = "https://api.x.com/2/tweets/search/recent"
while True:
    r = requests.get(url, headers=headers, params=params, cookies=cookies, timeout=TIMEOUT)
    if r.status_code != 200:
        break
    data = r.json()
    if 'data' in data:
        tweets.extend(data['data'])
    if 'includes' in data and 'users' in data['includes']:
        for u in data['includes']['users']:
            users[u['id']] = u
    if 'meta' in data and 'next_token' in data['meta']:
        params['next_token'] = data['meta']['next_token']
    else:
        break

if not tweets:
    # Fallback to page scrape if API fails
    r = requests.get(PURPLE, headers=headers, cookies=cookies, timeout=TIMEOUT)
    soup = BeautifulSoup(r.text, "html.parser")
    script = soup.find('script', id="__NEXT_DATA__")
    if script:
        data = json.loads(script.string)
        # Assume path to tweets; this may need adjustment based on current structure
        try:
            timeline = data['props']['pageProps']['timeline']
            items = timeline['instructions'][0]['addEntries']['entries']
            for item in items:
                if 'itemContent' in item['content']:
                    tweet = item['content']['itemContent']['tweet_results']['result']
                    tweets.append(tweet['legacy'])
                    user = tweet['core']['user_results']['result']['legacy']
                    users[tweet['legacy']['user_id_str']] = user
        except KeyError:
            pass

# Build tree
tree = {}
for t in tweets:
    tid = t['id'] if 'id' in t else t['id_str']
    parent = t.get('in_reply_to_status_id') or t.get('in_reply_to_status_id_str')
    if parent not in tree:
        tree[parent] = []
    tree[parent].append(t)

# Recursive build HTML
def build_nested(root, level=0):
    out = []
    main = next((t for t in tweets if (t['id'] if 'id' in t else t['id_str']) == root), None)
    if not main:
        return ''
    user_id = main['author_id'] if 'author_id' in main else main['user_id_str']
    name = users.get(user_id, {}).get('name', 'Unknown')
    handle = users.get(user_id, {}).get('username', 'unknown')
    text = main['text'] if 'text' in main else main['full_text']
    text = html.escape(text)
    li = f'<li><strong>{name}</strong> (@{handle})<p>{text}</p>'
    out.append(li)
    if root in tree:
        out.append('<ul>')
        for child in tree[root]:
            cid = child['id'] if 'id' in child else child['id_str']
            out.append(build_nested(cid, level+1))
        out.append('</ul>')
    out.append('</li>')
    return ''.join(out)

replies_html = '<ul>' + build_nested(post_id) + '</ul>'
if not re.search(r'<li>', replies_html):
    replies_html = f'<div class="ss3k-replies"><p><a href="{html.escape(PURPLE)}" target="_blank" rel="noopener">Open conversation on X (purple pill)</a></p></div>'
(ARTDIR / f"{BASE}_replies.html").write_text(replies_html, encoding="utf-8")

# Extract links
urls = set()
for t in tweets:
    text = t['text'] if 'text' in t else t['full_text']
    for match in re.finditer(r'https?://\S+', text):
        urls.add(match.group(0))
    if 'entities' in t and 'urls' in t['entities']:
        for u in t['entities']['urls']:
            urls.add(u['expanded_url'])

uniq = list(urls)[:FETCH_LIMIT]

items = []
kw_model = None
if LINK_LABEL_AI == "keybert":
    kw_model = KeyBERT(model=LINK_LABEL_MODEL)

for u in uniq:
    label = ''
    if FETCH_TITLES:
        try:
            r = requests.get(u, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            label = soup.title.string.strip() if soup.title and soup.title.string else ''
        except Exception:
            pass
    if not label and kw_model:
        try:
            r = requests.get(u, timeout=TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            text = ' '.join(soup.get_text().split())
            keywords = kw_model.extract_keywords(text, keyphrase_ngram_range=(KEYBERT_NGRAM_MIN, KEYBERT_NGRAM_MAX), stop_words='english', top_n=KEYBERT_TOPN, use_mmr=KEYBERT_USE_MMR, diversity=KEYBERT_DIVERSITY)
            label = ' '.join([k[0] for k in keywords])
        except Exception:
            pass
    if not label:
        ext = tldextract.extract(u)
        label = ".".join([p for p in [ext.subdomain, ext.domain, ext.suffix] if p and p != 'www']) or u
    items.append(f'<li><a href="{html.escape(u)}" target="_blank" rel="noopener">{html.escape(label)}</a></li>')

if items:
    (ARTDIR / f"{BASE}_links.html").write_text("<ul>\n" + "\n".join(items) + "\n</ul>\n", encoding="utf-8")

# Extract emoji reactions from text
emoji_pattern = re.compile(
    "["
    r"\u1F600-\u1F64F"  # emoticons
    r"\u1F300-\u1F5FF"  # symbols & pictographs
    r"\u1F680-\u1F6FF"  # transport & map symbols
    r"\u1F1E0-\u1F1FF"  # flags (iOS)
    r"\u2600-\u26FF\u2700-\u27BF" # dingbats
    "]+",
    flags=re.UNICODE
)
all_emoji = []
for t in tweets:
    text = t['text'] if 'text' in t else t['full_text']
    all_emoji.extend(emoji_pattern.findall(text))
count = Counter(all_emoji)
rmoji = [{"emoji": k, "count": v} for k, v in sorted(count.items(), key=lambda x: x[1], reverse=True)]
(ARTDIR / f"{BASE}_reactions.json").write_text(json.dumps(rmoji), encoding="utf-8")
