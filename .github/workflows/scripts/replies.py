name: Space Worker

on:
  workflow_dispatch:
    inputs:
      source_url:
        description: "Space or MP3 URL (preferred)"
        required: false
        type: string
        default: ""
      space_url:
        description: "Space or MP3 URL (legacy name)"
        required: false
        type: string
        default: ""
      title:
        description: "Title hint (optional; WP may set final)"
        required: false
        type: string
        default: ""
      post_id:
        description: "WordPress post ID to patch (optional)"
        required: false
        type: string
        default: ""
      gcs_prefix:
        description: "GCS prefix (default: spaces/YYYY/MM)"
        required: false
        type: string
        default: ""
      make_public:
        description: "Make uploaded objects public"
        required: false
        type: choice
        options: ["true","false"]
        default: "true"
      mode:
        description: "Limit processing"
        required: false
        type: choice
        options: ["","transcript_only","attendees_only","replies_only"]
        default: ""
      purple_tweet_url:
        description: "Purple-pill tweet URL (optional)"
        required: false
        type: string
        default: ""
      audio_profile:
        description: "Audio profile"
        required: false
        type: choice
        options: ["transparent","radio","aggressive"]
        default: "radio"
      opts_json:
        description: "Extra options JSON (small)"
        required: false
        type: string
        default: "{}"

  repository_dispatch:
    types: [space_worker]

permissions:
  contents: read
  packages: read

env:
  WORKDIR: ${{ github.workspace }}/work
  ARTDIR:  ${{ github.workspace }}/out

  # --- Google Cloud ---
  GCP_SA_KEY: ${{ secrets.GCP_SA_KEY || vars.GCP_SA_KEY }}
  GCS_BUCKET: ${{ secrets.GCS_BUCKET || vars.GCS_BUCKET }}

  # --- WordPress REST (App Password) ---
  WP_BASE_URL:     ${{ secrets.WP_BASE_URL     || secrets.WP_URL || vars.WP_BASE_URL || vars.WP_URL }}
  WP_USER:         ${{ secrets.WP_USER         || vars.WP_USER }}
  WP_APP_PASSWORD: ${{ secrets.WP_APP_PASSWORD || vars.WP_APP_PASSWORD }}

  # --- Transcription fallback (Deepgram) ---
  DEEPGRAM_API_KEY: ${{ secrets.DEEPGRAM_API_KEY || vars.DEEPGRAM_API_KEY }}

  # --- X/Twitter auth for crawler ---
  TWITTER_AUTHORIZATION: ${{ secrets.TWITTER_AUTHORIZATION || secrets.X_BEARER     || vars.TWITTER_AUTHORIZATION || vars.X_BEARER }}
  TWITTER_AUTH_TOKEN:    ${{ secrets.TWITTER_AUTH_TOKEN    || secrets.X_AUTH_TOKEN || vars.TWITTER_AUTH_TOKEN    || vars.X_AUTH_TOKEN }}
  TWITTER_CSRF_TOKEN:    ${{ secrets.TWITTER_CSRF_TOKEN    || secrets.X_CSRF       || vars.TWITTER_CSRF_TOKEN    || vars.X_CSRF }}

  # (Optional) Old v1 API tokens
  TW_API_CONSUMER_KEY:        ${{ secrets.TW_API_CONSUMER_KEY        || vars.TW_API_CONSUMER_KEY }}
  TW_API_CONSUMER_SECRET:     ${{ secrets.TW_API_CONSUMER_SECRET     || vars.TW_API_CONSUMER_SECRET }}
  TW_API_ACCESS_TOKEN:        ${{ secrets.TW_API_ACCESS_TOKEN        || vars.TW_API_ACCESS_TOKEN }}
  TW_API_ACCESS_TOKEN_SECRET: ${{ secrets.TW_API_ACCESS_TOKEN_SECRET || vars.TW_API_ACCESS_TOKEN_SECRET }}

jobs:
  process:
    name: Process Space
    runs-on: ubuntu-latest
    timeout-minutes: 180
    concurrency:
      group: ${{ format('space-worker-{0}-{1}', github.ref, github.event.inputs.post_id != '' && github.event.inputs.post_id || github.run_id) }}
      cancel-in-progress: false

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install deps
        shell: bash
        run: |
          set -euxo pipefail
          sudo apt-get update
          sudo apt-get install -y --no-install-recommends ffmpeg jq python3 python3-pip ca-certificates gnupg
          python3 -m pip install --upgrade pip
          python3 -m pip install --no-cache-dir yt-dlp requests beautifulsoup4 tldextract keybert "sentence-transformers>=2.2.0"
          echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | sudo tee /etc/apt/sources.list.d/google-cloud-sdk.list
          curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
          sudo apt-get update && sudo apt-get install -y google-cloud-sdk
          echo "${{ github.token }}" | docker login ghcr.io -u "${{ github.actor }}" --password-stdin || true

      - name: Resolve inputs (supports repository_dispatch)
        id: resolve
        shell: bash
        run: |
          set -euo pipefail
          mkdir -p "$WORKDIR" "$ARTDIR" "$ARTDIR/logs" ".github/workflows/scripts"

          SRC_URL="${{ github.event.inputs.source_url }}"
          LEGACY_URL="${{ github.event.inputs.space_url }}"
          TTL_HINT="${{ github.event.inputs.title }}"
          GCS_PFX_RAW="${{ github.event.inputs.gcs_prefix }}"
          PURPLE_URL="${{ github.event.inputs.purple_tweet_url }}"
          OPTS='${{ github.event.inputs.opts_json }}'
          SRC_KIND=""

          if [ -z "$SRC_URL" ] && [ -n "$LEGACY_URL" ]; then SRC_URL="$LEGACY_URL"; fi

          if [ "${{ github.event_name }}" = "repository_dispatch" ]; then
            J="$(cat "$GITHUB_EVENT_PATH")"
            get() { jq -r "$1 // empty" <<<"$J"; }
            SRC_URL="$(get '.client_payload.source_url' || echo "$SRC_URL")"
            [ -z "$SRC_URL" ] && SRC_URL="$(get '.client_payload.space_url' || echo "$SRC_URL")"
            SRC_KIND="$(get '.client_payload.source_kind' || echo "$SRC_KIND")"
            TTL_HINT="$(get '.client_payload.title' || echo "$TTL_HINT")"
            PURPLE_URL="$(get '.client_payload.purple_tweet_url' || echo "$PURPLE_URL")"
            GCS_PFX_RAW="$(get '.client_payload.gcs_prefix' || echo "$GCS_PFX_RAW")"
            OPTS="$(get '.client_payload.opts_json' || echo "$OPTS")"
          fi

          PFX="$(echo "${GCS_PFX_RAW}" | sed -E 's#^/*##; s#/*$##')"
          if [ -z "$PFX" ]; then PFX="spaces/$(date +%Y)/$(date +%m)"; fi
          echo "PREFIX=$PFX"                  >> "$GITHUB_ENV"
          echo "BUCKET_PREFIX=${PFX#spaces/}" >> "$GITHUB_ENV"

          if [ -z "$SRC_KIND" ] || [ "$SRC_KIND" = "auto" ]; then
            if echo "$SRC_URL" | grep -qi '/i/spaces/'; then SRC_KIND="space"
            elif echo "$SRC_URL" | grep -qiE '\.mp3($|\?)'; then SRC_KIND="mp3"
            else SRC_KIND=""
            fi
          fi

          echo "SOURCE_URL=$SRC_URL"   >> "$GITHUB_ENV"
          echo "SOURCE_KIND=$SRC_KIND" >> "$GITHUB_ENV"
          echo "TITLE_HINT=$TTL_HINT"  >> "$GITHUB_ENV"
          echo "PURPLE_TWEET_URL=$PURPLE_URL" >> "$GITHUB_ENV"

          OPTS="${OPTS:-{}}"
          echo "$OPTS" > "$WORKDIR/opts.json"
          jq -e . "$WORKDIR/opts.json" >/dev/null 2>&1 || echo '{}' > "$WORKDIR/opts.json"

          echo "LINK_LABEL_FETCH_TITLES=$(jq -r '.fetch_titles // "true"' "$WORKDIR/opts.json")" >> "$GITHUB_ENV"
          echo "LINK_LABEL_FETCH_LIMIT=$(jq -r '.fetch_limit // "18"' "$WORKDIR/opts.json")" >> "$GITHUB_ENV"
          echo "LINK_LABEL_TIMEOUT_SEC=$(jq -r '.fetch_timeout_sec // "4"' "$WORKDIR/opts.json")" >> "$GITHUB_ENV"

      - name: Validate config
        shell: bash
        run: |
          set -euxo pipefail
          test -n "${GCP_SA_KEY}" || { echo "GCP_SA_KEY missing"; exit 1; }
          test -n "${GCS_BUCKET}" || { echo "GCS_BUCKET missing"; exit 1; }

      - name: Derive Space ID and base
        id: ids
        shell: bash
        env:
          URL: ${{ env.SOURCE_URL }}
        run: |
          set -euxo pipefail
          SID=""
          if [ -n "$URL" ]; then
            SID="$(echo "$URL" | sed -nE 's#^.*/i/spaces/([^/?#]+).*#\1#p')"
          fi
          [ -z "$SID" ] && SID="unknown"
          BASE="space-$(date +%m-%d-%Y)-${SID}"
          echo "SPACE_ID=${SID}" >> "$GITHUB_ENV"
          echo "BASE=${BASE}"    >> "$GITHUB_ENV"
          echo "space_id=${SID}" >> "$GITHUB_OUTPUT"
          echo "base=${BASE}"    >> "$GITHUB_OUTPUT"

      - name: GCP auth
        if: ${{ github.event.inputs.mode != 'replies_only' }}
        shell: bash
        run: |
          set -euxo pipefail
          printf '%s' "${GCP_SA_KEY}" > "${HOME}/gcp-key.json"
          gcloud auth activate-service-account --key-file="${HOME}/gcp-key.json" >/dev/null

      - name: X preflight (Space only)
        id: x_preflight
        if: ${{ github.event.inputs.mode != 'replies_only' && env.SOURCE_KIND != 'mp3' }}
        shell: bash
        run: |
          set -euo pipefail
          AUTH="${TWITTER_AUTHORIZATION:-}"
          AT="${TWITTER_AUTH_TOKEN:-}"
          CT="${TWITTER_CSRF_TOKEN:-}"
          if [ -n "$AUTH" ] && ! printf '%s' "$AUTH" | grep -q '^Bearer '; then AUTH=""; fi
          [ -n "${TWITTER_AUTHORIZATION:-}" ] && echo "::add-mask::${TWITTER_AUTHORIZATION}"
          [ -n "$AT" ] && echo "::add-mask::${AT}"
          [ -n "$CT" ] && echo "::add-mask::${CT}"
          OK=0; REASON="no_creds"
          [ -n "$AT" ] && [ -n "$CT" ] && OK=1 && REASON="cookie_ok" || true
          [ -n "$AUTH" ] && OK=1 && REASON="${REASON}_bearer_present" || true
          echo "ok=${OK}"         >> "$GITHUB_OUTPUT"
          echo "reason=${REASON}" >> "$GITHUB_OUTPUT"
          [ -n "$AUTH" ] && echo "TWITTER_AUTHORIZATION=$AUTH" >> "$GITHUB_ENV"

      - name: Run twspace-crawler
        id: crawl
        if: ${{ github.event.inputs.mode != 'replies_only' && env.SOURCE_KIND != 'mp3' && steps.x_preflight.outputs.ok == '1' }}
        shell: bash
        env:
          SID: ${{ steps.ids.outputs.space_id }}
        run: |
          set -euxo pipefail
          mkdir -p "${ARTDIR}" "${ARTDIR}/logs"
          docker pull ghcr.io/hitomarukonpaku/twspace-crawler:latest || true
          LOG_STD="${ARTDIR}/logs/crawler_${SID}.out.log"
          LOG_ERR="${ARTDIR}/logs/crawler_${SID}.err.log"
          set +e
          timeout 20m docker run --rm \
            -e TWITTER_AUTHORIZATION \
            -e TWITTER_AUTH_TOKEN \
            -e TWITTER_CSRF_TOKEN \
            -v "${ARTDIR}:/app/download" \
            -v "${ARTDIR}/logs:/app/logs" \
            ghcr.io/hitomarukonpaku/twspace-crawler:latest \
            --id "${SID}" --force > >(tee -a "$LOG_STD") 2> >(tee -a "$LOG_ERR" >&2)
          RC=$?
          set -e
          AUDIO_FILE="$(find "${ARTDIR}" -type f \( -iname '*.m4a' -o -iname '*.mp3' -o -iname '*.mp4' -o -iname '*.aac' -o -iname '*.webm' -o -iname '*.ogg' -o -iname '*.wav' -o -iname '*.ts' \) -printf '%T@ %p\n' | sort -nr | head -n1 | cut -d' ' -f2- || true)"
          if [ -n "${AUDIO_FILE:-}" ] && [ -f "${AUDIO_FILE}" ]; then
            echo "INPUT_FILE=${AUDIO_FILE}" >> "$GITHUB_ENV"
            echo "audio_file=${AUDIO_FILE}" >> "$GITHUB_OUTPUT"
          fi
          RAW="$(grep -hF 'getAudioSpaceById |' "$LOG_STD" "$LOG_ERR" | tail -n1 || true)"
          if [ -z "$RAW" ]; then
            RAW="$(grep -hF 'getAudioSpaceByRestId |' "$LOG_STD" "$LOG_ERR" | tail -n1 || true)"
          fi
          if [ -n "$RAW" ]; then
            printf '%s\n' "$RAW" | awk -F'\\| ' '{print $NF}' > "${ARTDIR}/_as_line.json" || true
          fi
          [ -s "${ARTDIR}/_as_line.json" ] && echo "as_line=${ARTDIR}/_as_line.json" >> "$GITHUB_OUTPUT" || true
          CC_JSONL="$(find "${ARTDIR}" -type f \( -iname '*cc.jsonl' -o -iname '*caption*.jsonl' -o -iname '*captions*.jsonl' \) -print | head -n1 || true)"
          if [ -n "${CC_JSONL:-}" ]; then
            echo "CRAWLER_CC=${CC_JSONL}" >> "$GITHUB_ENV"
          fi

      - name: Fallback download via yt-dlp (Space URL)
        if: ${{ github.event.inputs.mode != 'attendees_only' && github.event.inputs.mode != 'replies_only' && env.SOURCE_KIND != 'mp3' && (steps.crawl.outputs.audio_file == '' || steps.crawl.outcome != 'success') && env.SOURCE_URL != '' }}
        shell: bash
        working-directory: ${{ env.WORKDIR }}
        env:
          URL: ${{ env.SOURCE_URL }}
        run: |
          set -euxo pipefail
          yt-dlp -o "%(title)s.%(ext)s" -f "bestaudio/best" "$URL"
          IN="$(ls -S | head -n1 || true)"
          test -f "$IN" || { echo "No file downloaded"; exit 1; }
          echo "INPUT_FILE=$PWD/$IN" >> "$GITHUB_ENV"

      - name: Use provided MP3 (transcript_only)
        if: ${{ github.event.inputs.mode == 'transcript_only' && env.SOURCE_KIND == 'mp3' && env.SOURCE_URL != '' }}
        shell: bash
        run: |
          set -euxo pipefail
          curl -L "${SOURCE_URL}" -o "${ARTDIR}/${BASE}.mp3"
          echo "INPUT_FILE=${ARTDIR}/${BASE}.mp3" >> "$GITHUB_ENV"

      - name: Detect lead silence seconds
        id: detect
        if: ${{ github.event.inputs.mode != 'attendees_only' && github.event.inputs.mode != 'replies_only' && env.INPUT_FILE != '' }}
        shell: bash
        run: |
          set -euxo pipefail
          LOG="${WORKDIR}/silence.log"
          ffmpeg -hide_banner -i "$INPUT_FILE" -af "silencedetect=noise=-45dB:d=1" -f null - 2> "$LOG" || true
          LEAD="$(awk '/silence_end/ {print $5; exit}' "$LOG" || true)"
          case "$LEAD" in ''|*[^0-9.]* ) LEAD="0.0" ;; esac
          echo "TRIM_LEAD=${LEAD}" >> "$GITHUB_ENV"
          echo "lead=${LEAD}"       >> "$GITHUB_OUTPUT"

      - name: Trim head and tail (RF64-safe)
        if: ${{ github.event.inputs.mode != 'attendees_only' && github.event.inputs.mode != 'replies_only' && env.INPUT_FILE != '' }}
        shell: bash
        run: |
          set -euxo pipefail
          TRIM_WAV="${WORKDIR}/trim_${{ github.run_id }}.wav"
          ffmpeg -hide_banner -y -i "$INPUT_FILE" -af "silenceremove=start_periods=1:start_silence=1:start_threshold=-45dB:detection=peak,areverse,silenceremove=start_periods=1:start_silence=1:start_threshold=-45dB:detection=peak,areverse" -rf64 always -c:a pcm_s16le "$TRIM_WAV"
          echo "AUDIO_IN=${TRIM_WAV}" >> "$GITHUB_ENV"

      - name: Probe audio format
        id: probe
        if: ${{ github.event.inputs.mode != 'attendees_only' && github.event.inputs.mode != 'replies_only' && env.AUDIO_IN != '' }}
        shell: bash
        run: |
          set -euxo pipefail
          J="$(ffprobe -v error -select_streams a:0 -show_entries stream=channels,sample_rate -of json "$AUDIO_IN")"
          CH=$(echo "$J" | jq -r '.streams[0].channels // 1')
          SR=$(echo "$J" | jq -r '.streams[0].sample_rate // "48000"')
          echo "SRC_CH=${CH}" >> "$GITHUB_ENV"
          echo "SRC_SR=${SR}" >> "$GITHUB_ENV"

      - name: Encode MP3 (profile)
        if: ${{ github.event.inputs.mode != 'attendees_only' && github.event.inputs.mode != 'replies_only' && env.AUDIO_IN != '' }}
        shell: bash
        env:
          PROF: ${{ github.event.inputs.audio_profile != '' && github.event.inputs.audio_profile || 'radio' }}
        run: |
          set -euxo pipefail
          OUT="${ARTDIR}/${BASE}.mp3"
          CH="${SRC_CH:-1}"
          SR="${SRC_SR:-48000}"
          if [ "${PROF}" = "transparent" ]; then
            ffmpeg -hide_banner -y -i "$AUDIO_IN" -map a:0 -c:a libmp3lame -q:a 0 -ar "$SR" -ac "$CH" "$OUT"
          elif [ "${PROF}" = "radio" ]; then
            PRE="highpass=f=60,lowpass=f=14000,afftdn=nr=4:nf=-28,deesser=i=0.12,acompressor=threshold=-18dB:ratio=2:attack=12:release=220:makeup=2"
            ffmpeg -hide_banner -y -i "$AUDIO_IN" -af "${PRE},loudnorm=I=-16:TP=-1.5:LRA=11" -c:a libmp3lame -q:a 2 -ar "$SR" -ac "$CH" "$OUT"
          else
            PRE="highpass=f=70,lowpass=f=11500,afftdn=nr=8:nf=-25,deesser=i=0.2,acompressor=threshold=-18dB:ratio=2.8:attack=8:release=200:makeup=3"
            ffmpeg -hide_banner -y -i "$AUDIO_IN" -af "${PRE},loudnorm=I=-16:TP=-1.5:LRA=11" -c:a libmp3lame -q:a 2 -ar "$SR" -ac "$CH" "$OUT"
          fi
          echo "MP3_PATH=${OUT}" >> "$GITHUB_ENV"

      - name: Upload MP3 (proxy link via media.chbmp.org)
        id: upload_mp3
        if: ${{ github.event.inputs.mode != 'attendees_only' && github.event.inputs.mode != 'replies_only' && env.MP3_PATH != '' }}
        shell: bash
        run: |
          set -euxo pipefail
          DEST="gs://${GCS_BUCKET}/${BUCKET_PREFIX}/${BASE}.mp3"
          RAW="https://storage.googleapis.com/${GCS_BUCKET}/${BUCKET_PREFIX}/${BASE}.mp3"
          PROXY="https://media.chbmp.org/${PREFIX}/${BASE}.mp3"
          gsutil -m cp "${MP3_PATH}" "$DEST"
          if [ "${{ github.event.inputs.make_public }}" = "true" ]; then
            (gsutil acl ch -u AllUsers:R "$DEST" || gsutil iam ch allUsers:objectViewer "gs://${GCS_BUCKET}") || true
          fi
          echo "audio_raw=${RAW}"     >> "$GITHUB_OUTPUT"
          echo "audio_proxy=${PROXY}" >> "$GITHUB_OUTPUT"

      - name: Helper scripts (gen_vtt, polish, replies)
        shell: bash
        run: |
          set -euo pipefail
          mkdir -p ".github/workflows/scripts"

          cat > ".github/workflows/scripts/gen_vtt.py" <<'PY'
          import os, json, html
          from pathlib import Path
          ARTDIR = Path(os.environ.get("ARTDIR","."))
          BASE   = os.environ.get("BASE","space")
          CC     = os.environ.get("CC_JSONL","")
          SHIFT  = float(os.environ.get("SHIFT_SECS","0") or "0")
          def parse_line(line):
              d = json.loads(line)
              s = (d.get("start") or d.get("s") or d.get("ts") or d.get("offset") or (d.get("startMs") and d["startMs"]/1000))
              e = (d.get("end") or d.get("e") or d.get("te") or (s and d.get("duration") and s+d["duration"]) or (d.get("endMs") and d["endMs"]/1000))
              t = d.get("text") or d.get("t") or d.get("caption") or ""
              if s is None or e is None: return None
              return max(0.0, float(s)-SHIFT), max(0.0, float(e)-SHIFT), str(t)
          def to_ts(sec):
              h=int(sec//3600); m=int((sec%3600)//60); s=sec-(h*3600+m*60)
              return f"{h:02d}:{m:02d}:{s:06.3f}".replace('.',',')
          if not CC or not Path(CC).exists(): raise SystemExit(0)
          cues=[]
          with open(CC, "r", encoding="utf-8") as fh:
              for line in fh:
                  line=line.strip()
                  if not line: continue
                  try:
                      r=parse_line(line)
                      if r: cues.append(r)
                  except Exception:
                      continue
          cues.sort(key=lambda x: x[0])
          if not cues: raise SystemExit(0)
          with open(ARTDIR/f"{BASE}.vtt","w",encoding="utf-8") as f:
              f.write("WEBVTT\n\n")
              for s,e,t in cues:
                  f.write(f"{to_ts(s)} --> {to_ts(e)}\n{html.escape(t).replace('\n',' ').strip()}\n\n")
          Path(ARTDIR/f"{BASE}_transcript.html").write_text(
              "<div id='ss3k-transcript'>\n" + "\n".join(
                f\"<div class='ss3k-seg' data-start='{s:.3f}' data-end='{e:.3f}'><div class='txt'>{html.escape(t)}</div></div>\"
                for s,e,t in cues
              ) + "\n</div>", encoding="utf-8")
          PY
          chmod +x ".github/workflows/scripts/gen_vtt.py"

          cat > ".github/workflows/scripts/polish_transcript.py" <<'PY'
          import os, re
          from pathlib import Path
          ARTDIR = Path(os.environ.get("ARTDIR","."))
          BASE   = os.environ.get("BASE","space")
          src = ARTDIR/f"{BASE}_transcript.html"
          if not src.exists(): raise SystemExit(0)
          s = src.read_text(encoding="utf-8")
          s = re.sub(r'\s+\n', '\n', s)
          s = re.sub(r'\n{3,}', '\n\n', s)
          Path(ARTDIR/f"{BASE}_transcript_polished.html").write_text(s, encoding="utf-8")
          PY
          chmod +x ".github/workflows/scripts/polish_transcript.py"

          cat > ".github/workflows/scripts/replies.py" <<'PY'
          import os, re, requests, tldextract
          from pathlib import Path
          from bs4 import BeautifulSoup
          ARTDIR = Path(os.environ.get("ARTDIR","."))
          BASE   = os.environ.get("BASE","space")
          PURPLE = os.environ.get("PURPLE_TWEET_URL","").strip()
          FETCH_TITLES = (os.environ.get("LINK_LABEL_FETCH_TITLES","true").lower() == "true")
          FETCH_LIMIT  = int(os.environ.get("LINK_LABEL_FETCH_LIMIT","18") or "18")
          TIMEOUT      = int(os.environ.get("LINK_LABEL_TIMEOUT_SEC","4") or "4")
          def extract_urls_from_html(path: Path):
              if not path.exists(): return []
              soup = BeautifulSoup(path.read_text(encoding="utf-8"), "html.parser")
              urls=set()
              for a in soup.find_all("a", href=True):
                  href=a["href"].strip()
                  if href.startswith(("http://","https://")): urls.add(href)
              return list(urls)
          def title_for(url):
              if not FETCH_TITLES: return None
              try:
                  r = requests.get(url, timeout=TIMEOUT, headers={"User-Agent":"Mozilla/5.0"})
                  r.raise_for_status()
                  soup = BeautifulSoup(r.text, "html.parser")
                  t = soup.title.string.strip() if soup.title and soup.title.string else None
                  return (re.sub(r'\s+',' ',t) if t else None)
              except Exception:
                  return None
          tx = ARTDIR/f"{BASE}_transcript_polished.html"
          if not tx.exists(): tx = ARTDIR/f"{BASE}_transcript.html"
          urls = extract_urls_from_html(tx)
          uniq=[]
          seen=set()
          for u in urls:
              if u not in seen:
                  seen.add(u); uniq.append(u)
          uniq=uniq[:FETCH_LIMIT]
          items=[]
          for u in uniq:
              ttl = title_for(u)
              if not ttl:
                  ext = tldextract.extract(u)
                  host = ".".join([p for p in [ext.domain, ext.suffix] if p])
                  ttl = host or u
              items.append(f'<li><a href="{u}" target="_blank" rel="noopener">{ttl}</a></li>')
          if items:
              (ARTDIR/f"{BASE}_links.html").write_text("<ul>\n" + "\n".join(items) + "\n</ul>\n", encoding="utf-8")
          if PURPLE:
              (ARTDIR/f"{BASE}_replies.html").write_text(
                f'<div class="ss3k-replies"><p><a href="{PURPLE}" target="_blank" rel="noopener">Open conversation on X (purple pill)</a></p></div>',
                encoding="utf-8"
              )
          PY
          chmod +x ".github/workflows/scripts/replies.py"

      - name: Build VTT + transcript (from crawler captions if present)
        if: ${{ github.event.inputs.mode != 'attendees_only' && github.event.inputs.mode != 'replies_only' }}
        shell: bash
        env:
          CC_JSONL: ${{ env.CRAWLER_CC }}
          SHIFT_SECS: ${{ steps.detect.outputs.lead || '0' }}
        run: |
          set -euxo pipefail
          if [ -n "${CC_JSONL:-}" ] && [ -s "${CC_JSONL}" ]; then
            CC_JSONL="${CC_JSONL}" ARTDIR="${ARTDIR}" BASE="${BASE}" SHIFT_SECS="${SHIFT_SECS}" python3 ".github/workflows/scripts/gen_vtt.py"
            [ -s "${ARTDIR}/${BASE}.vtt" ] && echo "VTT_PATH=${ARTDIR}/${BASE}.vtt" >> "$GITHUB_ENV" || true
            if [ -s "${ARTDIR}/${BASE}_transcript_polished.html" ]; then
              echo "TRANSCRIPT_PATH=${ARTDIR}/${BASE}_transcript_polished.html" >> "$GITHUB_ENV"
            elif [ -s "${ARTDIR}/${BASE}_transcript.html" ]; then
              echo "TRANSCRIPT_PATH=${ARTDIR}/${BASE}_transcript.html" >> "$GITHUB_ENV"
            fi
          else
            : > "${ARTDIR}/${BASE}.start.txt"
          fi

      - name: VTT via Deepgram (fallback)
        id: deepgram
        if: ${{ github.event.inputs.mode != 'attendees_only' && github.event.inputs.mode != 'replies_only' && env.VTT_PATH == '' && env.DEEPGRAM_API_KEY != '' && env.MP3_PATH != '' }}
        shell: bash
        run: |
          set -euxo pipefail
          curl -sS -X POST \
            -H "Authorization: Token ${DEEPGRAM_API_KEY}" \
            -H "Content-Type: audio/mpeg" \
            --data-binary @"${MP3_PATH}" \
            "https://api.deepgram.com/v1/listen?model=nova-2&smart_format=true&punctuate=true&format=vtt" \
            -o "${ARTDIR}/${BASE}.vtt" || true
          [ -s "${ARTDIR}/${BASE}.vtt" ] && echo "VTT_PATH=${ARTDIR}/${BASE}.vtt" >> "$GITHUB_ENV" || true

      - name: Upload VTT (proxy link via media.chbmp.org)
        id: upload_vtt
        if: ${{ github.event.inputs.mode != 'attendees_only' && github.event.inputs.mode != 'replies_only' && env.VTT_PATH != '' }}
        shell: bash
        run: |
          set -euxo pipefail
          DEST="gs://${GCS_BUCKET}/${BUCKET_PREFIX}/${BASE}.vtt"
          RAW="https://storage.googleapis.com/${GCS_BUCKET}/${BUCKET_PREFIX}/${BASE}.vtt"
          PROXY="https://media.chbmp.org/${PREFIX}/${BASE}.vtt"
          gsutil -m cp "${VTT_PATH}" "$DEST"
          if [ "${{ github.event.inputs.make_public }}" = "true" ]; then
            (gsutil acl ch -u AllUsers:R "$DEST" || gsutil iam ch allUsers:objectViewer "gs://${GCS_BUCKET}") || true
          fi
          echo "vtt_raw=${RAW}"     >> "$GITHUB_OUTPUT"
          echo "vtt_proxy=${PROXY}" >> "$GITHUB_OUTPUT"

      - name: Build attendees HTML
        id: attendees
        if: ${{ github.event.inputs.mode != 'replies_only' && steps.crawl.outcome == 'success' && steps.crawl.outputs.as_line != '' }}
        shell: bash
        env:
          CAND: ${{ steps.crawl.outputs.as_line }}
        run: |
          set -euxo pipefail
          OUT_HTML="${ARTDIR}/attendees.html"
          jq -r '
            def mkp:
              { handle: (.twitter_screen_name // .user_results?.result?.legacy?.screen_name),
                name:   (.display_name       // .user_results?.result?.legacy?.name)
              }
              | select(.handle!=null and .handle!="" )
              | . + { url: ("https://x.com/" + .handle) };
            (.audioSpace // .) as $a
            | ($a.metadata?.creator_results?.result?.legacy?) as $h
            | ($h.screen_name // empty) as $H
            | {
                host:    ( if $H != "" then [ {handle:$H, name:($h.name // ""), url:("https://x.com/" + $H)} ] else [] end ),
                cohosts: ( ($a.participants?.admins   // []) | map(mkp) | map(select(.handle != $H)) | unique_by(.handle) ),
                speakers:( ($a.participants?.speakers // []) | map(mkp) | unique_by(.handle) )
              }
            | def li(i): "  <li><a href=\"" + (i.url//"#") + "\">" + ((i.name // "") + " (@" + (i.handle // "") + ")") + "</a></li>";
            def section(title; items):
              if (items|length) > 0 then "<h3>" + title + "</h3>\n<ul>\n" + (items|map(li)|join("\n")) + "\n</ul>\n" else "" end;
            . as $d
            | section("Host"; $d.host)
            + section( (if ($d.cohosts|length)==1 then "Co-host" else "Co-hosts" end); $d.cohosts)
            + section("Speakers"; $d.speakers)
          ' "${CAND}" > "$OUT_HTML"
          if grep -qi '<li><a ' "$OUT_HTML"; then
            echo "ATTN_HTML=${OUT_HTML}" >> "$GITHUB_ENV"
            echo "ATTENDEES_OK=1"       >> "$GITHUB_ENV"
          fi

      - name: Scrape replies & shared links
        if: ${{ github.event.inputs.mode != 'attendees_only' }}
        shell: bash
        env:
          PURPLE_TWEET_URL: ${{ env.PURPLE_TWEET_URL }}
          LINK_LABEL_FETCH_TITLES: ${{ env.LINK_LABEL_FETCH_TITLES }}
          LINK_LABEL_FETCH_LIMIT: ${{ env.LINK_LABEL_FETCH_LIMIT }}
          LINK_LABEL_TIMEOUT_SEC: ${{ env.LINK_LABEL_TIMEOUT_SEC }}
        run: |
          set -euxo pipefail
          python3 ".github/workflows/scripts/replies.py" || true
          [ -s "${ARTDIR}/${BASE}_replies.html" ] && echo "REPLIES_PATH=${ARTDIR}/${BASE}_replies.html" >> "$GITHUB_ENV" || true
          [ -s "${ARTDIR}/${BASE}_links.html" ]   && echo "LINKS_PATH=${ARTDIR}/${BASE}_links.html"   >> "$GITHUB_ENV" || true

      - name: Derive title + start time
        id: meta
        shell: bash
        env:
          AS_LINE: ${{ steps.crawl.outputs.as_line }}
          TITLE_HINT: ${{ env.TITLE_HINT }}
        run: |
          set -euo pipefail
          TTL=""
          if [ -n "${AS_LINE:-}" ] && [ -s "${AS_LINE}" ]; then
            TTL="$(jq -r '(.audioSpace // .) as $a | ($a.metadata.title // $a.metadata.name // .title // "")' "${AS_LINE}" | sed -E 's/^[[:space:]]+|[[:space:]]+$//g')"
          fi
          if [ -z "$TTL" ] && [ -n "${TITLE_HINT:-}" ]; then TTL="${TITLE_HINT}"; fi
          if [ -z "$TTL" ]; then TTL="${BASE}"; fi
          echo "TTL_FINAL=$TTL" >> "$GITHUB_ENV"

          START_ISO=""
          if [ -n "${AS_LINE:-}" ] && [ -s "${AS_LINE}" ]; then
            MS="$(jq -r '(.audioSpace // .) as $a | ($a.metadata.started_at // $a.metadata.created_at // $a.metadata.start // empty)' "${AS_LINE}")" || true
            if [[ "$MS" =~ ^[0-9]+$ ]]; then
              if [ ${#MS} -gt 10 ]; then SECS=$((MS/1000)); else SECS=$MS; fi
              START_ISO="$(date -u -d "@$SECS" +%Y-%m-%dT%H:%M:%SZ || true)"
            fi
          fi
          if [ -z "$START_ISO" ] && [ -s "${ARTDIR}/${BASE}.start.txt" ]; then
            START_ISO="$(head -n1 "${ARTDIR}/${BASE}.start.txt" | tr -d '\r\n')"
          fi
          if [ -z "$START_ISO" ]; then START_ISO="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; fi
          echo "START_ISO=$START_ISO" >> "$GITHUB_ENV"

      - name: Register assets in WordPress
        if: ${{ github.event.inputs.mode == '' && env.WP_BASE_URL != '' && env.WP_USER != '' && env.WP_APP_PASSWORD != '' && github.event.inputs.post_id != '' && (steps.upload_mp3.outputs.audio_proxy != '' || steps.upload_mp3.outputs.audio_raw != '') }}
        shell: bash
        env:
          PID:  ${{ github.event.inputs.post_id }}
          AUD:  ${{ steps.upload_mp3.outputs.audio_proxy || steps.upload_mp3.outputs.audio_raw }}
          VTTU: ${{ steps.upload_vtt.outputs.vtt_proxy   || steps.upload_vtt.outputs.vtt_raw }}
        run: |
          set -euo pipefail
          TTL="${TTL_FINAL:-${BASE}}"
          ATH_FILE="${WORKDIR}/empty_attendees.html"; : > "$ATH_FILE"
          [ -n "${ATTN_HTML:-}" ] && [ -s "${ATTN_HTML:-}" ] && ATH_FILE="${ATTN_HTML}"
          TR_FILE="${WORKDIR}/empty_transcript.html"; : > "$TR_FILE"
          [ -n "${TRANSCRIPT_PATH:-}" ] && [ -s "${TRANSCRIPT_PATH}" ] && TR_FILE="${TRANSCRIPT_PATH}"
          REP_FILE="${WORKDIR}/empty_replies.html"; : > "$REP_FILE"
          [ -n "${REPLIES_PATH:-}" ] && [ -s "${REPLIES_PATH}" ] && REP_FILE="${REPLIES_PATH}"
          LNK_FILE="${WORKDIR}/empty_links.html"; : > "$LNK_FILE"
          [ -n "${LINKS_PATH:-}" ] && [ -s "${LINKS_PATH}" ] && LNK_FILE="${LINKS_PATH}"
          REQ="${WORKDIR}/wp_register_body.json"
          jq -n \
            --arg gcs   "${AUD}" \
            --arg mime  "audio/mpeg" \
            --arg pid   "${PID}" \
            --arg ttl   "${TTL}" \
            --arg vtt   "${VTTU}" \
            --arg when  "${START_ISO:-}" \
            --rawfile ath "${ATH_FILE}" \
            --rawfile tr  "${TR_FILE}" \
            --rawfile rep "${REP_FILE}" \
            --rawfile lnk "${LNK_FILE}" \
            '{
               gcs_url: $gcs, mime: $mime, post_id: ($pid|tonumber), title: $ttl
             }
             + (if ($when|length)>0 then {post_date_gmt:$when, space_started_at:$when, publish_date:$when} else {} end)
             + (if ($vtt|length)>0 then {vtt_url:$vtt} else {} end)
             + (if ($ath|gsub("\\s";"")|length)>0 then {attendees_html:$ath} else {} end)
             + (if ($tr|gsub("\\s";"")|length)>0 then {transcript:$tr} else {} end)
             + (if ($rep|gsub("\\s";"")|length)>0 then {ss3k_replies_html:$rep} else {} end)
             + (if ($lnk|gsub("\\s";"")|length)>0 then {shared_links_html:$lnk} else {} end)
            ' > "$REQ"
          curl -sS -u "${WP_USER}:${WP_APP_PASSWORD}" -H "Content-Type: application/json" -X POST "${WP_BASE_URL%/}/wp-json/ss3k/v1/register" --data-binary @"$REQ" | jq -r .

      - name: Patch WP attendees only
        if: ${{ github.event.inputs.mode == 'attendees_only' && env.WP_BASE_URL != '' && env.WP_USER != '' && env.WP_APP_PASSWORD != '' && github.event.inputs.post_id != '' }}
        shell: bash
        run: |
          set -euo pipefail
          AT_HTML=""
          if [ -n "${ATTN_HTML:-}" ] && [ -s "${ATTN_HTML:-}" ]; then AT_HTML="$(cat "${ATTN_HTML}")"; fi
          BODY="$(jq -n --arg pid "${{ github.event.inputs.post_id }}" --arg ath "${AT_HTML}" \
            '{post_id: ($pid|tonumber), status:"complete", progress:100}
             + (if ($ath|length)>0 then {attendees_html:$ath} else {} end)')"
          curl -sS -u "${WP_USER}:${WP_APP_PASSWORD}" -H "Content-Type: application/json" -X POST "${WP_BASE_URL%/}/wp-json/ss3k/v1/patch-assets" -d "$BODY" | jq -r .

      - name: Patch WP replies only
        if: ${{ github.event.inputs.mode == 'replies_only' && env.WP_BASE_URL != '' && env.WP_USER != '' && env.WP_APP_PASSWORD != '' && github.event.inputs.post_id != '' }}
        shell: bash
        run: |
          set -euo pipefail
          REP="$( [ -n "${REPLIES_PATH:-}" ] && [ -s "${REPLIES_PATH:-}" ] && cat "${REPLIES_PATH}" || echo "" )"
          LNK="$( [ -n "${LINKS_PATH:-}" ] && [ -s "${LINKS_PATH:-}" ] && cat "${LINKS_PATH}" || echo "" )"
          BODY="$(jq -n --arg pid "${{ github.event.inputs.post_id }}" --arg rep "${REP}" --arg lnk "${LNK}" \
            '{post_id: ($pid|tonumber), status:"complete", progress:100}
             + (if ($rep|length)>0 then {ss3k_replies_html:$rep} else {} end)
             + (if ($lnk|length)>0 then {shared_links_html:$lnk} else {} end)')"
          curl -sS -u "${WP_USER}:${WP_APP_PASSWORD}" -H "Content-Type: application/json" -X POST "${WP_BASE_URL%/}/wp-json/ss3k/v1/patch-assets" -d "$BODY" | jq -r .

      - name: Summary
        shell: bash
        env:
          SID: ${{ steps.ids.outputs.space_id }}
        run: |
          {
            echo "### Space Worker Summary"
            echo "- Mode:        ${{ github.event.inputs.mode }}"
            echo "- Source kind: ${SOURCE_KIND}"
            echo "- Source URL:  ${SOURCE_URL}"
            echo "- Purple URL:  ${PURPLE_TWEET_URL}"
            echo "- Space ID:    ${SID}"
            echo "- Post ID:     ${{ github.event.inputs.post_id }}"
            echo "- Title:       ${TTL_FINAL:-}"
            echo "- Start (UTC): ${START_ISO:-}"
            if [ -n "${{ steps.upload_mp3.outputs.audio_proxy }}" ]; then
              echo "- Audio:       ${{ steps.upload_mp3.outputs.audio_proxy }}"
            elif [ -n "${{ steps.upload_mp3.outputs.audio_raw }}" ]; then
              echo "- Audio:       ${{ steps.upload_mp3.outputs.audio_raw }}"
            fi
            if [ -n "${{ steps.upload_vtt.outputs.vtt_proxy }}" ]; then
              echo "- VTT:         ${{ steps.upload_vtt.outputs.vtt_proxy }}"
            elif [ -n "${{ steps.upload_vtt.outputs.vtt_raw }}" ]; then
              echo "- VTT:         ${{ steps.upload_vtt.outputs.vtt_raw }}"
            fi
          } >> "$GITHUB_STEP_SUMMARY"
