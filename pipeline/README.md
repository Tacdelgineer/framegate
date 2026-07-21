# News pipeline

Crash-resumable, SQLite-backed production of a five-shot, 50-second vertical
news video.

## Run

Copy `.env.example` to `/home/alireza/content-factory/pipeline/.env` and set
`XAI_API_KEY`, `OPENAI_API_KEY`, `TELEGRAM_BOT_TOKEN`, and
`TELEGRAM_CHAT_ID`, then:

```bash
cd /home/alireza/content-factory/pipeline
./.venv/bin/python news_pipeline.py --new --topic "AI infrastructure news"
```

Resume the newest non-terminal run:

```bash
./.venv/bin/python news_pipeline.py
```

Resume a specific run:

```bash
./.venv/bin/python news_pipeline.py --run-id <uuid>
```

Cheap validation stops after the five low-quality portrait first frames:

```bash
./.venv/bin/python news_pipeline.py --new --dry-run
```

## State machine

The successful-stage states are:

`fetched → scripted → framed → rendered → voiced → assembled →
pending_approval → published|rejected`

A newly-created row has a null state until `fetch_story` succeeds. SQLite is
in WAL mode. State advances only after a complete stage, generated files use
atomic replacement, Imagine request IDs and queue job IDs are persisted before
polling, and a restart resumes the newest unfinished run.

Each stage retries at most three times with exponential backoff. Regenerate
deletes generated media, rewinds to `scripted`, and reruns frames onward.

## DGX queue jobs

- `tts`: payload contains the combined narration and five timed shot entries;
  result is downloaded to `voiceover.wav`. The selected `TTS_VOICE_PRESET`
  adds the worker contract fields `voice_ref` and `voice_ref_text`.
- `transcribe`: input role `audio`, result downloaded to `captions.srt`.
- `assemble`: input roles `clip_1` through `clip_5`, `voiceover`, and
  `captions`; result downloaded to `final.mp4`.

Grok and OpenAI requests execute directly on the VPS. Only the three media
jobs above are sent to the queue.

The queue client is imported from the sibling `../queue` directory by default.
`JOB_QUEUE_CLIENT_ROOT` can override that location for development.

Voice presets are declared in `news_pipeline.py` as `VOICE_PRESETS` blocks with
`ref_audio` (the absolute DGX worker asset path) and `ref_text_file` (the
checked-in transcript read by the pipeline). `TTS_VOICE_PRESET` defaults to
`alireza`. After this change is deployed, the VPS only needs a `git pull`; the
pipeline is invoked fresh for each run, so no additional service restart is
required.

## Approval

The pipeline uploads the final MP4 and metadata through the configured Telegram
bot with Approve, Reject, and Regenerate buttons. It long-polls callback
updates, persists the update offset and decision before acting, and therefore
survives a crash at the approval gate. `publish()` currently writes a local
stub receipt and performs no platform upload.
