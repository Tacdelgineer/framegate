# News pipeline

Crash-resumable, SQLite-backed production of a five-shot, 50-second vertical
evergreen explainer video.

## Run

Copy `.env.example` to `/home/alireza/content-factory/pipeline/.env` and set
`TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`, then:

```bash
cd /home/alireza/content-factory/pipeline
./.venv/bin/python news_pipeline.py --new --topic "AI infrastructure news"
```

The default script provider is the DGX Ollama OpenAI-compatible endpoint, and
visuals default to the local DGX queue, so the default content path requires no
model API key. To use `gpt-image-1` for frames and xAI Imagine for video, set
`OPENAI_API_KEY` and `XAI_API_KEY` and run with `--visuals cloud`.

Resume the newest non-terminal run:

```bash
./.venv/bin/python news_pipeline.py
```

Resume a specific run:

```bash
./.venv/bin/python news_pipeline.py --run-id <uuid>
```

Cheap validation stops after frame generation, before Telegram frame approval:

```bash
./.venv/bin/python news_pipeline.py --new --dry-run
```

The frame gate is enabled by default. `--no-frame-gate` skips it.

## Presets

Every new run reads `../config/presets.yaml` and snapshots the resolved values
in SQLite so a later edit cannot change a resumed run. The human-editable
preset controls the verbatim frame-prompt `style_block`, local frame workflow,
I2V/first-last-frame selection, resolution, draft/final steps, negative prompt,
output FPS, and the independent OpenAI-compatible `script_provider`.

`script_provider` contains `base_url`, `model`, and optional `api_key_env`.
Use `https://api.x.ai/v1` with `XAI_API_KEY` for xAI, or
`https://api.openai.com/v1` with `OPENAI_API_KEY` for OpenAI. A selected key
environment variable must be configured before a run starts. `--visuals` does
not select or alter the script provider.

## State machine

The successful-stage states are:

`fetched → scripted → framed (frame gate) → rendered → voiced → assembled →
pending_approval → published|rejected`

A newly-created row has a null state until `fetch_story` turns its topic into
an evergreen content brief. SQLite is in WAL mode. State advances only after a
complete stage, generated files use atomic replacement, Imagine request IDs
and queue job IDs are persisted before polling, and a restart resumes the
newest unfinished run.

Each stage retries at most three times with exponential backoff. Frame
approval, generation number, seed, album/control message IDs, and callbacks are
persisted per frame. A frame Regenerate action changes its seed and requeues
only that frame. Final-video Regenerate deletes generated media, rewinds to
`scripted`, and reruns frames onward.

## DGX queue jobs

- `frame`: no input files; payload contains the configured worker workflow,
  prompt, negative prompt, resolution, draft steps, and seed.
- `video`: input role `frame` for I2V, or `first_frame` and `last_frame` for
  first/last-frame mode; payload contains the motion instruction, resolution,
  final steps, and seed.
- `tts`: payload contains the combined narration and five timed shot entries;
  result is downloaded to `voiceover.wav`. The selected `TTS_VOICE_PRESET`
  adds the worker contract fields `voice_ref` and `voice_ref_text`.
- `transcribe`: input role `audio`, result downloaded to `captions.srt`.
- `assemble`: input roles `clip_1` through `clip_5`, `voiceover`, and
  `captions`; 16fps clips pass through ffmpeg `minterpolate` to the configured
  `fps_out` before caption burn, and the result is downloaded to `final.mp4`.

Content-brief and script generation call the preset's OpenAI-compatible
provider directly from the VPS. In local visual mode all five job types above
use the queue; in cloud visual mode only TTS, transcription, and assembly use
it.

The queue client is imported from the sibling `../queue` directory by default.
`JOB_QUEUE_CLIENT_ROOT` can override that location for development.

Voice presets are declared in `news_pipeline.py` as `VOICE_PRESETS` blocks with
`ref_audio` (the absolute DGX worker asset path) and `ref_text_file` (the
checked-in transcript read by the pipeline). `TTS_VOICE_PRESET` defaults to
`alireza`. After this change is deployed, the VPS only needs a `git pull`; the
pipeline is invoked fresh for each run, so no additional service restart is
required.

## Approval

After frames finish, the default frame gate uploads them as a Telegram album
and posts one Approve/Regenerate control message per frame. Video for a shot is
not queued until all of that shot's frames are approved. The pipeline later
uploads the final MP4 and metadata with Approve, Reject, and Regenerate
buttons. Both gates persist callback decisions before acting and therefore
resume after a crash. `publish()` currently writes a local stub receipt and
performs no platform upload.
