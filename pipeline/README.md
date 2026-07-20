# News pipeline

Crash-resumable, SQLite-backed production of narration-timed vertical
evergreen explainer videos. The default preset targets 45 seconds.

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
preset controls `target_duration_seconds`, `clip_padding`,
`max_clip_seconds`, the verbatim frame-prompt `style_block`, local frame
workflow, I2V/first-last-frame selection, resolution, draft/final steps,
negative prompt, output FPS, and the independent OpenAI-compatible
`script_provider`. The free-text `narration_style` is injected into the script
prompt. `caption_style` controls caption enablement, size, base/highlight
colors, and vertical position.

The script prompt requests enough 4–6-second narration shots to fill
`target_duration_seconds` (nine shots at the 45-second default). TTS runs once
per shot before frame/video generation. Each clip requests the measured
narration duration plus `clip_padding`, rounded up to Wan's `4n+1` frame shape
at 16fps without exceeding `max_clip_seconds`. Narration over the cap gets one
script-provider shortening retry; if it is still long, assembly holds the last
video frame instead of cutting the audio.

`script_provider` contains `base_url`, `model`, optional `api_key_env`, and an
overall streaming-generation `timeout_seconds` ceiling (default 900 seconds).
Use `https://api.x.ai/v1` with `XAI_API_KEY` for xAI, or
`https://api.openai.com/v1` with `OPENAI_API_KEY` for OpenAI. A selected key
environment variable must be configured before a run starts. `--visuals` does
not select or alter the script provider. Script calls stream their response;
`API_REQUEST_TIMEOUT` is applied to each period of network inactivity, while
thinking/output chunks keep an otherwise slow generation alive.

## State machine

The successful-stage states are:

`fetched → scripted → voiced → framed (frame gate) → rendered → assembled →
pending_approval → published|rejected`

A newly-created row has a null state until `fetch_story` turns its topic into
an evergreen content brief. SQLite is in WAL mode. State advances only after a
complete stage, generated files use atomic replacement, Imagine request IDs,
queue job IDs, and each shot's TTS path/duration/timing are persisted before
the next shot, and a restart resumes the newest unfinished run.

Each stage retries at most three times with exponential backoff. Frame
approval, generation number, seed, album/control message IDs, and callbacks are
persisted per frame. A frame Regenerate action changes its seed and requeues
only that frame. Final-video Regenerate deletes generated media, rewinds to
`scripted`, and reruns frames onward.

## DGX queue jobs

- `frame`: no input files; payload contains the configured worker workflow,
  prompt, negative prompt, resolution, draft steps, and seed.
- `tts`: one job per shot; payload contains that shot's narration, shot index,
  target duration, and the selected `TTS_VOICE_PRESET` contract fields
  `voice_ref` and `voice_ref_text`. Each result is stored under `voiceover/`
  and all active results are joined into `voiceover.wav` with 0.5 seconds of
  silent tail room.
- `video`: input role `start_frame` for I2V, followed by `end_frame` for
  first/last-frame mode. The queue client uploads those as
  `input:start_frame` and optional `input:end_frame`; payload contains the
  motion instruction, resolution, final steps, seed, narration-derived
  `duration_seconds`, `frame_count`, and 16fps input rate.
- `transcribe`: input role `audio`; faster-whisper JSON is converted locally
  into two- or three-word ASS karaoke events using word-level timestamps.
- `assemble`: dynamic input roles `clip_1` through `clip_N` and `voiceover`;
  16fps clips pass through ffmpeg `minterpolate` to the configured `fps_out`.
  If audio is longer, `tpad=stop_mode=clone` extends the final frame through
  the complete voiceover (including its 0.5-second tail). The payload
  explicitly forbids trimming audio or using shortest-stream termination. The
  pipeline then burns the styled ASS track with ffmpeg's subtitles filter.

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
