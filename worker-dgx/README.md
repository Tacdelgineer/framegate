# DGX Hermes pipeline worker

This worker polls a VPS queue, processes one job at a time, uploads its artifact,
and exposes health only on the DGX Spark Tailscale address. Visual jobs use the
headless ComfyUI API bound only to `127.0.0.1:8188`.

## Queue API contract

The default client supports:

| Job type | Inputs | Result |
| --- | --- | --- |
| `tts` | one shot's `text`, `shot_index`, and target duration in the JSON payload | PCM WAV audio |
| `transcribe` | `audio` | SRT captions |
| `assemble` | `clip_1`…`clip_N`, `voiceover`, `captions` | final MP4 |

Local `video` handlers must honor the payload's `frame_count` and `fps`
(currently Wan `4n+1` frames at 16fps); `duration_seconds` is descriptive and
useful for backends that accept seconds directly.

For `assemble`, concatenate the dynamic `clip_roles` in order and apply
`pre_caption_video_filter` to that concatenated stream. The filter contains
`tpad=stop_mode=clone` whenever video would otherwise end before the
voiceover. Map the complete voiceover, do not use ffmpeg `-shortest`, and do
not apply `-t` or `atrim` in a way that cuts it. The voiceover already includes
0.5 seconds of silent tail room, so `output_duration_seconds` includes the
required gap between the last spoken word and the end of the final MP4.
Treat `trim_audio: false` and `shortest: false` as mandatory invariants.

- `GET /jobs?status=pending` followed by atomic
  `POST /jobs/{id}/claim` with `worker_id`.
- One-use `claim_token` propagation through the entire job.
- Multipart `POST /jobs/{id}/complete` with the result file, JSON metadata,
  `status=done`, and the claim token.
- JSON completion with `status=failed` after three total local attempts.
- `GET /health` for queue depth.

Submit jobs with `POST /jobs`:

```json
{"type":"tts","payload":{"text":"Hello","speed":1.0}}
```

```json
{"type":"transcribe","payload":{"audio_url":"http://VPS:8787/files/input.wav"}}
```

```json
{
  "type": "assemble",
  "payload": {
    "clips": ["http://VPS:8787/files/clip.mp4"],
    "voiceover_url": "http://VPS:8787/files/voiceover.wav",
    "captions": [{"start": 0, "end": 1.5, "text": "Hello"}]
  }
}
```

```json
{
  "type": "frame",
  "payload": {
    "prompt": "A cinematic portrait in warm window light",
    "aspect": "9:16"
  }
}
```

`frame` uses Apache-2.0 FLUX.2 Klein 4B FP8 at 576x1024, then the existing
RealESRGAN x4 model and a final Lanczos resize to produce an exact 1080x1920
PNG.

```json
{
  "type": "video",
  "payload": {
    "frame": "http://VPS:8787/files/start.png",
    "prompt": "The camera slowly pushes forward",
    "duration_seconds": 3,
    "frame_count": 49,
    "fps": 16,
    "aspect_ratio": "9:16"
  }
}
```

One input frame selects the plain Wan 2.2 I2V graph. Two frames select
`WanFirstLastFrameToVideo`:

```json
{
  "type": "video",
  "payload": {
    "frames": [
      "http://VPS:8787/files/start.png",
      "http://VPS:8787/files/end.png"
    ],
    "prompt": "A smooth continuous transformation",
    "duration_seconds": 5,
    "frame_count": 81,
    "fps": 16
  }
}
```

Accepted frame forms are `frame` (a URL or one/two-element list), `frames`,
`start_frame` plus optional `end_frame`, and the corresponding `_url` aliases.
Exactly one or two inputs are required. Video output is a 576x1024 H.264 MP4 at
16 fps. `frame_count` is authoritative when supplied and must be a positive
`4n+1` value no greater than 129, Wan's eight-second cap. The worker patches
that exact length into either Wan graph and does not trim the generated clip.
`duration_seconds` records the narration target and may be longer than the Wan
generation cap when `frame_count` is explicitly capped at 129; assembly holds
the last generated frame for the remaining narration. Legacy `seconds`
remains an alias when `frame_count` is omitted.

Queue-native multipart inputs are also supported. Use `start_frame` and
optional `end_frame` for video and `audio` for transcription. Multipart upload
field names are `input:start_frame` followed by optional `input:end_frame`.
Assembly must provide ordered `clip_roles` (for example,
`["clip_1", "clip_2"]`) plus `voiceover_role` and optional `captions_role`;
the worker resolves uploads in the declared order. During final muxing the
worker clone-pads the last video frame with `tpad` before applying
`-shortest`, so the complete narration is retained rather than trimmed to a
short visual stream. The worker injects each queue-provided `download_url`
into the handler payload after claiming.

TTS accepts optional zero-shot voice cloning through `voice_ref` and
`voice_ref_text`:

```json
{
  "type": "tts",
  "payload": {
    "text": "This uses the selected cloned voice.",
    "voice_ref": "/home/xxfactionsxx/content-factory/assets/alireza.wav",
    "voice_ref_text": "The exact transcript spoken in alireza.wav."
  }
}
```

Both fields must be supplied together. `voice_ref` must resolve to an existing
file under `/home/xxfactionsxx/content-factory/assets`; paths elsewhere and
symlinks escaping that directory are rejected. The worker stages the validated
audio into F5-TTS's mounted asset directory and passes it with
`voice_ref_text` as the F5 reference. When both fields are absent, the existing
configured Narrator voice remains the default. Legacy `ref_audio_url`,
`ref_audio_path` (restricted to `/srv/ai/assets`), and `ref_text` remain
supported, but cannot be combined with the new fields.

## ComfyUI service and models

Install the loopback-only service:

```bash
sudo install -m 0644 systemd/comfyui.service /etc/systemd/system/comfyui.service
sudo systemctl daemon-reload
sudo systemctl enable --now comfyui
```

The service reuses `dgx-ai-stack-comfyui:latest`. Its model mounts are
read-only, and `comfyui/extra_model_paths.yaml` references the existing
`/srv/ai/models/comfyui` tree and external RealESRGAN directory. It does not
copy or download weights.

The worker gates every visual job on `MemAvailable` from `/proc/meminfo`.
`VISUAL_MIN_AVAILABLE_GB` defaults to 40. If the first check is below the gate,
the worker asks ComfyUI to unload cached models and free memory, waits briefly,
then checks `MemAvailable` again before rejecting the job. After a successful
visual job, the worker retains the ComfyUI model and reuses it when the next
claimed job has the same family (`flux-frame` or `wan-video`). It calls
ComfyUI `/free` when the next claimed job changes families or is non-visual.
Each claimed job logs its model family and `state=load`, `state=cached`, or
`state=not-applicable` so cache impact can be measured. Ollama is never
unloaded; Qwen remains warm by design.

## Configure the VPS address

Edit `/etc/pipeline-worker.env`, replace `VPS_TAILSCALE_IP`, then run:

```bash
sudo systemctl restart pipeline-worker
```

The integration submitter preserves the original three-job test by default:

```bash
uv run --frozen python test_jobs.py \
  --jobs-url http://VPS_TAILSCALE_IP:8787/jobs \
  --wait
```

Add one real frame and one real video:

```bash
uv run --frozen python test_jobs.py \
  --jobs-url http://VPS_TAILSCALE_IP:8787/jobs \
  --visual-only --video-input-frames 2 --video-frame-count 49 --wait
```

Use `--video-input-frames 1` to exercise plain I2V, or `2` to exercise the
first/last-frame workflow.

Health is available at `http://100.103.129.82:8788/health`.
