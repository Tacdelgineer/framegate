# DGX Hermes pipeline worker

This worker polls a VPS queue, processes one job at a time, uploads its artifact,
and exposes health only on the DGX Spark Tailscale address. Visual jobs use the
headless ComfyUI API bound only to `127.0.0.1:8188`.

## Queue API contract

The default client supports:

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
    "seconds": 5,
    "aspect": "9:16"
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
    "seconds": 5
  }
}
```

Accepted frame forms are `frame` (a URL or one/two-element list), `frames`,
`start_frame` plus optional `end_frame`, and the corresponding `_url` aliases.
Exactly one or two inputs are required. Video output is a 576x1024 H.264 MP4 at
16 fps; lengths are rounded to Wan's required `4n+1` frame count.

Queue-native multipart inputs are also supported. Use roles `start_frame` and
optional `end_frame` for video, `audio` for transcription, and `clip_1` plus
`voiceover` for assembly. The worker injects each queue-provided
`download_url` into the handler payload after claiming.

TTS also accepts `ref_audio_url`, `ref_audio_path` (restricted to
`/srv/ai/assets`), and `ref_text`. Text-only jobs use the configured Narrator
voice profile.

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
then checks `MemAvailable` again before rejecting the job. It also frees
ComfyUI's cached models after every frame and video job. Ollama is never
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

Add one real frame and one real five-second video:

```bash
uv run --frozen python test_jobs.py \
  --jobs-url http://VPS_TAILSCALE_IP:8787/jobs \
  --visual-only --video-input-frames 2 --wait
```

Use `--video-input-frames 1` to exercise plain I2V, or `2` to exercise the
first/last-frame workflow.

Health is available at `http://100.103.129.82:8788/health`.
