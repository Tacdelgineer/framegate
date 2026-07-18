# DGX Hermes pipeline worker

This worker polls a VPS queue, processes one job at a time, uploads its artifact,
and exposes health only on the DGX Spark Tailscale address.

## Queue API contract

The default client supports:

- `POST /jobs/claim` with `worker_id`, `capabilities`, and `max_jobs: 1`. A
  response may be a job, `{ "job": ... }`, or HTTP 204. It falls back to
  `GET /jobs?claim=1&worker_id=...`.
- `POST /files` as multipart form data with `file`, `job_id`, and JSON
  `metadata`.
- `POST /jobs/{id}/complete` with the result and uploaded artifact metadata.
  If `/files` is unavailable, it falls back to multipart completion.
- `POST /jobs/{id}/fail` after three total attempts (the first attempt plus two
  retries).
- `GET /jobs/stats` or `GET /jobs?view=stats` for queue depth.

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

TTS also accepts `ref_audio_url`, `ref_audio_path` (restricted to
`/srv/ai/assets`), and `ref_text`. Text-only jobs use the configured Narrator
voice profile.

## Configure the VPS address

Edit `/etc/pipeline-worker.env`, replace `VPS_TAILSCALE_IP`, then run:

```bash
sudo systemctl restart pipeline-worker
```

The integration submitter creates and uploads fixtures before submitting one
job of each type:

```bash
uv run --frozen python test_jobs.py \
  --jobs-url http://VPS_TAILSCALE_IP:8787/jobs \
  --wait
```

Health is available at `http://100.103.129.82:8788/health`.

