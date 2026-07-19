# DGX job queue

A small FastAPI/SQLite queue bound only to the VPS Tailscale address. The VPS
submits media stages and waits; a DGX worker polls, claims, processes, and
uploads the result.

## API contract

- `POST /jobs` creates a job. Send JSON with `job_type` (the alias `type` is
  also accepted) and `payload`, or
  multipart data with those fields plus uploads named `input:<role>`.
- `GET /jobs?status=pending&job_type=transcribe` lists jobs oldest first.
- `POST /jobs/{id}/claim` atomically claims a pending job. JSON:
  `{"worker_id":"dgx-name"}`. The response contains a one-use `claim_token`.
  The body is optional and defaults to `dgx-worker`.
- `POST /jobs/{id}/complete` accepts JSON or multipart data. Include the
  `claim_token`, a `status` of `done` or `failed`, optional JSON `result`,
  optional `error`, and one uploaded result file.
  New workers should return the token to prevent a timed-out worker from
  completing someone else's claim; tokenless completion remains supported for
  simple workers.
- `GET /jobs/{id}` returns current state and file metadata.
- `GET /jobs/{id}/files/{file_id}` downloads an input or result file.
- `GET /health` reports queue health.

Claims older than 30 minutes are returned to `pending`. A stale worker cannot
finish a re-claimed job because completion requires the token from the current
claim.

## Pipeline client

`job_queue.client.JobQueueClient` uploads input files, polls until the worker
finishes, downloads the result atomically, raises `JobFailedError` on remote
failure, and defaults to `JOB_QUEUE_URL=http://100.123.208.90:8787`.

```python
from job_queue import JobQueueClient

with JobQueueClient() as queue:
    queue.run(
        "transcribe",
        {"language": "en"},
        input_files={"audio": voiceover_path},
        output_path=transcript_path,
    )
```

The news pipeline uses these job types and file roles:

| Job type | Input roles | Result |
| --- | --- | --- |
| `frame` | none | generated PNG frame |
| `video` (I2V) | `frame` | generated MP4 clip |
| `video` (first/last) | `first_frame`, `last_frame` | generated MP4 clip |
| `tts` | none | generated audio |
| `transcribe` | `audio` | transcript file |
| `assemble` | `clip_1`…`clip_5`, `voiceover`, `captions` | assembled video |

Story/script model requests remain in the VPS pipeline. Visual jobs use these
queue types in local mode and use OpenAI/xAI directly in cloud mode.
