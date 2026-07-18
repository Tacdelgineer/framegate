# DGX worker

Placeholder for the worker implementation that is synchronized from the DGX
separately. Do not treat this directory as the source of truth until that sync
is complete.

The worker initiates all traffic: it polls the queue at
`http://100.123.208.90:8787`, claims a job with a stable worker ID, downloads
inputs, and completes it with a result file or a failure. It needs handlers for:

| Job type | Inputs | Result |
| --- | --- | --- |
| `tts` | narration and shot timing in the JSON payload | WAV audio |
| `transcribe` | `audio` | SRT captions |
| `assemble` | `clip_1`…`clip_5`, `voiceover`, `captions` | final MP4 |

The worker must retain and return each claim token. Claims left unfinished for
30 minutes return to `pending`.
