#!/usr/bin/env python3
"""Create fixtures, upload them, and submit one tts/transcribe/assemble job."""

from __future__ import annotations

import argparse
import json
import math
import os
import struct
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import wave
from pathlib import Path
from typing import Any


def json_request(method: str, url: str, payload: Any | None = None) -> Any:
    data = None if payload is None else json.dumps(payload).encode()
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read()
            return json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        detail = exc.read(4096).decode("utf-8", "replace")
        raise SystemExit(f"{method} {url}: HTTP {exc.code}: {detail}") from exc


def make_wav(path: Path) -> None:
    rate = 16_000
    duration = 3
    with wave.open(str(path), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(rate)
        frames = bytearray()
        for index in range(rate * duration):
            value = int(2500 * math.sin(2 * math.pi * 220 * index / rate))
            frames.extend(struct.pack("<h", value))
        output.writeframes(frames)


def make_clip(path: Path, ffmpeg: str) -> None:
    result = subprocess.run(
        [
            ffmpeg,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-f",
            "lavfi",
            "-i",
            "color=c=0x111827:s=1080x1920:r=30:d=3",
            "-vf",
            (
                "drawtext=text='DGX pipeline test':fontcolor=white:fontsize=64:"
                "x=(w-text_w)/2:y=(h-text_h)/2"
            ),
            "-c:v",
            "libx264",
            "-pix_fmt",
            "yuv420p",
            str(path),
        ],
        check=False,
    )
    if result.returncode:
        raise SystemExit("ffmpeg could not create the test clip")


def upload(path: Path, upload_url: str, curl: str) -> str:
    result = subprocess.run(
        [
            curl,
            "--fail-with-body",
            "--silent",
            "--show-error",
            "--request",
            "POST",
            "--form",
            f"file=@{path}",
            "--form",
            "purpose=pipeline-test",
            upload_url,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        raise SystemExit(f"Upload failed: {result.stderr or result.stdout}")
    try:
        body = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Upload returned invalid JSON: {result.stdout!r}") from exc
    for key in ("url", "download_url", "file_url"):
        if body.get(key):
            return urllib.parse.urljoin(upload_url, str(body[key]))
    if isinstance(body.get("file"), dict):
        for key in ("url", "download_url"):
            if body["file"].get(key):
                return urllib.parse.urljoin(upload_url, str(body["file"][key]))
    raise SystemExit(f"Upload response has no file URL: {body}")


def submit(jobs_url: str, job_type: str, payload: dict[str, Any]) -> str:
    body = json_request("POST", jobs_url, {"type": job_type, "payload": payload})
    if not isinstance(body, dict):
        raise SystemExit(f"Job submission returned invalid response: {body!r}")
    job = body.get("job") if isinstance(body.get("job"), dict) else body
    job_id = job.get("id") or job.get("job_id")
    if not job_id:
        raise SystemExit(f"Job submission returned no id: {body}")
    return str(job_id)


def wait_for_jobs(jobs_url: str, job_ids: list[str], timeout: int) -> None:
    pending = set(job_ids)
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for job_id in list(pending):
            body = json_request(
                "GET", f"{jobs_url}/{urllib.parse.quote(job_id)}"
            )
            job = body.get("job") if isinstance(body, dict) and "job" in body else body
            status = str(job.get("status", "")).lower() if isinstance(job, dict) else ""
            if status in {"completed", "complete", "succeeded", "failed", "error"}:
                print(json.dumps({"id": job_id, "status": status}))
                pending.remove(job_id)
        if pending:
            time.sleep(2)
    if pending:
        raise SystemExit(f"Timed out waiting for jobs: {sorted(pending)}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--jobs-url",
        default=os.getenv(
            "PIPELINE_JOBS_URL", "http://VPS_TAILSCALE_IP:8787/jobs"
        ),
    )
    parser.add_argument("--upload-url", default=os.getenv("PIPELINE_UPLOAD_URL"))
    parser.add_argument("--curl", default="/usr/bin/curl")
    parser.add_argument(
        "--ffmpeg",
        default="/home/xxfactionsxx/pinokio/bin/ffmpeg-env/bin/ffmpeg",
    )
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--timeout", type=int, default=1800)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    jobs_url = args.jobs_url.rstrip("/")
    if "VPS_TAILSCALE_IP" in jobs_url or "<" in jobs_url:
        raise SystemExit("Set --jobs-url or PIPELINE_JOBS_URL to the VPS Tailscale URL")
    api_root = jobs_url.removesuffix("/jobs")
    upload_url = (args.upload_url or f"{api_root}/files").rstrip("/")
    with tempfile.TemporaryDirectory(prefix="pipeline-test-") as raw_dir:
        directory = Path(raw_dir)
        audio = directory / "fixture.wav"
        clip = directory / "fixture.mp4"
        make_wav(audio)
        make_clip(clip, args.ffmpeg)
        audio_url = upload(audio, upload_url, args.curl)
        clip_url = upload(clip, upload_url, args.curl)
        jobs = [
            submit(
                jobs_url,
                "tts",
                {
                    "text": (
                        "The DGX Spark media pipeline is online and ready "
                        "for the remote Hermes agent."
                    )
                },
            ),
            submit(jobs_url, "transcribe", {"audio_url": audio_url}),
            submit(
                jobs_url,
                "assemble",
                {
                    "clips": [clip_url],
                    "voiceover_url": audio_url,
                    "captions": [
                        {
                            "start": 0.0,
                            "end": 2.8,
                            "text": "DGX pipeline integration test",
                        }
                    ],
                },
            ),
        ]
        print(json.dumps({"submitted": jobs}, indent=2))
        if args.wait:
            wait_for_jobs(jobs_url, jobs, args.timeout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
