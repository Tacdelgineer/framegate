#!/usr/bin/env python3
"""Create fixtures and submit classic and/or visual worker integration jobs."""

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


def make_frame(path: Path, ffmpeg: str, color: str, label: str) -> None:
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
            f"color=c={color}:s=576x1024:d=1",
            "-vf",
            (
                f"drawtext=text='{label}':fontcolor=white:fontsize=48:"
                "x=(w-text_w)/2:y=(h-text_h)/2"
            ),
            "-frames:v",
            "1",
            str(path),
        ],
        check=False,
    )
    if result.returncode:
        raise SystemExit(f"ffmpeg could not create the {label.lower()} fixture")


def submit(
    jobs_url: str,
    job_type: str,
    payload: dict[str, Any],
    *,
    curl: str,
    input_files: dict[str, Path] | None = None,
) -> str:
    if input_files:
        command = [
            curl,
            "--fail-with-body",
            "--silent",
            "--show-error",
            "--request",
            "POST",
            "--form",
            f"job_type={job_type}",
            "--form",
            f"payload={json.dumps(payload, separators=(',', ':'))}",
        ]
        for role, path in input_files.items():
            command.extend(["--form", f"input:{role}=@{path}"])
        command.append(jobs_url)
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            raise SystemExit(
                f"Job submission failed: {result.stderr or result.stdout}"
            )
        try:
            body = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise SystemExit(
                f"Job submission returned invalid JSON: {result.stdout!r}"
            ) from exc
    else:
        body = json_request("POST", jobs_url, {"type": job_type, "payload": payload})
    if not isinstance(body, dict):
        raise SystemExit(f"Job submission returned invalid response: {body!r}")
    job = body.get("job") if isinstance(body.get("job"), dict) else body
    job_id = job.get("id") or job.get("job_id")
    if not job_id:
        raise SystemExit(f"Job submission returned no id: {body}")
    return str(job_id)


def wait_for_jobs(
    jobs_url: str,
    job_ids: list[str],
    timeout: int,
    submitted_at: dict[str, float],
) -> None:
    pending = set(job_ids)
    failed: list[str] = []
    deadline = time.monotonic() + timeout
    while pending and time.monotonic() < deadline:
        for job_id in list(pending):
            body = json_request(
                "GET", f"{jobs_url}/{urllib.parse.quote(job_id)}"
            )
            job = body.get("job") if isinstance(body, dict) and "job" in body else body
            status = str(job.get("status", "")).lower() if isinstance(job, dict) else ""
            if status in {
                "done",
                "completed",
                "complete",
                "succeeded",
                "failed",
                "error",
            }:
                result = job.get("result") if isinstance(job, dict) else None
                print(
                    json.dumps(
                        {
                            "id": job_id,
                            "status": status,
                            "elapsed_seconds": round(
                                time.monotonic() - submitted_at[job_id], 3
                            ),
                            "worker_processing_seconds": (
                                result.get("processing_time")
                                if isinstance(result, dict)
                                else None
                            ),
                            "result": result,
                        }
                    ),
                    flush=True,
                )
                if status in {"failed", "error"}:
                    failed.append(job_id)
                pending.remove(job_id)
        if pending:
            time.sleep(2)
    if pending:
        raise SystemExit(f"Timed out waiting for jobs: {sorted(pending)}")
    if failed:
        raise SystemExit(f"Jobs failed: {failed}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--jobs-url",
        default=os.getenv(
            "PIPELINE_JOBS_URL", "http://VPS_TAILSCALE_IP:8787/jobs"
        ),
    )
    parser.add_argument("--curl", default="/usr/bin/curl")
    parser.add_argument(
        "--ffmpeg",
        default="/home/xxfactionsxx/pinokio/bin/ffmpeg-env/bin/ffmpeg",
    )
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--timeout", type=int, default=7200)
    parser.add_argument(
        "--visual",
        action="store_true",
        help="Also submit one FLUX.2 frame and one Wan 2.2 five-second video.",
    )
    parser.add_argument(
        "--visual-only",
        action="store_true",
        help="Submit only the frame and video integration jobs.",
    )
    parser.add_argument(
        "--video-input-frames",
        type=int,
        choices=(1, 2),
        default=2,
        help="Use plain I2V (1) or Wan first/last-frame-to-video (2).",
    )
    parser.add_argument(
        "--video-frame-count",
        type=int,
        default=81,
        help="Wan frame count in 4n+1 form, up to 129 (default: 81).",
    )
    parser.add_argument(
        "--frame-prompt",
        default=(
            "A cinematic vertical photograph of a neon-lit desert observatory "
            "under a star-filled night sky, crisp detail, no text"
        ),
    )
    parser.add_argument(
        "--video-prompt",
        default=(
            "The camera slowly pushes toward the observatory while stars drift "
            "overhead and warm window lights flicker naturally, smooth motion"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    jobs_url = args.jobs_url.rstrip("/")
    if "VPS_TAILSCALE_IP" in jobs_url or "<" in jobs_url:
        raise SystemExit("Set --jobs-url or PIPELINE_JOBS_URL to the VPS Tailscale URL")
    with tempfile.TemporaryDirectory(prefix="pipeline-test-") as raw_dir:
        directory = Path(raw_dir)
        audio = directory / "fixture.wav"
        clip = directory / "fixture.mp4"
        jobs: list[str] = []
        submitted_at: dict[str, float] = {}

        if not args.visual_only:
            make_wav(audio)
            make_clip(clip, args.ffmpeg)
            classic_jobs = [
                submit(
                    jobs_url,
                    "tts",
                    {
                        "text": (
                            "The DGX Spark media pipeline is online and ready "
                            "for the remote Hermes agent."
                        )
                    },
                    curl=args.curl,
                ),
                submit(
                    jobs_url,
                    "transcribe",
                    {},
                    curl=args.curl,
                    input_files={"audio": audio},
                ),
                submit(
                    jobs_url,
                    "assemble",
                    {
                        "captions": [
                            {
                                "start": 0.0,
                                "end": 2.8,
                                "text": "DGX pipeline integration test",
                            }
                        ],
                    },
                    curl=args.curl,
                    input_files={"clip_1": clip, "voiceover": audio},
                ),
            ]
            jobs.extend(classic_jobs)
            submitted_at.update({job_id: time.monotonic() for job_id in classic_jobs})

        if args.visual or args.visual_only:
            start_frame = directory / "start.png"
            end_frame = directory / "end.png"
            make_frame(start_frame, args.ffmpeg, "0x172554", "START FRAME")
            video_inputs = {"start_frame": start_frame}
            if args.video_input_frames == 2:
                make_frame(end_frame, args.ffmpeg, "0x6d214f", "END FRAME")
                video_inputs["end_frame"] = end_frame
            visual_jobs = [
                submit(
                    jobs_url,
                    "frame",
                    {"prompt": args.frame_prompt, "aspect": "9:16"},
                    curl=args.curl,
                ),
                submit(
                    jobs_url,
                    "video",
                    {
                        "prompt": args.video_prompt,
                        "duration_seconds": (
                            args.video_frame_count - 1
                        ) / 16,
                        "frame_count": args.video_frame_count,
                        "fps": 16,
                        "aspect_ratio": "9:16",
                    },
                    curl=args.curl,
                    input_files=video_inputs,
                ),
            ]
            jobs.extend(visual_jobs)
            submitted_at.update({job_id: time.monotonic() for job_id in visual_jobs})

        if not jobs:
            raise SystemExit("No jobs selected")
        print(
            json.dumps(
                {
                    "submitted": jobs,
                    "visual_variant": (
                        "first_last"
                        if (args.visual or args.visual_only)
                        and args.video_input_frames == 2
                        else "i2v"
                        if args.visual or args.visual_only
                        else None
                    ),
                },
                indent=2,
            ),
            flush=True,
        )
        if args.wait:
            wait_for_jobs(jobs_url, jobs, args.timeout, submitted_at)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
