#!/usr/bin/env python3
"""Single-job DGX media worker with a Tailscale-only health endpoint."""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import http.client
import http.server
import ipaddress
import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any

LOG = logging.getLogger("pipeline-worker")
SUPPORTED_JOBS = ("tts", "transcribe", "assemble", "frame", "video")
VISUAL_WORKFLOWS = (
    "flux2_klein_frame_api.json",
    "wan22_i2v_api.json",
    "wan22_first_last_api.json",
)


class PipelineError(RuntimeError):
    """A job or service failed in an expected, reportable way."""


class NoJobAvailable(Exception):
    """The queue has no work for this worker."""


@dataclasses.dataclass(frozen=True)
class Config:
    jobs_url: str
    worker_id: str
    poll_interval: float
    request_timeout: float
    inference_timeout: float
    health_host: str
    health_port: int
    work_dir: Path
    asset_dir: Path
    output_dir: Path
    max_download_bytes: int
    docker_bin: str
    curl_bin: str
    ffmpeg_bin: str
    ffprobe_bin: str
    f5tts_container: str
    whisper_container: str
    ollama_url: str
    comfyui_url: str
    comfyui_timeout: float
    comfyui_input_dir: Path
    comfyui_output_dir: Path
    comfyui_workflow_dir: Path
    visual_min_available_gb: float
    default_ref_audio: str
    default_ref_text: str

    @classmethod
    def from_env(cls) -> "Config":
        jobs_url = os.getenv(
            "PIPELINE_JOBS_URL", "http://VPS_TAILSCALE_IP:8787/jobs"
        ).rstrip("/")
        hostname = socket.gethostname().split(".", 1)[0]
        config = cls(
            jobs_url=jobs_url,
            worker_id=os.getenv("PIPELINE_WORKER_ID", f"{hostname}-dgx"),
            poll_interval=float(os.getenv("PIPELINE_POLL_INTERVAL", "5")),
            request_timeout=float(os.getenv("PIPELINE_HTTP_TIMEOUT", "30")),
            inference_timeout=float(os.getenv("PIPELINE_INFERENCE_TIMEOUT", "900")),
            health_host=os.getenv("PIPELINE_HEALTH_HOST", "100.103.129.82"),
            health_port=int(os.getenv("PIPELINE_HEALTH_PORT", "8788")),
            work_dir=Path(
                os.getenv("PIPELINE_WORK_DIR", "/var/lib/pipeline-worker/jobs")
            ),
            asset_dir=Path(
                os.getenv(
                    "PIPELINE_ASSET_DIR", "/srv/ai/assets/pipeline-worker"
                )
            ),
            output_dir=Path(
                os.getenv("PIPELINE_OUTPUT_DIR", "/srv/ai/outputs")
            ),
            max_download_bytes=int(
                os.getenv("PIPELINE_MAX_DOWNLOAD_BYTES", str(2 * 1024**3))
            ),
            docker_bin=os.getenv("DOCKER_BIN", "/usr/bin/docker"),
            curl_bin=os.getenv("CURL_BIN", "/usr/bin/curl"),
            ffmpeg_bin=os.getenv(
                "FFMPEG_BIN",
                "/home/xxfactionsxx/pinokio/bin/ffmpeg-env/bin/ffmpeg",
            ),
            ffprobe_bin=os.getenv(
                "FFPROBE_BIN",
                "/home/xxfactionsxx/pinokio/bin/ffmpeg-env/bin/ffprobe",
            ),
            f5tts_container=os.getenv("F5TTS_CONTAINER", "ai-f5tts"),
            whisper_container=os.getenv("WHISPER_CONTAINER", "ai-whisper"),
            ollama_url=os.getenv(
                "OLLAMA_BASE_URL", "http://100.103.129.82:11434"
            ).rstrip("/"),
            comfyui_url=os.getenv(
                "COMFYUI_URL", "http://127.0.0.1:8188"
            ).rstrip("/"),
            comfyui_timeout=float(os.getenv("COMFYUI_TIMEOUT", "3600")),
            comfyui_input_dir=Path(
                os.getenv(
                    "COMFYUI_INPUT_DIR", "/srv/ai/outputs/comfyui/input"
                )
            ),
            comfyui_output_dir=Path(
                os.getenv(
                    "COMFYUI_OUTPUT_DIR", "/srv/ai/outputs/comfyui/output"
                )
            ),
            comfyui_workflow_dir=Path(
                os.getenv(
                    "COMFYUI_WORKFLOW_DIR",
                    str(Path(__file__).resolve().parent / "workflows"),
                )
            ),
            visual_min_available_gb=float(
                os.getenv("VISUAL_MIN_AVAILABLE_GB", "40")
            ),
            default_ref_audio=os.getenv(
                "F5TTS_DEFAULT_REF_AUDIO",
                (
                    "/srv/ai/assets/voice_profiles/"
                    "decec4bc-1a6a-4eba-a116-74c236a4910b.wav"
                ),
            ),
            default_ref_text=os.getenv(
                "F5TTS_DEFAULT_REF_TEXT",
                (
                    "Jackson not only sounds great, he also communicates "
                    "with feeling and intelligence."
                ),
            ),
        )
        config.validate()
        return config

    def validate(self) -> None:
        try:
            bind_ip = ipaddress.ip_address(self.health_host)
        except ValueError as exc:
            raise PipelineError(
                f"PIPELINE_HEALTH_HOST must be a literal IP, got {self.health_host!r}"
            ) from exc
        if bind_ip.is_unspecified:
            raise PipelineError("Refusing wildcard health bind; use the Tailscale IP")
        if self.health_port < 1 or self.health_port > 65535:
            raise PipelineError("PIPELINE_HEALTH_PORT is outside 1..65535")
        parsed = urllib.parse.urlparse(self.jobs_url)
        if parsed.scheme not in {"http", "https"}:
            raise PipelineError("PIPELINE_JOBS_URL must use http:// or https://")
        if not parsed.path.endswith("/jobs"):
            raise PipelineError("PIPELINE_JOBS_URL must end in /jobs")
        comfyui = urllib.parse.urlparse(self.comfyui_url)
        if comfyui.scheme != "http" or comfyui.hostname not in {
            "127.0.0.1",
            "localhost",
            "::1",
        }:
            raise PipelineError("COMFYUI_URL must be a loopback http:// URL")
        if self.comfyui_timeout <= 0:
            raise PipelineError("COMFYUI_TIMEOUT must be positive")
        if self.visual_min_available_gb <= 0:
            raise PipelineError("VISUAL_MIN_AVAILABLE_GB must be positive")

    @property
    def configured(self) -> bool:
        return (
            "VPS_TAILSCALE_IP" not in self.jobs_url
            and "<" not in self.jobs_url
            and ">" not in self.jobs_url
        )


@dataclasses.dataclass
class WorkerState:
    started_at: str = dataclasses.field(
        default_factory=lambda: dt.datetime.now(dt.UTC).isoformat()
    )
    current_job_id: str | None = None
    current_job_type: str | None = None
    current_attempt: int | None = None
    jobs_completed: int = 0
    jobs_failed: int = 0
    last_success_at: str | None = None
    last_error: str | None = None
    last_queue_depth: int | None = None


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def compact_error(exc: BaseException, limit: int = 1200) -> str:
    text = f"{type(exc).__name__}: {exc}"
    return text if len(text) <= limit else text[: limit - 3] + "..."


def json_request(
    method: str,
    url: str,
    payload: Any | None = None,
    *,
    timeout: float = 30,
) -> tuple[int, Any | None]:
    data = None
    headers = {"Accept": "application/json", "User-Agent": "dgx-pipeline-worker/0.1"}
    if payload is not None:
        data = json.dumps(payload, separators=(",", ":")).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            if not body:
                return response.status, None
            try:
                return response.status, json.loads(body)
            except json.JSONDecodeError as exc:
                raise PipelineError(
                    f"{method} {url} returned non-JSON data"
                ) from exc
    except urllib.error.HTTPError as exc:
        body = exc.read(4096).decode("utf-8", "replace")
        raise PipelineError(
            f"{method} {url} returned HTTP {exc.code}: {body}"
        ) from exc
    except (
        urllib.error.URLError,
        http.client.HTTPException,
        TimeoutError,
        OSError,
        ValueError,
    ) as exc:
        raise PipelineError(f"{method} {url} failed: {exc}") from exc


def run_command(
    args: list[str],
    *,
    timeout: float,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    LOG.debug("Running: %s", " ".join(args))
    try:
        result = subprocess.run(
            args,
            cwd=str(cwd) if cwd else None,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PipelineError(f"Command failed to run: {args[0]}: {exc}") from exc
    if result.returncode != 0:
        stderr = result.stderr.strip()
        stdout = result.stdout.strip()
        detail = stderr or stdout or f"exit status {result.returncode}"
        raise PipelineError(f"{Path(args[0]).name} failed: {detail[-4000:]}")
    return result


class QueueClient:
    def __init__(self, config: Config) -> None:
        self.config = config

    def claim(self) -> dict[str, Any]:
        query = urllib.parse.urlencode({"status": "pending", "limit": 100})
        _, body = json_request(
            "GET",
            f"{self.config.jobs_url}?{query}",
            timeout=self.config.request_timeout,
        )
        if body in (None, {}, []):
            raise NoJobAvailable
        if not isinstance(body, list):
            raise PipelineError("Pending-jobs response was not a list")
        for pending in body:
            if not isinstance(pending, dict):
                continue
            job_type = str(pending.get("type") or pending.get("job_type") or "").lower()
            if job_type not in SUPPORTED_JOBS:
                continue
            job_id = pending.get("id") or pending.get("job_id")
            if not job_id:
                continue
            try:
                _, claimed = json_request(
                    "POST",
                    (
                        f"{self.config.jobs_url}/"
                        f"{urllib.parse.quote(str(job_id))}/claim"
                    ),
                    {"worker_id": self.config.worker_id},
                    timeout=self.config.request_timeout,
                )
            except PipelineError as exc:
                if "HTTP 409:" in str(exc):
                    continue
                raise
            if not isinstance(claimed, dict) or not claimed.get("claim_token"):
                raise PipelineError("Claim response is missing claim_token")
            return claimed
        raise NoJobAvailable

    def queue_depth(self) -> int | None:
        candidates = (
            f"{self.config.jobs_url.removesuffix('/jobs')}/health",
            f"{self.config.jobs_url}/stats",
        )
        for url in candidates:
            try:
                _, body = json_request("GET", url, timeout=min(3, self.config.request_timeout))
            except PipelineError as exc:
                if "HTTP 404:" in str(exc) or "HTTP 405:" in str(exc):
                    continue
                return None
            depth = self._parse_depth(body)
            if depth is not None:
                return depth
        return None

    @staticmethod
    def _parse_depth(body: Any) -> int | None:
        if not isinstance(body, dict):
            return None
        for source in (body, body.get("stats"), body.get("queue")):
            if not isinstance(source, dict):
                continue
            for key in ("queue_depth", "queued", "pending", "depth"):
                try:
                    return int(source[key])
                except (KeyError, TypeError, ValueError):
                    pass
            try:
                return int(source["pending_jobs"])
            except (KeyError, TypeError, ValueError):
                pass
        return None

    def complete(
        self,
        job_id: str,
        artifact: Path,
        result: dict[str, Any],
        claim_token: str,
    ) -> None:
        complete_url = f"{self.config.jobs_url}/{urllib.parse.quote(job_id)}/complete"
        media_type = str(result.get("media_type") or "application/octet-stream")
        args = [
            self.config.curl_bin,
            "--fail-with-body",
            "--silent",
            "--show-error",
            "--max-time",
            str(int(self.config.inference_timeout)),
            "--request",
            "POST",
            "--form",
            f"file=@{artifact};type={media_type}",
            "--form",
            f"claim_token={claim_token}",
            "--form",
            "status=done",
            "--form",
            f"result={json.dumps(result, separators=(',', ':'))}",
            complete_url,
        ]
        run_command(args, timeout=self.config.inference_timeout + 10)

    def fail(
        self,
        job_id: str,
        error: str,
        attempts: int,
        claim_token: str,
    ) -> None:
        url = f"{self.config.jobs_url}/{urllib.parse.quote(job_id)}/complete"
        json_request(
            "POST",
            url,
            {
                "claim_token": claim_token,
                "status": "failed",
                "error": error,
                "attempts": attempts,
                "result": {"worker_attempts": attempts},
            },
            timeout=self.config.request_timeout,
        )


class DockerService:
    def __init__(self, config: Config, container: str) -> None:
        self.config = config
        self.container = container

    def _inspect(self, template: str) -> str:
        result = run_command(
            [
                self.config.docker_bin,
                "inspect",
                "--format",
                template,
                self.container,
            ],
            timeout=20,
        )
        return result.stdout.strip()

    def _network_ip(self) -> str:
        raw = self._inspect(
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
        )
        try:
            address = ipaddress.ip_address(raw)
        except ValueError as exc:
            raise PipelineError(
                f"{self.container} has no valid Docker network IP: {raw!r}"
            ) from exc
        if address.is_unspecified:
            raise PipelineError(f"{self.container} has no Docker network IP")
        return str(address)

    def ensure_started(self, ready_path: str, timeout: float) -> str:
        running = self._inspect("{{.State.Running}}") == "true"
        if not running:
            LOG.info("Starting container %s", self.container)
            run_command(
                [self.config.docker_bin, "start", self.container],
                timeout=60,
            )
        deadline = time.monotonic() + timeout
        last_error = "not checked"
        while time.monotonic() < deadline:
            try:
                ip = self._network_ip()
                url = f"http://{ip}:8000{ready_path}"
                _, _ = json_request("GET", url, timeout=5)
                return f"http://{ip}:8000"
            except PipelineError as exc:
                last_error = str(exc)
                time.sleep(3)
        raise PipelineError(
            f"{self.container} did not become ready within {timeout:.0f}s: {last_error}"
        )

    def probe(self, path: str) -> dict[str, Any]:
        try:
            if self._inspect("{{.State.Running}}") != "true":
                return {"container": self.container, "running": False}
            ip = self._network_ip()
            _, body = json_request("GET", f"http://{ip}:8000{path}", timeout=2)
            result = body if isinstance(body, dict) else {}
            return {"container": self.container, "running": True, **result}
        except Exception as exc:
            return {
                "container": self.container,
                "running": False,
                "error": compact_error(exc, 300),
            }


class ComfyUIClient:
    def __init__(self, config: Config) -> None:
        self.config = config

    def probe(self) -> dict[str, Any]:
        try:
            _, body = json_request(
                "GET",
                f"{self.config.comfyui_url}/system_stats",
                timeout=2,
            )
        except PipelineError as exc:
            return {"ready": False, "error": compact_error(exc, 300)}
        system = body.get("system", {}) if isinstance(body, dict) else {}
        return {
            "ready": True,
            "url": self.config.comfyui_url,
            "version": system.get("comfyui_version"),
        }

    def load_workflow(self, filename: str) -> dict[str, Any]:
        if filename not in VISUAL_WORKFLOWS:
            raise PipelineError(f"Visual workflow is not allowlisted: {filename}")
        workflow_root = self.config.comfyui_workflow_dir.resolve()
        path = (workflow_root / filename).resolve()
        if workflow_root not in path.parents or not path.is_file():
            raise PipelineError(f"Visual workflow is missing: {path}")
        try:
            graph = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(f"Cannot load visual workflow {filename}: {exc}") from exc
        if not isinstance(graph, dict) or not graph:
            raise PipelineError(f"Visual workflow is not an API prompt graph: {filename}")
        for node_id, node in graph.items():
            if not isinstance(node, dict) or not node.get("class_type"):
                raise PipelineError(
                    f"Visual workflow {filename} has invalid node {node_id}"
                )
            if not isinstance(node.get("inputs"), dict):
                raise PipelineError(
                    f"Visual workflow {filename} node {node_id} has invalid inputs"
                )
        return graph

    def run(
        self,
        graph: dict[str, Any],
        *,
        expected_suffixes: tuple[str, ...],
    ) -> tuple[Path, str]:
        _, response = json_request(
            "POST",
            f"{self.config.comfyui_url}/prompt",
            {"prompt": graph, "client_id": str(uuid.uuid4())},
            timeout=self.config.request_timeout,
        )
        if not isinstance(response, dict) or not response.get("prompt_id"):
            detail = response.get("node_errors") if isinstance(response, dict) else response
            raise PipelineError(f"ComfyUI rejected the workflow: {detail}")
        prompt_id = str(response["prompt_id"])
        deadline = time.monotonic() + self.config.comfyui_timeout
        while time.monotonic() < deadline:
            _, history = json_request(
                "GET",
                f"{self.config.comfyui_url}/history/{urllib.parse.quote(prompt_id)}",
                timeout=self.config.request_timeout,
            )
            record = history.get(prompt_id) if isinstance(history, dict) else None
            if isinstance(record, dict):
                outputs = record.get("outputs")
                if isinstance(outputs, dict) and outputs:
                    artifact = self._artifact_from_outputs(
                        outputs, expected_suffixes=expected_suffixes
                    )
                    return artifact, prompt_id
                status = record.get("status")
                if isinstance(status, dict) and status.get("completed"):
                    raise PipelineError(
                        "ComfyUI completed without the expected output: "
                        f"{self._status_error(status)}"
                    )
            time.sleep(2)
        raise PipelineError(
            f"ComfyUI prompt {prompt_id} timed out after "
            f"{self.config.comfyui_timeout:.0f}s"
        )

    def _artifact_from_outputs(
        self,
        outputs: dict[str, Any],
        *,
        expected_suffixes: tuple[str, ...],
    ) -> Path:
        candidates: list[dict[str, Any]] = []

        def collect(value: Any) -> None:
            if isinstance(value, dict):
                if isinstance(value.get("filename"), str):
                    candidates.append(value)
                for nested in value.values():
                    collect(nested)
            elif isinstance(value, list):
                for nested in value:
                    collect(nested)

        collect(outputs)
        root = self.config.comfyui_output_dir.resolve()
        for candidate in reversed(candidates):
            if str(candidate.get("type") or "output") != "output":
                continue
            filename = str(candidate["filename"])
            subfolder = str(candidate.get("subfolder") or "")
            path = (root / subfolder / filename).resolve()
            if path != root and root not in path.parents:
                continue
            if path.suffix.lower() not in expected_suffixes:
                continue
            if path.is_file():
                return path
        raise PipelineError(
            "ComfyUI history did not reference an existing "
            f"{'/'.join(expected_suffixes)} artifact"
        )

    @staticmethod
    def _status_error(status: dict[str, Any]) -> str:
        messages = status.get("messages")
        if isinstance(messages, list):
            for message in reversed(messages):
                if (
                    isinstance(message, list)
                    and len(message) > 1
                    and isinstance(message[1], dict)
                ):
                    detail = message[1].get("exception_message")
                    if detail:
                        return str(detail)
                if isinstance(message, str):
                    return message
        return str(status.get("status_str") or "no output metadata")


class JobProcessor:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.f5tts = DockerService(config, config.f5tts_container)
        self.whisper = DockerService(config, config.whisper_container)
        self.comfyui = ComfyUIClient(config)

    def process(self, job: dict[str, Any], work_dir: Path) -> tuple[Path, dict[str, Any]]:
        job_type = str(job["type"]).lower()
        payload = job.get("payload") or job.get("input") or {}
        if not isinstance(payload, dict):
            raise PipelineError("Job payload/input must be an object")
        payload = dict(payload)
        self._inject_queue_inputs(job_type, payload, job.get("input_files"))
        if job_type == "tts":
            return self.tts(payload, work_dir)
        if job_type == "transcribe":
            return self.transcribe(payload, work_dir)
        if job_type == "assemble":
            return self.assemble(payload, work_dir)
        if job_type == "frame":
            return self.frame(payload, work_dir)
        if job_type == "video":
            return self.video(payload, work_dir)
        raise PipelineError(f"Unsupported job type: {job_type}")

    @staticmethod
    def _inject_queue_inputs(
        job_type: str,
        payload: dict[str, Any],
        input_files: Any,
    ) -> None:
        if not isinstance(input_files, list):
            return
        by_role: dict[str, str] = {}
        for item in input_files:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "")
            url = item.get("download_url") or item.get("url")
            if role and isinstance(url, str) and url:
                by_role[role] = url

        if job_type == "video" and not any(
            payload.get(key) is not None
            for key in (
                "frame",
                "frames",
                "frame_url",
                "start_frame",
                "start_frame_url",
            )
        ):
            start = (
                by_role.get("start_frame")
                or by_role.get("frame")
                or by_role.get("start")
            )
            end = by_role.get("end_frame") or by_role.get("end")
            if start:
                payload["frames"] = [start, end] if end else [start]
        elif job_type == "transcribe" and not (
            payload.get("audio_url") or payload.get("url")
        ):
            if by_role.get("audio"):
                payload["audio_url"] = by_role["audio"]
        elif job_type == "assemble":
            if not (payload.get("voiceover_url") or payload.get("vo_url")):
                if by_role.get("voiceover"):
                    payload["voiceover_url"] = by_role["voiceover"]
            if not (payload.get("clips") or payload.get("clip_urls")):
                clips = [
                    url
                    for role, url in sorted(by_role.items())
                    if role.startswith("clip")
                ]
                if clips:
                    payload["clips"] = clips
            if not payload.get("captions_url") and by_role.get("captions"):
                payload["captions_url"] = by_role["captions"]
        elif job_type == "tts" and not (
            payload.get("ref_audio_url") or payload.get("ref_audio_path")
        ):
            if by_role.get("ref_audio"):
                payload["ref_audio_url"] = by_role["ref_audio"]

    def download(self, url: str, destination: Path) -> Path:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise PipelineError(f"Input URL must use http or https: {url!r}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        request = urllib.request.Request(
            url, headers={"User-Agent": "dgx-pipeline-worker/0.1"}
        )
        total = 0
        try:
            with urllib.request.urlopen(
                request, timeout=self.config.request_timeout
            ) as response, destination.open("wb") as output:
                declared = response.headers.get("Content-Length")
                if declared and int(declared) > self.config.max_download_bytes:
                    raise PipelineError(f"Input exceeds download limit: {url}")
                while chunk := response.read(1024 * 1024):
                    total += len(chunk)
                    if total > self.config.max_download_bytes:
                        raise PipelineError(f"Input exceeds download limit: {url}")
                    output.write(chunk)
        except (urllib.error.URLError, OSError, ValueError) as exc:
            destination.unlink(missing_ok=True)
            raise PipelineError(f"Download failed for {url}: {exc}") from exc
        return destination

    @staticmethod
    def _safe_existing_path(raw: str, root: Path) -> Path:
        path = Path(raw).expanduser().resolve()
        resolved_root = root.resolve()
        if path != resolved_root and resolved_root not in path.parents:
            raise PipelineError(f"Local path must be under {resolved_root}")
        if not path.is_file():
            raise PipelineError(f"Local input does not exist: {path}")
        return path

    def _container_asset_path(self, host_path: Path) -> str:
        asset_root = Path("/srv/ai/assets").resolve()
        resolved = host_path.resolve()
        if asset_root not in resolved.parents:
            raise PipelineError(f"F5 reference must be under {asset_root}")
        relative = resolved.relative_to(asset_root)
        return str(Path("/app/data/assets") / relative)

    @staticmethod
    def _prompt(payload: dict[str, Any], job_type: str) -> str:
        prompt = str(payload.get("prompt") or "").strip()
        if not prompt:
            raise PipelineError(f"{job_type} payload requires a non-empty prompt")
        if len(prompt) > 20_000:
            raise PipelineError(f"{job_type} prompt exceeds 20,000 characters")
        return prompt

    @staticmethod
    def _seed(payload: dict[str, Any]) -> int:
        raw = payload.get("seed")
        if raw is None:
            return uuid.uuid4().int & ((1 << 63) - 1)
        if isinstance(raw, bool):
            raise PipelineError("seed must be an integer")
        try:
            seed = int(raw)
        except (TypeError, ValueError) as exc:
            raise PipelineError("seed must be an integer") from exc
        if not 0 <= seed <= 18_446_744_073_709_551_615:
            raise PipelineError("seed is outside the unsigned 64-bit range")
        return seed

    def _require_visual_headroom(self) -> float:
        try:
            for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
                if line.startswith("MemAvailable:"):
                    available_gb = int(line.split()[1]) / 1024**2
                    break
            else:
                raise ValueError("MemAvailable is missing")
        except (OSError, ValueError, IndexError) as exc:
            raise PipelineError(f"Cannot read UMA memory headroom: {exc}") from exc
        minimum = self.config.visual_min_available_gb
        if available_gb < minimum:
            raise PipelineError(
                f"Visual job requires {minimum:.1f} GiB MemAvailable; "
                f"only {available_gb:.1f} GiB is available"
            )
        return round(available_gb, 2)

    @staticmethod
    def _frame_url(value: Any, label: str) -> str:
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            for key in ("url", "download_url", "file_url"):
                nested = value.get(key)
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()
            for key in ("artifact", "file"):
                nested = value.get(key)
                if isinstance(nested, dict):
                    try:
                        return JobProcessor._frame_url(nested, label)
                    except PipelineError:
                        pass
        raise PipelineError(f"{label} must be an http(s) URL or file object")

    @classmethod
    def _video_frame_urls(cls, payload: dict[str, Any]) -> list[str]:
        plural = payload.get("frames")
        singular = payload.get("frame")
        if plural is not None and singular is not None:
            raise PipelineError("video payload cannot contain both frame and frames")

        raw_frames: list[Any]
        if plural is not None:
            if not isinstance(plural, list):
                raise PipelineError("video frames must be a list of one or two URLs")
            raw_frames = plural
        elif singular is not None:
            raw_frames = singular if isinstance(singular, list) else [singular]
        else:
            start_values = [
                payload[key]
                for key in ("start_frame", "start_frame_url", "frame_url")
                if payload.get(key) is not None
            ]
            if len(start_values) > 1:
                raise PipelineError("video payload has multiple start-frame aliases")
            raw_frames = start_values

        end_values = [
            payload[key]
            for key in ("end_frame", "end_frame_url")
            if payload.get(key) is not None
        ]
        if len(end_values) > 1:
            raise PipelineError("video payload has multiple end-frame aliases")
        if end_values:
            if len(raw_frames) != 1:
                raise PipelineError(
                    "end_frame/end_frame_url requires exactly one start frame"
                )
            raw_frames.append(end_values[0])

        if len(raw_frames) not in {1, 2}:
            raise PipelineError("video payload requires exactly one or two input frames")
        return [
            cls._frame_url(value, f"video frame {index}")
            for index, value in enumerate(raw_frames, 1)
        ]

    def _stage_comfy_frame(
        self,
        url: str,
        work_dir: Path,
        label: str,
    ) -> tuple[Path, str]:
        suffix = Path(urllib.parse.urlparse(url).path).suffix.lower()
        if suffix not in {".png", ".jpg", ".jpeg", ".webp", ".bmp", ".tiff"}:
            suffix = ".png"
        downloaded = self.download(url, work_dir / f"{label}{suffix}")
        self.config.comfyui_input_dir.mkdir(parents=True, exist_ok=True)
        staged_name = f"content-factory-{uuid.uuid4().hex}{suffix}"
        staged = self.config.comfyui_input_dir / staged_name
        try:
            shutil.copy2(downloaded, staged)
        except OSError as exc:
            raise PipelineError(f"Cannot stage ComfyUI input frame: {exc}") from exc
        return staged, staged_name

    def frame(
        self, payload: dict[str, Any], work_dir: Path
    ) -> tuple[Path, dict[str, Any]]:
        prompt = self._prompt(payload, "frame")
        aspect = str(payload.get("aspect") or "9:16")
        if aspect != "9:16":
            raise PipelineError("frame currently supports only aspect='9:16'")
        available_gb = self._require_visual_headroom()
        seed = self._seed(payload)
        graph = self.comfyui.load_workflow("flux2_klein_frame_api.json")
        graph["3"]["inputs"]["text"] = prompt
        graph["7"]["inputs"]["noise_seed"] = seed
        graph["16"]["inputs"]["filename_prefix"] = (
            f"content-factory/frame-{uuid.uuid4().hex}"
        )
        started = time.monotonic()
        output, prompt_id = self.comfyui.run(
            graph, expected_suffixes=(".png", ".jpg", ".jpeg", ".webp")
        )
        processing_time = round(time.monotonic() - started, 3)
        width, height = self._dimensions(output)
        if (width, height) != (1080, 1920):
            raise PipelineError(
                f"frame workflow returned {width}x{height}; expected 1080x1920"
            )
        return output, {
            "type": "frame",
            "media_type": "image/png",
            "filename": output.name,
            "width": width,
            "height": height,
            "aspect": aspect,
            "model": "FLUX.2-klein-4b-fp8",
            "seed": seed,
            "comfyui_prompt_id": prompt_id,
            "processing_time": processing_time,
            "mem_available_gb_at_start": available_gb,
        }

    def video(
        self, payload: dict[str, Any], work_dir: Path
    ) -> tuple[Path, dict[str, Any]]:
        prompt = self._prompt(payload, "video")
        aspect = str(payload.get("aspect") or "9:16")
        if aspect != "9:16":
            raise PipelineError("video currently supports only aspect='9:16'")
        try:
            seconds = float(payload.get("seconds", 5))
        except (TypeError, ValueError) as exc:
            raise PipelineError("video seconds must be a number") from exc
        if not 1 <= seconds <= 10:
            raise PipelineError("video seconds must be between 1 and 10")
        frame_urls = self._video_frame_urls(payload)
        available_gb = self._require_visual_headroom()
        seed = self._seed(payload)
        fps = 16
        frame_count = round(seconds * fps / 4) * 4 + 1
        nominal_seconds = frame_count / fps
        first_last = len(frame_urls) == 2
        workflow_name = (
            "wan22_first_last_api.json" if first_last else "wan22_i2v_api.json"
        )
        graph = self.comfyui.load_workflow(workflow_name)
        staged: list[Path] = []
        try:
            start_path, start_name = self._stage_comfy_frame(
                frame_urls[0], work_dir, "start-frame"
            )
            staged.append(start_path)
            graph["2"]["inputs"]["text"] = prompt
            if first_last:
                end_path, end_name = self._stage_comfy_frame(
                    frame_urls[1], work_dir, "end-frame"
                )
                staged.append(end_path)
                graph["5"]["inputs"]["image"] = start_name
                graph["6"]["inputs"]["image"] = end_name
                graph["7"]["inputs"]["length"] = frame_count
                graph["11"]["inputs"]["noise_seed"] = seed
                graph["18"]["inputs"]["filename_prefix"] = (
                    f"content-factory/video-{uuid.uuid4().hex}"
                )
            else:
                graph["5"]["inputs"]["image"] = start_name
                graph["6"]["inputs"]["length"] = frame_count
                graph["10"]["inputs"]["noise_seed"] = seed
                graph["17"]["inputs"]["filename_prefix"] = (
                    f"content-factory/video-{uuid.uuid4().hex}"
                )
            started = time.monotonic()
            output, prompt_id = self.comfyui.run(
                graph, expected_suffixes=(".mp4",)
            )
        finally:
            for path in staged:
                path.unlink(missing_ok=True)
        processing_time = round(time.monotonic() - started, 3)
        width, height = self._dimensions(output)
        measured_duration = self._duration(output)
        return output, {
            "type": "video",
            "media_type": "video/mp4",
            "filename": output.name,
            "width": width,
            "height": height,
            "aspect": aspect,
            "model": "Wan-2.2-I2V-A14B-fp8",
            "workflow_variant": "first_last" if first_last else "i2v",
            "input_frame_count": len(frame_urls),
            "fps": fps,
            "frame_count": frame_count,
            "requested_seconds": seconds,
            "duration": measured_duration or nominal_seconds,
            "seed": seed,
            "comfyui_prompt_id": prompt_id,
            "processing_time": processing_time,
            "mem_available_gb_at_start": available_gb,
        }

    def tts(
        self, payload: dict[str, Any], work_dir: Path
    ) -> tuple[Path, dict[str, Any]]:
        text = str(payload.get("text") or "").strip()
        if not text:
            raise PipelineError("tts payload requires non-empty text")
        if len(text) > 20_000:
            raise PipelineError("tts text exceeds 20,000 characters")
        ref_audio_url = payload.get("ref_audio_url")
        ref_audio_path = payload.get("ref_audio_path")
        if ref_audio_url:
            suffix = Path(urllib.parse.urlparse(str(ref_audio_url)).path).suffix or ".wav"
            host_ref = self.download(
                str(ref_audio_url), self.config.asset_dir / f"{uuid.uuid4()}{suffix}"
            )
        elif ref_audio_path:
            host_ref = self._safe_existing_path(str(ref_audio_path), Path("/srv/ai/assets"))
        else:
            host_ref = self._safe_existing_path(
                self.config.default_ref_audio, Path("/srv/ai/assets")
            )
        ref_text = str(payload.get("ref_text") or self.config.default_ref_text)
        speed = float(payload.get("speed", 1.0))
        if not 0.5 <= speed <= 2.0:
            raise PipelineError("tts speed must be between 0.5 and 2.0")
        base_url = self.f5tts.ensure_started("/healthz", min(180, self.config.inference_timeout))
        request = {
            "text": text,
            "ref_audio_path": self._container_asset_path(host_ref),
            "ref_text": ref_text,
            "speed": speed,
        }
        _, body = json_request(
            "POST",
            f"{base_url}/synthesize",
            request,
            timeout=self.config.inference_timeout,
        )
        if not isinstance(body, dict) or not body.get("output_path"):
            raise PipelineError("F5-TTS returned no output_path")
        output = (self.config.output_dir / str(body["output_path"])).resolve()
        if not output.is_file():
            raise PipelineError(f"F5-TTS output is missing: {output}")
        result = {
            "type": "tts",
            "media_type": "audio/wav",
            "filename": output.name,
            "sample_rate": body.get("sample_rate"),
            "duration": body.get("duration"),
            "processing_time": body.get("processing_time"),
        }
        return output, result

    def transcribe(
        self, payload: dict[str, Any], work_dir: Path
    ) -> tuple[Path, dict[str, Any]]:
        audio_url = payload.get("audio_url") or payload.get("url")
        if not audio_url:
            raise PipelineError("transcribe payload requires audio_url")
        suffix = Path(urllib.parse.urlparse(str(audio_url)).path).suffix or ".wav"
        audio_path = self.download(str(audio_url), work_dir / f"input{suffix}")
        base_url = self.whisper.ensure_started(
            "/readyz", min(600, self.config.inference_timeout)
        )
        args = [
            self.config.curl_bin,
            "--fail-with-body",
            "--silent",
            "--show-error",
            "--max-time",
            str(int(self.config.inference_timeout)),
            "--request",
            "POST",
            "--form",
            f"audio=@{audio_path}",
            f"{base_url}/transcribe",
        ]
        response = run_command(args, timeout=self.config.inference_timeout + 10)
        try:
            body = json.loads(response.stdout)
        except json.JSONDecodeError as exc:
            raise PipelineError("Whisper returned invalid JSON") from exc
        output = work_dir / "transcription.json"
        output.write_text(
            json.dumps(body, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        result = {
            "type": "transcribe",
            "media_type": "application/json",
            "filename": output.name,
            "language": body.get("language"),
            "duration": body.get("duration"),
            "word_count": len(body.get("words") or []),
            "processing_time": body.get("processing_time"),
        }
        return output, result

    def assemble(
        self, payload: dict[str, Any], work_dir: Path
    ) -> tuple[Path, dict[str, Any]]:
        clips = payload.get("clips") or payload.get("clip_urls")
        if not isinstance(clips, list) or not clips:
            raise PipelineError("assemble payload requires a non-empty clips list")
        if len(clips) > 100:
            raise PipelineError("assemble accepts at most 100 clips")
        voiceover_url = (
            payload.get("voiceover_url")
            or payload.get("vo_url")
            or payload.get("audio_url")
        )
        if not voiceover_url:
            raise PipelineError("assemble payload requires voiceover_url/vo_url")

        normalized: list[Path] = []
        for index, clip in enumerate(clips):
            clip_url = clip.get("url") if isinstance(clip, dict) else clip
            if not clip_url:
                raise PipelineError(f"Clip {index} has no URL")
            suffix = Path(urllib.parse.urlparse(str(clip_url)).path).suffix or ".mp4"
            source = self.download(str(clip_url), work_dir / f"clip-{index:03d}{suffix}")
            target = work_dir / f"normalized-{index:03d}.mp4"
            command = [
                self.config.ffmpeg_bin,
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                str(source),
                "-vf",
                (
                    "scale=1080:1920:force_original_aspect_ratio=decrease,"
                    "pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,setsar=1,fps=30"
                ),
                "-an",
                "-c:v",
                "libx264",
                "-preset",
                str(payload.get("preset") or "medium"),
                "-crf",
                str(int(payload.get("crf", 20))),
                "-pix_fmt",
                "yuv420p",
                str(target),
            ]
            run_command(command, timeout=self.config.inference_timeout)
            normalized.append(target)

        concat_file = work_dir / "clips.txt"
        concat_lines = []
        for path in normalized:
            escaped = str(path).replace("'", "'\\''")
            concat_lines.append(f"file '{escaped}'")
        concat_file.write_text("\n".join(concat_lines) + "\n", encoding="utf-8")
        video = work_dir / "video.mp4"
        run_command(
            [
                self.config.ffmpeg_bin,
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
                str(concat_file),
                "-c",
                "copy",
                str(video),
            ],
            timeout=self.config.inference_timeout,
        )

        vo_suffix = (
            Path(urllib.parse.urlparse(str(voiceover_url)).path).suffix or ".wav"
        )
        voiceover = self.download(
            str(voiceover_url), work_dir / f"voiceover{vo_suffix}"
        )
        captions = self._load_captions(payload, work_dir)
        output = work_dir / "assembled-1080x1920.mp4"
        command = [
            self.config.ffmpeg_bin,
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(video),
            "-i",
            str(voiceover),
        ]
        if captions:
            subtitle_path = self._write_srt(captions, work_dir / "captions.srt")
            escaped = (
                str(subtitle_path)
                .replace("\\", "\\\\")
                .replace(":", "\\:")
                .replace("'", "\\'")
            )
            command += [
                "-vf",
                (
                    f"subtitles=filename='{escaped}':"
                    "force_style='Alignment=2,FontSize=18,MarginV=110,"
                    "Outline=2,Shadow=1'"
                ),
            ]
        command += [
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c:v",
            "libx264",
            "-preset",
            str(payload.get("preset") or "medium"),
            "-crf",
            str(int(payload.get("crf", 20))),
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-ar",
            "48000",
            "-shortest",
            "-movflags",
            "+faststart",
            str(output),
        ]
        run_command(command, timeout=self.config.inference_timeout)
        duration = self._duration(output)
        result = {
            "type": "assemble",
            "media_type": "video/mp4",
            "filename": output.name,
            "width": 1080,
            "height": 1920,
            "codec": "h264",
            "duration": duration,
            "caption_count": len(captions),
        }
        return output, result

    def _load_captions(
        self, payload: dict[str, Any], work_dir: Path
    ) -> list[dict[str, Any]]:
        captions: Any = payload.get("captions") or []
        captions_url = payload.get("captions_url")
        if captions_url:
            path = self.download(str(captions_url), work_dir / "captions.json")
            try:
                captions = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise PipelineError(f"Invalid captions JSON: {exc}") from exc
        if isinstance(captions, dict):
            captions = captions.get("segments") or captions.get("words") or []
        if not isinstance(captions, list):
            raise PipelineError("captions must be a list or transcription JSON object")
        normalized = []
        for item in captions:
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or item.get("word") or "").strip()
            try:
                start = float(item["start"])
                end = float(item["end"])
            except (KeyError, TypeError, ValueError):
                continue
            if text and end > start >= 0:
                normalized.append({"start": start, "end": end, "text": text})
        return normalized

    @staticmethod
    def _srt_timestamp(seconds: float) -> str:
        milliseconds = max(0, round(seconds * 1000))
        hours, remainder = divmod(milliseconds, 3_600_000)
        minutes, remainder = divmod(remainder, 60_000)
        secs, millis = divmod(remainder, 1000)
        return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"

    def _write_srt(self, captions: list[dict[str, Any]], path: Path) -> Path:
        lines: list[str] = []
        for index, item in enumerate(captions, 1):
            text = str(item["text"]).replace("\r", " ").replace("\n", " ")
            lines.extend(
                [
                    str(index),
                    (
                        f"{self._srt_timestamp(float(item['start']))} --> "
                        f"{self._srt_timestamp(float(item['end']))}"
                    ),
                    text,
                    "",
                ]
            )
        path.write_text("\n".join(lines), encoding="utf-8")
        return path

    def _duration(self, path: Path) -> float | None:
        try:
            result = run_command(
                [
                    self.config.ffprobe_bin,
                    "-v",
                    "error",
                    "-show_entries",
                    "format=duration",
                    "-of",
                    "default=noprint_wrappers=1:nokey=1",
                    str(path),
                ],
                timeout=30,
            )
            return round(float(result.stdout.strip()), 3)
        except (PipelineError, ValueError):
            return None

    def _dimensions(self, path: Path) -> tuple[int, int]:
        result = run_command(
            [
                self.config.ffprobe_bin,
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=width,height",
                "-of",
                "json",
                str(path),
            ],
            timeout=30,
        )
        try:
            streams = json.loads(result.stdout).get("streams")
            width = int(streams[0]["width"])
            height = int(streams[0]["height"])
        except (AttributeError, IndexError, KeyError, TypeError, ValueError) as exc:
            raise PipelineError(f"Cannot determine media dimensions for {path}") from exc
        return width, height


class Worker:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.queue = QueueClient(config)
        self.processor = JobProcessor(config)
        self.state = WorkerState()
        self.state_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.health_server: http.server.ThreadingHTTPServer | None = None

    def prepare(self) -> None:
        try:
            self.config.work_dir.mkdir(parents=True, exist_ok=True)
            self.config.asset_dir.mkdir(parents=True, exist_ok=True)
            self.config.output_dir.mkdir(parents=True, exist_ok=True)
            self.config.comfyui_input_dir.mkdir(parents=True, exist_ok=True)
            self.config.comfyui_output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise PipelineError(f"Cannot create worker directory: {exc}") from exc
        for workflow in VISUAL_WORKFLOWS:
            path = self.config.comfyui_workflow_dir / workflow
            if not path.is_file():
                raise PipelineError(f"Required visual workflow is missing: {path}")
        for executable in (
            self.config.docker_bin,
            self.config.curl_bin,
            self.config.ffmpeg_bin,
            self.config.ffprobe_bin,
        ):
            if not Path(executable).is_file():
                raise PipelineError(f"Required executable is missing: {executable}")

    def start_health(self) -> None:
        worker = self

        class HealthHandler(http.server.BaseHTTPRequestHandler):
            server_version = "DGXPipelineHealth/0.1"

            def do_GET(self) -> None:  # noqa: N802
                if urllib.parse.urlparse(self.path).path != "/health":
                    self.send_error(http.client.NOT_FOUND)
                    return
                body = json.dumps(worker.health_snapshot(), separators=(",", ":")).encode()
                self.send_response(http.client.OK)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, fmt: str, *args: Any) -> None:
                LOG.debug("health: " + fmt, *args)

        try:
            self.health_server = http.server.ThreadingHTTPServer(
                (self.config.health_host, self.config.health_port), HealthHandler
            )
        except OSError as exc:
            raise PipelineError(
                f"Cannot bind health endpoint to "
                f"{self.config.health_host}:{self.config.health_port}: {exc}"
            ) from exc
        thread = threading.Thread(
            target=self.health_server.serve_forever,
            name="health-server",
            daemon=True,
        )
        thread.start()
        LOG.info(
            "Health endpoint listening on http://%s:%d/health",
            self.config.health_host,
            self.config.health_port,
        )

    def stop(self) -> None:
        self.stop_event.set()
        if self.health_server:
            self.health_server.shutdown()

    def run(self, *, once: bool = False) -> int:
        self.prepare()
        self.start_health()
        if not self.config.configured:
            LOG.warning(
                "PIPELINE_JOBS_URL still contains VPS_TAILSCALE_IP; "
                "health is available but queue polling is paused"
            )
        while not self.stop_event.is_set():
            if not self.config.configured:
                if once:
                    return 0
                self.stop_event.wait(30)
                continue
            try:
                job = self.queue.claim()
                self._run_job(job)
            except NoJobAvailable:
                if once:
                    return 0
                self.stop_event.wait(self.config.poll_interval)
            except PipelineError as exc:
                error = compact_error(exc)
                with self.state_lock:
                    self.state.last_error = error
                LOG.error("Queue polling error: %s", error)
                if once:
                    return 1
                self.stop_event.wait(self.config.poll_interval)
        return 0

    def _run_job(self, job: dict[str, Any]) -> None:
        job_id = str(job.get("id") or job.get("job_id"))
        job_type = str(job["type"]).lower()
        claim_token = str(job.get("claim_token") or "")
        if not claim_token:
            raise PipelineError(f"Claimed job {job_id} is missing claim_token")
        safe_id = "".join(c if c.isalnum() or c in "-_." else "_" for c in job_id)
        job_dir = self.config.work_dir / safe_id
        if job_dir.exists():
            shutil.rmtree(job_dir)
        job_dir.mkdir(parents=True)
        with self.state_lock:
            self.state.current_job_id = job_id
            self.state.current_job_type = job_type
            self.state.last_error = None
        LOG.info("Claimed job %s (%s)", job_id, job_type)

        last_error = "unknown error"
        for attempt in range(1, 4):
            with self.state_lock:
                self.state.current_attempt = attempt
            try:
                artifact, result = self.processor.process(job, job_dir)
                result["attempts"] = attempt
                result["completed_at"] = utc_now()
                self.queue.complete(job_id, artifact, result, claim_token)
                with self.state_lock:
                    self.state.jobs_completed += 1
                    self.state.last_success_at = utc_now()
                LOG.info(
                    "Completed job %s on attempt %d: %s",
                    job_id,
                    attempt,
                    artifact,
                )
                break
            except Exception as exc:
                last_error = compact_error(exc)
                LOG.exception(
                    "Job %s attempt %d/3 failed: %s",
                    job_id,
                    attempt,
                    last_error,
                )
                if attempt < 3:
                    self.stop_event.wait(min(5 * attempt, 10))
        else:
            with self.state_lock:
                self.state.jobs_failed += 1
                self.state.last_error = last_error
            try:
                self.queue.fail(job_id, last_error, 3, claim_token)
            except PipelineError as exc:
                LOG.error(
                    "Could not report final failure for %s: %s",
                    job_id,
                    compact_error(exc),
                )
            LOG.error("Job %s failed after two retries", job_id)
        with self.state_lock:
            self.state.current_job_id = None
            self.state.current_job_type = None
            self.state.current_attempt = None

    def health_snapshot(self) -> dict[str, Any]:
        memory = self._unified_memory()
        ollama = self._ollama_models()
        f5tts = self.processor.f5tts.probe("/readyz")
        whisper = self.processor.whisper.probe("/readyz")
        comfyui = self.processor.comfyui.probe()
        queue_depth = self.queue.queue_depth() if self.config.configured else None
        with self.state_lock:
            if queue_depth is not None:
                self.state.last_queue_depth = queue_depth
            state = dataclasses.asdict(self.state)
        degraded = (
            not self.config.configured
            or queue_depth is None
            or not comfyui.get("ready")
        )
        return {
            "status": "degraded" if degraded else "ok",
            "worker_id": self.config.worker_id,
            "tailscale_bind": f"{self.config.health_host}:{self.config.health_port}",
            "gpu_memory": memory,
            "loaded_models": {
                "ollama": ollama,
                "f5tts": f5tts,
                "faster_whisper": whisper,
                "comfyui": comfyui,
            },
            "queue_depth": queue_depth,
            "queue_configured": self.config.configured,
            "worker": state,
            "checked_at": utc_now(),
        }

    @staticmethod
    def _unified_memory() -> dict[str, Any]:
        values: dict[str, int] = {}
        try:
            for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
                key, raw = line.split(":", 1)
                fields = raw.split()
                if fields:
                    values[key] = int(fields[0])
        except (OSError, ValueError):
            return {"architecture": "UMA", "source": "/proc/meminfo", "error": "unavailable"}
        total = values.get("MemTotal", 0)
        available = values.get("MemAvailable", 0)
        return {
            "architecture": "UMA",
            "source": "/proc/meminfo",
            "total_gb": round(total / 1024**2, 1),
            "available_gb": round(available / 1024**2, 1),
            "used_gb": round((total - available) / 1024**2, 1),
        }

    def _ollama_models(self) -> list[dict[str, Any]]:
        try:
            _, body = json_request("GET", f"{self.config.ollama_url}/api/ps", timeout=2)
        except PipelineError as exc:
            return [{"error": compact_error(exc, 300)}]
        models = body.get("models", []) if isinstance(body, dict) else []
        return [
            {
                "name": model.get("name"),
                "size": model.get("size"),
                "size_vram": model.get("size_vram"),
                "expires_at": model.get("expires_at"),
            }
            for model in models
            if isinstance(model, dict)
        ]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--once",
        action="store_true",
        help="Poll once, then exit after one job or an empty queue.",
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="Validate configuration and executables without binding or polling.",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=os.getenv("PIPELINE_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args()
    try:
        config = Config.from_env()
        worker = Worker(config)
        if args.check_config:
            worker.prepare()
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "jobs_url": config.jobs_url,
                        "queue_configured": config.configured,
                        "health_bind": f"{config.health_host}:{config.health_port}",
                    }
                )
            )
            return 0
        signal.signal(signal.SIGTERM, lambda *_: worker.stop())
        signal.signal(signal.SIGINT, lambda *_: worker.stop())
        return worker.run(once=args.once)
    except PipelineError as exc:
        LOG.error("%s", compact_error(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
