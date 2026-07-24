"""xAI Grok Imagine image and image-to-video HTTP client.

The wire contract mirrors the installed Hermes xAI image/video plugins.  The
client accepts an already-resolved bearer so it cannot refresh or persist the
Hermes OAuth grant.
"""

from __future__ import annotations

import base64
import io
import math
import os
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

import httpx
from PIL import Image, ImageOps

from .xai_auth import XAICredentials


IMAGE_MODEL = "grok-imagine-image"
VIDEO_MODEL = "grok-imagine-video-1.5"
FRAME_SIZE = (1080, 1920)
MAX_VIDEO_DURATION_SECONDS = 15
TERMINAL_VIDEO_STATES = {"failed", "error", "expired", "cancelled"}


class XAIImagineError(RuntimeError):
    """Base error for a failed xAI Imagine operation."""


class XAIEntitlementError(XAIImagineError):
    """The bearer does not have access to the requested Imagine endpoint."""


class XAIRateLimitError(XAIImagineError):
    """xAI continued returning HTTP 429 after the bounded retry policy."""


class ImagineCallCapError(XAIImagineError):
    """The persisted per-run Imagine request cap has been reached."""


class XAIVideoTerminalError(XAIImagineError):
    """An asynchronous video request reached a terminal failure state."""

    def __init__(self, status: str, message: str):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class VideoCompletion:
    request_id: str
    url: str
    duration_seconds: float | None


def _user_agent() -> str:
    try:
        from hermes_cli import __version__
    except Exception:
        __version__ = "framegate"
    return f"Hermes-Agent/{__version__}"


def normalize_frame_bytes(content: bytes, destination: Path) -> None:
    """Center-crop/upscale an Imagine result to the FLUX 1080x1920 contract."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(f".{destination.stem}.{uuid.uuid4().hex}.part.png")
    try:
        with Image.open(io.BytesIO(content)) as source:
            normalized = ImageOps.fit(
                source.convert("RGB"),
                FRAME_SIZE,
                method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.5),
            )
            normalized.save(partial, format="PNG", optimize=True)
        os.replace(partial, destination)
    except (OSError, ValueError) as exc:
        raise XAIImagineError(f"Could not normalize Grok frame: {exc}") from exc
    finally:
        partial.unlink(missing_ok=True)


def _probe_video_duration(path: Path, ffprobe_bin: str) -> float:
    try:
        result = subprocess.run(
            [
                ffprobe_bin,
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        duration = float(result.stdout.strip())
    except (FileNotFoundError, subprocess.CalledProcessError, ValueError) as exc:
        raise XAIImagineError(f"Could not measure Grok video duration: {path}") from exc
    if duration <= 0:
        raise XAIImagineError(f"Grok video duration is not positive: {path}")
    return duration


def conform_video_duration(
    source: Path,
    destination: Path,
    target_seconds: float,
    *,
    ffmpeg_bin: str = "ffmpeg",
    ffprobe_bin: str = "ffprobe",
) -> tuple[float, float]:
    """Pad or trim only video frames to the narration-derived shot duration."""
    if target_seconds <= 0:
        raise ValueError("target_seconds must be positive")
    source_seconds = _probe_video_duration(source, ffprobe_bin)
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(
        f".{destination.stem}.{uuid.uuid4().hex}.part{destination.suffix}"
    )
    video_filter = (
        f"tpad=stop_mode=clone:stop_duration={target_seconds:.6f},"
        f"trim=duration={target_seconds:.6f},setpts=PTS-STARTPTS"
    )
    try:
        subprocess.run(
            [
                ffmpeg_bin,
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                str(source),
                "-vf",
                video_filter,
                "-an",
                "-c:v",
                "libx264",
                "-preset",
                "medium",
                "-crf",
                "20",
                "-pix_fmt",
                "yuv420p",
                "-movflags",
                "+faststart",
                "-t",
                f"{target_seconds:.6f}",
                str(partial),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        os.replace(partial, destination)
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        raise XAIImagineError(
            f"Could not conform Grok video duration: {str(detail)[-1000:]}"
        ) from exc
    finally:
        partial.unlink(missing_ok=True)
    return source_seconds, _probe_video_duration(destination, ffprobe_bin)


class GrokImagineClient:
    """Synchronous client matching Hermes' xAI Imagine wire behavior."""

    def __init__(
        self,
        credentials: XAICredentials,
        http_client: httpx.Client,
        *,
        request_timeout: float = 120,
        poll_interval: float = 5,
        poll_timeout: float = 240,
        retry_base_seconds: float = 2,
        consume_call: Callable[[], None] | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.credentials = credentials
        self.http = http_client
        self.request_timeout = request_timeout
        self.poll_interval = poll_interval
        self.poll_timeout = poll_timeout
        self.retry_base_seconds = retry_base_seconds
        self.consume_call = consume_call or (lambda: None)
        self.sleep = sleep

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.credentials.bearer}",
            "Content-Type": "application/json",
            "User-Agent": _user_agent(),
        }

    @staticmethod
    def _error_detail(response: httpx.Response) -> str:
        try:
            body = response.json()
            error = body.get("error") if isinstance(body, Mapping) else None
            if isinstance(error, Mapping) and error.get("message"):
                return str(error["message"])[:500]
            if isinstance(error, str):
                return error[:500]
        except ValueError:
            pass
        return response.text[:500].strip()

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        payload: Mapping[str, Any] | None = None,
        billable: bool = False,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        for attempt in range(1, 4):
            if billable:
                self.consume_call()
            response = self.http.request(
                method,
                url,
                headers=dict(headers or self._headers()),
                json=dict(payload) if payload is not None else None,
                timeout=timeout or self.request_timeout,
            )
            if response.status_code == 403:
                raise XAIEntitlementError(
                    "xAI Imagine returned HTTP 403 (tier/entitlement problem). "
                    "Set XAI_API_KEY or switch back to `--visuals local`."
                )
            if response.status_code == 429:
                if attempt < 3:
                    retry_after = response.headers.get("Retry-After", "")
                    try:
                        delay = min(60.0, max(0.0, float(retry_after)))
                    except ValueError:
                        delay = min(
                            60.0,
                            self.retry_base_seconds * (2 ** (attempt - 1)),
                        )
                    self.sleep(delay)
                    continue
                raise XAIRateLimitError(
                    "xAI Imagine returned HTTP 429 after 3 attempts (rate limit). "
                    "Wait for the subscription allowance to recover or switch "
                    "back to `--visuals local`."
                )
            try:
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise XAIImagineError(
                    f"xAI Imagine returned HTTP {response.status_code}: "
                    f"{self._error_detail(response)}"
                ) from exc
            try:
                body = response.json()
            except ValueError as exc:
                raise XAIImagineError("xAI Imagine returned invalid JSON") from exc
            if not isinstance(body, dict):
                raise XAIImagineError("xAI Imagine returned a non-object JSON body")
            return body
        raise AssertionError("unreachable")

    def _download(self, url: str) -> bytes:
        response = self.http.get(url, timeout=self.request_timeout)
        if response.status_code == 403:
            raise XAIEntitlementError(
                "xAI Imagine media download returned HTTP 403 (tier/entitlement "
                "problem). Set XAI_API_KEY or switch back to `--visuals local`."
            )
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise XAIImagineError(
                f"Could not download xAI Imagine media: HTTP {response.status_code}"
            ) from exc
        return response.content

    def generate_image(self, prompt: str) -> bytes:
        body = self._request_json(
            "POST",
            f"{self.credentials.base_url}/images/generations",
            payload={
                "model": IMAGE_MODEL,
                "prompt": prompt,
                "aspect_ratio": "9:16",
                "resolution": "1k",
            },
            billable=True,
            timeout=120,
        )
        data = body.get("data")
        if not isinstance(data, list) or not data or not isinstance(data[0], Mapping):
            raise XAIImagineError("xAI Imagine returned no image data")
        image = data[0]
        file_output = image.get("file_output")
        public_url = (
            file_output.get("public_url")
            if isinstance(file_output, Mapping)
            else None
        )
        if isinstance(public_url, str) and public_url:
            return self._download(public_url)
        encoded = image.get("b64_json")
        if isinstance(encoded, str) and encoded:
            try:
                return base64.b64decode(encoded, validate=True)
            except (ValueError, TypeError) as exc:
                raise XAIImagineError("xAI Imagine returned invalid base64 image data") from exc
        url = image.get("url")
        if isinstance(url, str) and url:
            return self._download(url)
        raise XAIImagineError("xAI Imagine image had neither b64_json nor URL")

    @staticmethod
    def _image_data_uri(path: Path) -> str:
        suffix = path.suffix.lower()
        media_type = "image/jpeg" if suffix in {".jpg", ".jpeg"} else "image/png"
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        return f"data:{media_type};base64,{encoded}"

    def start_image_to_video(
        self,
        image_path: Path,
        prompt: str,
        duration_seconds: float,
    ) -> str:
        duration = max(1, min(MAX_VIDEO_DURATION_SECONDS, math.ceil(duration_seconds)))
        headers = {**self._headers(), "x-idempotency-key": str(uuid.uuid4())}
        body = self._request_json(
            "POST",
            f"{self.credentials.base_url}/videos/generations",
            headers=headers,
            payload={
                "model": VIDEO_MODEL,
                "prompt": prompt,
                "duration": duration,
                "aspect_ratio": "9:16",
                "resolution": "720p",
                "image": {"url": self._image_data_uri(image_path)},
            },
            billable=True,
            timeout=60,
        )
        request_id = body.get("request_id")
        if not request_id:
            raise XAIImagineError("xAI video response did not include request_id")
        return str(request_id)

    def poll_video(self, request_id: str) -> VideoCompletion:
        deadline = time.monotonic() + self.poll_timeout
        last_status = "queued"
        while time.monotonic() < deadline:
            body = self._request_json(
                "GET",
                f"{self.credentials.base_url}/videos/{request_id}",
                timeout=30,
            )
            last_status = str(body.get("status") or "").lower()
            if last_status == "done":
                video = body.get("video")
                if not isinstance(video, Mapping):
                    video = {}
                file_output = video.get("file_output")
                public_url = (
                    file_output.get("public_url")
                    if isinstance(file_output, Mapping)
                    else None
                )
                url = public_url or video.get("url")
                if not isinstance(url, str) or not url:
                    raise XAIImagineError(
                        "xAI video request completed without a video URL"
                    )
                duration = video.get("duration")
                return VideoCompletion(
                    request_id=request_id,
                    url=url,
                    duration_seconds=(
                        float(duration) if isinstance(duration, (int, float)) else None
                    ),
                )
            if last_status in TERMINAL_VIDEO_STATES:
                error = body.get("error")
                message = (
                    error.get("message")
                    if isinstance(error, Mapping)
                    else body.get("message")
                )
                raise XAIVideoTerminalError(
                    last_status,
                    str(message or f"xAI video request ended with status '{last_status}'"),
                )
            self.sleep(self.poll_interval)
        raise TimeoutError(
            f"Timed out waiting for xAI video request after {self.poll_timeout:g}s "
            f"(last status: {last_status or 'unknown'})"
        )

    def download_video(self, completion: VideoCompletion, destination: Path) -> None:
        content = self._download(completion.url)
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(
            f".{destination.stem}.{uuid.uuid4().hex}.part{destination.suffix}"
        )
        try:
            partial.write_bytes(content)
            os.replace(partial, destination)
        finally:
            partial.unlink(missing_ok=True)
