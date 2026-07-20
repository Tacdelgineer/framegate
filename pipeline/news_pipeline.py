#!/usr/bin/env python3
"""Crash-resumable short-form evergreen explainer video pipeline.

Expensive model calls remain on the VPS. GPU/media stages are transferred to a
DGX worker through the Tailscale job queue.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import math
import mimetypes
import os
import re
import secrets
import sqlite3
import subprocess
import sys
import time
import uuid
import wave
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

import httpx
import yaml
from dotenv import dotenv_values

PROJECT_ROOT = Path(__file__).resolve().parent
MONOREPO_ROOT = PROJECT_ROOT.parent
PROJECT_ENV = dotenv_values(PROJECT_ROOT / ".env")
JOB_QUEUE_CLIENT_ROOT = Path(
    os.getenv("JOB_QUEUE_CLIENT_ROOT")
    or PROJECT_ENV.get("JOB_QUEUE_CLIENT_ROOT")
    or MONOREPO_ROOT / "queue"
).expanduser()
if str(JOB_QUEUE_CLIENT_ROOT) not in sys.path:
    sys.path.insert(0, str(JOB_QUEUE_CLIENT_ROOT))

from job_queue import JobFailedError, JobQueueClient, JobQueueError  # noqa: E402


LOG = logging.getLogger("news_pipeline")

STATUSES = (
    "fetched",
    "scripted",
    "voiced",
    "framed",
    "rendered",
    "assembled",
    "pending_approval",
    "published",
    "rejected",
)
STATUS_INDEX = {status: index for index, status in enumerate(STATUSES)}
TERMINAL_STATUSES = {"published", "rejected"}
WAN_FPS = 16
WAN_FRAME_STRIDE = 4
WAN_FRAME_OFFSET = 1
NARRATION_SECONDS_MIN = 4.0
NARRATION_SECONDS_MAX = 6.0
ASSEMBLY_TAIL_SECONDS = 0.5
TELEGRAM_MEDIA_GROUP_LIMIT = 10

XAI_VIDEO_CREATE_URL = "https://api.x.ai/v1/videos/generations"
XAI_VIDEO_STATUS_URL = "https://api.x.ai/v1/videos/{request_id}"
OPENAI_IMAGE_URL = "https://api.openai.com/v1/images/generations"
OPENAI_FRAME_MODEL = "gpt-image-1"
DEFAULT_PRESET_PATH = MONOREPO_ROOT / "config" / "presets.yaml"
VISUAL_BACKENDS = {"local", "cloud"}
VIDEO_MODES = {"i2v", "flf", "auto"}
DEFAULT_STYLE_BLOCK = "Cinematic news documentary photography, realistic lighting. "
DEFAULT_NARRATION_STYLE = "Conversational, curious, direct, and warm."
DEFAULT_SCRIPT_BASE_URL = "http://100.103.129.82:11434/v1"
DEFAULT_SCRIPT_MODEL = "qwen3.6:35b-a3b"
DEFAULT_SCRIPT_TIMEOUT_SECONDS = 900.0

VOICE_PRESETS = {
    "alireza": {
        "ref_audio": "/home/xxfactionsxx/content-factory/assets/alireza.wav",
        "ref_text_file": MONOREPO_ROOT / "assets" / "alireza.txt",
    },
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def new_seed(previous: int | None = None) -> int:
    seed = secrets.randbelow(2**31)
    if previous is not None and seed == previous:
        return (seed + 1) % (2**31)
    return seed


def valid_file(path: str | Path | None) -> bool:
    return bool(path) and Path(path).is_file() and Path(path).stat().st_size > 0


def json_load(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return fallback


def parse_json_text(text: str) -> Any:
    candidate = text.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", candidate, re.DOTALL)
    if fenced:
        candidate = fenced.group(1)
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        decoder = json.JSONDecoder()
        for marker in ("{", "["):
            position = candidate.find(marker)
            if position >= 0:
                try:
                    value, _ = decoder.raw_decode(candidate[position:])
                    return value
                except json.JSONDecodeError:
                    pass
        raise ValueError("Model response did not contain valid JSON")


def strip_think_blocks(text: str) -> str:
    return re.sub(
        r"<think\b[^>]*>.*?</think\s*>",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    ).strip()


def atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    try:
        partial.write_bytes(content)
        os.replace(partial, path)
    finally:
        partial.unlink(missing_ok=True)


def target_shot_count(target_duration_seconds: float) -> int:
    """Aim for roughly five seconds of narration per shot."""
    return max(1, math.ceil(target_duration_seconds / 5.0))


def wan_frame_count(
    clip_seconds: float,
    *,
    fps: int = WAN_FPS,
    max_clip_seconds: float | None = None,
) -> int:
    """Round up to Wan's 4n+1 frame shape without crossing a hard cap."""
    if clip_seconds <= 0:
        raise ValueError("clip_seconds must be positive")
    if fps <= 0:
        raise ValueError("fps must be positive")
    requested_frames = math.ceil(clip_seconds * fps)
    frame_count = (
        math.ceil((requested_frames - WAN_FRAME_OFFSET) / WAN_FRAME_STRIDE)
        * WAN_FRAME_STRIDE
        + WAN_FRAME_OFFSET
    )
    frame_count = max(WAN_FRAME_OFFSET, frame_count)
    if max_clip_seconds is None:
        return frame_count
    if max_clip_seconds <= 0:
        raise ValueError("max_clip_seconds must be positive")
    max_frames = math.floor(max_clip_seconds * fps)
    max_compatible = (
        math.floor((max_frames - WAN_FRAME_OFFSET) / WAN_FRAME_STRIDE)
        * WAN_FRAME_STRIDE
        + WAN_FRAME_OFFSET
    )
    return min(frame_count, max(WAN_FRAME_OFFSET, max_compatible))


def clip_timing(
    narration_seconds: float,
    *,
    clip_padding: float,
    max_clip_seconds: float,
    fps: int = WAN_FPS,
) -> dict[str, float | int | bool]:
    if narration_seconds <= 0:
        raise ValueError("narration_seconds must be positive")
    requested_seconds = narration_seconds + clip_padding
    capped_seconds = min(requested_seconds, max_clip_seconds)
    frames = wan_frame_count(
        capped_seconds,
        fps=fps,
        max_clip_seconds=max_clip_seconds,
    )
    return {
        "narration_seconds": narration_seconds,
        "requested_clip_seconds": requested_seconds,
        "clip_seconds": capped_seconds,
        "frame_count": frames,
        "generated_seconds": frames / fps,
        "capped": requested_seconds > max_clip_seconds,
    }


def build_local_video_job(
    *,
    mode: str,
    first_frame: Path,
    last_frame: Path | None,
    prompt: str,
    negative_prompt: str,
    resolution: str,
    steps: int,
    seed: int,
    duration_seconds: float,
    frame_count: int,
    fps: int = WAN_FPS,
) -> tuple[dict[str, Any], dict[str, Path]]:
    """Build the canonical queue-native worker payload for one video clip."""
    if mode not in {"i2v", "flf"}:
        raise ValueError(f"Unsupported local video mode: {mode!r}")
    if mode == "i2v" and last_frame is not None:
        raise ValueError("I2V video jobs accept exactly one input frame")
    if mode == "flf" and last_frame is None:
        raise ValueError("FLF video jobs require start and end frames")
    if duration_seconds <= 0:
        raise ValueError("Video duration_seconds must be positive")
    if fps != WAN_FPS:
        raise ValueError(f"Local Wan video jobs require {WAN_FPS}fps")
    if (
        isinstance(frame_count, bool)
        or not isinstance(frame_count, int)
        or frame_count < WAN_FRAME_OFFSET
        or (frame_count - WAN_FRAME_OFFSET) % WAN_FRAME_STRIDE
    ):
        raise ValueError("Local Wan frame_count must have the 4n+1 shape")

    payload: dict[str, Any] = {
        "mode": mode,
        "prompt": prompt,
        "motion_instruction": prompt,
        "negative_prompt": negative_prompt,
        "resolution": resolution,
        "steps": steps,
        "seed": seed,
        "duration_seconds": duration_seconds,
        "frame_count": frame_count,
        "fps": fps,
        "aspect_ratio": "9:16",
        "output_format": "mp4",
    }
    input_files = {"start_frame": first_frame}
    if last_frame is not None:
        input_files["end_frame"] = last_frame
    return payload, input_files


def wav_duration_seconds(path: Path) -> float:
    try:
        with wave.open(str(path), "rb") as source:
            frame_rate = source.getframerate()
            if frame_rate <= 0:
                raise ValueError(f"WAV has invalid frame rate: {path}")
            return source.getnframes() / frame_rate
    except (wave.Error, EOFError) as exc:
        raise ValueError(f"Could not measure WAV duration: {path}") from exc


def concatenate_wavs(
    sources: list[Path],
    destination: Path,
    *,
    tail_seconds: float = ASSEMBLY_TAIL_SECONDS,
) -> None:
    if not sources:
        raise ValueError("Cannot concatenate an empty WAV list")
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
    try:
        with ExitStack() as stack:
            readers = [
                stack.enter_context(wave.open(str(source), "rb")) for source in sources
            ]
            expected = (
                readers[0].getnchannels(),
                readers[0].getsampwidth(),
                readers[0].getframerate(),
                readers[0].getcomptype(),
            )
            if expected[3] != "NONE":
                raise ValueError("Per-shot narration WAV must be uncompressed PCM")
            for source, reader in zip(sources[1:], readers[1:], strict=True):
                actual = (
                    reader.getnchannels(),
                    reader.getsampwidth(),
                    reader.getframerate(),
                    reader.getcomptype(),
                )
                if actual != expected:
                    raise ValueError(
                        f"Per-shot narration WAV format mismatch: {source}"
                    )
            output = stack.enter_context(wave.open(str(partial), "wb"))
            output.setnchannels(expected[0])
            output.setsampwidth(expected[1])
            output.setframerate(expected[2])
            for reader in readers:
                output.writeframes(reader.readframes(reader.getnframes()))
            tail_frames = round(tail_seconds * expected[2])
            output.writeframes(b"\0" * tail_frames * expected[0] * expected[1])
        os.replace(partial, destination)
    finally:
        partial.unlink(missing_ok=True)


def media_duration_seconds(path: Path) -> float:
    if path.suffix.lower() == ".wav":
        return wav_duration_seconds(path)
    try:
        result = subprocess.run(
            [
                "ffprobe",
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
        raise ValueError(f"Could not measure media duration: {path}") from exc
    if duration <= 0:
        raise ValueError(f"Media duration must be positive: {path}")
    return duration


def assembly_timing(
    *,
    video_seconds: float,
    voiceover_seconds: float,
    tail_seconds: float = ASSEMBLY_TAIL_SECONDS,
) -> dict[str, float]:
    if video_seconds <= 0 or voiceover_seconds <= 0:
        raise ValueError("Assembly durations must be positive")
    output_seconds = max(video_seconds, voiceover_seconds)
    return {
        "video_seconds": video_seconds,
        "voiceover_seconds": voiceover_seconds,
        "tail_seconds": tail_seconds,
        "video_pad_seconds": max(0.0, voiceover_seconds - video_seconds),
        "output_seconds": output_seconds,
    }


@dataclass(frozen=True)
class ScriptProviderConfig:
    base_url: str = DEFAULT_SCRIPT_BASE_URL
    model: str = DEFAULT_SCRIPT_MODEL
    api_key_env: str | None = None
    timeout_seconds: float = DEFAULT_SCRIPT_TIMEOUT_SECONDS

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "ScriptProviderConfig":
        expected = set(cls.__dataclass_fields__)
        unknown = set(values) - expected
        if unknown:
            raise ValueError(
                "Unknown script_provider keys: " + ", ".join(sorted(unknown))
            )
        config = cls(
            base_url=values.get("base_url", DEFAULT_SCRIPT_BASE_URL),
            model=values.get("model", DEFAULT_SCRIPT_MODEL),
            api_key_env=values.get("api_key_env"),
            timeout_seconds=values.get(
                "timeout_seconds", DEFAULT_SCRIPT_TIMEOUT_SECONDS
            ),
        )
        config.validate()
        return config

    def validate(self) -> None:
        for name in ("base_url", "model"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"script_provider.{name} must be a non-empty string")
        if not self.base_url.startswith(("http://", "https://")):
            raise ValueError("script_provider.base_url must use http:// or https://")
        if self.api_key_env is not None and (
            not isinstance(self.api_key_env, str) or not self.api_key_env
        ):
            raise ValueError(
                "script_provider.api_key_env must be null or a non-empty string"
            )
        if (
            isinstance(self.timeout_seconds, bool)
            or not isinstance(self.timeout_seconds, (int, float))
            or self.timeout_seconds <= 0
        ):
            raise ValueError("script_provider.timeout_seconds must be positive")

    def to_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "model": self.model,
            "api_key_env": self.api_key_env,
            "timeout_seconds": self.timeout_seconds,
        }


@dataclass(frozen=True)
class CaptionStyleConfig:
    captions_enabled: bool = True
    font_size: int = 72
    base_color: str = "#FFFFFF"
    highlight_color: str = "#FFD54A"
    position: float = 20.0

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "CaptionStyleConfig":
        expected = set(cls.__dataclass_fields__)
        unknown = set(values) - expected
        if unknown:
            raise ValueError(
                "Unknown caption_style keys: " + ", ".join(sorted(unknown))
            )
        config = cls(
            captions_enabled=values.get("captions_enabled", True),
            font_size=values.get("font_size", 72),
            base_color=values.get("base_color", "#FFFFFF"),
            highlight_color=values.get("highlight_color", "#FFD54A"),
            position=values.get("position", 20.0),
        )
        config.validate()
        return config

    def validate(self) -> None:
        if not isinstance(self.captions_enabled, bool):
            raise ValueError("caption_style.captions_enabled must be a boolean")
        if (
            isinstance(self.font_size, bool)
            or not isinstance(self.font_size, int)
            or self.font_size <= 0
        ):
            raise ValueError("caption_style.font_size must be a positive integer")
        for name in ("base_color", "highlight_color"):
            value = getattr(self, name)
            if not isinstance(value, str) or not re.fullmatch(
                r"#[0-9A-Fa-f]{6}", value
            ):
                raise ValueError(f"caption_style.{name} must use #RRGGBB format")
        if self.base_color.casefold() == self.highlight_color.casefold():
            raise ValueError(
                "caption_style.highlight_color must differ from base_color"
            )
        if (
            isinstance(self.position, bool)
            or not isinstance(self.position, (int, float))
            or not 0 < self.position < 50
        ):
            raise ValueError(
                "caption_style.position must be a number between 0 and 50 "
                "representing percent of frame height from the bottom"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "captions_enabled": self.captions_enabled,
            "font_size": self.font_size,
            "base_color": self.base_color,
            "highlight_color": self.highlight_color,
            "position": self.position,
        }


@dataclass(frozen=True)
class PipelineConfig:
    script_provider: ScriptProviderConfig = field(default_factory=ScriptProviderConfig)
    target_duration_seconds: float = 45.0
    clip_padding: float = 0.4
    max_clip_seconds: float = 8.0
    style_block: str = DEFAULT_STYLE_BLOCK
    frame_model: str = "flux2_klein"
    video_mode: str = "i2v"
    video_resolution: str = "720p"
    steps_draft: int = 4
    steps_final: int = 8
    negative_prompt: str = (
        "text, typography, captions, logos, watermarks, user interface"
    )
    fps_out: int = 30
    caption_style: CaptionStyleConfig = field(default_factory=CaptionStyleConfig)
    narration_style: str = DEFAULT_NARRATION_STYLE

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "PipelineConfig":
        expected = set(cls.__dataclass_fields__)
        unknown = set(values) - expected
        if unknown:
            raise ValueError(f"Unknown preset keys: {', '.join(sorted(unknown))}")
        merged = {**cls().to_dict(), **dict(values)}
        raw_script_provider = merged["script_provider"]
        if not isinstance(raw_script_provider, Mapping):
            raise ValueError("script_provider must be a YAML mapping")
        raw_caption_style = merged["caption_style"]
        if not isinstance(raw_caption_style, Mapping):
            raise ValueError("caption_style must be a YAML mapping")
        config = cls(
            script_provider=ScriptProviderConfig.from_mapping(raw_script_provider),
            target_duration_seconds=merged["target_duration_seconds"],
            clip_padding=merged["clip_padding"],
            max_clip_seconds=merged["max_clip_seconds"],
            style_block=merged["style_block"],
            frame_model=merged["frame_model"],
            video_mode=merged["video_mode"],
            video_resolution=merged["video_resolution"],
            steps_draft=merged["steps_draft"],
            steps_final=merged["steps_final"],
            negative_prompt=merged["negative_prompt"],
            fps_out=merged["fps_out"],
            caption_style=CaptionStyleConfig.from_mapping(raw_caption_style),
            narration_style=merged["narration_style"],
        )
        config.validate()
        return config

    @classmethod
    def load(cls, path: Path) -> "PipelineConfig":
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ValueError(f"Pipeline preset file does not exist: {path}") from exc
        except yaml.YAMLError as exc:
            raise ValueError(f"Invalid YAML in pipeline preset {path}: {exc}") from exc
        if not isinstance(raw, Mapping):
            raise ValueError(f"Pipeline preset must be a YAML mapping: {path}")
        return cls.from_mapping(raw)

    def validate(self) -> None:
        self.script_provider.validate()
        self.caption_style.validate()
        for name in ("style_block", "frame_model", "video_resolution"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be a non-empty string")
        if not isinstance(self.negative_prompt, str):
            raise ValueError("negative_prompt must be a string")
        if not isinstance(self.narration_style, str):
            raise ValueError("narration_style must be a string")
        if self.video_mode not in VIDEO_MODES:
            raise ValueError(
                "video_mode must be one of: " + ", ".join(sorted(VIDEO_MODES))
            )
        for name in (
            "target_duration_seconds",
            "clip_padding",
            "max_clip_seconds",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"{name} must be a number")
        if self.target_duration_seconds <= 0:
            raise ValueError("target_duration_seconds must be positive")
        if self.clip_padding < 0:
            raise ValueError("clip_padding must be non-negative")
        if self.max_clip_seconds <= 0:
            raise ValueError("max_clip_seconds must be positive")
        if self.clip_padding >= self.max_clip_seconds:
            raise ValueError("clip_padding must be less than max_clip_seconds")
        for name in ("steps_draft", "steps_final", "fps_out"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")

    def to_dict(self) -> dict[str, Any]:
        return {
            "script_provider": self.script_provider.to_dict(),
            "target_duration_seconds": self.target_duration_seconds,
            "clip_padding": self.clip_padding,
            "max_clip_seconds": self.max_clip_seconds,
            "style_block": self.style_block,
            "frame_model": self.frame_model,
            "video_mode": self.video_mode,
            "video_resolution": self.video_resolution,
            "steps_draft": self.steps_draft,
            "steps_final": self.steps_final,
            "negative_prompt": self.negative_prompt,
            "fps_out": self.fps_out,
            "caption_style": self.caption_style.to_dict(),
            "narration_style": self.narration_style,
        }


def extract_timestamped_words(
    transcription: Mapping[str, Any],
) -> list[dict[str, float | str]]:
    """Normalize faster-whisper word timestamps from common response shapes."""
    raw_words = transcription.get("words")
    if not isinstance(raw_words, list) or not raw_words:
        raw_words = []
        segments = transcription.get("segments")
        if isinstance(segments, list):
            for segment in segments:
                if isinstance(segment, Mapping) and isinstance(
                    segment.get("words"), list
                ):
                    raw_words.extend(segment["words"])

    words: list[dict[str, float | str]] = []
    for item in raw_words:
        if not isinstance(item, Mapping):
            continue
        text = str(item.get("word") or item.get("text") or "").strip()
        try:
            start = float(item["start"])
            end = float(item["end"])
        except (KeyError, TypeError, ValueError):
            continue
        if text and end > start >= 0:
            words.append({"start": start, "end": end, "text": text})
    if not words:
        raise ValueError("Transcription contains no valid word-level timestamps")
    return words


def chunk_caption_words(
    words: list[dict[str, float | str]],
) -> list[list[dict[str, float | str]]]:
    """Group a transcript into compact two- or three-word caption events."""
    chunks: list[list[dict[str, float | str]]] = []
    position = 0
    while position < len(words):
        remaining = len(words) - position
        if remaining == 4:
            size = 2
        else:
            size = min(3, remaining)
        chunks.append(words[position : position + size])
        position += size
    return chunks


def _centiseconds(seconds: float) -> int:
    return max(0, math.floor(seconds * 100 + 0.5))


def _ass_timestamp(centiseconds: int) -> str:
    hours, remainder = divmod(max(0, centiseconds), 360_000)
    minutes, remainder = divmod(remainder, 6_000)
    seconds, hundredths = divmod(remainder, 100)
    return f"{hours}:{minutes:02d}:{seconds:02d}.{hundredths:02d}"


def _ass_color(rgb: str) -> str:
    red, green, blue = rgb[1:3], rgb[3:5], rgb[5:7]
    return f"&H00{blue}{green}{red}".upper()


def _ass_text(text: str) -> str:
    return (
        text.replace("\\", r"\\")
        .replace("{", r"\{")
        .replace("}", r"\}")
        .replace("\r", " ")
        .replace("\n", " ")
    )


def _karaoke_text(chunk: list[dict[str, float | str]]) -> tuple[int, int, str]:
    event_start = _centiseconds(float(chunk[0]["start"]))
    cursor = event_start
    parts = [r"{\q2}"]
    for index, word in enumerate(chunk):
        word_start = max(cursor, _centiseconds(float(word["start"])))
        word_end = max(word_start + 1, _centiseconds(float(word["end"])))
        if index:
            gap = word_start - cursor
            parts.append(f"{{\\k{gap}}} " if gap else " ")
        parts.append(f"{{\\k{word_end - word_start}}}{_ass_text(str(word['text']))}")
        cursor = word_end
    return event_start, cursor, "".join(parts)


def build_ass_subtitles(
    transcription: Mapping[str, Any],
    style: CaptionStyleConfig,
    *,
    width: int = 1080,
    height: int = 1920,
) -> str:
    """Build one-line, lower-third ASS karaoke captions from word timestamps."""
    style.validate()
    if width <= 0 or height <= 0:
        raise ValueError("ASS play resolution must be positive")
    words = extract_timestamped_words(transcription)
    margin_v = round(height * float(style.position) / 100)
    lines = [
        "[Script Info]",
        "ScriptType: v4.00+",
        f"PlayResX: {width}",
        f"PlayResY: {height}",
        "WrapStyle: 2",
        "ScaledBorderAndShadow: yes",
        "",
        "[V4+ Styles]",
        (
            "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, "
            "OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, "
            "ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, "
            "Alignment, MarginL, MarginR, MarginV, Encoding"
        ),
        (
            f"Style: Karaoke,Arial,{style.font_size},"
            f"{_ass_color(style.highlight_color)},{_ass_color(style.base_color)},"
            "&H00000000,&H96000000,-1,0,0,0,100,100,0,0,1,4,3,2,"
            f"60,60,{margin_v},1"
        ),
        "",
        "[Events]",
        "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text",
    ]
    for chunk in chunk_caption_words(words):
        start, end, text = _karaoke_text(chunk)
        lines.append(
            f"Dialogue: 0,{_ass_timestamp(start)},{_ass_timestamp(end)},"
            f"Karaoke,,0,0,0,,{text}"
        )
    return "\n".join(lines) + "\n"


def write_ass_subtitles(
    transcription_path: Path,
    destination: Path,
    style: CaptionStyleConfig,
) -> None:
    try:
        transcription = json.loads(transcription_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"Could not read transcription JSON: {transcription_path}"
        ) from exc
    if not isinstance(transcription, Mapping):
        raise ValueError("Transcription JSON must be an object")
    atomic_write_bytes(
        destination,
        build_ass_subtitles(transcription, style).encode("utf-8"),
    )


def burn_ass_subtitles(
    video_path: Path,
    captions_path: Path,
    destination: Path,
) -> None:
    escaped = (
        str(captions_path.resolve())
        .replace("\\", "\\\\")
        .replace(":", "\\:")
        .replace("'", "\\'")
    )
    partial = destination.with_name(
        f".{destination.stem}.{uuid.uuid4().hex}.part{destination.suffix}"
    )
    try:
        subprocess.run(
            [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "error",
                "-y",
                "-i",
                str(video_path),
                "-vf",
                f"subtitles=filename='{escaped}'",
                "-map",
                "0:v:0",
                "-map",
                "0:a:0?",
                "-c:v",
                "libx264",
                "-preset",
                "medium",
                "-crf",
                "20",
                "-pix_fmt",
                "yuv420p",
                "-c:a",
                "copy",
                "-movflags",
                "+faststart",
                str(partial),
            ],
            check=True,
        )
        os.replace(partial, destination)
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise ValueError(f"Could not burn ASS captions into {video_path}") from exc
    finally:
        partial.unlink(missing_ok=True)


def load_environment(project_root: Path) -> None:
    """Load non-empty values without overriding the process environment."""
    project_values = dotenv_values(project_root / ".env")
    candidates = [
        Path(
            os.getenv("HERMES_ENV_FILE")
            or project_values.get("HERMES_ENV_FILE")
            or "~/.hermes/.env"
        ).expanduser(),
        Path(
            os.getenv("TELEGRAM_ENV_FILE")
            or project_values.get("TELEGRAM_ENV_FILE")
            or "/home/alireza/signal-terminal/.env"
        ).expanduser(),
        project_root / ".env",
    ]
    merged: dict[str, str] = {}
    for path in candidates:
        if not path.is_file():
            continue
        for key, value in dotenv_values(path).items():
            if value:
                merged[key] = value
    for key, value in merged.items():
        os.environ.setdefault(key, value)


@dataclass(frozen=True)
class Settings:
    project_root: Path
    database_path: Path
    work_root: Path
    queue_url: str
    xai_api_key: str
    openai_api_key: str
    openai_image_model: str
    openai_image_quality: str
    video_model: str
    video_poll_interval: float
    video_poll_timeout: float
    queue_poll_interval: float
    queue_timeout: float
    retry_base_seconds: float
    request_timeout: float
    telegram_bot_token: str
    telegram_chat_id: str
    telegram_poll_timeout: int
    approval_wait_timeout: float
    voice_preset: str

    @classmethod
    def from_environment(
        cls,
        project_root: Path,
        *,
        database_path: Path | None = None,
        work_root: Path | None = None,
    ) -> "Settings":
        return cls(
            project_root=project_root,
            database_path=database_path
            or Path(
                os.getenv(
                    "NEWS_PIPELINE_DB", project_root / "data" / "pipeline.sqlite3"
                )
            ),
            work_root=work_root
            or Path(os.getenv("NEWS_PIPELINE_WORK_ROOT", project_root / "runs")),
            queue_url=os.getenv("JOB_QUEUE_URL", "http://100.123.208.90:8787").rstrip(
                "/"
            ),
            xai_api_key=os.getenv("XAI_API_KEY", ""),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            openai_image_model=os.getenv("OPENAI_IMAGE_MODEL", "gpt-image-1"),
            openai_image_quality=os.getenv("OPENAI_IMAGE_QUALITY", "low"),
            video_model=os.getenv("XAI_VIDEO_MODEL", "grok-imagine-video"),
            video_poll_interval=float(os.getenv("VIDEO_POLL_INTERVAL", "5")),
            video_poll_timeout=float(os.getenv("VIDEO_POLL_TIMEOUT", "2700")),
            queue_poll_interval=float(os.getenv("QUEUE_POLL_INTERVAL", "2")),
            queue_timeout=float(os.getenv("QUEUE_STAGE_TIMEOUT", "21600")),
            retry_base_seconds=float(os.getenv("STAGE_RETRY_BASE_SECONDS", "2")),
            request_timeout=float(os.getenv("API_REQUEST_TIMEOUT", "120")),
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=(
                os.getenv("TELEGRAM_CHAT_ID")
                or os.getenv("TELEGRAM_DEFAULT_CHAT_ID", "")
            ),
            telegram_poll_timeout=int(os.getenv("TELEGRAM_POLL_TIMEOUT", "25")),
            approval_wait_timeout=float(
                os.getenv("APPROVAL_WAIT_TIMEOUT", "0")
            ),
            voice_preset=os.getenv("TTS_VOICE_PRESET", "alireza").strip(),
        )


def script_provider_api_key(
    settings: Settings,
    provider: ScriptProviderConfig,
) -> str:
    if not provider.api_key_env:
        return ""
    configured_keys = {
        "XAI_API_KEY": settings.xai_api_key,
        "OPENAI_API_KEY": settings.openai_api_key,
    }
    return (
        os.getenv(provider.api_key_env, "")
        or configured_keys.get(provider.api_key_env, "")
    ).strip()


RUN_COLUMNS = {
    "status",
    "story_json",
    "script_json",
    "frames_json",
    "frame_gate_json",
    "run_config_json",
    "clips_json",
    "video_requests_json",
    "queue_jobs_json",
    "shot_audio_json",
    "voiceover_path",
    "captions_path",
    "final_path",
    "title",
    "description",
    "telegram_chat_id",
    "telegram_message_id",
    "last_error",
    "publish_json",
    "published_at",
}


class StateStore:
    def __init__(self, database_path: Path, *, read_only: bool = False):
        self.database_path = database_path
        self.read_only = read_only
        if read_only:
            return
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
        if self.read_only:
            database_uri = (
                f"{self.database_path.expanduser().resolve().as_uri()}?mode=ro"
            )
            connection = sqlite3.connect(
                database_uri,
                timeout=10,
                uri=True,
            )
            connection.execute("PRAGMA query_only = ON")
        else:
            connection = sqlite3.connect(self.database_path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def initialize(self) -> None:
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    status TEXT CHECK (
                        status IS NULL OR status IN (
                            'fetched', 'scripted', 'framed', 'rendered',
                            'voiced', 'assembled', 'pending_approval',
                            'published', 'rejected'
                        )
                    ),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    story_json TEXT,
                    script_json TEXT,
                    frames_json TEXT,
                    frame_gate_json TEXT,
                    run_config_json TEXT,
                    clips_json TEXT,
                    video_requests_json TEXT,
                    queue_jobs_json TEXT,
                    shot_audio_json TEXT,
                    voiceover_path TEXT,
                    captions_path TEXT,
                    final_path TEXT,
                    title TEXT,
                    description TEXT,
                    telegram_chat_id TEXT,
                    telegram_message_id TEXT,
                    last_error TEXT,
                    publish_json TEXT,
                    published_at TEXT
                );

                CREATE INDEX IF NOT EXISTS runs_updated_at
                    ON runs(updated_at DESC);

                CREATE TABLE IF NOT EXISTS stage_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    stage TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    outcome TEXT NOT NULL CHECK (
                        outcome IN ('running', 'succeeded', 'failed')
                    ),
                    error TEXT
                );

                CREATE INDEX IF NOT EXISTS attempts_run_stage
                    ON stage_attempts(run_id, stage, id);

                CREATE TABLE IF NOT EXISTS approvals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    action TEXT NOT NULL CHECK (
                        action IN ('approve', 'reject', 'regenerate')
                    ),
                    telegram_update_id INTEGER NOT NULL UNIQUE,
                    telegram_user_id TEXT,
                    created_at TEXT NOT NULL,
                    handled_at TEXT
                );

                CREATE TABLE IF NOT EXISTS frame_approvals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
                    frame_index INTEGER NOT NULL,
                    generation INTEGER NOT NULL,
                    action TEXT NOT NULL CHECK (
                        action IN ('approve', 'regenerate')
                    ),
                    telegram_update_id INTEGER NOT NULL UNIQUE,
                    telegram_user_id TEXT,
                    created_at TEXT NOT NULL,
                    handled_at TEXT
                );

                CREATE INDEX IF NOT EXISTS frame_approvals_run
                    ON frame_approvals(run_id, handled_at, id);

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(runs)")
            }
            if "frame_gate_json" not in columns:
                connection.execute("ALTER TABLE runs ADD COLUMN frame_gate_json TEXT")
            if "run_config_json" not in columns:
                connection.execute("ALTER TABLE runs ADD COLUMN run_config_json TEXT")
            if "shot_audio_json" not in columns:
                connection.execute("ALTER TABLE runs ADD COLUMN shot_audio_json TEXT")

    def create_run(self, topic: str) -> dict[str, Any]:
        run_id = str(uuid.uuid4())
        now = utc_now()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO runs(
                    id, topic, status, created_at, updated_at,
                    frames_json, frame_gate_json, clips_json,
                    video_requests_json, queue_jobs_json, shot_audio_json
                ) VALUES (?, ?, NULL, ?, ?, '[]', '{}', '[]', '{}', '{}', '{}')
                """,
                (run_id, topic, now, now),
            )
        return self.get_run(run_id)

    def get_run(self, run_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
        if row is None:
            raise ValueError(f"Unknown pipeline run: {run_id}")
        return dict(row)

    def latest_resumable(self) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM runs
                WHERE status IS NULL OR status NOT IN ('published', 'rejected')
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ).fetchone()
        return dict(row) if row is not None else None

    def current_or_latest(self) -> dict[str, Any] | None:
        """Return the newest unfinished run, or the newest terminal run."""
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM runs
                ORDER BY
                    CASE
                        WHEN status IS NULL
                            OR status NOT IN ('published', 'rejected')
                        THEN 0
                        ELSE 1
                    END,
                    updated_at DESC
                LIMIT 1
                """
            ).fetchone()
        return dict(row) if row is not None else None

    def attempts_for_run(self, run_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT stage, attempt, started_at, finished_at, outcome, error
                FROM stage_attempts
                WHERE run_id = ?
                ORDER BY id
                """,
                (run_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_run(self, run_id: str, **values: Any) -> dict[str, Any]:
        unknown = set(values) - RUN_COLUMNS
        if unknown:
            raise ValueError(f"Unknown run columns: {sorted(unknown)}")
        if not values:
            return self.get_run(run_id)
        if "status" in values:
            status = values["status"]
            if status is not None and status not in STATUSES:
                raise ValueError(f"Invalid pipeline status: {status}")
        values["updated_at"] = utc_now()
        columns = list(values)
        assignments = ", ".join(f"{column} = ?" for column in columns)
        with self.connect() as connection:
            cursor = connection.execute(
                f"UPDATE runs SET {assignments} WHERE id = ?",
                (*[values[column] for column in columns], run_id),
            )
            if cursor.rowcount != 1:
                raise ValueError(f"Unknown pipeline run: {run_id}")
        return self.get_run(run_id)

    def start_attempt(self, run_id: str, stage: str) -> int:
        with self.connect() as connection:
            previous = connection.execute(
                """
                SELECT COUNT(*) FROM stage_attempts
                WHERE run_id = ? AND stage = ?
                """,
                (run_id, stage),
            ).fetchone()[0]
            cursor = connection.execute(
                """
                INSERT INTO stage_attempts(
                    run_id, stage, attempt, started_at, outcome
                ) VALUES (?, ?, ?, ?, 'running')
                """,
                (run_id, stage, previous + 1, utc_now()),
            )
            return int(cursor.lastrowid)

    def finish_attempt(
        self, attempt_id: int, outcome: str, error: str | None = None
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                UPDATE stage_attempts
                SET finished_at = ?, outcome = ?, error = ?
                WHERE id = ?
                """,
                (utc_now(), outcome, error, attempt_id),
            )

    def get_setting(self, key: str, default: str = "") -> str:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT value FROM settings WHERE key = ?", (key,)
            ).fetchone()
        return str(row["value"]) if row is not None else default

    def set_setting(self, key: str, value: str) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO settings(key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    def record_approval(
        self,
        run_id: str,
        action: str,
        update_id: int,
        user_id: str | None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO approvals(
                    run_id, action, telegram_update_id, telegram_user_id,
                    created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, action, update_id, user_id, utc_now()),
            )

    def pending_approval(self, run_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM approvals
                WHERE run_id = ? AND handled_at IS NULL
                ORDER BY id
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def mark_approval_handled(self, approval_id: int) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE approvals SET handled_at = ? WHERE id = ?",
                (utc_now(), approval_id),
            )

    def record_frame_approval(
        self,
        run_id: str,
        frame_index: int,
        generation: int,
        action: str,
        update_id: int,
        user_id: str | None,
    ) -> None:
        with self.connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO frame_approvals(
                    run_id, frame_index, generation, action,
                    telegram_update_id, telegram_user_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    frame_index,
                    generation,
                    action,
                    update_id,
                    user_id,
                    utc_now(),
                ),
            )

    def pending_frame_approval(self, run_id: str) -> dict[str, Any] | None:
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM frame_approvals
                WHERE run_id = ? AND handled_at IS NULL
                ORDER BY id
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def mark_frame_approval_handled(self, approval_id: int) -> None:
        with self.connect() as connection:
            connection.execute(
                "UPDATE frame_approvals SET handled_at = ? WHERE id = ?",
                (utc_now(), approval_id),
            )


class RemoteAPIError(RuntimeError):
    pass


class NonRetryableWorkerError(RuntimeError):
    """A deterministic worker rejection that stage retries cannot repair."""


def is_worker_validation_error(exc: JobFailedError) -> bool:
    worker_error = getattr(exc, "worker_error", "") or str(exc)
    if worker_error.startswith("JobValidationError:"):
        return True
    # Compatibility with workers deployed before validation errors had their
    # own exception type. Keep this deliberately exact so memory gates,
    # timeouts, and other transient PipelineError failures still retry.
    return worker_error == (
        "PipelineError: video payload requires exactly one or two input frames"
    )


class NewsPipeline:
    def __init__(
        self,
        settings: Settings,
        store: StateStore,
        run: Mapping[str, Any],
        *,
        config: PipelineConfig | None = None,
        visuals: str = "local",
        frame_gate: bool = True,
        http_client: httpx.Client | None = None,
        queue_client: JobQueueClient | None = None,
    ):
        if visuals not in VISUAL_BACKENDS:
            raise ValueError(
                "visuals must be one of: " + ", ".join(sorted(VISUAL_BACKENDS))
            )
        self.settings = settings
        self.config = config or PipelineConfig()
        self.config.validate()
        self.visuals = visuals
        self.frame_gate = frame_gate
        self.store = store
        self.run_id = str(run["id"])
        self.run_dir = settings.work_root / self.run_id
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.http = http_client or httpx.Client(
            timeout=httpx.Timeout(settings.request_timeout, connect=15.0),
            follow_redirects=True,
        )
        self._owns_http = http_client is None
        self.queue = queue_client or JobQueueClient(
            settings.queue_url,
            poll_interval=settings.queue_poll_interval,
            timeout=settings.queue_timeout,
            request_timeout=settings.request_timeout,
        )
        self._owns_queue = queue_client is None

    def close(self) -> None:
        if self._owns_queue:
            self.queue.close()
        if self._owns_http:
            self.http.close()

    def current(self) -> dict[str, Any]:
        return self.store.get_run(self.run_id)

    def require_key(self, name: str, value: str) -> str:
        if not value:
            raise RuntimeError(
                f"{name} is not configured. Add it to "
                f"{self.settings.project_root / '.env'}."
            )
        return value

    def _response_json(self, response: httpx.Response, provider: str) -> Any:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = response.text[:1500].strip()
            raise RemoteAPIError(
                f"{provider} returned HTTP {response.status_code}: {detail}"
            ) from exc
        try:
            return response.json()
        except ValueError as exc:
            raise RemoteAPIError(f"{provider} returned invalid JSON") from exc

    def _script_chat(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
    ) -> tuple[Any, dict[str, Any]]:
        provider = self.config.script_provider
        payload: dict[str, Any] = {
            "model": provider.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.2,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        headers = {}
        if provider.api_key_env:
            api_key = script_provider_api_key(self.settings, provider)
            if not api_key:
                raise RuntimeError(
                    f"{provider.api_key_env} is required by script_provider. "
                    f"Add it to {self.settings.project_root / '.env'}."
                )
            headers["Authorization"] = f"Bearer {api_key}"
        url = f"{provider.base_url.rstrip('/')}/chat/completions"
        request_timeout = httpx.Timeout(
            connect=min(15.0, float(provider.timeout_seconds)),
            read=self.settings.request_timeout,
            write=self.settings.request_timeout,
            pool=self.settings.request_timeout,
        )
        started = time.monotonic()
        status = "failed"
        usage: dict[str, Any] = {}
        content_parts: list[str] = []
        response_metadata: dict[str, Any] = {}
        try:
            with self.http.stream(
                "POST",
                url,
                headers=headers,
                json=payload,
                timeout=request_timeout,
            ) as response:
                if response.is_error:
                    response.read()
                    self._response_json(
                        response,
                        "Script provider Chat Completions",
                    )
                for line in response.iter_lines():
                    elapsed = time.monotonic() - started
                    if elapsed >= provider.timeout_seconds:
                        raise httpx.ReadTimeout(
                            "Script provider exceeded its overall generation "
                            f"ceiling of {provider.timeout_seconds:g} seconds",
                            request=response.request,
                        )
                    stripped = line.strip()
                    if not stripped or stripped.startswith(":"):
                        continue
                    if stripped.startswith(("event:", "id:", "retry:")):
                        continue
                    if stripped.startswith("data:"):
                        stripped = stripped[5:].strip()
                    if stripped == "[DONE]":
                        break
                    try:
                        event = json.loads(stripped)
                    except json.JSONDecodeError as exc:
                        raise RemoteAPIError(
                            "Script provider returned an invalid streaming event"
                        ) from exc
                    if not isinstance(event, Mapping):
                        raise RemoteAPIError(
                            "Script provider returned a non-object streaming event"
                        )
                    error = event.get("error")
                    if error:
                        raise RemoteAPIError(f"Script provider stream failed: {error}")
                    event_usage = event.get("usage")
                    if isinstance(event_usage, Mapping):
                        usage.update(event_usage)
                    for key in ("id", "model", "created", "system_fingerprint"):
                        if event.get(key) is not None:
                            response_metadata[key] = event[key]
                    choices = event.get("choices")
                    if not isinstance(choices, list) or not choices:
                        continue
                    choice = choices[0]
                    if not isinstance(choice, Mapping):
                        continue
                    message = choice.get("delta")
                    if not isinstance(message, Mapping):
                        message = choice.get("message")
                    if not isinstance(message, Mapping):
                        continue
                    content = message.get("content")
                    if content is not None:
                        content_parts.append(str(content))

            if time.monotonic() - started >= provider.timeout_seconds:
                raise httpx.ReadTimeout(
                    "Script provider exceeded its overall generation "
                    f"ceiling of {provider.timeout_seconds:g} seconds"
                )
            content = "".join(content_parts)
            if not content:
                raise RemoteAPIError(
                    "Script provider response did not contain message content"
                )
            data = {
                **response_metadata,
                "choices": [{"message": {"content": content}}],
                "usage": usage,
            }
            parsed = parse_json_text(strip_think_blocks(content))
            status = "succeeded"
            return parsed, data
        finally:
            duration = time.monotonic() - started
            log_level = logging.INFO if status == "succeeded" else logging.WARNING
            LOG.log(
                log_level,
                "Script provider generation %s model=%s duration_seconds=%.2f "
                "prompt_tokens=%s completion_tokens=%s total_tokens=%s",
                status,
                provider.model,
                duration,
                usage.get("prompt_tokens", "unknown"),
                usage.get("completion_tokens", "unknown"),
                usage.get("total_tokens", "unknown"),
            )

    def fetch_story(self) -> None:
        topic = self.current()["topic"]
        target_seconds = self.config.target_duration_seconds
        system_prompt = (
            "You are an expert explainer producer. Develop an original, "
            "evergreen content brief from the supplied topic itself. Do not "
            "claim to have performed live research and do not invent sources, "
            "quotes, statistics, or current events. Return only valid JSON."
        )
        user_prompt = f"""
Create a focused brief for a roughly {target_seconds:g}-second vertical
explainer about this exact topic:

{topic}

Return exactly this shape:
{{
  "title": "concise evergreen explainer title",
  "summary": "two to four sentences defining the topic and central thesis",
  "why_it_matters": "one sentence",
  "audience": "who this explainer is for",
  "angle": "the original narrative angle",
  "key_points": [
    "specific point to explain",
    "specific point to explain",
    "specific point to explain"
  ]
}}

Treat the topic as the complete editorial brief. Favor durable explanations
over time-sensitive claims. Build a clear progression that can be expressed
visually without on-screen text.
""".strip()
        story, _ = self._script_chat(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        if not isinstance(story, dict):
            raise ValueError("fetch_story must return a JSON object")
        for key in (
            "title",
            "summary",
            "why_it_matters",
            "audience",
            "angle",
            "key_points",
        ):
            if key not in story:
                raise ValueError(f"Brief JSON is missing {key}")
        if not isinstance(story["key_points"], list) or len(story["key_points"]) < 3:
            raise ValueError("Brief must include at least three key points")
        self.store.update_run(
            self.run_id,
            status="fetched",
            story_json=json.dumps(story),
            title=str(story["title"]),
            last_error=None,
        )

    def write_script(self) -> None:
        story = json_load(self.current()["story_json"], None)
        if not isinstance(story, dict):
            raise ValueError("Cannot write a script without story JSON")
        mode_instruction = {
            "i2v": (
                'Set "video_mode" to "i2v" and "last_frame_prompt" to null '
                "for every shot."
            ),
            "flf": (
                'Set "video_mode" to "flf" and provide a matching detailed '
                '"last_frame_prompt" for every shot.'
            ),
            "auto": (
                'Choose "i2v" or "flf" per shot. Use "flf" only when a '
                "specific ending composition materially improves the shot; "
                "otherwise use i2v. Provide last_frame_prompt only for flf."
            ),
        }[self.config.video_mode]
        target_seconds = self.config.target_duration_seconds
        shot_count = target_shot_count(target_seconds)
        average_seconds = target_seconds / shot_count
        system_prompt = (
            "You write original, accurate, fast-paced evergreen explainer "
            "scripts for vertical video. Write narration for the ear, not the "
            "page. Return only valid JSON. Stay within the supplied topic "
            "brief and do not invent sources, quotes, statistics, or timely "
            "claims. Each shot must have exactly one simple camera move as "
            "its motion instruction."
        )
        user_prompt = f"""
Turn this topic brief into an original evergreen YouTube explainer targeting
about {target_seconds:g} seconds, split into exactly {shot_count}
narration-driven shots.

TOPIC BRIEF:
{json.dumps(story, ensure_ascii=False)}

STYLE BLOCK (copy this exact string at the beginning of every frame prompt):
{json.dumps(self.config.style_block, ensure_ascii=False)}

NARRATION STYLE (apply this free-text direction to the spoken voice):
{json.dumps(self.config.narration_style, ensure_ascii=False)}

Return exactly:
{{
  "title": "YouTube Shorts explainer title, clear and compelling",
  "description": "Two short paragraphs describing the explainer",
  "shots": [
    {{
      "voiceover_text": "spoken narration for this roughly 4-6 second shot",
      "motion_instruction": "one camera move, for example: slow push-in",
      "video_mode": "i2v or flf",
      "first_frame_prompt": "STYLE BLOCK followed by a detailed 9:16 opening frame prompt",
      "last_frame_prompt": "STYLE BLOCK followed by a detailed ending frame prompt, or null for i2v"
    }}
  ]
}}

Requirements:
- Exactly {shot_count} shots.
- Aim for about {average_seconds:.1f} seconds of spoken narration per shot,
  keeping every shot in the 4-6 second range.
- Total voiceover should sound natural in about {target_seconds:g} seconds.
- Shot 1 hooks immediately; the middle shots build the explanation; the final
  shot lands the central insight and why it matters.
- Exactly one short camera move per motion_instruction (for example "slow
  push-in", "gentle pan left", or "static locked-off"); do not combine moves.
- {mode_instruction}
- Begin every non-null frame prompt with the STYLE BLOCK exactly as supplied.
- No visible text, logos, watermarks, captions, or UI in image prompts.
- Write for the ear, not the page. Use contractions always; never use an
  expanded form when a natural contraction exists.
- Keep every narration sentence under 12 words.
- Use second person ("you") where it sounds natural.
- Choose concrete images over abstractions.
- Never use stock AI phrasing, including "delve", "tapestry", "in today's
  world", "imagine a world", or "it's important to note".
- Every shot after the first must connect to the previous shot with a natural
  spoken-language transition.
- The final narration line must open a question or land a punchy fact. It must
  never summarize the explainer.
- Make each narration line original and explanatory, with no claim of live
  reporting or external sourcing.
- Before returning JSON, reread all narration as if speaking it aloud. Rewrite
  any sentence a person wouldn't say to a friend.
""".strip()
        script, _ = self._script_chat(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
        )
        if not isinstance(script, dict):
            raise ValueError("write_script must return a JSON object")
        shots = script.get("shots")
        if not isinstance(shots, list) or len(shots) != shot_count:
            raise ValueError(f"Script must contain exactly {shot_count} shots")
        for index, shot in enumerate(shots, start=1):
            if not isinstance(shot, dict):
                raise ValueError(f"Shot {index} must be an object")
            motion = str(
                shot.get("motion_instruction") or shot.get("visual_prompt") or ""
            ).strip()
            if not str(shot.get("voiceover_text", "")).strip():
                raise ValueError(f"Shot {index} is missing voiceover_text")
            if not motion:
                raise ValueError(f"Shot {index} is missing motion_instruction")
            if (
                len(motion.split()) > 12
                or any(mark in motion for mark in ("\n", ";", ",", "/"))
                or re.search(r"\b(and|then|followed by)\b", motion, re.IGNORECASE)
            ):
                raise ValueError(
                    f"Shot {index} motion_instruction must be one camera move"
                )
            mode = (
                self.config.video_mode
                if self.config.video_mode != "auto"
                else str(shot.get("video_mode", "")).lower()
            )
            if mode not in {"i2v", "flf"}:
                raise ValueError(f"Shot {index} has invalid video_mode: {mode}")
            first_prompt = str(shot.get("first_frame_prompt", ""))
            if not first_prompt.strip():
                raise ValueError(f"Shot {index} is missing first_frame_prompt")
            last_prompt = str(shot.get("last_frame_prompt") or "")
            if mode == "flf" and not last_prompt.strip():
                raise ValueError(f"Shot {index} is missing last_frame_prompt")
            shot["motion_instruction"] = motion
            shot["visual_prompt"] = motion
            shot["video_mode"] = mode
            shot["first_frame_prompt"] = self._with_style(first_prompt)
            shot["last_frame_prompt"] = (
                self._with_style(last_prompt) if mode == "flf" else None
            )
        title = str(script.get("title") or story["title"]).strip()
        description = str(script.get("description") or story["summary"]).strip()
        self.store.update_run(
            self.run_id,
            status="scripted",
            script_json=json.dumps(script),
            title=title,
            description=description,
            last_error=None,
        )

    def _with_style(self, prompt: str) -> str:
        if prompt.startswith(self.config.style_block):
            return prompt
        return f"{self.config.style_block}{prompt}"

    def _shot_mode(self, shot: Mapping[str, Any], index: int) -> str:
        mode = (
            self.config.video_mode
            if self.config.video_mode != "auto"
            else str(shot.get("video_mode", "")).lower()
        )
        if mode not in {"i2v", "flf"}:
            raise ValueError(f"Shot {index} has invalid video_mode: {mode}")
        return mode

    def _frame_specs(self, script: Mapping[str, Any]) -> list[dict[str, Any]]:
        shots = script.get("shots", [])
        if not isinstance(shots, list) or not shots:
            raise ValueError("Cannot resolve frames without scripted shots")
        specs: list[dict[str, Any]] = []
        for shot_index, shot in enumerate(shots, start=1):
            if not isinstance(shot, Mapping):
                raise ValueError(f"Shot {shot_index} must be an object")
            mode = self._shot_mode(shot, shot_index)
            first_prompt = str(shot.get("first_frame_prompt", ""))
            if not first_prompt.strip():
                raise ValueError(f"Shot {shot_index} is missing first_frame_prompt")
            specs.append(
                {
                    "index": len(specs) + 1,
                    "shot_index": shot_index,
                    "role": "first",
                    "prompt": self._with_style(first_prompt),
                    "path": self.run_dir / "frames" / f"shot_{shot_index:02d}.png",
                }
            )
            if mode == "flf":
                last_prompt = str(shot.get("last_frame_prompt") or "")
                if not last_prompt.strip():
                    raise ValueError(f"Shot {shot_index} is missing last_frame_prompt")
                specs.append(
                    {
                        "index": len(specs) + 1,
                        "shot_index": shot_index,
                        "role": "last",
                        "prompt": self._with_style(last_prompt),
                        "path": (
                            self.run_dir / "frames" / f"shot_{shot_index:02d}_last.png"
                        ),
                    }
                )
        return specs

    def _openai_image(self, prompt: str) -> bytes:
        api_key = self.require_key("OPENAI_API_KEY", self.settings.openai_api_key)
        response = self.http.post(
            OPENAI_IMAGE_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": OPENAI_FRAME_MODEL,
                "prompt": prompt,
                "size": "1024x1536",
                "quality": self.settings.openai_image_quality,
                "output_format": "png",
                "n": 1,
            },
        )
        data = self._response_json(response, "OpenAI Images API")
        try:
            image = data["data"][0]
        except (KeyError, IndexError, TypeError) as exc:
            raise RemoteAPIError("OpenAI response did not contain image data") from exc
        if image.get("b64_json"):
            try:
                return base64.b64decode(image["b64_json"], validate=True)
            except (ValueError, TypeError) as exc:
                raise RemoteAPIError(
                    "OpenAI returned invalid base64 image data"
                ) from exc
        if image.get("url"):
            download = self.http.get(image["url"])
            try:
                download.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise RemoteAPIError("Could not download OpenAI image") from exc
            return download.content
        raise RemoteAPIError("OpenAI image response had neither b64_json nor url")

    def _save_frame_gate(self, state: Mapping[str, Any]) -> None:
        self.store.update_run(
            self.run_id,
            frame_gate_json=json.dumps(dict(state)),
            last_error=None,
        )

    def _ensure_frame_gate_state(self, specs: list[dict[str, Any]]) -> dict[str, Any]:
        state = json_load(self.current().get("frame_gate_json"), {})
        frames = state.get("frames") if isinstance(state, Mapping) else None
        expected = [
            (spec["shot_index"], spec["role"], str(spec["path"])) for spec in specs
        ]
        actual = (
            [
                (
                    frame.get("shot_index"),
                    frame.get("role"),
                    frame.get("path"),
                )
                for frame in frames
                if isinstance(frame, Mapping)
            ]
            if isinstance(frames, list)
            else []
        )
        if actual != expected:
            state = {
                "version": 1,
                "chat_id": None,
                "album_message_ids": [],
                "frames": [
                    {
                        "index": spec["index"],
                        "shot_index": spec["shot_index"],
                        "role": spec["role"],
                        "path": str(spec["path"]),
                        "seed": new_seed(),
                        "generation": 0,
                        "status": "generating",
                        "album_message_id": None,
                        "control_message_id": None,
                    }
                    for spec in specs
                ],
            }
            self._save_frame_gate(state)
        return dict(state)

    def _frame_prompt(self, spec: Mapping[str, Any], seed: int) -> str:
        prompt = (
            f"{spec['prompt']}\n\n"
            "Vertical 9:16 composition. No words, typography, logos, "
            "watermarks, captions, or user interface."
        )
        if self.config.negative_prompt:
            prompt += f"\nAvoid: {self.config.negative_prompt}."
        if self.visuals == "cloud":
            prompt += f"\nVariation seed: {seed}."
        return prompt

    def _generate_frame(
        self,
        spec: Mapping[str, Any],
        gate_frame: Mapping[str, Any],
    ) -> None:
        path = Path(spec["path"])
        seed = int(gate_frame["seed"])
        generation = int(gate_frame["generation"])
        if self.visuals == "local":
            self._queue_job(
                (f"frame:{spec['shot_index']}:{spec['role']}:generation:{generation}"),
                "frame",
                {
                    "workflow": self.config.frame_model,
                    "prompt": self._frame_prompt(spec, seed),
                    "negative_prompt": self.config.negative_prompt,
                    "aspect_ratio": "9:16",
                    "resolution": self.config.video_resolution,
                    "steps": self.config.steps_draft,
                    "seed": seed,
                    "output_format": "png",
                },
                {},
                path,
            )
        elif not valid_file(path):
            atomic_write_bytes(
                path,
                self._openai_image(self._frame_prompt(spec, seed)),
            )

    def generate_first_frames(self) -> None:
        script = json_load(self.current()["script_json"], {})
        specs = self._frame_specs(script)
        state = self._ensure_frame_gate_state(specs)
        gate_frames = state["frames"]
        paths = [str(spec["path"]) for spec in specs]
        self.store.update_run(self.run_id, frames_json=json.dumps(paths))
        for spec, gate_frame in zip(specs, gate_frames, strict=True):
            LOG.info(
                "Generating %s frame for shot %s (%s/%s)",
                spec["role"],
                spec["shot_index"],
                spec["index"],
                len(specs),
            )
            self._generate_frame(spec, gate_frame)
            gate_frame["status"] = "pending" if self.frame_gate else "generated"
            self._save_frame_gate(state)
        self.store.update_run(
            self.run_id,
            status="framed",
            frames_json=json.dumps(paths),
            frame_gate_json=json.dumps(state),
            last_error=None,
        )

    def _image_data_uri(self, path: Path) -> str:
        media_type = mimetypes.guess_type(path.name)[0] or "image/png"
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        return f"data:{media_type};base64,{encoded}"

    def _persist_video_requests(self, requests: Mapping[str, str]) -> None:
        self.store.update_run(
            self.run_id, video_requests_json=json.dumps(dict(requests))
        )

    def _start_video(
        self,
        first_frame: Path,
        prompt: str,
        duration_seconds: float,
        last_frame: Path | None = None,
    ) -> str:
        api_key = self.require_key("XAI_API_KEY", self.settings.xai_api_key)
        payload: dict[str, Any] = {
            "model": self.settings.video_model,
            "prompt": prompt,
            "image": {"url": self._image_data_uri(first_frame)},
            "duration": math.ceil(duration_seconds),
            "aspect_ratio": "9:16",
            "resolution": self.config.video_resolution,
        }
        if last_frame is not None:
            payload["last_frame"] = {"url": self._image_data_uri(last_frame)}
        response = self.http.post(
            XAI_VIDEO_CREATE_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
        )
        data = self._response_json(response, "xAI Imagine Video")
        request_id = data.get("request_id")
        if not request_id:
            raise RemoteAPIError("xAI Imagine did not return request_id")
        return str(request_id)

    def _poll_video(self, request_id: str) -> str:
        api_key = self.require_key("XAI_API_KEY", self.settings.xai_api_key)
        deadline = time.monotonic() + self.settings.video_poll_timeout
        while True:
            response = self.http.get(
                XAI_VIDEO_STATUS_URL.format(request_id=request_id),
                headers={"Authorization": f"Bearer {api_key}"},
            )
            data = self._response_json(response, "xAI Imagine Video status")
            state = str(data.get("status", "")).lower()
            if state == "done":
                try:
                    return str(data["video"]["url"])
                except (KeyError, TypeError) as exc:
                    raise RemoteAPIError(
                        "Completed xAI video response did not contain a URL"
                    ) from exc
            if state in {"failed", "expired"}:
                raise RemoteAPIError(
                    f"xAI video request {request_id} ended as {state}: "
                    f"{json.dumps(data)[:1000]}"
                )
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"xAI video request {request_id} exceeded "
                    f"{self.settings.video_poll_timeout} seconds"
                )
            time.sleep(self.settings.video_poll_interval)

    def _download_file(self, url: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        partial = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.part")
        try:
            with self.http.stream("GET", url) as response:
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    raise RemoteAPIError(
                        f"Download failed with HTTP {response.status_code}"
                    ) from exc
                with partial.open("wb") as output:
                    for chunk in response.iter_bytes():
                        output.write(chunk)
            os.replace(partial, destination)
        finally:
            partial.unlink(missing_ok=True)

    def _shot_audio_state(self) -> dict[str, Any]:
        state = json_load(self.current().get("shot_audio_json"), {})
        if not isinstance(state, Mapping):
            return {"version": 1, "shots": {}}
        shots = state.get("shots")
        return {
            **dict(state),
            "version": 1,
            "shots": dict(shots) if isinstance(shots, Mapping) else {},
        }

    def _persist_shot_audio_state(
        self,
        state: Mapping[str, Any],
        **run_values: Any,
    ) -> None:
        self.store.update_run(
            self.run_id,
            shot_audio_json=json.dumps(dict(state)),
            last_error=None,
            **run_values,
        )

    def _shot_timing(self, state: Mapping[str, Any], index: int) -> dict[str, Any]:
        shots = state.get("shots")
        entry = shots.get(str(index)) if isinstance(shots, Mapping) else None
        timing = entry.get("timing") if isinstance(entry, Mapping) else None
        if not isinstance(timing, Mapping):
            raise ValueError(
                f"Shot {index} has no persisted narration timing; "
                "voice the script before generating video"
            )
        return dict(timing)

    def generate_clips(self) -> None:
        row = self.current()
        script = json_load(row["script_json"], {})
        shots = script.get("shots", [])
        if not isinstance(shots, list) or not shots:
            raise ValueError("Cannot generate clips without scripted shots")
        shot_audio_state = self._shot_audio_state()
        specs = self._frame_specs(script)
        frames = [Path(path) for path in json_load(row["frames_json"], [])]
        if len(frames) != len(specs) or not all(valid_file(path) for path in frames):
            raise ValueError("Cannot generate clips without all scripted frames")
        if self.frame_gate:
            gate = self._ensure_frame_gate_state(specs)
            if not all(
                frame.get("status") == "approved" for frame in gate.get("frames", [])
            ):
                raise RuntimeError(
                    "Cannot enqueue video jobs until every frame is approved"
                )
        frames_by_shot: dict[int, dict[str, Path]] = {}
        for spec, path in zip(specs, frames, strict=True):
            frames_by_shot.setdefault(int(spec["shot_index"]), {})[
                str(spec["role"])
            ] = path
        requests: dict[str, str] = json_load(row["video_requests_json"], {})
        clips: list[str] = []
        for index, shot in enumerate(shots, start=1):
            key = str(index)
            path = self.run_dir / "clips" / f"shot_{index:02d}.mp4"
            if not valid_file(path):
                timing = self._shot_timing(shot_audio_state, index)
                mode = self._shot_mode(shot, index)
                shot_frames = frames_by_shot[index]
                first_frame = shot_frames["first"]
                last_frame = shot_frames.get("last")
                motion = str(
                    shot.get("motion_instruction") or shot.get("visual_prompt") or ""
                ).strip()
                if self.visuals == "local":
                    payload, input_files = build_local_video_job(
                        mode=mode,
                        first_frame=first_frame,
                        last_frame=last_frame,
                        prompt=motion,
                        negative_prompt=self.config.negative_prompt,
                        resolution=self.config.video_resolution,
                        steps=self.config.steps_final,
                        seed=new_seed(),
                        duration_seconds=float(timing["clip_seconds"]),
                        frame_count=int(timing["frame_count"]),
                    )
                    self._queue_job(
                        f"video:{index}",
                        "video",
                        payload,
                        input_files,
                        path,
                    )
                else:
                    request_id = requests.get(key)
                    if not request_id:
                        LOG.info("Submitting xAI video %s/%s", index, len(shots))
                        request_id = self._start_video(
                            first_frame,
                            motion,
                            float(timing["clip_seconds"]),
                            last_frame,
                        )
                        requests[key] = request_id
                        self._persist_video_requests(requests)
                    try:
                        video_url = self._poll_video(request_id)
                    except RemoteAPIError as exc:
                        if "ended as failed" in str(exc) or "ended as expired" in str(
                            exc
                        ):
                            requests.pop(key, None)
                            self._persist_video_requests(requests)
                        raise
                    LOG.info("Downloading xAI video %s/%s", index, len(shots))
                    self._download_file(video_url, path)
            clips.append(str(path))
            self.store.update_run(
                self.run_id,
                clips_json=json.dumps(clips),
                video_requests_json=json.dumps(requests),
                last_error=None,
            )
        self.store.update_run(
            self.run_id,
            status="rendered",
            clips_json=json.dumps(clips),
            last_error=None,
        )

    def _queue_job(
        self,
        key: str,
        job_type: str,
        payload: Mapping[str, Any],
        input_files: Mapping[str, str | Path],
        output_path: Path,
    ) -> None:
        if valid_file(output_path):
            return
        row = self.current()
        jobs: dict[str, str] = json_load(row["queue_jobs_json"], {})
        job_id = jobs.get(key)
        if job_id:
            try:
                queued = self.queue.get(job_id)
            except JobQueueError:
                queued = None
            if queued and queued.get("status") == "failed":
                jobs.pop(key, None)
                job_id = None
                self.store.update_run(self.run_id, queue_jobs_json=json.dumps(jobs))
        if not job_id:
            job = self.queue.submit(
                job_type,
                dict(payload),
                input_files=input_files,
            )
            job_id = str(job["id"])
            jobs[key] = job_id
            self.store.update_run(self.run_id, queue_jobs_json=json.dumps(jobs))
        try:
            self.queue.wait(
                job_id,
                output_path=output_path,
                timeout=self.settings.queue_timeout,
            )
        except JobFailedError as exc:
            jobs.pop(key, None)
            self.store.update_run(self.run_id, queue_jobs_json=json.dumps(jobs))
            if is_worker_validation_error(exc):
                match = re.fullmatch(r"video:(\d+)", key)
                label = f"Shot {match.group(1)}" if match else key
                detail = getattr(exc, "worker_error", "") or str(exc)
                raise NonRetryableWorkerError(
                    f"{label} was rejected by worker payload validation; "
                    f"not retrying: {detail}"
                ) from exc
            raise

    def _rewrite_long_narration(
        self,
        *,
        shot: Mapping[str, Any],
        index: int,
        shot_count: int,
        measured_seconds: float,
    ) -> str:
        target_seconds = min(
            NARRATION_SECONDS_MAX,
            self.config.max_clip_seconds - self.config.clip_padding,
        )
        max_words = max(4, math.floor(target_seconds * 2.3))
        result, _ = self._script_chat(
            system_prompt=(
                "You tighten one spoken narration segment without changing its "
                "meaning, factual claims, tone, or relationship to the visuals. "
                "Return only valid JSON."
            ),
            user_prompt=f"""
Shot {index} of {shot_count} measured {measured_seconds:.2f} seconds, exceeding
the {self.config.max_clip_seconds:g}-second clip cap. Rewrite only its narration
so it speaks in at most about {target_seconds:.1f} seconds and no more than
{max_words} words.

Current shot:
{json.dumps(dict(shot), ensure_ascii=False)}

Return exactly:
{{"voiceover_text": "shortened spoken narration"}}
""".strip(),
        )
        if not isinstance(result, Mapping):
            raise ValueError(f"Shot {index} narration retry did not return an object")
        text = str(result.get("voiceover_text") or "").strip()
        if not text:
            raise ValueError(f"Shot {index} narration retry returned empty text")
        return text

    def generate_voiceover_and_captions(self) -> None:
        row = self.current()
        script = json_load(row["script_json"], {})
        shots = script.get("shots", [])
        if not isinstance(shots, list) or not shots:
            raise ValueError("Cannot voice a pipeline without scripted shots")
        state = self._shot_audio_state()
        entries = state["shots"]
        desired_seconds = min(
            NARRATION_SECONDS_MAX,
            max(
                NARRATION_SECONDS_MIN,
                self.config.target_duration_seconds / len(shots),
            ),
        )
        voice_reference = self._voice_reference_payload()
        active_paths: list[Path] = []

        for index, shot in enumerate(shots, start=1):
            if not isinstance(shot, dict):
                raise ValueError(f"Shot {index} must be an object")
            text = str(shot.get("voiceover_text") or "").strip()
            if not text:
                raise ValueError(f"Shot {index} is missing voiceover_text")
            key = str(index)
            entry = entries.get(key)
            if not isinstance(entry, Mapping) or entry.get("text") != text:
                entry = {
                    "index": index,
                    "text": text,
                    "shorten_retry_used": False,
                    "tts_attempts": 0,
                }
            else:
                entry = dict(entry)

            retry_used = bool(entry.get("shorten_retry_used"))
            attempt = 2 if retry_used else 1
            shot_path = (
                self.run_dir / "voiceover" / f"shot_{index:02d}_attempt_{attempt}.wav"
            )
            self._queue_job(
                f"tts:{index}:attempt:{attempt}",
                "tts",
                {
                    "text": text,
                    "shot_index": index,
                    "target_duration_seconds": desired_seconds,
                    "output_format": "wav",
                    **voice_reference,
                },
                {},
                shot_path,
            )
            duration = wav_duration_seconds(shot_path)
            entry.update(
                {
                    "path": str(shot_path),
                    "duration_seconds": duration,
                    "tts_attempts": attempt,
                }
            )
            entries[key] = entry
            self._persist_shot_audio_state(state)

            if duration > self.config.max_clip_seconds and not retry_used:
                LOG.warning(
                    "Shot %s narration is %.2fs, above max_clip_seconds %.2fs; "
                    "asking the script provider for one shorter retry",
                    index,
                    duration,
                    self.config.max_clip_seconds,
                )
                shortened = self._rewrite_long_narration(
                    shot=shot,
                    index=index,
                    shot_count=len(shots),
                    measured_seconds=duration,
                )
                shot["voiceover_text"] = shortened
                text = shortened
                entry.update(
                    {
                        "text": text,
                        "shorten_retry_used": True,
                        "original_duration_seconds": duration,
                        "path": None,
                        "duration_seconds": None,
                        "tts_attempts": 1,
                    }
                )
                entries[key] = entry
                self.store.update_run(
                    self.run_id,
                    script_json=json.dumps(script),
                    shot_audio_json=json.dumps(state),
                    last_error=None,
                )

                attempt = 2
                shot_path = (
                    self.run_dir
                    / "voiceover"
                    / f"shot_{index:02d}_attempt_{attempt}.wav"
                )
                self._queue_job(
                    f"tts:{index}:attempt:{attempt}",
                    "tts",
                    {
                        "text": text,
                        "shot_index": index,
                        "target_duration_seconds": desired_seconds,
                        "output_format": "wav",
                        **voice_reference,
                    },
                    {},
                    shot_path,
                )
                duration = wav_duration_seconds(shot_path)
                entry.update(
                    {
                        "path": str(shot_path),
                        "duration_seconds": duration,
                        "tts_attempts": attempt,
                    }
                )
                entries[key] = entry
                self._persist_shot_audio_state(state)
                if duration > self.config.max_clip_seconds:
                    LOG.warning(
                        "Shot %s narration retry is still %.2fs; accepting the "
                        "%.2fs clip cap and relying on assembly final-frame padding",
                        index,
                        duration,
                        self.config.max_clip_seconds,
                    )

            timing = clip_timing(
                duration,
                clip_padding=self.config.clip_padding,
                max_clip_seconds=self.config.max_clip_seconds,
            )
            entry["timing"] = timing
            entries[key] = entry
            active_paths.append(shot_path)
            self._persist_shot_audio_state(state)

        state["shots"] = {
            str(index): entries[str(index)] for index in range(1, len(shots) + 1)
        }
        narration_seconds = sum(
            float(entry["duration_seconds"]) for entry in state["shots"].values()
        )
        voiceover_path = self.run_dir / "voiceover.wav"
        concatenate_wavs(
            active_paths,
            voiceover_path,
            tail_seconds=ASSEMBLY_TAIL_SECONDS,
        )
        voiceover_seconds = wav_duration_seconds(voiceover_path)
        fingerprint = hashlib.sha256(voiceover_path.read_bytes()).hexdigest()
        state.update(
            {
                "narration_duration_seconds": narration_seconds,
                "voiceover_duration_seconds": voiceover_seconds,
                "tail_seconds": ASSEMBLY_TAIL_SECONDS,
                "voiceover_sha256": fingerprint,
            }
        )
        self._persist_shot_audio_state(
            state,
            voiceover_path=str(voiceover_path),
        )

        captions_path: Path | None = None
        if self.config.caption_style.captions_enabled:
            transcription_path = self.run_dir / "transcription.json"
            captions_path = self.run_dir / "captions.ass"
            if state.get("captions_for_voiceover_sha256") != fingerprint:
                transcription_path.unlink(missing_ok=True)
                captions_path.unlink(missing_ok=True)
            self._queue_job(
                f"transcribe:{fingerprint[:16]}",
                "transcribe",
                {
                    "format": "json",
                    "language": "en",
                    "word_timestamps": True,
                },
                {"audio": voiceover_path},
                transcription_path,
            )
            write_ass_subtitles(
                transcription_path,
                captions_path,
                self.config.caption_style,
            )
            state["captions_for_voiceover_sha256"] = fingerprint
        self.store.update_run(
            self.run_id,
            status="voiced",
            script_json=json.dumps(script),
            shot_audio_json=json.dumps(state),
            voiceover_path=str(voiceover_path),
            captions_path=str(captions_path) if captions_path else None,
            last_error=None,
        )

    def _voice_reference_payload(self) -> dict[str, str]:
        preset_name = self.settings.voice_preset
        preset = VOICE_PRESETS.get(preset_name)
        if preset is None:
            raise ValueError(
                f"Unknown TTS_VOICE_PRESET {preset_name!r}; "
                f"choose one of {sorted(VOICE_PRESETS)}"
            )
        transcript_path = Path(preset["ref_text_file"])
        try:
            transcript = transcript_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ValueError(
                f"Cannot read voice preset transcript {transcript_path}: {exc}"
            ) from exc
        if not transcript:
            raise ValueError(
                f"Voice preset transcript is empty: {transcript_path}"
            )
        return {
            "voice_ref": str(preset["ref_audio"]),
            "voice_ref_text": transcript,
        }

    def assemble(self) -> None:
        row = self.current()
        clips = [Path(path) for path in json_load(row["clips_json"], [])]
        script = json_load(row["script_json"], {})
        shots = script.get("shots", [])
        voiceover = Path(row["voiceover_path"] or "")
        captions_enabled = self.config.caption_style.captions_enabled
        captions = Path(row["captions_path"] or "") if captions_enabled else None
        if (
            not isinstance(shots, list)
            or not shots
            or len(clips) != len(shots)
            or not all(valid_file(path) for path in clips)
            or not valid_file(voiceover)
            or (captions_enabled and not valid_file(captions))
        ):
            raise ValueError("Assemble inputs are incomplete")
        shot_audio_state = self._shot_audio_state()
        try:
            video_seconds = sum(media_duration_seconds(path) for path in clips)
        except ValueError:
            video_seconds = sum(
                float(self._shot_timing(shot_audio_state, index)["generated_seconds"])
                for index in range(1, len(shots) + 1)
            )
        voiceover_seconds = wav_duration_seconds(voiceover)
        timing = assembly_timing(
            video_seconds=video_seconds,
            voiceover_seconds=voiceover_seconds,
        )
        video_filter = f"minterpolate=fps={self.config.fps_out}"
        if timing["video_pad_seconds"] > 0:
            video_filter += (
                f",tpad=stop_mode=clone:stop_duration={timing['video_pad_seconds']:.6f}"
            )
        final_path = self.run_dir / "final.mp4"
        assembled_path = (
            self.run_dir / "assembled_without_captions.mp4"
            if captions_enabled
            else final_path
        )
        inputs: dict[str, Path] = {
            f"clip_{index}": path for index, path in enumerate(clips, start=1)
        }
        inputs["voiceover"] = voiceover
        self._queue_job(
            "assemble:ass-v1" if captions_enabled else "assemble:no-captions-v1",
            "assemble",
            {
                "clip_roles": [f"clip_{index}" for index in range(1, len(clips) + 1)],
                "voiceover_role": "voiceover",
                "clip_timings": [
                    self._shot_timing(shot_audio_state, index)
                    for index in range(1, len(shots) + 1)
                ],
                "aspect_ratio": "9:16",
                "input_fps": WAN_FPS,
                "fps_out": self.config.fps_out,
                "pre_caption_video_filter": video_filter,
                "video_duration_seconds": timing["video_seconds"],
                "voiceover_duration_seconds": timing["voiceover_seconds"],
                "video_pad_seconds": timing["video_pad_seconds"],
                "output_duration_seconds": timing["output_seconds"],
                "tail_room_seconds": timing["tail_seconds"],
                "trim_audio": False,
                "shortest": False,
                "burn_captions": False,
                "output_format": "mp4",
            },
            inputs,
            assembled_path,
        )
        if captions_enabled:
            assert captions is not None
            burn_ass_subtitles(assembled_path, captions, final_path)
        self.store.update_run(
            self.run_id,
            status="assembled",
            final_path=str(final_path),
            last_error=None,
        )

    def _telegram_api(
        self,
        method: str,
        *,
        data: Mapping[str, Any] | None = None,
        files: Mapping[str, Any] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        token = self.require_key("TELEGRAM_BOT_TOKEN", self.settings.telegram_bot_token)
        url = f"https://api.telegram.org/bot{token}/{method}"
        response = self.http.post(
            url,
            data=data,
            files=files,
            timeout=timeout or self.settings.request_timeout,
        )
        result = self._response_json(response, f"Telegram {method}")
        if not result.get("ok"):
            raise RemoteAPIError(
                f"Telegram {method} failed: {result.get('description', result)}"
            )
        return result

    def _frame_keyboard(self, frame: Mapping[str, Any]) -> str:
        suffix = f"{self.run_id}:{int(frame['index'])}:{int(frame['generation'])}"
        return json.dumps(
            {
                "inline_keyboard": [
                    [
                        {
                            "text": "✅ Approve",
                            "callback_data": f"np:f:a:{suffix}",
                        },
                        {
                            "text": "🔄 Regenerate",
                            "callback_data": f"np:f:g:{suffix}",
                        },
                    ]
                ]
            }
        )

    def _send_frame_control(
        self,
        state: dict[str, Any],
        frame: dict[str, Any],
    ) -> None:
        chat_id = str(state["chat_id"])
        message_id = frame.get("control_message_id")
        if message_id:
            self._telegram_api(
                "editMessageReplyMarkup",
                data={
                    "chat_id": chat_id,
                    "message_id": str(message_id),
                    "reply_markup": self._frame_keyboard(frame),
                },
            )
            return
        label = f"Shot {frame['shot_index']} · {str(frame['role']).title()} frame"
        response = self._telegram_api(
            "sendMessage",
            data={
                "chat_id": chat_id,
                "text": label,
                "reply_to_message_id": str(frame["album_message_id"]),
                "reply_markup": self._frame_keyboard(frame),
            },
        )
        frame["control_message_id"] = str(response["result"]["message_id"])
        self._save_frame_gate(state)

    def _send_frame_album(
        self,
        state: dict[str, Any],
        specs: list[dict[str, Any]],
    ) -> None:
        chat_id = self.require_key(
            "TELEGRAM_CHAT_ID or TELEGRAM_DEFAULT_CHAT_ID",
            self.settings.telegram_chat_id,
        )
        state["chat_id"] = str(chat_id)
        message_ids = list(state.get("album_message_ids") or [])
        start = len(message_ids)
        while start < len(specs):
            remaining = len(specs) - start
            batch_size = min(TELEGRAM_MEDIA_GROUP_LIMIT, remaining)
            if remaining - batch_size == 1:
                batch_size -= 1
            batch = specs[start : start + batch_size]
            media = []
            with ExitStack() as stack:
                files: dict[str, Any] = {}
                for spec in batch:
                    attachment = f"frame_{spec['index']}"
                    path = Path(spec["path"])
                    handle = stack.enter_context(path.open("rb"))
                    files[attachment] = (path.name, handle, "image/png")
                    item: dict[str, Any] = {
                        "type": "photo",
                        "media": f"attach://{attachment}",
                    }
                    if spec["index"] == 1:
                        item["caption"] = (
                            "Frame approval · "
                            f"{self.current()['title'] or 'News Short'}"
                            f"\nRun: {self.run_id}"
                        )[:1024]
                    media.append(item)
                if len(batch) == 1:
                    item = media[0]
                    data = {
                        "chat_id": chat_id,
                        "photo": item["media"],
                    }
                    if item.get("caption"):
                        data["caption"] = item["caption"]
                    response = self._telegram_api(
                        "sendPhoto",
                        data=data,
                        files=files,
                        timeout=max(self.settings.request_timeout, 300),
                    )
                    messages = [response.get("result")]
                else:
                    response = self._telegram_api(
                        "sendMediaGroup",
                        data={"chat_id": chat_id, "media": json.dumps(media)},
                        files=files,
                        timeout=max(self.settings.request_timeout, 300),
                    )
                    messages = response.get("result")
            if not isinstance(messages, list) or len(messages) != len(batch):
                raise RemoteAPIError(
                    "Telegram frame upload returned an unexpected message count"
                )
            batch_ids = [str(message["message_id"]) for message in messages]
            message_ids.extend(batch_ids)
            for frame, message_id in zip(
                state["frames"][start : start + len(batch)],
                batch_ids,
                strict=True,
            ):
                frame["album_message_id"] = message_id
            start += len(batch)
            state["album_message_ids"] = message_ids
            state["requested_at"] = utc_now()
            self._save_frame_gate(state)

    def _edit_frame_album_item(
        self,
        state: Mapping[str, Any],
        frame: Mapping[str, Any],
    ) -> None:
        path = Path(str(frame["path"]))
        with path.open("rb") as image:
            self._telegram_api(
                "editMessageMedia",
                data={
                    "chat_id": str(state["chat_id"]),
                    "message_id": str(frame["album_message_id"]),
                    "media": json.dumps({"type": "photo", "media": "attach://frame"}),
                },
                files={"frame": (path.name, image, "image/png")},
                timeout=max(self.settings.request_timeout, 300),
            )

    def request_frame_approval(self) -> None:
        script = json_load(self.current()["script_json"], {})
        specs = self._frame_specs(script)
        if not all(valid_file(spec["path"]) for spec in specs):
            raise ValueError("Cannot request approval with missing frames")
        state = self._ensure_frame_gate_state(specs)
        if len(state.get("album_message_ids") or []) < len(specs):
            self._send_frame_album(state, specs)
        for frame in state["frames"]:
            if frame.get("status") == "pending" and not frame.get("control_message_id"):
                self._send_frame_control(state, frame)

    def _consume_frame_callback(self, update: Mapping[str, Any]) -> str | None:
        callback = update.get("callback_query")
        if not isinstance(callback, Mapping):
            return None
        match = re.fullmatch(
            r"np:f:(a|g):([0-9a-fA-F-]{36}):(\d+):(\d+)",
            str(callback.get("data", "")),
        )
        if not match or match.group(2) != self.run_id:
            return None
        frame_index = int(match.group(3))
        generation = int(match.group(4))
        state = json_load(self.current().get("frame_gate_json"), {})
        frames = state.get("frames", []) if isinstance(state, Mapping) else []
        frame = next(
            (
                item
                for item in frames
                if isinstance(item, Mapping)
                and int(item.get("index", -1)) == frame_index
            ),
            None,
        )
        message = callback.get("message")
        chat = message.get("chat") if isinstance(message, Mapping) else None
        if (
            frame is None
            or frame.get("status") != "pending"
            or int(frame.get("generation", -1)) != generation
            or not isinstance(chat, Mapping)
            or str(chat.get("id", "")) != str(state.get("chat_id"))
            or str(message.get("message_id", ""))
            != str(frame.get("control_message_id"))
        ):
            return None
        action = {"a": "approve", "g": "regenerate"}[match.group(1)]
        user = callback.get("from")
        user_id = str(user.get("id")) if isinstance(user, Mapping) else None
        self.store.record_frame_approval(
            self.run_id,
            frame_index,
            generation,
            action,
            int(update["update_id"]),
            user_id,
        )
        self._answer_callback(
            str(callback.get("id", "")),
            "Approved" if action == "approve" else "Regenerating",
        )
        return action

    def wait_for_frame_approval(self) -> dict[str, Any] | None:
        pending = self.store.pending_frame_approval(self.run_id)
        if pending is not None:
            return pending
        self.require_key("TELEGRAM_BOT_TOKEN", self.settings.telegram_bot_token)
        deadline = (
            time.monotonic() + self.settings.approval_wait_timeout
            if self.settings.approval_wait_timeout > 0
            else None
        )
        offset = int(self.store.get_setting("telegram_update_offset", "0"))
        while deadline is None or time.monotonic() < deadline:
            result = self._telegram_api(
                "getUpdates",
                data={
                    "offset": str(offset),
                    "timeout": str(self.settings.telegram_poll_timeout),
                    "allowed_updates": json.dumps(["callback_query"]),
                },
                timeout=self.settings.telegram_poll_timeout + 10,
            )
            for update in result.get("result", []):
                update_id = int(update["update_id"])
                action = self._consume_frame_callback(update)
                offset = max(offset, update_id + 1)
                self.store.set_setting("telegram_update_offset", str(offset))
                if action:
                    return self.store.pending_frame_approval(self.run_id)
        return None

    def _process_frame_approval(self, decision: Mapping[str, Any]) -> None:
        state = json_load(self.current().get("frame_gate_json"), {})
        frame = next(
            (
                item
                for item in state.get("frames", [])
                if int(item.get("index", -1)) == int(decision["frame_index"])
            ),
            None,
        )
        if (
            frame is None
            or frame.get("status") != "pending"
            or int(frame.get("generation", -1)) != int(decision["generation"])
        ):
            self.store.mark_frame_approval_handled(int(decision["id"]))
            return
        if decision["action"] == "approve":
            frame["status"] = "approved"
            frame["approved_at"] = utc_now()
            self._save_frame_gate(state)
            self.store.mark_frame_approval_handled(int(decision["id"]))
            self._remove_keyboard(
                str(state["chat_id"]), str(frame["control_message_id"])
            )
            return
        Path(str(frame["path"])).unlink(missing_ok=True)
        frame["generation"] = int(frame["generation"]) + 1
        frame["seed"] = new_seed(int(frame["seed"]))
        frame["status"] = "generating"
        frame["approved_at"] = None
        self._save_frame_gate(state)
        self.store.mark_frame_approval_handled(int(decision["id"]))

    def _resume_frame_regenerations(self) -> None:
        script = json_load(self.current()["script_json"], {})
        specs = self._frame_specs(script)
        specs_by_index = {int(spec["index"]): spec for spec in specs}
        state = self._ensure_frame_gate_state(specs)
        for frame in state["frames"]:
            if frame.get("status") != "generating":
                continue
            spec = specs_by_index[int(frame["index"])]
            LOG.info(
                "Regenerating shot %s %s frame with seed %s",
                frame["shot_index"],
                frame["role"],
                frame["seed"],
            )
            self._generate_frame(spec, frame)
            self._edit_frame_album_item(state, frame)
            self._send_frame_control(state, frame)
            frame["status"] = "pending"
            self._save_frame_gate(state)

    def run_frame_gate(self) -> bool:
        state = json_load(self.current().get("frame_gate_json"), {})
        if state.get("album_message_ids"):
            self._resume_frame_regenerations()
        self.request_frame_approval()
        while True:
            self._resume_frame_regenerations()
            state = json_load(self.current().get("frame_gate_json"), {})
            if all(
                frame.get("status") == "approved" for frame in state.get("frames", [])
            ):
                return True
            decision = self.wait_for_frame_approval()
            if decision is None:
                return False
            self._process_frame_approval(decision)

    def _approval_keyboard(self) -> str:
        return json.dumps(
            {
                "inline_keyboard": [
                    [
                        {
                            "text": "✅ Approve",
                            "callback_data": f"np:a:{self.run_id}",
                        },
                        {
                            "text": "❌ Reject",
                            "callback_data": f"np:r:{self.run_id}",
                        },
                    ],
                    [
                        {
                            "text": "🔄 Regenerate",
                            "callback_data": f"np:g:{self.run_id}",
                        }
                    ],
                ]
            }
        )

    def request_approval(self) -> None:
        row = self.current()
        if row["status"] == "pending_approval" and row["telegram_message_id"]:
            return
        chat_id = self.require_key(
            "TELEGRAM_CHAT_ID or TELEGRAM_DEFAULT_CHAT_ID",
            self.settings.telegram_chat_id,
        )
        final_path = Path(row["final_path"] or "")
        if not valid_file(final_path):
            raise ValueError("Final MP4 is missing")
        caption = (
            f"{row['title'] or 'News Short'}\n\n"
            f"{row['description'] or ''}\n\n"
            f"Run: {self.run_id}"
        )[:1000]
        common = {
            "chat_id": chat_id,
            "caption": caption,
            "reply_markup": self._approval_keyboard(),
        }
        with final_path.open("rb") as video:
            try:
                response = self._telegram_api(
                    "sendVideo",
                    data={**common, "supports_streaming": "true"},
                    files={"video": (final_path.name, video, "video/mp4")},
                    timeout=max(self.settings.request_timeout, 300),
                )
            except RemoteAPIError:
                video.seek(0)
                response = self._telegram_api(
                    "sendDocument",
                    data=common,
                    files={"document": (final_path.name, video, "video/mp4")},
                    timeout=max(self.settings.request_timeout, 300),
                )
        message_id = str(response["result"]["message_id"])
        self.store.update_run(
            self.run_id,
            status="pending_approval",
            telegram_chat_id=str(chat_id),
            telegram_message_id=message_id,
            last_error=None,
        )

    def _answer_callback(self, callback_id: str, text: str) -> None:
        try:
            self._telegram_api(
                "answerCallbackQuery",
                data={"callback_query_id": callback_id, "text": text},
            )
        except Exception:
            LOG.warning("Could not answer Telegram callback", exc_info=True)

    def _remove_keyboard(self, chat_id: str, message_id: str) -> None:
        try:
            self._telegram_api(
                "editMessageReplyMarkup",
                data={
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "reply_markup": json.dumps({"inline_keyboard": []}),
                },
            )
        except Exception:
            LOG.warning("Could not remove Telegram approval buttons", exc_info=True)

    def _consume_callback(self, update: Mapping[str, Any]) -> str | None:
        callback = update.get("callback_query")
        if not isinstance(callback, Mapping):
            return None
        data = str(callback.get("data", ""))
        match = re.fullmatch(
            r"np:(a|r|g):([0-9a-fA-F-]{36})",
            data,
        )
        if not match or match.group(2) != self.run_id:
            return None
        row = self.current()
        message = callback.get("message")
        if not isinstance(message, Mapping):
            return None
        chat = message.get("chat")
        if not isinstance(chat, Mapping):
            return None
        chat_id = str(chat.get("id", ""))
        message_id = str(message.get("message_id", ""))
        if chat_id != str(row["telegram_chat_id"]) or message_id != str(
            row["telegram_message_id"]
        ):
            return None
        actions = {"a": "approve", "r": "reject", "g": "regenerate"}
        action = actions[match.group(1)]
        update_id = int(update["update_id"])
        user = callback.get("from")
        user_id = str(user.get("id")) if isinstance(user, Mapping) else None
        self.store.record_approval(self.run_id, action, update_id, user_id)
        self._answer_callback(str(callback.get("id", "")), action.title())
        self._remove_keyboard(chat_id, message_id)
        return action

    def wait_for_approval(self) -> dict[str, Any] | None:
        pending = self.store.pending_approval(self.run_id)
        if pending is not None:
            return pending
        self.require_key("TELEGRAM_BOT_TOKEN", self.settings.telegram_bot_token)
        deadline = (
            time.monotonic() + self.settings.approval_wait_timeout
            if self.settings.approval_wait_timeout > 0
            else None
        )
        offset = int(self.store.get_setting("telegram_update_offset", "0"))
        while deadline is None or time.monotonic() < deadline:
            result = self._telegram_api(
                "getUpdates",
                data={
                    "offset": str(offset),
                    "timeout": str(self.settings.telegram_poll_timeout),
                    "allowed_updates": json.dumps(["callback_query"]),
                },
                timeout=self.settings.telegram_poll_timeout + 10,
            )
            for update in result.get("result", []):
                update_id = int(update["update_id"])
                action = self._consume_callback(update)
                offset = max(offset, update_id + 1)
                self.store.set_setting("telegram_update_offset", str(offset))
                if action:
                    return self.store.pending_approval(self.run_id)
        return None

    def publish(self) -> dict[str, Any]:
        """Publication integration placeholder; intentionally no external upload."""
        row = self.current()
        receipt = {
            "stub": True,
            "run_id": self.run_id,
            "title": row["title"],
            "file": row["final_path"],
            "approved_at": utc_now(),
        }
        atomic_write_bytes(
            self.run_dir / "publish.json",
            json.dumps(receipt, indent=2).encode("utf-8"),
        )
        return receipt

    def reset_for_regeneration(self) -> None:
        row = self.current()
        shot_audio = json_load(row.get("shot_audio_json"), {})
        shot_audio_paths = (
            [
                entry.get("path")
                for entry in shot_audio.get("shots", {}).values()
                if isinstance(entry, Mapping)
            ]
            if isinstance(shot_audio, Mapping)
            and isinstance(shot_audio.get("shots"), Mapping)
            else []
        )
        voiceover_attempts = list(
            (self.run_dir / "voiceover").glob("shot_*_attempt_*.wav")
        )
        paths = [
            *json_load(row["frames_json"], []),
            *json_load(row["clips_json"], []),
            *shot_audio_paths,
            *voiceover_attempts,
            row["voiceover_path"],
            row["captions_path"],
            row["final_path"],
            self.run_dir / "transcription.json",
            self.run_dir / "assembled_without_captions.mp4",
        ]
        for raw_path in paths:
            if raw_path:
                Path(raw_path).unlink(missing_ok=True)
        self.store.update_run(
            self.run_id,
            status="scripted",
            frames_json="[]",
            frame_gate_json="{}",
            clips_json="[]",
            video_requests_json="{}",
            queue_jobs_json="{}",
            shot_audio_json="{}",
            voiceover_path=None,
            captions_path=None,
            final_path=None,
            telegram_chat_id=None,
            telegram_message_id=None,
            last_error=None,
        )

    def repair_state(self) -> None:
        """Roll state back to the last durable artifact after manual file loss."""
        row = self.current()
        status = row["status"]
        if status in {None, "fetched", "scripted", "published", "rejected"}:
            return
        frames = json_load(row["frames_json"], [])
        clips = json_load(row["clips_json"], [])
        script = json_load(row["script_json"], {})
        shots = script.get("shots", []) if isinstance(script, Mapping) else []
        shot_count = len(shots) if isinstance(shots, list) else 0
        shot_audio = json_load(row.get("shot_audio_json"), {})
        shot_entries = (
            shot_audio.get("shots", {})
            if isinstance(shot_audio, Mapping)
            and isinstance(shot_audio.get("shots"), Mapping)
            else {}
        )
        narration_complete = (
            shot_count > 0
            and len(shot_entries) == shot_count
            and all(
                isinstance(shot_entries.get(str(index)), Mapping)
                and valid_file(shot_entries[str(index)].get("path"))
                and float(shot_entries[str(index)].get("duration_seconds") or 0) > 0
                and isinstance(shot_entries[str(index)].get("timing"), Mapping)
                for index in range(1, shot_count + 1)
            )
            and valid_file(row["voiceover_path"])
            and (
                not self.config.caption_style.captions_enabled
                or valid_file(row["captions_path"])
            )
        )
        if STATUS_INDEX[status] >= STATUS_INDEX["voiced"] and not narration_complete:
            for path in clips:
                if path:
                    Path(path).unlink(missing_ok=True)
            jobs = json_load(row["queue_jobs_json"], {})
            if not isinstance(jobs, Mapping):
                jobs = {}
            jobs = {
                key: value
                for key, value in jobs.items()
                if not key.startswith(("video:", "assemble"))
            }
            self.store.update_run(
                self.run_id,
                status="scripted",
                clips_json="[]",
                video_requests_json="{}",
                queue_jobs_json=json.dumps(jobs),
                voiceover_path=None,
                captions_path=None,
                final_path=None,
            )
            return
        try:
            expected_frames = self._frame_specs(script)
        except ValueError:
            expected_frames = []
        gate = json_load(row.get("frame_gate_json"), {})
        regenerating_paths = {
            str(frame.get("path"))
            for frame in gate.get("frames", [])
            if isinstance(frame, Mapping) and frame.get("status") == "generating"
        }
        frames_complete = len(frames) == len(expected_frames) and all(
            valid_file(path) or str(path) in regenerating_paths for path in frames
        )
        if STATUS_INDEX[status] >= STATUS_INDEX["framed"] and (not frames_complete):
            jobs = json_load(row["queue_jobs_json"], {})
            if not isinstance(jobs, Mapping):
                jobs = {}
            jobs = {
                key: value
                for key, value in jobs.items()
                if not key.startswith(("frame:", "video:", "assemble"))
            }
            self.store.update_run(
                self.run_id,
                status="voiced",
                frames_json="[]",
                frame_gate_json="{}",
                clips_json="[]",
                video_requests_json="{}",
                queue_jobs_json=json.dumps(jobs),
                final_path=None,
            )
            return
        if STATUS_INDEX[status] >= STATUS_INDEX["rendered"] and (
            len(clips) != shot_count or not all(valid_file(path) for path in clips)
        ):
            self.store.update_run(
                self.run_id,
                status="framed",
                clips_json="[]",
                video_requests_json="{}",
                final_path=None,
            )
            return
        if STATUS_INDEX[status] >= STATUS_INDEX["assembled"] and not valid_file(
            row["final_path"]
        ):
            self.store.update_run(
                self.run_id,
                status="rendered",
                final_path=None,
                telegram_chat_id=None,
                telegram_message_id=None,
            )

    def run_stage(self, stage: str, operation: Callable[[], None]) -> None:
        for retry in range(1, 4):
            attempt_id = self.store.start_attempt(self.run_id, stage)
            LOG.info("Stage %s attempt %s/3", stage, retry)
            try:
                operation()
            except (KeyboardInterrupt, SystemExit):
                self.store.finish_attempt(
                    attempt_id, "failed", "interrupted by operator"
                )
                raise
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                self.store.finish_attempt(attempt_id, "failed", error)
                self.store.update_run(self.run_id, last_error=error)
                if isinstance(exc, NonRetryableWorkerError):
                    LOG.error("Stage %s failed permanently: %s", stage, error)
                    raise
                if retry == 3:
                    raise
                delay = self.settings.retry_base_seconds * (2 ** (retry - 1))
                LOG.warning(
                    "Stage %s failed (%s); retrying in %.1fs",
                    stage,
                    error,
                    delay,
                )
                time.sleep(delay)
            else:
                self.store.finish_attempt(attempt_id, "succeeded")
                return

    def run(self, *, dry_run: bool = False) -> dict[str, Any]:
        self.repair_state()
        while True:
            row = self.current()
            status = row["status"]
            if status is None:
                self.run_stage("fetch_story", self.fetch_story)
                continue
            if status == "fetched":
                self.run_stage("write_script", self.write_script)
                continue
            if status == "scripted":
                self.run_stage(
                    "generate_voiceover_and_captions",
                    self.generate_voiceover_and_captions,
                )
                continue
            if status == "voiced":
                self.run_stage("generate_first_frames", self.generate_first_frames)
                continue
            if status == "framed":
                if dry_run:
                    LOG.info(
                        "Dry run complete after narration and frame generation; "
                        "run %s is framed",
                        self.run_id,
                    )
                    return self.current()
                if self.frame_gate:
                    gate_holder: dict[str, bool] = {}

                    def gate() -> None:
                        gate_holder["approved"] = self.run_frame_gate()

                    self.run_stage("frame_gate", gate)
                    if not gate_holder.get("approved"):
                        LOG.info("Frame approval wait timed out; run remains framed")
                        return self.current()
                self.run_stage("generate_clips", self.generate_clips)
                continue
            if status == "rendered":
                self.run_stage("assemble", self.assemble)
                continue
            if status == "assembled":
                self.run_stage("request_approval", self.request_approval)
                continue
            if status == "pending_approval":
                decision_holder: dict[str, Any] = {}

                def wait() -> None:
                    decision_holder["decision"] = self.wait_for_approval()

                self.run_stage("approval_gate", wait)
                decision = decision_holder.get("decision")
                if decision is None:
                    LOG.info("Approval wait timed out; run remains pending")
                    return self.current()
                action = decision["action"]
                if action == "approve":
                    receipt = self.publish()
                    self.store.update_run(
                        self.run_id,
                        status="published",
                        publish_json=json.dumps(receipt),
                        published_at=utc_now(),
                        last_error=None,
                    )
                elif action == "reject":
                    self.store.update_run(
                        self.run_id, status="rejected", last_error=None
                    )
                elif action == "regenerate":
                    self.reset_for_regeneration()
                self.store.mark_approval_handled(int(decision["id"]))
                continue
            if status in TERMINAL_STATUSES:
                return row
            raise RuntimeError(f"Unsupported pipeline state: {status}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create a crash-resumable, narration-timed vertical explainer video."
        )
    )
    parser.add_argument(
        "--topic",
        default=None,
        help="Editorial topic for a new run.",
    )
    parser.add_argument(
        "--run-id",
        help="Resume a specific run ID.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show read-only status for a specific or current/most-recent run.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Show status as JSON (implies --status).",
    )
    parser.add_argument(
        "--new",
        action="store_true",
        help="Start a new run instead of resuming the latest unfinished run.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Stop after frame generation, before frame approval.",
    )
    parser.add_argument(
        "--visuals",
        choices=sorted(VISUAL_BACKENDS),
        default="local",
        help="Visual generation backend (default: local).",
    )
    parser.add_argument(
        "--no-frame-gate",
        action="store_false",
        dest="frame_gate",
        default=True,
        help="Skip per-frame Telegram approval.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_PRESET_PATH,
        help=f"Pipeline preset YAML (default: {DEFAULT_PRESET_PATH}).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        help="Override the SQLite database path.",
    )
    parser.add_argument(
        "--work-root",
        type=Path,
        help="Override the run artifact directory.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser


def choose_run(
    store: StateStore,
    *,
    run_id: str | None,
    topic: str | None,
    new: bool,
) -> dict[str, Any]:
    if run_id:
        if new:
            raise ValueError("--run-id and --new cannot be used together")
        return store.get_run(run_id)
    if not new:
        resumable = store.latest_resumable()
        if resumable is not None:
            LOG.info(
                "Resuming run %s at status %s",
                resumable["id"],
                resumable["status"] or "new",
            )
            return resumable
    selected_topic = (
        topic
        or os.getenv("NEWS_TOPIC")
        or "how artificial intelligence turns data and compute into useful tools"
    )
    return store.create_run(selected_topic)


def configure_run(
    store: StateStore,
    run: Mapping[str, Any],
    *,
    config: PipelineConfig,
    visuals: str,
    frame_gate: bool,
    preset_path: Path | None = None,
) -> tuple[dict[str, Any], PipelineConfig, str, bool]:
    stored = json_load(run.get("run_config_json"), {})
    if isinstance(stored, Mapping) and stored.get("preset"):
        run_config = PipelineConfig.from_mapping(stored["preset"])
        run_visuals = str(stored.get("visuals", "local"))
        if run_visuals not in VISUAL_BACKENDS:
            raise ValueError(
                f"Run {run['id']} has invalid visuals backend: {run_visuals}"
            )
        return dict(run), run_config, run_visuals, bool(stored.get("frame_gate", True))
    snapshot = {
        "preset": config.to_dict(),
        "visuals": visuals,
        "frame_gate": frame_gate,
        "preset_path": str(preset_path) if preset_path else None,
        "loaded_at": utc_now(),
    }
    updated = store.update_run(str(run["id"]), run_config_json=json.dumps(snapshot))
    return updated, config, visuals, frame_gate


def validate_startup(
    settings: Settings,
    visuals: str,
    config: PipelineConfig,
) -> None:
    provider = config.script_provider
    if provider.api_key_env and not script_provider_api_key(settings, provider):
        raise RuntimeError(
            f"{provider.api_key_env} is required by the selected script_provider; "
            "refusing to start before any API spend or queue activity."
        )
    if visuals == "cloud" and not settings.xai_api_key:
        raise RuntimeError(
            "XAI_API_KEY is required for --visuals cloud; refusing to start "
            "before any API spend or queue activity."
        )


def _frame_gate_summary(run: Mapping[str, Any]) -> dict[str, Any]:
    run_config = json_load(run.get("run_config_json"), {})
    enabled = (
        run_config.get("frame_gate")
        if isinstance(run_config, Mapping)
        and isinstance(run_config.get("frame_gate"), bool)
        else None
    )
    gate = json_load(run.get("frame_gate_json"), {})
    raw_frames = gate.get("frames", []) if isinstance(gate, Mapping) else []
    script = json_load(run.get("script_json"), {})
    scripted_shots = script.get("shots", []) if isinstance(script, Mapping) else []
    if isinstance(scripted_shots, list) and scripted_shots:
        shot_count = len(scripted_shots)
    else:
        preset = run_config.get("preset", {}) if isinstance(run_config, Mapping) else {}
        target_seconds = (
            preset.get(
                "target_duration_seconds", PipelineConfig().target_duration_seconds
            )
            if isinstance(preset, Mapping)
            else PipelineConfig().target_duration_seconds
        )
        try:
            shot_count = target_shot_count(float(target_seconds))
        except (TypeError, ValueError):
            shot_count = target_shot_count(PipelineConfig().target_duration_seconds)
    grouped: dict[int, list[dict[str, Any]]] = {
        shot_index: [] for shot_index in range(1, shot_count + 1)
    }
    if isinstance(raw_frames, list):
        ordered_frames = sorted(
            (frame for frame in raw_frames if isinstance(frame, Mapping)),
            key=lambda frame: (
                frame.get("index")
                if isinstance(frame.get("index"), int)
                else sys.maxsize
            ),
        )
        for frame in ordered_frames:
            try:
                shot_index = int(frame.get("shot_index"))
            except (TypeError, ValueError):
                continue
            if shot_index not in grouped:
                continue
            grouped[shot_index].append(
                {
                    "role": str(frame.get("role") or "frame"),
                    "state": str(frame.get("status") or "unknown"),
                    "generation": frame.get("generation"),
                }
            )

    shots = []
    for shot_index, frames in grouped.items():
        states = {frame["state"] for frame in frames}
        shot_state = states.pop() if len(states) == 1 else "mixed"
        if not frames:
            shot_state = "not_started"
        shots.append(
            {
                "shot": shot_index,
                "state": shot_state,
                "frames": frames,
            }
        )
    return {"enabled": enabled, "shots": shots}


def build_status_summary(
    store: StateStore,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    run = store.get_run(run_id) if run_id else store.current_or_latest()
    if run is None:
        raise ValueError("No pipeline runs found")
    attempts = store.attempts_for_run(str(run["id"]))
    running = (
        [attempts[-1]] if attempts and attempts[-1]["outcome"] == "running" else []
    )
    current_stage = (
        str(running[-1]["stage"]) if running else str(run["status"] or "not_started")
    )
    return {
        "run_id": str(run["id"]),
        "topic": str(run["topic"]),
        "state": str(run["status"] or "not_started"),
        "current_stage": current_stage,
        "frame_gate": _frame_gate_summary(run),
        "jobs": {
            "completed": [
                attempt for attempt in attempts if attempt["outcome"] == "succeeded"
            ],
            "failed": [
                attempt for attempt in attempts if attempt["outcome"] == "failed"
            ],
            "running": running,
        },
        "timestamps": {
            "created_at": run["created_at"],
            "updated_at": run["updated_at"],
            "published_at": run["published_at"],
        },
        "last_error": run["last_error"],
    }


def format_status_summary(summary: Mapping[str, Any]) -> str:
    timestamps = summary["timestamps"]
    frame_gate = summary["frame_gate"]
    enabled = frame_gate["enabled"]
    enabled_label = "unknown" if enabled is None else ("enabled" if enabled else "off")
    lines = [
        f"Run ID: {summary['run_id']}",
        f"Topic: {summary['topic']}",
        f"State: {summary['state']}",
        f"Current stage: {summary['current_stage']}",
        "Timestamps:",
        f"  Created: {timestamps['created_at']}",
        f"  Updated: {timestamps['updated_at']}",
        f"  Published: {timestamps['published_at'] or '-'}",
        f"Frame gate: {enabled_label}",
    ]
    for shot in frame_gate["shots"]:
        frame_details = ", ".join(
            (
                f"{frame['role']}={frame['state']}"
                + (
                    f" (generation {frame['generation']})"
                    if frame["generation"] is not None
                    else ""
                )
            )
            for frame in shot["frames"]
        )
        suffix = f" [{frame_details}]" if frame_details else ""
        lines.append(f"  Shot {shot['shot']}: {shot['state']}{suffix}")

    for label, key in (
        ("Completed jobs", "completed"),
        ("Failed jobs", "failed"),
        ("Running jobs", "running"),
    ):
        jobs = summary["jobs"][key]
        lines.append(f"{label} ({len(jobs)}):")
        if not jobs:
            lines.append("  none")
            continue
        for job in jobs:
            finished = job["finished_at"] or "running"
            detail = (
                f"  {job['stage']} attempt {job['attempt']}: "
                f"{job['started_at']} -> {finished}"
            )
            if job["error"]:
                detail += f" ({job['error']})"
            lines.append(detail)
    if summary["last_error"]:
        lines.append(f"Last error: {summary['last_error']}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    project_root = Path(__file__).resolve().parent
    load_environment(project_root)
    if args.status or args.json:
        database_path = args.db or Path(
            os.getenv("NEWS_PIPELINE_DB", project_root / "data" / "pipeline.sqlite3")
        )
        store = StateStore(database_path, read_only=True)
        try:
            summary = build_status_summary(store, run_id=args.run_id)
        except (sqlite3.Error, ValueError) as exc:
            LOG.error("Could not read pipeline status: %s", exc)
            return 1
        print(
            json.dumps(summary, indent=2)
            if args.json
            else format_status_summary(summary)
        )
        return 0
    try:
        preset = PipelineConfig.load(args.config.expanduser().resolve())
    except ValueError as exc:
        parser.error(str(exc))
    settings = Settings.from_environment(
        project_root,
        database_path=args.db,
        work_root=args.work_root,
    )
    if args.new:
        try:
            validate_startup(settings, args.visuals, preset)
        except RuntimeError as exc:
            LOG.error("%s", exc)
            return 2
    store = StateStore(settings.database_path)
    try:
        run = choose_run(
            store,
            run_id=args.run_id,
            topic=args.topic,
            new=args.new,
        )
        run, preset, visuals, frame_gate = configure_run(
            store,
            run,
            config=preset,
            visuals=args.visuals,
            frame_gate=args.frame_gate,
            preset_path=args.config.expanduser().resolve(),
        )
    except ValueError as exc:
        parser.error(str(exc))
    try:
        validate_startup(settings, visuals, preset)
    except RuntimeError as exc:
        LOG.error("%s", exc)
        return 2
    pipeline = NewsPipeline(
        settings,
        store,
        run,
        config=preset,
        visuals=visuals,
        frame_gate=frame_gate,
    )
    try:
        result = pipeline.run(dry_run=args.dry_run)
    except Exception:
        LOG.exception("Pipeline run %s stopped with an error", run["id"])
        return 1
    finally:
        pipeline.close()
    print(
        json.dumps(
            {
                "id": result["id"],
                "status": result["status"],
                "topic": result["topic"],
                "final_path": result["final_path"],
                "last_error": result["last_error"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
