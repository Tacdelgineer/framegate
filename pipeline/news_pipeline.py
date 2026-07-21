#!/usr/bin/env python3
"""Crash-resumable short-form news video pipeline.

Expensive model calls remain on the VPS. GPU/media stages are transferred to a
DGX worker through the Tailscale job queue.
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import mimetypes
import os
import re
import sqlite3
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

import httpx
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
    "framed",
    "rendered",
    "voiced",
    "assembled",
    "pending_approval",
    "published",
    "rejected",
)
STATUS_INDEX = {status: index for index, status in enumerate(STATUSES)}
TERMINAL_STATUSES = {"published", "rejected"}
SHOT_COUNT = 5

XAI_CHAT_URL = "https://api.x.ai/v1/chat/completions"
XAI_VIDEO_CREATE_URL = "https://api.x.ai/v1/videos/generations"
XAI_VIDEO_STATUS_URL = "https://api.x.ai/v1/videos/{request_id}"
OPENAI_IMAGE_URL = "https://api.openai.com/v1/images/generations"

VOICE_PRESETS = {
    "alireza": {
        "ref_audio": "/home/xxfactionsxx/content-factory/assets/alireza.wav",
        "ref_text_file": MONOREPO_ROOT / "assets" / "alireza.txt",
    },
}


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


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


def atomic_write_bytes(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f".{path.name}.{uuid.uuid4().hex}.part")
    try:
        partial.write_bytes(content)
        os.replace(partial, path)
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
    xai_text_model: str
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
            queue_url=os.getenv(
                "JOB_QUEUE_URL", "http://100.123.208.90:8787"
            ).rstrip("/"),
            xai_api_key=os.getenv("XAI_API_KEY", ""),
            openai_api_key=os.getenv("OPENAI_API_KEY", ""),
            xai_text_model=os.getenv("XAI_TEXT_MODEL", "grok-4.5"),
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


RUN_COLUMNS = {
    "status",
    "story_json",
    "script_json",
    "frames_json",
    "clips_json",
    "video_requests_json",
    "queue_jobs_json",
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
    def __init__(self, database_path: Path):
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(self) -> sqlite3.Connection:
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
                    clips_json TEXT,
                    video_requests_json TEXT,
                    queue_jobs_json TEXT,
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

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )

    def create_run(self, topic: str) -> dict[str, Any]:
        run_id = str(uuid.uuid4())
        now = utc_now()
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO runs(
                    id, topic, status, created_at, updated_at,
                    frames_json, clips_json, video_requests_json, queue_jobs_json
                ) VALUES (?, ?, NULL, ?, ?, '[]', '[]', '{}', '{}')
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


class RemoteAPIError(RuntimeError):
    pass


class NewsPipeline:
    def __init__(
        self,
        settings: Settings,
        store: StateStore,
        run: Mapping[str, Any],
        *,
        http_client: httpx.Client | None = None,
        queue_client: JobQueueClient | None = None,
    ):
        self.settings = settings
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

    def _xai_chat(
        self,
        *,
        system_prompt: str,
        user_prompt: str,
        live_search: bool,
    ) -> tuple[Any, dict[str, Any]]:
        api_key = self.require_key("XAI_API_KEY", self.settings.xai_api_key)
        payload: dict[str, Any] = {
            "model": self.settings.xai_text_model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.2,
        }
        if live_search:
            payload["search_parameters"] = {
                "mode": "on",
                "max_search_results": 10,
                "return_citations": True,
            }
        response = self.http.post(
            XAI_CHAT_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            json=payload,
        )
        data = self._response_json(response, "xAI Chat Completions")
        try:
            content = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise RemoteAPIError("xAI response did not contain message content") from exc
        return parse_json_text(str(content)), data

    def fetch_story(self) -> None:
        topic = self.current()["topic"]
        system_prompt = (
            "You are a rigorous breaking-news editor. Use live search, verify "
            "claims across multiple recent sources, reject rumors, and return "
            "only one valid JSON object."
        )
        user_prompt = f"""
Find the strongest current story for a 50-second vertical news Short.
Editorial topic: {topic}

Return exactly this shape:
{{
  "title": "concise headline",
  "summary": "two to four factual sentences",
  "why_it_matters": "one sentence",
  "score": 0,
  "score_breakdown": {{
    "recency": 0,
    "impact": 0,
    "visual_potential": 0,
    "source_confidence": 0
  }},
  "sources": [
    {{"title": "source title", "url": "https://..."}}
  ]
}}

The overall score and each component must be integers from 0 to 100.
Only select a story supported by at least two credible sources.
""".strip()
        story, raw = self._xai_chat(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            live_search=True,
        )
        if not isinstance(story, dict):
            raise ValueError("fetch_story must return a JSON object")
        for key in ("title", "summary", "why_it_matters", "score", "sources"):
            if key not in story:
                raise ValueError(f"Story JSON is missing {key}")
        try:
            score = int(story["score"])
        except (TypeError, ValueError) as exc:
            raise ValueError("Story score must be an integer") from exc
        if not 0 <= score <= 100:
            raise ValueError("Story score must be between 0 and 100")
        if not isinstance(story["sources"], list) or len(story["sources"]) < 2:
            raise ValueError("Story must include at least two sources")
        citations = raw.get("citations")
        if citations:
            story["xai_citations"] = citations
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
        system_prompt = (
            "You write accurate, fast-paced vertical news video scripts. "
            "Return only valid JSON. Do not invent facts beyond the supplied "
            "verified story."
        )
        user_prompt = f"""
Turn this verified story into a 50-second YouTube Short split into exactly five
10-second shots.

STORY:
{json.dumps(story, ensure_ascii=False)}

Return exactly:
{{
  "title": "YouTube Shorts title, factual and compelling",
  "description": "Two short paragraphs plus source URLs",
  "shots": [
    {{
      "voiceover_text": "spoken narration for this 10-second shot",
      "visual_prompt": "motion/camera prompt for animating the shot",
      "first_frame_prompt": "detailed photorealistic 9:16 opening frame prompt"
    }}
  ]
}}

Requirements:
- Exactly five shots.
- Total voiceover should sound natural in about 50 seconds.
- Shot 1 hooks immediately; shot 5 explains why the story matters.
- No visible text, logos, watermarks, captions, or UI in image prompts.
- Preserve uncertainty and attribution from the source story.
""".strip()
        script, _ = self._xai_chat(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            live_search=False,
        )
        if not isinstance(script, dict):
            raise ValueError("write_script must return a JSON object")
        shots = script.get("shots")
        if not isinstance(shots, list) or len(shots) != SHOT_COUNT:
            raise ValueError(f"Script must contain exactly {SHOT_COUNT} shots")
        for index, shot in enumerate(shots, start=1):
            if not isinstance(shot, dict):
                raise ValueError(f"Shot {index} must be an object")
            for key in ("voiceover_text", "visual_prompt", "first_frame_prompt"):
                if not str(shot.get(key, "")).strip():
                    raise ValueError(f"Shot {index} is missing {key}")
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

    def _openai_image(self, prompt: str) -> bytes:
        api_key = self.require_key(
            "OPENAI_API_KEY", self.settings.openai_api_key
        )
        response = self.http.post(
            OPENAI_IMAGE_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": self.settings.openai_image_model,
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
                raise RemoteAPIError("OpenAI returned invalid base64 image data") from exc
        if image.get("url"):
            download = self.http.get(image["url"])
            try:
                download.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise RemoteAPIError("Could not download OpenAI image") from exc
            return download.content
        raise RemoteAPIError("OpenAI image response had neither b64_json nor url")

    def generate_first_frames(self) -> None:
        script = json_load(self.current()["script_json"], {})
        shots = script.get("shots", [])
        if len(shots) != SHOT_COUNT:
            raise ValueError("Cannot generate frames without five scripted shots")
        frames: list[str] = []
        for index, shot in enumerate(shots, start=1):
            path = self.run_dir / "frames" / f"shot_{index:02d}.png"
            if not valid_file(path):
                prompt = (
                    f"{shot['first_frame_prompt']}\n\n"
                    "Vertical 9:16 composition, cinematic news documentary "
                    "photography, realistic lighting, no words, no typography, "
                    "no logos, no watermark."
                )
                LOG.info("Generating first frame %s/%s", index, SHOT_COUNT)
                atomic_write_bytes(path, self._openai_image(prompt))
            frames.append(str(path))
            self.store.update_run(
                self.run_id, frames_json=json.dumps(frames), last_error=None
            )
        self.store.update_run(
            self.run_id,
            status="framed",
            frames_json=json.dumps(frames),
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

    def _start_video(self, frame: Path, prompt: str) -> str:
        api_key = self.require_key("XAI_API_KEY", self.settings.xai_api_key)
        response = self.http.post(
            XAI_VIDEO_CREATE_URL,
            headers={"Authorization": f"Bearer {api_key}"},
            json={
                "model": self.settings.video_model,
                "prompt": prompt,
                "image": {"url": self._image_data_uri(frame)},
                "duration": 10,
                "aspect_ratio": "9:16",
                "resolution": "720p",
            },
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
        partial = destination.with_name(
            f".{destination.name}.{uuid.uuid4().hex}.part"
        )
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

    def generate_clips(self) -> None:
        row = self.current()
        script = json_load(row["script_json"], {})
        frames = [Path(path) for path in json_load(row["frames_json"], [])]
        if len(frames) != SHOT_COUNT or not all(valid_file(path) for path in frames):
            raise ValueError("Cannot generate clips without all five frames")
        requests: dict[str, str] = json_load(row["video_requests_json"], {})
        clips: list[str] = []
        for index, (shot, frame) in enumerate(
            zip(script["shots"], frames, strict=True), start=1
        ):
            key = str(index)
            path = self.run_dir / "clips" / f"shot_{index:02d}.mp4"
            if not valid_file(path):
                request_id = requests.get(key)
                if not request_id:
                    LOG.info("Submitting xAI video %s/%s", index, SHOT_COUNT)
                    request_id = self._start_video(
                        frame,
                        str(shot["visual_prompt"]),
                    )
                    requests[key] = request_id
                    self._persist_video_requests(requests)
                try:
                    video_url = self._poll_video(request_id)
                except RemoteAPIError as exc:
                    if "ended as failed" in str(exc) or "ended as expired" in str(exc):
                        requests.pop(key, None)
                        self._persist_video_requests(requests)
                    raise
                LOG.info("Downloading xAI video %s/%s", index, SHOT_COUNT)
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
                self.store.update_run(
                    self.run_id, queue_jobs_json=json.dumps(jobs)
                )
        if not job_id:
            job = self.queue.submit(
                job_type,
                dict(payload),
                input_files=input_files,
            )
            job_id = str(job["id"])
            jobs[key] = job_id
            self.store.update_run(
                self.run_id, queue_jobs_json=json.dumps(jobs)
            )
        try:
            self.queue.wait(
                job_id,
                output_path=output_path,
                timeout=self.settings.queue_timeout,
            )
        except JobFailedError:
            jobs.pop(key, None)
            self.store.update_run(
                self.run_id, queue_jobs_json=json.dumps(jobs)
            )
            raise

    def generate_voiceover_and_captions(self) -> None:
        script = json_load(self.current()["script_json"], {})
        shots = script.get("shots", [])
        if len(shots) != SHOT_COUNT:
            raise ValueError("Cannot voice a pipeline without five shots")
        voiceover_path = self.run_dir / "voiceover.wav"
        captions_path = self.run_dir / "captions.srt"
        voiceover_text = "\n\n".join(
            str(shot["voiceover_text"]).strip() for shot in shots
        )
        voice_reference = self._voice_reference_payload()
        self._queue_job(
            "tts",
            "tts",
            {
                "text": voiceover_text,
                "shots": [
                    {
                        "index": index,
                        "text": shot["voiceover_text"],
                        "target_duration_seconds": 10,
                    }
                    for index, shot in enumerate(shots, start=1)
                ],
                "target_duration_seconds": 50,
                "output_format": "wav",
                **voice_reference,
            },
            {},
            voiceover_path,
        )
        self._queue_job(
            "transcribe",
            "transcribe",
            {
                "format": "srt",
                "language": "en",
                "word_timestamps": True,
            },
            {"audio": voiceover_path},
            captions_path,
        )
        self.store.update_run(
            self.run_id,
            status="voiced",
            voiceover_path=str(voiceover_path),
            captions_path=str(captions_path),
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
        voiceover = Path(row["voiceover_path"] or "")
        captions = Path(row["captions_path"] or "")
        if (
            len(clips) != SHOT_COUNT
            or not all(valid_file(path) for path in clips)
            or not valid_file(voiceover)
            or not valid_file(captions)
        ):
            raise ValueError("Assemble inputs are incomplete")
        final_path = self.run_dir / "final.mp4"
        inputs: dict[str, Path] = {
            f"clip_{index}": path
            for index, path in enumerate(clips, start=1)
        }
        inputs["voiceover"] = voiceover
        inputs["captions"] = captions
        self._queue_job(
            "assemble",
            "assemble",
            {
                "clip_roles": [f"clip_{index}" for index in range(1, 6)],
                "voiceover_role": "voiceover",
                "captions_role": "captions",
                "shot_duration_seconds": 10,
                "aspect_ratio": "9:16",
                "burn_captions": True,
                "output_format": "mp4",
            },
            inputs,
            final_path,
        )
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
        token = self.require_key(
            "TELEGRAM_BOT_TOKEN", self.settings.telegram_bot_token
        )
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
        if (
            chat_id != str(row["telegram_chat_id"])
            or message_id != str(row["telegram_message_id"])
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
        self.require_key(
            "TELEGRAM_BOT_TOKEN", self.settings.telegram_bot_token
        )
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
        paths = [
            *json_load(row["frames_json"], []),
            *json_load(row["clips_json"], []),
            row["voiceover_path"],
            row["captions_path"],
            row["final_path"],
        ]
        for raw_path in paths:
            if raw_path:
                Path(raw_path).unlink(missing_ok=True)
        self.store.update_run(
            self.run_id,
            status="scripted",
            frames_json="[]",
            clips_json="[]",
            video_requests_json="{}",
            queue_jobs_json="{}",
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
        if STATUS_INDEX[status] >= STATUS_INDEX["framed"] and (
            len(frames) != SHOT_COUNT or not all(valid_file(path) for path in frames)
        ):
            self.store.update_run(
                self.run_id,
                status="scripted",
                frames_json="[]",
                clips_json="[]",
                video_requests_json="{}",
                queue_jobs_json="{}",
                voiceover_path=None,
                captions_path=None,
                final_path=None,
            )
            return
        if STATUS_INDEX[status] >= STATUS_INDEX["rendered"] and (
            len(clips) != SHOT_COUNT or not all(valid_file(path) for path in clips)
        ):
            self.store.update_run(
                self.run_id,
                status="framed",
                clips_json="[]",
                video_requests_json="{}",
                queue_jobs_json="{}",
                voiceover_path=None,
                captions_path=None,
                final_path=None,
            )
            return
        if STATUS_INDEX[status] >= STATUS_INDEX["voiced"] and (
            not valid_file(row["voiceover_path"])
            or not valid_file(row["captions_path"])
        ):
            self.store.update_run(
                self.run_id,
                status="rendered",
                queue_jobs_json="{}",
                voiceover_path=None,
                captions_path=None,
                final_path=None,
            )
            return
        if STATUS_INDEX[status] >= STATUS_INDEX["assembled"] and not valid_file(
            row["final_path"]
        ):
            self.store.update_run(
                self.run_id,
                status="voiced",
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
                    "generate_first_frames", self.generate_first_frames
                )
                continue
            if status == "framed":
                if dry_run:
                    LOG.info(
                        "Dry run complete after stage 3; run %s is framed",
                        self.run_id,
                    )
                    return self.current()
                self.run_stage("generate_clips", self.generate_clips)
                continue
            if status == "rendered":
                self.run_stage(
                    "generate_voiceover_and_captions",
                    self.generate_voiceover_and_captions,
                )
                continue
            if status == "voiced":
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
        description="Create a crash-resumable 50-second vertical news video."
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
        "--new",
        action="store_true",
        help="Start a new run instead of resuming the latest unfinished run.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Stop after stage 3 (five first-frame images).",
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
        or "the most consequential, verifiable breaking news story right now"
    )
    return store.create_run(selected_topic)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    project_root = Path(__file__).resolve().parent
    load_environment(project_root)
    settings = Settings.from_environment(
        project_root,
        database_path=args.db,
        work_root=args.work_root,
    )
    store = StateStore(settings.database_path)
    try:
        run = choose_run(
            store,
            run_id=args.run_id,
            topic=args.topic,
            new=args.new,
        )
    except ValueError as exc:
        parser.error(str(exc))
    pipeline = NewsPipeline(settings, store, run)
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
