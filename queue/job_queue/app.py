from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.responses import FileResponse
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError
from starlette.datastructures import UploadFile

JobStatus = Literal["pending", "claimed", "done", "failed"]
FINAL_STATUSES = {"done", "failed"}


@dataclass(frozen=True)
class Settings:
    database_path: Path
    storage_path: Path
    claim_timeout_seconds: int = 30 * 60
    max_upload_bytes: int = 20 * 1024 * 1024 * 1024

    @classmethod
    def from_environment(cls) -> "Settings":
        root = Path(__file__).resolve().parents[1]
        return cls(
            database_path=Path(
                os.getenv("JOB_QUEUE_DB", root / "data" / "jobs.sqlite3")
            ),
            storage_path=Path(
                os.getenv("JOB_QUEUE_STORAGE", root / "data" / "files")
            ),
            claim_timeout_seconds=int(
                os.getenv("JOB_QUEUE_CLAIM_TIMEOUT_SECONDS", str(30 * 60))
            ),
            max_upload_bytes=int(
                os.getenv(
                    "JOB_QUEUE_MAX_UPLOAD_BYTES", str(20 * 1024 * 1024 * 1024)
                )
            ),
        )


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    job_type: str = Field(
        min_length=1,
        max_length=100,
        pattern=r"^[A-Za-z0-9_.-]+$",
        validation_alias=AliasChoices("job_type", "type"),
    )
    payload: dict[str, Any] = Field(default_factory=dict)


class ClaimRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(default="dgx-worker", min_length=1, max_length=200)


def _now() -> float:
    return time.time()


def _timestamp(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, UTC).isoformat()


def _decode_json(raw: str | None, fallback: Any) -> Any:
    if not raw:
        return fallback
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return fallback


class Store:
    def __init__(self, settings: Settings):
        self.settings = settings

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.settings.database_path, timeout=10, isolation_level=None
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def initialize(self) -> None:
        self.settings.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.settings.storage_path.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    job_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL
                        CHECK (status IN ('pending', 'claimed', 'done', 'failed')),
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    claimed_at REAL,
                    completed_at REAL,
                    worker_id TEXT,
                    claim_token TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    result TEXT
                );

                CREATE INDEX IF NOT EXISTS jobs_status_created
                    ON jobs(status, created_at);
                CREATE INDEX IF NOT EXISTS jobs_type_status_created
                    ON jobs(job_type, status, created_at);

                CREATE TABLE IF NOT EXISTS job_files (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
                    kind TEXT NOT NULL CHECK (kind IN ('input', 'result')),
                    role TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    stored_name TEXT NOT NULL,
                    content_type TEXT,
                    size INTEGER NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS job_files_job
                    ON job_files(job_id, kind, created_at);
                """
            )

    def requeue_expired(self, connection: sqlite3.Connection | None = None) -> int:
        owns_connection = connection is None
        connection = connection or self.connect()
        cutoff = _now() - self.settings.claim_timeout_seconds
        try:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET status = 'pending',
                    updated_at = ?,
                    claimed_at = NULL,
                    worker_id = NULL,
                    claim_token = NULL
                WHERE status = 'claimed' AND claimed_at <= ?
                """,
                (_now(), cutoff),
            )
            return cursor.rowcount
        finally:
            if owns_connection:
                connection.close()

    def get_job_row(
        self, connection: sqlite3.Connection, job_id: str
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Job not found")
        return row


def _clean_filename(filename: str | None) -> str:
    cleaned = Path(filename or "upload.bin").name.strip()
    return cleaned or "upload.bin"


async def _save_upload(
    upload: UploadFile, destination: Path, max_upload_bytes: int
) -> int:
    size = 0
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with destination.open("wb") as output:
            while chunk := await upload.read(1024 * 1024):
                size += len(chunk)
                if size > max_upload_bytes:
                    raise HTTPException(
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                        detail=f"Upload exceeds {max_upload_bytes} bytes",
                    )
                output.write(chunk)
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    finally:
        await upload.close()
    return size


def _serialize_job(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    request: Request,
    *,
    include_claim_token: bool = False,
) -> dict[str, Any]:
    file_rows = connection.execute(
        """
        SELECT id, kind, role, filename, content_type, size, created_at
        FROM job_files
        WHERE job_id = ?
        ORDER BY created_at, id
        """,
        (row["id"],),
    ).fetchall()
    files: dict[str, list[dict[str, Any]]] = {"input": [], "result": []}
    for file_row in file_rows:
        item = {
            "id": file_row["id"],
            "role": file_row["role"],
            "filename": file_row["filename"],
            "content_type": file_row["content_type"],
            "size": file_row["size"],
            "created_at": _timestamp(file_row["created_at"]),
            "download_url": str(
                request.url_for(
                    "download_job_file",
                    job_id=row["id"],
                    file_id=file_row["id"],
                )
            ),
        }
        files[file_row["kind"]].append(item)

    job = {
        "id": row["id"],
        "type": row["job_type"],
        "job_type": row["job_type"],
        "payload": _decode_json(row["payload"], {}),
        "status": row["status"],
        "created_at": _timestamp(row["created_at"]),
        "updated_at": _timestamp(row["updated_at"]),
        "claimed_at": _timestamp(row["claimed_at"]),
        "completed_at": _timestamp(row["completed_at"]),
        "worker_id": row["worker_id"],
        "attempts": row["attempts"],
        "error": row["error"],
        "result": _decode_json(row["result"], None),
        "input_files": files["input"],
        "result_files": files["result"],
    }
    if include_claim_token:
        job["claim_token"] = row["claim_token"]
    return job


async def _parse_create_request(
    request: Request,
) -> tuple[JobCreate, list[tuple[str, UploadFile]]]:
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/json"):
        try:
            return JobCreate.model_validate(await request.json()), []
        except (ValidationError, json.JSONDecodeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    if content_type.startswith("multipart/form-data"):
        form = await request.form()
        try:
            payload = json.loads(str(form.get("payload", "{}")))
            create = JobCreate.model_validate(
                {
                    "job_type": form.get("job_type", form.get("type")),
                    "payload": payload,
                }
            )
        except (ValidationError, json.JSONDecodeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        uploads: list[tuple[str, UploadFile]] = []
        for field_name, value in form.multi_items():
            if isinstance(value, UploadFile):
                role = field_name.removeprefix("input:")
                uploads.append((role or "input", value))
        return create, uploads

    raise HTTPException(
        status_code=415,
        detail="Use application/json or multipart/form-data",
    )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_environment()
    store = Store(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        store.initialize()
        stop = asyncio.Event()

        async def reap_loop() -> None:
            interval = max(1, min(60, settings.claim_timeout_seconds // 2))
            while not stop.is_set():
                try:
                    await asyncio.to_thread(store.requeue_expired)
                    await asyncio.wait_for(stop.wait(), timeout=interval)
                except TimeoutError:
                    pass

        reaper = asyncio.create_task(reap_loop())
        try:
            yield
        finally:
            stop.set()
            reaper.cancel()
            with suppress(asyncio.CancelledError):
                await reaper

    application = FastAPI(
        title="DGX Job Queue",
        version="1.0.0",
        lifespan=lifespan,
    )
    application.state.store = store

    @application.get("/health")
    def health() -> dict[str, Any]:
        with store.connect() as connection:
            connection.execute("SELECT 1").fetchone()
            pending = connection.execute(
                "SELECT COUNT(*) FROM jobs WHERE status = 'pending'"
            ).fetchone()[0]
        return {"ok": True, "pending_jobs": pending}

    @application.post("/jobs", status_code=status.HTTP_201_CREATED)
    async def create_job(request: Request) -> dict[str, Any]:
        create, uploads = await _parse_create_request(request)
        job_id = str(uuid.uuid4())
        now = _now()
        job_directory = settings.storage_path / job_id
        saved_files: list[dict[str, Any]] = []

        try:
            for role, upload in uploads:
                file_id = str(uuid.uuid4())
                stored_name = file_id
                size = await _save_upload(
                    upload, job_directory / stored_name, settings.max_upload_bytes
                )
                saved_files.append(
                    {
                        "id": file_id,
                        "role": role,
                        "filename": _clean_filename(upload.filename),
                        "stored_name": stored_name,
                        "content_type": upload.content_type,
                        "size": size,
                    }
                )

            with store.connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        """
                        INSERT INTO jobs(
                            id, job_type, payload, status, created_at, updated_at
                        ) VALUES (?, ?, ?, 'pending', ?, ?)
                        """,
                        (
                            job_id,
                            create.job_type,
                            json.dumps(create.payload),
                            now,
                            now,
                        ),
                    )
                    for item in saved_files:
                        connection.execute(
                            """
                            INSERT INTO job_files(
                                id, job_id, kind, role, filename, stored_name,
                                content_type, size, created_at
                            ) VALUES (?, ?, 'input', ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                item["id"],
                                job_id,
                                item["role"],
                                item["filename"],
                                item["stored_name"],
                                item["content_type"],
                                item["size"],
                                now,
                            ),
                        )
                    connection.execute("COMMIT")
                except BaseException:
                    connection.execute("ROLLBACK")
                    raise
                row = store.get_job_row(connection, job_id)
                return _serialize_job(connection, row, request)
        except BaseException:
            shutil.rmtree(job_directory, ignore_errors=True)
            raise

    @application.get("/jobs")
    def list_jobs(
        request: Request,
        status_filter: JobStatus | None = Query(default=None, alias="status"),
        job_type: str | None = Query(default=None, max_length=100),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> list[dict[str, Any]]:
        with store.connect() as connection:
            store.requeue_expired(connection)
            clauses: list[str] = []
            parameters: list[Any] = []
            if status_filter is not None:
                clauses.append("status = ?")
                parameters.append(status_filter)
            if job_type is not None:
                clauses.append("job_type = ?")
                parameters.append(job_type)
            where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            rows = connection.execute(
                f"""
                SELECT * FROM jobs
                {where}
                ORDER BY created_at
                LIMIT ?
                """,
                (*parameters, limit),
            ).fetchall()
            return [_serialize_job(connection, row, request) for row in rows]

    @application.post("/jobs/{job_id}/claim")
    def claim_job(
        job_id: str, request: Request, claim: ClaimRequest | None = None
    ) -> dict[str, Any]:
        claim = claim or ClaimRequest()
        with store.connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                store.requeue_expired(connection)
                row = store.get_job_row(connection, job_id)
                if row["status"] != "pending":
                    raise HTTPException(
                        status_code=409,
                        detail=f"Job is {row['status']}, not pending",
                    )
                now = _now()
                claim_token = uuid.uuid4().hex
                cursor = connection.execute(
                    """
                    UPDATE jobs
                    SET status = 'claimed',
                        updated_at = ?,
                        claimed_at = ?,
                        worker_id = ?,
                        claim_token = ?,
                        attempts = attempts + 1
                    WHERE id = ? AND status = 'pending'
                    """,
                    (now, now, claim.worker_id, claim_token, job_id),
                )
                if cursor.rowcount != 1:
                    raise HTTPException(status_code=409, detail="Job was already claimed")
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise
            row = store.get_job_row(connection, job_id)
            return _serialize_job(
                connection, row, request, include_claim_token=True
            )

    @application.post("/jobs/{job_id}/complete")
    async def complete_job(job_id: str, request: Request) -> dict[str, Any]:
        content_type = request.headers.get("content-type", "")
        upload: UploadFile | None = None
        if content_type.startswith("multipart/form-data"):
            form = await request.form()
            claim_token = str(form.get("claim_token", ""))
            final_status = str(form.get("status", "done"))
            error = str(form["error"]) if form.get("error") is not None else None
            result_raw = str(form["result"]) if form.get("result") is not None else None
            for _, value in form.multi_items():
                if isinstance(value, UploadFile):
                    upload = value
                    break
        elif content_type.startswith("application/json"):
            body = await request.json()
            claim_token = str(body.get("claim_token", ""))
            final_status = str(body.get("status", "done"))
            error = body.get("error")
            result_raw = (
                json.dumps(body["result"]) if body.get("result") is not None else None
            )
        else:
            raise HTTPException(
                status_code=415,
                detail="Use application/json or multipart/form-data",
            )

        if final_status not in FINAL_STATUSES:
            raise HTTPException(status_code=422, detail="status must be done or failed")
        if final_status == "failed" and not error:
            error = "Worker reported failure"

        result_json: str | None = None
        if result_raw:
            try:
                result_json = json.dumps(json.loads(result_raw))
            except json.JSONDecodeError as exc:
                raise HTTPException(
                    status_code=422, detail="result must be valid JSON"
                ) from exc

        temp_path: Path | None = None
        result_file: dict[str, Any] | None = None
        if upload is not None:
            file_id = str(uuid.uuid4())
            temp_path = settings.storage_path / ".tmp" / file_id
            size = await _save_upload(
                upload, temp_path, settings.max_upload_bytes
            )
            result_file = {
                "id": file_id,
                "role": "result",
                "filename": _clean_filename(upload.filename),
                "stored_name": file_id,
                "content_type": upload.content_type,
                "size": size,
            }

        try:
            with store.connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    store.requeue_expired(connection)
                    row = store.get_job_row(connection, job_id)
                    if row["status"] != "claimed":
                        raise HTTPException(
                            status_code=409,
                            detail=f"Job is {row['status']}, not claimed",
                        )
                    if claim_token and row["claim_token"] != claim_token:
                        raise HTTPException(status_code=409, detail="Stale claim token")

                    now = _now()
                    if result_file is not None and temp_path is not None:
                        destination = (
                            settings.storage_path
                            / job_id
                            / result_file["stored_name"]
                        )
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(temp_path, destination)
                        connection.execute(
                            """
                            INSERT INTO job_files(
                                id, job_id, kind, role, filename, stored_name,
                                content_type, size, created_at
                            ) VALUES (?, ?, 'result', ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                result_file["id"],
                                job_id,
                                result_file["role"],
                                result_file["filename"],
                                result_file["stored_name"],
                                result_file["content_type"],
                                result_file["size"],
                                now,
                            ),
                        )
                    connection.execute(
                        """
                        UPDATE jobs
                        SET status = ?,
                            updated_at = ?,
                            completed_at = ?,
                            error = ?,
                            result = ?,
                            claim_token = NULL
                        WHERE id = ?
                        """,
                        (final_status, now, now, error, result_json, job_id),
                    )
                    connection.execute("COMMIT")
                except BaseException:
                    connection.execute("ROLLBACK")
                    raise
                row = store.get_job_row(connection, job_id)
                return _serialize_job(connection, row, request)
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

    @application.get("/jobs/{job_id}")
    def get_job(job_id: str, request: Request) -> dict[str, Any]:
        with store.connect() as connection:
            store.requeue_expired(connection)
            row = store.get_job_row(connection, job_id)
            return _serialize_job(connection, row, request)

    @application.get(
        "/jobs/{job_id}/files/{file_id}",
        name="download_job_file",
    )
    def download_job_file(job_id: str, file_id: str) -> FileResponse:
        with store.connect() as connection:
            row = connection.execute(
                """
                SELECT filename, stored_name, content_type
                FROM job_files
                WHERE id = ? AND job_id = ?
                """,
                (file_id, job_id),
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="File not found")
        path = settings.storage_path / job_id / row["stored_name"]
        if not path.is_file():
            raise HTTPException(status_code=404, detail="File is missing from storage")
        return FileResponse(
            path=path,
            media_type=row["content_type"] or "application/octet-stream",
            filename=row["filename"],
        )

    return application


app = create_app()
