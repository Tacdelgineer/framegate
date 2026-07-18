from __future__ import annotations

import json
import mimetypes
import os
import time
from contextlib import ExitStack
from pathlib import Path
from typing import Any, Mapping

import httpx


class JobQueueError(RuntimeError):
    """Raised when the queue cannot accept or return a job."""


class JobFailedError(JobQueueError):
    """Raised when a worker marks a job as failed."""


class JobQueueClient:
    """Synchronous client used by the VPS news pipeline."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        poll_interval: float = 2.0,
        timeout: float = 6 * 60 * 60,
        request_timeout: float = 120.0,
    ):
        self.base_url = (
            base_url
            or os.getenv("JOB_QUEUE_URL")
            or "http://100.123.208.90:8787"
        ).rstrip("/")
        self.poll_interval = poll_interval
        self.timeout = timeout
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=httpx.Timeout(request_timeout, connect=10.0),
        )

    def __enter__(self) -> "JobQueueClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def _response_json(self, response: httpx.Response) -> Any:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            try:
                detail = response.json().get("detail", response.text)
            except (ValueError, AttributeError):
                detail = response.text
            raise JobQueueError(
                f"Queue returned HTTP {response.status_code}: {detail}"
            ) from exc
        return response.json()

    def submit(
        self,
        job_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        input_files: Mapping[str, str | Path] | None = None,
    ) -> dict[str, Any]:
        body = dict(payload or {})
        paths = {
            role: Path(path).expanduser().resolve()
            for role, path in (input_files or {}).items()
        }
        missing = [str(path) for path in paths.values() if not path.is_file()]
        if missing:
            raise JobQueueError(f"Input file does not exist: {', '.join(missing)}")

        if not paths:
            response = self._client.post(
                "/jobs", json={"job_type": job_type, "payload": body}
            )
            return self._response_json(response)

        with ExitStack() as stack:
            files = []
            for role, path in paths.items():
                handle = stack.enter_context(path.open("rb"))
                media_type = mimetypes.guess_type(path.name)[0]
                files.append(
                    (
                        f"input:{role}",
                        (path.name, handle, media_type or "application/octet-stream"),
                    )
                )
            response = self._client.post(
                "/jobs",
                data={"job_type": job_type, "payload": json.dumps(body)},
                files=files,
            )
        return self._response_json(response)

    def get(self, job_id: str) -> dict[str, Any]:
        return self._response_json(self._client.get(f"/jobs/{job_id}"))

    def wait(
        self,
        job_id: str,
        *,
        output_path: str | Path | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        while True:
            job = self.get(job_id)
            if job["status"] == "failed":
                raise JobFailedError(
                    f"Remote {job['job_type']} job {job_id} failed: "
                    f"{job.get('error') or 'unknown worker error'}"
                )
            if job["status"] == "done":
                if output_path is not None:
                    result_files = job.get("result_files") or []
                    if not result_files:
                        raise JobQueueError(
                            f"Remote job {job_id} completed without a result file"
                        )
                    destination = Path(output_path).expanduser()
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    self._download(result_files[0]["download_url"], destination)
                    job["downloaded_to"] = str(destination)
                return job
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"Timed out waiting for remote job {job_id} "
                    f"after {timeout if timeout is not None else self.timeout} seconds"
                )
            time.sleep(self.poll_interval)

    def run(
        self,
        job_type: str,
        payload: Mapping[str, Any] | None = None,
        *,
        input_files: Mapping[str, str | Path] | None = None,
        output_path: str | Path | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        job = self.submit(job_type, payload, input_files=input_files)
        return self.wait(job["id"], output_path=output_path, timeout=timeout)

    def _download(self, url: str, destination: Path) -> None:
        temp_path = destination.with_name(f".{destination.name}.part")
        try:
            with self._client.stream("GET", url) as response:
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as exc:
                    raise JobQueueError(
                        f"Could not download job result: HTTP "
                        f"{response.status_code}"
                    ) from exc
                with temp_path.open("wb") as output:
                    for chunk in response.iter_bytes():
                        output.write(chunk)
            os.replace(temp_path, destination)
        finally:
            temp_path.unlink(missing_ok=True)
