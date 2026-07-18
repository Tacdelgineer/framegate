from __future__ import annotations

from pathlib import Path

import httpx

from job_queue.client import JobFailedError, JobQueueClient


def test_client_submits_waits_and_downloads(tmp_path: Path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        if request.method == "POST":
            return httpx.Response(
                201,
                json={"id": "job-1", "status": "pending"},
            )
        if request.url.path == "/jobs/job-1":
            calls += 1
            if calls == 1:
                return httpx.Response(
                    200,
                    json={
                        "id": "job-1",
                        "job_type": "assemble",
                        "status": "claimed",
                    },
                )
            return httpx.Response(
                200,
                json={
                    "id": "job-1",
                    "job_type": "assemble",
                    "status": "done",
                    "result_files": [
                        {"download_url": "http://queue/result/video"}
                    ],
                },
            )
        if request.url.path == "/result/video":
            return httpx.Response(200, content=b"video")
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    output = tmp_path / "output.mp4"
    client = JobQueueClient("http://queue", poll_interval=0)
    client._client.close()
    client._client = httpx.Client(
        base_url="http://queue", transport=httpx.MockTransport(handler)
    )
    try:
        result = client.run("assemble", {}, output_path=output)
    finally:
        client.close()

    assert result["status"] == "done"
    assert output.read_bytes() == b"video"


def test_client_raises_worker_error() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "job-2",
                "job_type": "transcribe",
                "status": "failed",
                "error": "bad audio",
            },
        )

    client = JobQueueClient("http://queue", poll_interval=0)
    client._client.close()
    client._client = httpx.Client(
        base_url="http://queue", transport=httpx.MockTransport(handler)
    )
    try:
        try:
            client.wait("job-2")
        except JobFailedError as exc:
            assert "bad audio" in str(exc)
        else:
            raise AssertionError("expected JobFailedError")
    finally:
        client.close()
