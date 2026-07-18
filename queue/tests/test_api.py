from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from job_queue.app import Settings, create_app


def make_client(tmp_path: Path) -> tuple[TestClient, Settings]:
    settings = Settings(
        database_path=tmp_path / "jobs.sqlite3",
        storage_path=tmp_path / "files",
        claim_timeout_seconds=1800,
        max_upload_bytes=1024 * 1024,
    )
    return TestClient(create_app(settings)), settings


def test_json_job_claim_complete_and_download(tmp_path: Path) -> None:
    client, _ = make_client(tmp_path)
    with client:
        created = client.post(
            "/jobs",
            json={"job_type": "generate_voiceover", "payload": {"text": "hello"}},
        )
        assert created.status_code == 201
        job = created.json()
        assert job["status"] == "pending"

        pending = client.get("/jobs", params={"status": "pending"}).json()
        assert [item["id"] for item in pending] == [job["id"]]

        claimed = client.post(
            f"/jobs/{job['id']}/claim", json={"worker_id": "dgx-1"}
        )
        assert claimed.status_code == 200
        claim = claimed.json()
        assert claim["status"] == "claimed"
        assert claim["attempts"] == 1
        assert claim["claim_token"]

        duplicate = client.post(
            f"/jobs/{job['id']}/claim", json={"worker_id": "dgx-2"}
        )
        assert duplicate.status_code == 409

        completed = client.post(
            f"/jobs/{job['id']}/complete",
            data={
                "claim_token": claim["claim_token"],
                "status": "done",
                "result": '{"duration": 1.25}',
            },
            files={"file": ("voice.wav", b"RIFF-result", "audio/wav")},
        )
        assert completed.status_code == 200
        done = completed.json()
        assert done["status"] == "done"
        assert done["result"] == {"duration": 1.25}
        assert len(done["result_files"]) == 1

        downloaded = client.get(done["result_files"][0]["download_url"])
        assert downloaded.status_code == 200
        assert downloaded.content == b"RIFF-result"


def test_multipart_inputs_and_failed_completion(tmp_path: Path) -> None:
    client, _ = make_client(tmp_path)
    with client:
        created = client.post(
            "/jobs",
            data={"job_type": "transcribe", "payload": '{"language":"en"}'},
            files={"input:audio": ("voice.mp3", b"audio-data", "audio/mpeg")},
        )
        assert created.status_code == 201
        job = created.json()
        assert job["input_files"][0]["role"] == "audio"
        assert (
            client.get(job["input_files"][0]["download_url"]).content
            == b"audio-data"
        )

        claim = client.post(
            f"/jobs/{job['id']}/claim", json={"worker_id": "dgx-1"}
        ).json()
        failed = client.post(
            f"/jobs/{job['id']}/complete",
            json={
                "claim_token": claim["claim_token"],
                "status": "failed",
                "error": "whisper ran out of memory",
            },
        )
        assert failed.status_code == 200
        assert failed.json()["status"] == "failed"
        assert failed.json()["error"] == "whisper ran out of memory"


def test_legacy_type_and_bodyless_claim_are_supported(tmp_path: Path) -> None:
    client, _ = make_client(tmp_path)
    with client:
        created = client.post(
            "/jobs", json={"type": "generate_voiceover", "payload": {}}
        )
        assert created.status_code == 201
        assert created.json()["type"] == "generate_voiceover"

        claimed = client.post(f"/jobs/{created.json()['id']}/claim")
        assert claimed.status_code == 200
        assert claimed.json()["worker_id"] == "dgx-worker"

        completed = client.post(
            f"/jobs/{created.json()['id']}/complete",
            files={"file": ("voice.wav", b"audio", "audio/wav")},
        )
        assert completed.status_code == 200
        assert completed.json()["status"] == "done"


def test_expired_claim_reverts_and_old_token_is_rejected(tmp_path: Path) -> None:
    client, settings = make_client(tmp_path)
    with client:
        job = client.post(
            "/jobs", json={"job_type": "assemble", "payload": {}}
        ).json()
        first_claim = client.post(
            f"/jobs/{job['id']}/claim", json={"worker_id": "dgx-1"}
        ).json()

        with sqlite3.connect(settings.database_path) as connection:
            connection.execute(
                "UPDATE jobs SET claimed_at = 0 WHERE id = ?", (job["id"],)
            )

        refreshed = client.get(f"/jobs/{job['id']}").json()
        assert refreshed["status"] == "pending"
        second_claim = client.post(
            f"/jobs/{job['id']}/claim", json={"worker_id": "dgx-2"}
        ).json()
        assert second_claim["attempts"] == 2

        stale = client.post(
            f"/jobs/{job['id']}/complete",
            json={"claim_token": first_claim["claim_token"], "status": "done"},
        )
        assert stale.status_code == 409


def test_validation_and_missing_jobs(tmp_path: Path) -> None:
    client, _ = make_client(tmp_path)
    with client:
        assert (
            client.post(
                "/jobs", json={"job_type": "bad type", "payload": {}}
            ).status_code
            == 422
        )
        assert client.get("/jobs/missing").status_code == 404
        assert (
            client.get("/jobs", params={"status": "not-a-status"}).status_code
            == 422
        )
