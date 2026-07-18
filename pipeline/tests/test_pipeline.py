from __future__ import annotations

import json
import base64
from pathlib import Path

import httpx

import news_pipeline as module


class FakeQueue:
    def close(self) -> None:
        pass


class RecordingQueue(FakeQueue):
    def __init__(self):
        self.submissions = []
        self._jobs = {}

    def submit(self, job_type, payload, *, input_files=None):
        job_id = f"job-{len(self.submissions) + 1}"
        self.submissions.append(
            {
                "id": job_id,
                "job_type": job_type,
                "payload": payload,
                "input_files": dict(input_files or {}),
            }
        )
        self._jobs[job_id] = {"id": job_id, "status": "pending"}
        return self._jobs[job_id]

    def get(self, job_id):
        return self._jobs[job_id]

    def wait(self, job_id, *, output_path, timeout):
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"result for {job_id}".encode())
        self._jobs[job_id]["status"] = "done"
        return self._jobs[job_id]


def settings(tmp_path: Path) -> module.Settings:
    return module.Settings(
        project_root=tmp_path,
        database_path=tmp_path / "pipeline.sqlite3",
        work_root=tmp_path / "runs",
        queue_url="http://queue",
        xai_api_key="xai-test",
        openai_api_key="openai-test",
        xai_text_model="grok-test",
        openai_image_model="gpt-image-1",
        openai_image_quality="low",
        video_model="grok-imagine-video",
        video_poll_interval=0,
        video_poll_timeout=10,
        queue_poll_interval=0,
        queue_timeout=10,
        retry_base_seconds=0,
        request_timeout=10,
        telegram_bot_token="telegram-test",
        telegram_chat_id="123",
        telegram_poll_timeout=1,
        approval_wait_timeout=1,
    )


class DryRunPipeline(module.NewsPipeline):
    def fetch_story(self) -> None:
        self.store.update_run(
            self.run_id,
            status="fetched",
            story_json=json.dumps(
                {
                    "title": "Test",
                    "summary": "Summary",
                    "why_it_matters": "Reason",
                    "score": 80,
                    "sources": [{"url": "a"}, {"url": "b"}],
                }
            ),
        )

    def write_script(self) -> None:
        shots = [
            {
                "voiceover_text": f"Voice {index}",
                "visual_prompt": f"Visual {index}",
                "first_frame_prompt": f"Frame {index}",
            }
            for index in range(1, 6)
        ]
        self.store.update_run(
            self.run_id,
            status="scripted",
            script_json=json.dumps(
                {"title": "Test", "description": "Description", "shots": shots}
            ),
        )

    def generate_first_frames(self) -> None:
        frames = []
        for index in range(1, 6):
            path = self.run_dir / "frames" / f"shot_{index:02d}.png"
            module.atomic_write_bytes(path, b"png")
            frames.append(str(path))
        self.store.update_run(
            self.run_id,
            status="framed",
            frames_json=json.dumps(frames),
        )


def test_dry_run_stops_after_framing_and_resumes(tmp_path: Path) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test topic")
    pipeline = DryRunPipeline(config, store, run, queue_client=FakeQueue())
    try:
        result = pipeline.run(dry_run=True)
    finally:
        pipeline.close()

    assert result["status"] == "framed"
    assert len(json.loads(result["frames_json"])) == 5
    assert store.latest_resumable()["id"] == run["id"]


def test_repair_rolls_missing_frames_back_to_scripted(tmp_path: Path) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    run = store.update_run(
        run["id"],
        status="framed",
        story_json="{}",
        script_json='{"shots":[]}',
        frames_json=json.dumps([str(tmp_path / "missing.png")] * 5),
    )
    pipeline = DryRunPipeline(config, store, run, queue_client=FakeQueue())
    try:
        pipeline.repair_state()
    finally:
        pipeline.close()
    repaired = store.get_run(run["id"])
    assert repaired["status"] == "scripted"
    assert json.loads(repaired["frames_json"]) == []


def test_stage_retries_three_times_and_preserves_previous_state(
    tmp_path: Path,
) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    pipeline = DryRunPipeline(config, store, run, queue_client=FakeQueue())
    calls = 0

    def fail() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("boom")

    try:
        try:
            pipeline.run_stage("broken", fail)
        except RuntimeError:
            pass
        else:
            raise AssertionError("expected failure")
    finally:
        pipeline.close()

    assert calls == 3
    assert store.get_run(run["id"])["status"] is None
    with store.connect() as connection:
        outcomes = connection.execute(
            "SELECT outcome FROM stage_attempts ORDER BY id"
        ).fetchall()
    assert [row["outcome"] for row in outcomes] == ["failed"] * 3


def test_persisted_approval_is_available_after_restart(tmp_path: Path) -> None:
    store = module.StateStore(tmp_path / "pipeline.sqlite3")
    run = store.create_run("test")
    store.record_approval(run["id"], "approve", 42, "7")
    decision = store.pending_approval(run["id"])
    assert decision is not None
    assert decision["action"] == "approve"
    store.mark_approval_handled(decision["id"])
    assert store.pending_approval(run["id"]) is None


def test_choose_run_resumes_unfinished_unless_new(tmp_path: Path) -> None:
    store = module.StateStore(tmp_path / "pipeline.sqlite3")
    first = store.create_run("first")
    resumed = module.choose_run(store, run_id=None, topic=None, new=False)
    assert resumed["id"] == first["id"]
    second = module.choose_run(store, run_id=None, topic="second", new=True)
    assert second["id"] != first["id"]
    assert second["topic"] == "second"


def test_fetch_story_uses_xai_chat_live_search(tmp_path: Path) -> None:
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        story = {
            "title": "Verified story",
            "summary": "Summary",
            "why_it_matters": "Reason",
            "score": 91,
            "score_breakdown": {
                "recency": 95,
                "impact": 90,
                "visual_potential": 85,
                "source_confidence": 94,
            },
            "sources": [
                {"title": "A", "url": "https://a.test"},
                {"title": "B", "url": "https://b.test"},
            ],
        }
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": json.dumps(story)}}],
                "citations": ["https://a.test", "https://b.test"],
            },
        )

    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    pipeline = module.NewsPipeline(
        config, store, run, http_client=client, queue_client=FakeQueue()
    )
    try:
        pipeline.fetch_story()
    finally:
        pipeline.close()
        client.close()

    assert captured["model"] == "grok-test"
    assert captured["search_parameters"]["mode"] == "on"
    assert captured["search_parameters"]["return_citations"] is True
    assert store.get_run(run["id"])["status"] == "fetched"


def test_first_frames_use_gpt_image_1_portrait_contract(tmp_path: Path) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        calls.append(payload)
        return httpx.Response(
            200,
            json={
                "data": [
                    {"b64_json": base64.b64encode(b"image").decode("ascii")}
                ]
            },
        )

    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    shots = [
        {
            "voiceover_text": f"Voice {index}",
            "visual_prompt": f"Visual {index}",
            "first_frame_prompt": f"Frame {index}",
        }
        for index in range(1, 6)
    ]
    run = store.update_run(
        run["id"],
        status="scripted",
        story_json="{}",
        script_json=json.dumps({"shots": shots}),
    )
    client = httpx.Client(transport=httpx.MockTransport(handler))
    pipeline = module.NewsPipeline(
        config, store, run, http_client=client, queue_client=FakeQueue()
    )
    try:
        pipeline.generate_first_frames()
    finally:
        pipeline.close()
        client.close()

    assert len(calls) == 5
    assert all(call["model"] == "gpt-image-1" for call in calls)
    assert all(call["size"] == "1024x1536" for call in calls)
    assert store.get_run(run["id"])["status"] == "framed"


def test_queue_media_stages_use_required_job_types_and_roles(
    tmp_path: Path,
) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    run_dir = config.work_root / run["id"]
    clips = []
    for index in range(1, 6):
        clip = run_dir / "clips" / f"shot_{index:02d}.mp4"
        clip.parent.mkdir(parents=True, exist_ok=True)
        clip.write_bytes(b"clip")
        clips.append(str(clip))
    shots = [
        {
            "voiceover_text": f"Voice {index}",
            "visual_prompt": f"Visual {index}",
            "first_frame_prompt": f"Frame {index}",
        }
        for index in range(1, 6)
    ]
    run = store.update_run(
        run["id"],
        status="rendered",
        script_json=json.dumps({"shots": shots}),
        clips_json=json.dumps(clips),
    )
    queue = RecordingQueue()
    pipeline = module.NewsPipeline(config, store, run, queue_client=queue)
    try:
        pipeline.generate_voiceover_and_captions()
        pipeline.assemble()
    finally:
        pipeline.close()

    assert [job["job_type"] for job in queue.submissions] == [
        "tts",
        "transcribe",
        "assemble",
    ]
    assert set(queue.submissions[1]["input_files"]) == {"audio"}
    assert set(queue.submissions[2]["input_files"]) == {
        "clip_1",
        "clip_2",
        "clip_3",
        "clip_4",
        "clip_5",
        "voiceover",
        "captions",
    }
    assert store.get_run(run["id"])["status"] == "assembled"
