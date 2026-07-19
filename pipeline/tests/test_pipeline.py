from __future__ import annotations

import json
import base64
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

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
        voice_preset="alireza",
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
        config,
        store,
        run,
        visuals="cloud",
        frame_gate=False,
        http_client=client,
        queue_client=FakeQueue(),
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
            json={"data": [{"b64_json": base64.b64encode(b"image").decode("ascii")}]},
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
        config,
        store,
        run,
        visuals="cloud",
        frame_gate=False,
        http_client=client,
        queue_client=FakeQueue(),
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
    assert queue.submissions[0]["payload"]["voice_ref"] == (
        "/home/xxfactionsxx/content-factory/assets/alireza.wav"
    )
    assert queue.submissions[0]["payload"]["voice_ref_text"] == (
        module.MONOREPO_ROOT / "assets" / "alireza.txt"
    ).read_text(encoding="utf-8").strip()
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
    assert queue.submissions[2]["payload"]["input_fps"] == 16
    assert queue.submissions[2]["payload"]["fps_out"] == 30
    assert (
        queue.submissions[2]["payload"]["pre_caption_video_filter"]
        == "minterpolate=fps=30"
    )
    assert store.get_run(run["id"])["status"] == "assembled"


def test_default_preset_loads_and_cli_defaults_local() -> None:
    preset = module.PipelineConfig.load(module.DEFAULT_PRESET_PATH)
    args = module.build_parser().parse_args([])

    assert preset.frame_model == "flux2_klein"
    assert preset.video_mode == "i2v"
    assert preset.fps_out == 30
    assert args.visuals == "local"
    assert args.frame_gate is True


def test_status_interfaces_report_sqlite_state_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "pipeline.sqlite3"
    store = module.StateStore(database_path)
    run = store.create_run("status topic")
    gate = {
        "frames": [
            {
                "index": 1,
                "shot_index": 1,
                "role": "first",
                "generation": 2,
                "status": "approved",
            },
            {
                "index": 2,
                "shot_index": 2,
                "role": "first",
                "generation": 0,
                "status": "pending",
            },
        ]
    }
    run = store.update_run(
        run["id"],
        status="framed",
        frame_gate_json=json.dumps(gate),
        run_config_json=json.dumps({"frame_gate": True}),
        last_error="most recent error",
    )
    completed = store.start_attempt(run["id"], "generate_first_frames")
    store.finish_attempt(completed, "succeeded")
    failed = store.start_attempt(run["id"], "frame_gate")
    store.finish_attempt(failed, "failed", "approval timeout")
    store.start_attempt(run["id"], "frame_gate")
    monkeypatch.setattr(module, "load_environment", lambda _: None)

    result = module.main(
        [
            "--run-id",
            run["id"],
            "--status",
            "--json",
            "--db",
            str(database_path),
            "--config",
            str(tmp_path / "missing-preset.yaml"),
        ]
    )

    assert result == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["run_id"] == run["id"]
    assert summary["topic"] == "status topic"
    assert summary["state"] == "framed"
    assert summary["current_stage"] == "frame_gate"
    assert summary["frame_gate"]["enabled"] is True
    assert summary["frame_gate"]["shots"][0] == {
        "shot": 1,
        "state": "approved",
        "frames": [
            {
                "role": "first",
                "state": "approved",
                "generation": 2,
            }
        ],
    }
    assert summary["frame_gate"]["shots"][2]["state"] == "not_started"
    assert [job["stage"] for job in summary["jobs"]["completed"]] == [
        "generate_first_frames"
    ]
    assert summary["jobs"]["failed"][0]["error"] == "approval timeout"
    assert summary["jobs"]["running"][0]["stage"] == "frame_gate"
    assert summary["timestamps"]["created_at"]
    assert summary["timestamps"]["updated_at"]

    result = module.main(["--status", "--db", str(database_path)])

    assert result == 0
    output = capsys.readouterr().out
    assert f"Run ID: {run['id']}" in output
    assert "Current stage: frame_gate" in output
    assert "Shot 1: approved [first=approved (generation 2)]" in output
    assert "Completed jobs (1):" in output
    assert "Failed jobs (1):" in output


def test_json_implies_status_and_prefers_unfinished_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database_path = tmp_path / "pipeline.sqlite3"
    store = module.StateStore(database_path)
    unfinished = store.create_run("unfinished")
    finished = store.create_run("finished")
    store.update_run(finished["id"], status="published")
    monkeypatch.setattr(module, "load_environment", lambda _: None)

    result = module.main(["--json", "--db", str(database_path)])

    assert result == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["run_id"] == unfinished["id"]


def test_status_does_not_create_a_missing_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "missing" / "pipeline.sqlite3"
    monkeypatch.setattr(module, "load_environment", lambda _: None)

    result = module.main(["--status", "--db", str(database_path)])

    assert result == 1
    assert not database_path.exists()
    assert not database_path.parent.exists()


def test_local_visuals_enqueue_frame_and_video_jobs(tmp_path: Path) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    shots = [
        {
            "voiceover_text": f"Voice {index}",
            "motion_instruction": "slow push-in",
            "video_mode": "i2v",
            "first_frame_prompt": f"Frame {index}",
            "last_frame_prompt": None,
        }
        for index in range(1, 6)
    ]
    run = store.update_run(
        run["id"],
        status="scripted",
        story_json="{}",
        script_json=json.dumps({"shots": shots}),
    )
    queue = RecordingQueue()
    pipeline = module.NewsPipeline(
        config,
        store,
        run,
        config=module.PipelineConfig(),
        visuals="local",
        frame_gate=False,
        queue_client=queue,
    )
    try:
        pipeline.generate_first_frames()
        pipeline.generate_clips()
    finally:
        pipeline.close()

    assert [job["job_type"] for job in queue.submissions] == [
        *(["frame"] * 5),
        *(["video"] * 5),
    ]
    frame_job = queue.submissions[0]
    assert frame_job["payload"]["workflow"] == "flux2_klein"
    assert frame_job["payload"]["steps"] == 4
    assert isinstance(frame_job["payload"]["seed"], int)
    assert frame_job["payload"]["prompt"].startswith(module.DEFAULT_STYLE_BLOCK)
    assert all(set(job["input_files"]) == {"frame"} for job in queue.submissions[5:])


def test_flf_uses_two_approved_frames_per_video(tmp_path: Path) -> None:
    config = settings(tmp_path)
    preset = module.PipelineConfig(video_mode="flf")
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    shots = [
        {
            "voiceover_text": f"Voice {index}",
            "motion_instruction": "gentle pan left",
            "video_mode": "flf",
            "first_frame_prompt": f"Opening {index}",
            "last_frame_prompt": f"Ending {index}",
        }
        for index in range(1, 6)
    ]
    run = store.update_run(
        run["id"],
        status="scripted",
        story_json="{}",
        script_json=json.dumps({"shots": shots}),
    )
    queue = RecordingQueue()
    pipeline = module.NewsPipeline(
        config,
        store,
        run,
        config=preset,
        visuals="local",
        frame_gate=True,
        queue_client=queue,
    )
    try:
        pipeline.generate_first_frames()
        with pytest.raises(RuntimeError, match="until every frame is approved"):
            pipeline.generate_clips()
        state = json.loads(store.get_run(run["id"])["frame_gate_json"])
        for frame in state["frames"]:
            frame["status"] = "approved"
        store.update_run(run["id"], frame_gate_json=json.dumps(state))
        pipeline.generate_clips()
    finally:
        pipeline.close()

    assert [job["job_type"] for job in queue.submissions[:10]] == ["frame"] * 10
    assert all(
        set(job["input_files"]) == {"first_frame", "last_frame"}
        for job in queue.submissions[10:]
    )


def test_frame_regeneration_only_requeues_selected_frame_with_new_seed(
    tmp_path: Path,
) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    shots = [
        {
            "voiceover_text": f"Voice {index}",
            "motion_instruction": "slow push-in",
            "video_mode": "i2v",
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
    queue = RecordingQueue()
    pipeline = module.NewsPipeline(
        config,
        store,
        run,
        visuals="local",
        frame_gate=True,
        queue_client=queue,
    )
    try:
        pipeline.generate_first_frames()
        state = json.loads(store.get_run(run["id"])["frame_gate_json"])
        for index, frame in enumerate(state["frames"], start=1):
            frame["album_message_id"] = str(index)
            frame["control_message_id"] = str(100 + index)
        state["chat_id"] = "123"
        state["album_message_ids"] = [str(index) for index in range(1, 6)]
        store.update_run(run["id"], frame_gate_json=json.dumps(state))
        original_seed = state["frames"][1]["seed"]
        selected_path = Path(state["frames"][1]["path"])
        store.record_frame_approval(run["id"], 2, 0, "regenerate", 999, "7")
        persisted = store.pending_frame_approval(run["id"])
        assert persisted is not None
        pipeline._process_frame_approval(persisted)
        assert not selected_path.exists()
        pipeline._edit_frame_album_item = lambda *_: None
        pipeline._send_frame_control = lambda *_: None
        pipeline.wait_for_frame_approval = lambda: None
        assert pipeline.run_frame_gate() is False
    finally:
        pipeline.close()

    updated = json.loads(store.get_run(run["id"])["frame_gate_json"])
    assert len(queue.submissions) == 6
    assert queue.submissions[-1]["job_type"] == "frame"
    assert updated["frames"][1]["seed"] != original_seed
    assert updated["frames"][1]["generation"] == 1
    assert updated["frames"][1]["status"] == "pending"
    assert selected_path.is_file()


def test_frame_album_and_controls_are_not_resent_after_restart(
    tmp_path: Path,
) -> None:
    class TelegramPipeline(module.NewsPipeline):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.telegram_calls = []

        def _telegram_api(self, method, **kwargs):
            self.telegram_calls.append((method, kwargs))
            if method == "sendMediaGroup":
                return {
                    "ok": True,
                    "result": [{"message_id": index} for index in range(1, 6)],
                }
            if method == "sendMessage":
                return {
                    "ok": True,
                    "result": {"message_id": 100 + len(self.telegram_calls)},
                }
            raise AssertionError(f"unexpected Telegram method: {method}")

    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    shots = [
        {
            "voiceover_text": f"Voice {index}",
            "motion_instruction": "slow push-in",
            "video_mode": "i2v",
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
    queue = RecordingQueue()
    first = TelegramPipeline(
        config,
        store,
        run,
        visuals="local",
        frame_gate=True,
        queue_client=queue,
    )
    try:
        first.generate_first_frames()
        first.request_frame_approval()
    finally:
        first.close()

    assert [call[0] for call in first.telegram_calls] == [
        "sendMediaGroup",
        *(["sendMessage"] * 5),
    ]
    resumed = TelegramPipeline(
        config,
        store,
        store.get_run(run["id"]),
        visuals="local",
        frame_gate=True,
        queue_client=queue,
    )
    try:
        resumed.request_frame_approval()
    finally:
        resumed.close()

    assert resumed.telegram_calls == []


def test_script_normalizes_motion_mode_and_verbatim_style(tmp_path: Path) -> None:
    style = "ARCHIVAL COLLAGE — "
    response_script = {
        "title": "Title",
        "description": "Description",
        "shots": [
            {
                "voiceover_text": f"Voice {index}",
                "motion_instruction": "slow push-in",
                "video_mode": "i2v",
                "first_frame_prompt": f"Subject {index}",
                "last_frame_prompt": None,
            }
            for index in range(1, 6)
        ],
    }
    request_payload = {}

    def handler(request: httpx.Request) -> httpx.Response:
        request_payload.update(json.loads(request.content))
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": json.dumps(response_script)}}]},
        )

    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    run = store.update_run(
        run["id"],
        status="fetched",
        story_json=json.dumps({"title": "Story", "summary": "Summary", "sources": []}),
    )
    client = httpx.Client(transport=httpx.MockTransport(handler))
    pipeline = module.NewsPipeline(
        config,
        store,
        run,
        config=module.PipelineConfig(style_block=style, video_mode="auto"),
        visuals="cloud",
        frame_gate=False,
        http_client=client,
        queue_client=FakeQueue(),
    )
    try:
        pipeline.write_script()
    finally:
        pipeline.close()
        client.close()

    script = json.loads(store.get_run(run["id"])["script_json"])
    assert all(shot["first_frame_prompt"].startswith(style) for shot in script["shots"])
    assert all(shot["motion_instruction"] == "slow push-in" for shot in script["shots"])
    assert "Exactly one short camera move" in request_payload["messages"][1]["content"]


def test_run_configuration_is_snapshotted_for_resume(tmp_path: Path) -> None:
    store = module.StateStore(tmp_path / "pipeline.sqlite3")
    run = store.create_run("test")
    first = module.PipelineConfig(style_block="FIRST ", fps_out=30)
    run, _, visuals, frame_gate = module.configure_run(
        store,
        run,
        config=first,
        visuals="local",
        frame_gate=False,
    )
    _, resumed, resumed_visuals, resumed_gate = module.configure_run(
        store,
        run,
        config=module.PipelineConfig(style_block="CHANGED ", fps_out=60),
        visuals="cloud",
        frame_gate=True,
    )

    assert visuals == "local"
    assert frame_gate is False
    assert resumed.style_block == "FIRST "
    assert resumed.fps_out == 30
    assert resumed_visuals == "local"
    assert resumed_gate is False


def test_cloud_startup_requires_xai_key_before_pipeline_activity(
    tmp_path: Path,
) -> None:
    config = replace(settings(tmp_path), xai_api_key="")

    with pytest.raises(
        RuntimeError,
        match="before any API spend or queue activity",
    ):
        module.validate_startup(config, "cloud")

    module.validate_startup(config, "local")


def test_cloud_cli_fails_before_creating_state_without_xai_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = replace(settings(tmp_path), xai_api_key="")
    monkeypatch.setattr(module, "load_environment", lambda _: None)
    monkeypatch.setattr(
        module.Settings,
        "from_environment",
        classmethod(lambda cls, *args, **kwargs: config),
    )

    result = module.main(
        [
            "--new",
            "--visuals",
            "cloud",
            "--config",
            str(module.DEFAULT_PRESET_PATH),
        ]
    )

    assert result == 2
    assert not config.database_path.exists()
