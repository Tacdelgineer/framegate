from __future__ import annotations

import base64
import json
import wave
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

import news_pipeline as module


def write_wav(path: Path, duration_seconds: float, sample_rate: int = 1000) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with wave.open(str(path), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(sample_rate)
        output.writeframes(b"\0\0" * round(duration_seconds * sample_rate))


class FakeQueue:
    def close(self) -> None:
        pass


class RecordingQueue(FakeQueue):
    def __init__(self, tts_durations: list[float] | None = None):
        self.submissions = []
        self._jobs = {}
        self.tts_durations = list(tts_durations or [])

    def submit(self, job_type, payload, *, input_files=None):
        job_id = f"job-{len(self.submissions) + 1}"
        submission = {
            "id": job_id,
            "job_type": job_type,
            "payload": payload,
            "input_files": dict(input_files or {}),
        }
        self.submissions.append(submission)
        self._jobs[job_id] = {
            "id": job_id,
            "status": "pending",
            "submission": submission,
        }
        return self._jobs[job_id]

    def get(self, job_id):
        return self._jobs[job_id]

    def wait(self, job_id, *, output_path, timeout):
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        submission = self._jobs[job_id]["submission"]
        if submission["job_type"] == "tts":
            duration = (
                self.tts_durations.pop(0)
                if self.tts_durations
                else float(submission["payload"]["target_duration_seconds"])
            )
            write_wav(path, duration)
        else:
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

    def generate_voiceover_and_captions(self) -> None:
        shots = json.loads(self.current()["script_json"])["shots"]
        paths = []
        entries = {}
        for index, shot in enumerate(shots, start=1):
            path = self.run_dir / "voiceover" / f"shot_{index:02d}_attempt_1.wav"
            write_wav(path, 5)
            paths.append(path)
            entries[str(index)] = {
                "index": index,
                "text": shot["voiceover_text"],
                "path": str(path),
                "duration_seconds": 5,
                "tts_attempts": 1,
                "shorten_retry_used": False,
                "timing": module.clip_timing(
                    5,
                    clip_padding=self.config.clip_padding,
                    max_clip_seconds=self.config.max_clip_seconds,
                ),
            }
        voiceover = self.run_dir / "voiceover.wav"
        module.concatenate_wavs(paths, voiceover)
        captions = self.run_dir / "captions.srt"
        captions.write_text("1\n00:00:00,000 --> 00:00:01,000\nTest\n")
        self.store.update_run(
            self.run_id,
            status="voiced",
            shot_audio_json=json.dumps({"version": 1, "shots": entries}),
            voiceover_path=str(voiceover),
            captions_path=str(captions),
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
    assert [
        attempt["stage"] for attempt in store.attempts_for_run(run["id"])
    ] == [
        "fetch_story",
        "write_script",
        "generate_voiceover_and_captions",
        "generate_first_frames",
    ]


def test_repair_rolls_missing_frames_back_to_voiced(tmp_path: Path) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    run_dir = config.work_root / run["id"]
    shot_path = run_dir / "voiceover" / "shot_01_attempt_1.wav"
    voiceover_path = run_dir / "voiceover.wav"
    captions_path = run_dir / "captions.srt"
    write_wav(shot_path, 5)
    write_wav(voiceover_path, 5.5)
    captions_path.write_text("captions")
    run = store.update_run(
        run["id"],
        status="framed",
        story_json="{}",
        script_json='{"shots":[{"voiceover_text":"Voice"}]}',
        shot_audio_json=json.dumps(
            {
                "version": 1,
                "shots": {
                    "1": {
                        "path": str(shot_path),
                        "duration_seconds": 5,
                        "timing": module.clip_timing(
                            5,
                            clip_padding=0.4,
                            max_clip_seconds=8,
                        ),
                    }
                },
            }
        ),
        voiceover_path=str(voiceover_path),
        captions_path=str(captions_path),
        frames_json=json.dumps([str(tmp_path / "missing.png")]),
    )
    pipeline = DryRunPipeline(config, store, run, queue_client=FakeQueue())
    try:
        pipeline.repair_state()
    finally:
        pipeline.close()
    repaired = store.get_run(run["id"])
    assert repaired["status"] == "voiced"
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


def test_narration_timing_rounds_to_wan_frames_and_honors_cap() -> None:
    assert module.target_shot_count(45) == 9

    ordinary = module.clip_timing(
        5.1,
        clip_padding=0.4,
        max_clip_seconds=8,
    )
    assert ordinary == {
        "narration_seconds": 5.1,
        "requested_clip_seconds": 5.5,
        "clip_seconds": 5.5,
        "frame_count": 89,
        "generated_seconds": 5.5625,
        "capped": False,
    }

    capped = module.clip_timing(
        7.9,
        clip_padding=0.4,
        max_clip_seconds=8,
    )
    assert capped["requested_clip_seconds"] == pytest.approx(8.3)
    assert capped["clip_seconds"] == 8
    assert capped["frame_count"] == 125
    assert capped["generated_seconds"] == 7.8125
    assert capped["capped"] is True

    assembly = module.assembly_timing(
        video_seconds=25.33,
        voiceover_seconds=25.82,
    )
    assert assembly["video_pad_seconds"] == pytest.approx(0.49)
    assert assembly["output_seconds"] == 25.82
    assert assembly["tail_seconds"] == 0.5


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


def test_fetch_story_uses_keyless_ollama_and_strips_think_block(
    tmp_path: Path,
) -> None:
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers.get("Authorization")
        captured["payload"] = json.loads(request.content)
        brief = {
            "title": "Evergreen explainer",
            "summary": "A durable explanation of the topic.",
            "why_it_matters": "Reason",
            "audience": "Curious viewers",
            "angle": "Explain the hidden mechanism",
            "key_points": ["First", "Second", "Third"],
        }
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": (
                                "<think>private chain of thought</think>\n"
                                + json.dumps(brief)
                            )
                        }
                    }
                ],
            },
        )

    config = replace(
        settings(tmp_path),
        xai_api_key="",
        openai_api_key="",
    )
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    pipeline = module.NewsPipeline(
        config,
        store,
        run,
        visuals="local",
        frame_gate=False,
        http_client=client,
        queue_client=FakeQueue(),
    )
    try:
        pipeline.fetch_story()
    finally:
        pipeline.close()
        client.close()

    assert captured["url"] == "http://100.103.129.82:11434/v1/chat/completions"
    assert captured["authorization"] is None
    assert captured["payload"]["model"] == "qwen3.6:35b-a3b"
    assert "search_parameters" not in captured["payload"]
    assert (
        "Treat the topic as the complete editorial brief"
        in captured["payload"]["messages"][1]["content"]
    )
    stored = store.get_run(run["id"])
    assert stored["status"] == "fetched"
    assert json.loads(stored["story_json"])["title"] == "Evergreen explainer"


@pytest.mark.parametrize(
    ("base_url", "model", "key_env", "api_key"),
    [
        ("https://api.x.ai/v1", "grok-test", "XAI_API_KEY", "xai-selected"),
        (
            "https://api.openai.com/v1",
            "openai-test",
            "OPENAI_API_KEY",
            "openai-selected",
        ),
    ],
)
def test_script_provider_selection_uses_configured_endpoint_and_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    base_url: str,
    model: str,
    key_env: str,
    api_key: str,
) -> None:
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["authorization"] = request.headers.get("Authorization")
        captured["payload"] = json.loads(request.content)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": '{"ok": true}'}}]},
        )

    monkeypatch.setenv(key_env, api_key)
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    pipeline = module.NewsPipeline(
        config,
        store,
        run,
        config=module.PipelineConfig(
            script_provider=module.ScriptProviderConfig(
                base_url=base_url,
                model=model,
                api_key_env=key_env,
            )
        ),
        visuals="local",
        http_client=client,
        queue_client=FakeQueue(),
    )
    try:
        result, _ = pipeline._script_chat(
            system_prompt="Return JSON.",
            user_prompt="Test.",
        )
    finally:
        pipeline.close()
        client.close()

    assert result == {"ok": True}
    assert captured["url"] == f"{base_url}/chat/completions"
    assert captured["authorization"] == f"Bearer {api_key}"
    assert captured["payload"]["model"] == model


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
        *(["tts"] * 5),
        "transcribe",
        "assemble",
    ]
    assert queue.submissions[0]["payload"]["voice_ref"] == (
        "/home/xxfactionsxx/content-factory/assets/alireza.wav"
    )
    assert queue.submissions[0]["payload"]["voice_ref_text"] == (
        module.MONOREPO_ROOT / "assets" / "alireza.txt"
    ).read_text(encoding="utf-8").strip()
    assert set(queue.submissions[5]["input_files"]) == {"audio"}
    assert set(queue.submissions[6]["input_files"]) == {
        "clip_1",
        "clip_2",
        "clip_3",
        "clip_4",
        "clip_5",
        "voiceover",
        "captions",
    }
    assembly = queue.submissions[6]["payload"]
    assert assembly["input_fps"] == 16
    assert assembly["fps_out"] == 30
    assert assembly["pre_caption_video_filter"] == "minterpolate=fps=30"
    assert assembly["trim_audio"] is False
    assert assembly["shortest"] is False
    assert assembly["tail_room_seconds"] == 0.5
    assert store.get_run(run["id"])["status"] == "assembled"


def test_per_shot_tts_state_resumes_without_requeue(tmp_path: Path) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    shots = [{"voiceover_text": f"Voice {index}"} for index in range(1, 6)]
    run = store.update_run(
        run["id"],
        status="scripted",
        script_json=json.dumps({"shots": shots}),
    )
    queue = RecordingQueue()
    pipeline = module.NewsPipeline(config, store, run, queue_client=queue)
    try:
        pipeline.generate_voiceover_and_captions()
        first_submission_count = len(queue.submissions)
        pipeline.generate_voiceover_and_captions()
    finally:
        pipeline.close()

    assert first_submission_count == 6
    assert len(queue.submissions) == first_submission_count
    saved = store.get_run(run["id"])
    state = json.loads(saved["shot_audio_json"])
    assert sorted(state["shots"]) == ["1", "2", "3", "4", "5"]
    assert all(
        Path(entry["path"]).is_file() and entry["duration_seconds"] == 6
        for entry in state["shots"].values()
    )
    assert module.wav_duration_seconds(Path(saved["voiceover_path"])) == 30.5


def test_long_narration_gets_one_shortening_retry_and_persists_it(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    shots = [{"voiceover_text": f"Voice {index}"} for index in range(1, 6)]
    run = store.update_run(
        run["id"],
        status="scripted",
        script_json=json.dumps({"shots": shots}),
    )
    queue = RecordingQueue(tts_durations=[9, 9, 5, 5, 5, 5])
    pipeline = module.NewsPipeline(config, store, run, queue_client=queue)
    pipeline._rewrite_long_narration = lambda **_: "Shortened voice"
    try:
        pipeline.generate_voiceover_and_captions()
    finally:
        pipeline.close()

    saved = store.get_run(run["id"])
    state = json.loads(saved["shot_audio_json"])
    first = state["shots"]["1"]
    assert [job["job_type"] for job in queue.submissions].count("tts") == 6
    assert first["shorten_retry_used"] is True
    assert first["tts_attempts"] == 2
    assert first["original_duration_seconds"] == 9
    assert first["duration_seconds"] == 9
    assert first["timing"]["capped"] is True
    assert first["timing"]["frame_count"] == 125
    assert json.loads(saved["script_json"])["shots"][0]["voiceover_text"] == (
        "Shortened voice"
    )
    assert "above max_clip_seconds" in caplog.text
    assert "retry is still 9.00s" in caplog.text


def test_assembly_tpad_holds_last_frame_and_never_trims_audio(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    run_dir = config.work_root / run["id"]
    clip = run_dir / "clips" / "shot_01.mp4"
    clip.parent.mkdir(parents=True, exist_ok=True)
    clip.write_bytes(b"clip")
    voiceover = run_dir / "voiceover.wav"
    write_wav(voiceover, 10.5)
    captions = run_dir / "captions.srt"
    captions.write_text("captions")
    timing = module.clip_timing(
        10,
        clip_padding=0.4,
        max_clip_seconds=8,
    )
    run = store.update_run(
        run["id"],
        status="rendered",
        script_json=json.dumps({"shots": [{"voiceover_text": "Long voice"}]}),
        clips_json=json.dumps([str(clip)]),
        voiceover_path=str(voiceover),
        captions_path=str(captions),
        shot_audio_json=json.dumps(
            {
                "version": 1,
                "shots": {
                    "1": {
                        "path": str(voiceover),
                        "duration_seconds": 10,
                        "timing": timing,
                    }
                },
            }
        ),
    )
    monkeypatch.setattr(module, "media_duration_seconds", lambda _: 5.0)
    queue = RecordingQueue()
    pipeline = module.NewsPipeline(config, store, run, queue_client=queue)
    try:
        pipeline.assemble()
    finally:
        pipeline.close()

    payload = queue.submissions[0]["payload"]
    assert payload["pre_caption_video_filter"] == (
        "minterpolate=fps=30,"
        "tpad=stop_mode=clone:stop_duration=5.500000"
    )
    assert payload["video_pad_seconds"] == 5.5
    assert payload["output_duration_seconds"] == 10.5
    assert payload["tail_room_seconds"] == 0.5
    assert payload["trim_audio"] is False
    assert payload["shortest"] is False


def test_default_preset_loads_and_cli_defaults_local() -> None:
    preset = module.PipelineConfig.load(module.DEFAULT_PRESET_PATH)
    args = module.build_parser().parse_args([])

    assert preset.script_provider.base_url == "http://100.103.129.82:11434/v1"
    assert preset.script_provider.model == "qwen3.6:35b-a3b"
    assert preset.script_provider.api_key_env is None
    assert preset.target_duration_seconds == 45
    assert preset.clip_padding == 0.4
    assert preset.max_clip_seconds == 8
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
        pipeline.generate_voiceover_and_captions()
        pipeline.generate_first_frames()
        pipeline.generate_clips()
    finally:
        pipeline.close()

    assert [job["job_type"] for job in queue.submissions] == [
        *(["tts"] * 5),
        "transcribe",
        *(["frame"] * 5),
        *(["video"] * 5),
    ]
    frame_job = queue.submissions[6]
    assert frame_job["payload"]["workflow"] == "flux2_klein"
    assert frame_job["payload"]["steps"] == 4
    assert isinstance(frame_job["payload"]["seed"], int)
    assert frame_job["payload"]["prompt"].startswith(module.DEFAULT_STYLE_BLOCK)
    video_jobs = queue.submissions[11:]
    assert all(set(job["input_files"]) == {"frame"} for job in video_jobs)
    assert all(job["payload"]["fps"] == 16 for job in video_jobs)
    assert all(job["payload"]["frame_count"] == 105 for job in video_jobs)
    assert all(job["payload"]["duration_seconds"] == 6.4 for job in video_jobs)


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
        pipeline.generate_voiceover_and_captions()
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

    assert [job["job_type"] for job in queue.submissions[6:16]] == ["frame"] * 10
    assert all(
        set(job["input_files"]) == {"first_frame", "last_frame"}
        for job in queue.submissions[16:]
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
    shot_count = module.target_shot_count(45)
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
            for index in range(1, shot_count + 1)
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
    prompt = request_payload["messages"][1]["content"]
    assert f"Exactly {shot_count} shots" in prompt
    assert "4-6 second range" in prompt
    assert "about 45 seconds" in prompt
    assert "Exactly one short camera move" in prompt


def test_run_configuration_is_snapshotted_for_resume(tmp_path: Path) -> None:
    store = module.StateStore(tmp_path / "pipeline.sqlite3")
    run = store.create_run("test")
    first = module.PipelineConfig(
        script_provider=module.ScriptProviderConfig(
            base_url="https://api.openai.com/v1",
            model="openai-test",
            api_key_env="OPENAI_API_KEY",
        ),
        style_block="FIRST ",
        fps_out=30,
    )
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
    assert resumed.script_provider.base_url == "https://api.openai.com/v1"
    assert resumed.script_provider.model == "openai-test"
    assert resumed.script_provider.api_key_env == "OPENAI_API_KEY"
    assert resumed_visuals == "local"
    assert resumed_gate is False


def test_cloud_startup_requires_xai_key_before_pipeline_activity(
    tmp_path: Path,
) -> None:
    config = replace(settings(tmp_path), xai_api_key="")
    preset = module.PipelineConfig()

    with pytest.raises(
        RuntimeError,
        match="before any API spend or queue activity",
    ):
        module.validate_startup(config, "cloud", preset)

    module.validate_startup(config, "local", preset)


def test_script_provider_key_requirement_is_independent_of_visuals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("XAI_API_KEY", raising=False)
    config = replace(settings(tmp_path), xai_api_key="")
    xai_script = module.PipelineConfig(
        script_provider=module.ScriptProviderConfig(
            base_url="https://api.x.ai/v1",
            model="grok-test",
            api_key_env="XAI_API_KEY",
        )
    )
    openai_script = module.PipelineConfig(
        script_provider=module.ScriptProviderConfig(
            base_url="https://api.openai.com/v1",
            model="openai-test",
            api_key_env="OPENAI_API_KEY",
        )
    )

    with pytest.raises(RuntimeError, match="required by the selected script_provider"):
        module.validate_startup(config, "local", xai_script)

    module.validate_startup(config, "local", openai_script)


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
