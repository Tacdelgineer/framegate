from __future__ import annotations

import base64
import importlib.util
import json
import re
import sys
import wave
from dataclasses import replace
from pathlib import Path

import httpx
import pytest

import news_pipeline as module


def load_worker_contract_module():
    name = "content_factory_worker_contract"
    if name in sys.modules:
        return sys.modules[name]
    path = module.MONOREPO_ROOT / "worker-dgx" / "pipeline_worker.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    worker = importlib.util.module_from_spec(spec)
    sys.modules[name] = worker
    spec.loader.exec_module(worker)
    return worker


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
        elif submission["job_type"] == "transcribe":
            path.write_text(
                json.dumps(
                    {
                        "language": "en",
                        "words": [
                            {
                                "word": word,
                                "start": index * 0.5,
                                "end": index * 0.5 + 0.4,
                            }
                            for index, word in enumerate(
                                ("These", "are", "timed", "caption", "test", "words")
                            )
                        ],
                    }
                ),
                encoding="utf-8",
            )
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
        captions = self.run_dir / "captions.ass"
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
    assert [attempt["stage"] for attempt in store.attempts_for_run(run["id"])] == [
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
    captions_path = run_dir / "captions.ass"
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


def test_stage_does_not_retry_non_transient_worker_validation(
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
        raise module.NonRetryableWorkerError(
            "Shot 9 was rejected by worker payload validation"
        )

    try:
        with pytest.raises(
            module.NonRetryableWorkerError,
            match="Shot 9",
        ):
            pipeline.run_stage("generate_clips", fail)
    finally:
        pipeline.close()

    assert calls == 1
    with store.connect() as connection:
        outcomes = connection.execute(
            "SELECT outcome FROM stage_attempts ORDER BY id"
        ).fetchall()
    assert [row["outcome"] for row in outcomes] == ["failed"]


def test_worker_error_classification_preserves_transient_retries() -> None:
    validation = module.JobFailedError(
        "validation",
        job={
            "error": (
                "JobValidationError: video payload requires exactly one or "
                "two input frames"
            )
        },
    )
    legacy_validation = module.JobFailedError(
        "validation",
        job={
            "error": (
                "PipelineError: video payload requires exactly one or two input frames"
            )
        },
    )
    memory_gate = module.JobFailedError(
        "memory",
        job={
            "error": (
                "PipelineError: Visual job requires 40.0 GiB MemAvailable; "
                "only 32.0 GiB is available after ComfyUI cleanup"
            )
        },
    )
    timeout = module.JobFailedError(
        "timeout",
        job={"error": "PipelineError: ComfyUI prompt abc timed out after 3600s"},
    )

    assert module.is_worker_validation_error(validation)
    assert module.is_worker_validation_error(legacy_validation)
    assert not module.is_worker_validation_error(memory_gate)
    assert not module.is_worker_validation_error(timeout)


def test_queue_job_reports_non_retryable_validation_for_the_shot(
    tmp_path: Path,
) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    queue = RecordingQueue()

    def reject(*_args, **_kwargs):
        raise module.JobFailedError(
            "worker rejected video",
            job={
                "id": "job-1",
                "job_type": "video",
                "status": "failed",
                "error": (
                    "JobValidationError: video payload requires exactly one "
                    "or two input frames"
                ),
            },
        )

    queue.wait = reject
    pipeline = DryRunPipeline(config, store, run, queue_client=queue)
    try:
        with pytest.raises(
            module.NonRetryableWorkerError,
            match=("Shot 9 was rejected by worker payload validation; not retrying"),
        ):
            pipeline._queue_job(
                "video:9",
                "video",
                {"prompt": "slow pull back"},
                {"start_frame": tmp_path / "start.png"},
                tmp_path / "shot-09.mp4",
            )
    finally:
        pipeline.close()

    assert json.loads(store.get_run(run["id"])["queue_jobs_json"]) == {}


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


def test_ass_captions_use_two_or_three_word_chunks() -> None:
    transcription = {
        "words": [
            {"word": word, "start": index * 0.25, "end": index * 0.25 + 0.2}
            for index, word in enumerate(
                ("one", "two", "three", "four", "five", "six", "seven", "eight")
            )
        ]
    }

    words = module.extract_timestamped_words(transcription)
    chunks = module.chunk_caption_words(words)
    ass = module.build_ass_subtitles(transcription, module.CaptionStyleConfig())
    dialogues = [line for line in ass.splitlines() if line.startswith("Dialogue:")]

    assert [len(chunk) for chunk in chunks] == [3, 3, 2]
    assert len(dialogues) == 3
    assert all("\\N" not in line for line in dialogues)
    assert "WrapStyle: 2" in ass
    assert "Arial,72" in ass
    assert "-1,0,0,0,100,100,0,0,1,4,3,2,60,60,384,1" in ass


def test_ass_karaoke_timing_tracks_words_and_silence_in_centiseconds() -> None:
    transcription = {
        "words": [
            {"word": "Look", "start": 1.0, "end": 1.35},
            {"word": "right", "start": 1.4, "end": 1.72},
            {"word": "here", "start": 1.72, "end": 2.01},
            {"word": "Then", "start": 2.2, "end": 2.5},
            {"word": "watch", "start": 2.55, "end": 2.9},
        ]
    }

    ass = module.build_ass_subtitles(
        transcription,
        module.CaptionStyleConfig(
            base_color="#FFFFFF",
            highlight_color="#FF0000",
        ),
    )
    dialogues = [line for line in ass.splitlines() if line.startswith("Dialogue:")]

    assert dialogues == [
        (
            "Dialogue: 0,0:00:01.00,0:00:02.01,Karaoke,,0,0,0,,"
            r"{\q2}{\k35}Look{\k5} {\k32}right {\k29}here"
        ),
        (
            "Dialogue: 0,0:00:02.20,0:00:02.90,Karaoke,,0,0,0,,"
            r"{\q2}{\k30}Then{\k5} {\k35}watch"
        ),
    ]
    assert "&H000000FF,&H00FFFFFF" in ass


def test_caption_highlight_color_must_be_distinct() -> None:
    with pytest.raises(ValueError, match="must differ from base_color"):
        module.CaptionStyleConfig(
            base_color="#ffffff",
            highlight_color="#FFFFFF",
        ).validate()


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
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        module,
        "burn_ass_subtitles",
        lambda _video, _captions, destination: destination.write_bytes(b"captioned"),
    )
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
    }
    assembly = queue.submissions[6]["payload"]
    assert assembly["input_fps"] == 16
    assert assembly["fps_out"] == 30
    assert assembly["pre_caption_video_filter"] == "minterpolate=fps=30"
    assert assembly["trim_audio"] is False
    assert assembly["shortest"] is False
    assert assembly["burn_captions"] is False
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


def test_disabled_captions_skip_transcription(tmp_path: Path) -> None:
    config = settings(tmp_path)
    store = module.StateStore(config.database_path)
    run = store.create_run("test")
    shots = [{"voiceover_text": f"Voice {index}"} for index in range(1, 3)]
    run = store.update_run(
        run["id"],
        status="scripted",
        script_json=json.dumps({"shots": shots}),
    )
    queue = RecordingQueue()
    pipeline = module.NewsPipeline(
        config,
        store,
        run,
        config=module.PipelineConfig(
            caption_style=module.CaptionStyleConfig(captions_enabled=False)
        ),
        queue_client=queue,
    )
    try:
        pipeline.generate_voiceover_and_captions()
        pipeline.repair_state()
    finally:
        pipeline.close()

    saved = store.get_run(run["id"])
    assert [job["job_type"] for job in queue.submissions] == ["tts", "tts"]
    assert saved["captions_path"] is None
    assert saved["status"] == "voiced"


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
    captions = run_dir / "captions.ass"
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
    monkeypatch.setattr(
        module,
        "burn_ass_subtitles",
        lambda _video, _captions, destination: destination.write_bytes(b"captioned"),
    )
    queue = RecordingQueue()
    pipeline = module.NewsPipeline(config, store, run, queue_client=queue)
    try:
        pipeline.assemble()
    finally:
        pipeline.close()

    payload = queue.submissions[0]["payload"]
    assert payload["pre_caption_video_filter"] == (
        "minterpolate=fps=30,tpad=stop_mode=clone:stop_duration=5.500000"
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
    assert preset.max_clip_seconds == 6
    assert preset.frame_model == "flux2_klein"
    assert preset.video_mode == "auto"
    assert preset.video_resolution == "720x1280"
    assert preset.fps_out == 30
    assert preset.caption_style == module.CaptionStyleConfig(
        captions_enabled=True,
        font_size=72,
        base_color="#FFFFFF",
        highlight_color="#FFD54A",
        position=20,
    )
    assert preset.narration_style == "Conversational, curious, direct, and warm."
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
    assert all(set(job["input_files"]) == {"start_frame"} for job in video_jobs)
    assert all(job["payload"]["fps"] == 16 for job in video_jobs)
    assert all(job["payload"]["frame_count"] == 105 for job in video_jobs)
    assert all(job["payload"]["duration_seconds"] == 6.4 for job in video_jobs)


@pytest.mark.parametrize(
    ("mode", "with_last_frame", "expected_roles", "expected_frame_count"),
    [
        ("i2v", False, ["start_frame"], 89),
        ("flf", True, ["start_frame", "end_frame"], 117),
    ],
)
def test_video_job_builder_matches_worker_contract(
    tmp_path: Path,
    mode: str,
    with_last_frame: bool,
    expected_roles: list[str],
    expected_frame_count: int,
) -> None:
    first_frame = tmp_path / "start.png"
    last_frame = tmp_path / "end.png" if with_last_frame else None
    first_frame.write_bytes(b"start")
    if last_frame is not None:
        last_frame.write_bytes(b"end")
    payload, input_files = module.build_local_video_job(
        mode=mode,
        first_frame=first_frame,
        last_frame=last_frame,
        prompt="slow pull back",
        negative_prompt="text",
        resolution="720p",
        steps=8,
        seed=42,
        duration_seconds=expected_frame_count / module.WAN_FPS,
        frame_count=expected_frame_count,
    )

    assert list(input_files) == expected_roles
    assert payload["frame_count"] == expected_frame_count
    assert payload["fps"] == 16
    assert payload["aspect_ratio"] == "9:16"
    assert "frame" not in payload and "frames" not in payload

    request_body = b""

    def capture(request: httpx.Request) -> httpx.Response:
        nonlocal request_body
        request_body = request.content
        return httpx.Response(201, json={"id": "job-1", "status": "pending"})

    queue = module.JobQueueClient("http://queue")
    queue._client.close()
    queue._client = httpx.Client(
        base_url="http://queue",
        transport=httpx.MockTransport(capture),
    )
    try:
        queue.submit("video", payload, input_files=input_files)
    finally:
        queue.close()
    multipart_roles = re.findall(rb'name="(input:[^"]+)"', request_body)
    assert multipart_roles == [f"input:{role}".encode() for role in expected_roles]

    worker = load_worker_contract_module()
    worker_payload = dict(payload)
    queue_files = [
        {
            "role": role,
            "download_url": f"https://queue.test/{position}.png",
        }
        for position, role in enumerate(input_files, start=1)
    ]
    worker.JobProcessor._inject_queue_inputs(
        "video",
        worker_payload,
        queue_files,
    )
    assert worker.JobProcessor._video_frame_urls(worker_payload) == [
        item["download_url"] for item in queue_files
    ]
    assert worker.JobProcessor._video_timing(worker_payload) == (
        expected_frame_count / module.WAN_FPS,
        16,
        expected_frame_count,
    )


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
        list(job["input_files"]) == ["start_frame", "end_frame"]
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
    assert "Write for the ear, not the page" in prompt
    assert "Keep every narration sentence under 12 words" in prompt
    assert 'Use second person ("you")' in prompt
    assert '"delve", "tapestry"' in prompt
    assert "connect to the previous shot" in prompt
    assert "never summarize the explainer" in prompt
    assert "a person wouldn't say to a friend" in prompt


def test_narration_style_is_injected_into_script_prompt(tmp_path: Path) -> None:
    shot_count = module.target_shot_count(45)
    response_script = {
        "title": "Test",
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
        story_json=json.dumps({"title": "Story", "summary": "Summary"}),
    )
    client = httpx.Client(transport=httpx.MockTransport(handler))
    pipeline = module.NewsPipeline(
        config,
        store,
        run,
        config=module.PipelineConfig(narration_style="Dry, wry, and understated."),
        visuals="local",
        frame_gate=False,
        http_client=client,
        queue_client=FakeQueue(),
    )
    try:
        pipeline.write_script()
    finally:
        pipeline.close()
        client.close()

    prompt = request_payload["messages"][1]["content"]
    assert '"Dry, wry, and understated."' in prompt


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
