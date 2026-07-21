from __future__ import annotations

import dataclasses
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pipeline_worker


class ConfigTests(unittest.TestCase):
    def test_wildcard_health_bind_is_rejected(self) -> None:
        with mock.patch.dict(os.environ, {"PIPELINE_HEALTH_HOST": "0.0.0.0"}):
            with self.assertRaisesRegex(
                pipeline_worker.PipelineError, "Refusing wildcard"
            ):
                pipeline_worker.Config.from_env()

    def test_placeholder_queue_is_marked_unconfigured(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertFalse(pipeline_worker.Config.from_env().configured)

    def test_non_loopback_comfyui_url_is_rejected(self) -> None:
        with mock.patch.dict(
            os.environ, {"COMFYUI_URL": "http://100.103.129.82:8188"}
        ):
            with self.assertRaisesRegex(
                pipeline_worker.PipelineError, "loopback"
            ):
                pipeline_worker.Config.from_env()


class MediaHelpersTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = pipeline_worker.Config.from_env()
        self.processor = pipeline_worker.JobProcessor(self.config)

    def test_srt_timestamp(self) -> None:
        self.assertEqual(self.processor._srt_timestamp(3661.234), "01:01:01,234")

    def test_caption_object_uses_segments(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            captions = self.processor._load_captions(
                {
                    "captions": {
                        "segments": [
                            {"start": 0, "end": 1.2, "text": "Hello"},
                            {"start": "bad", "end": 2, "text": "Ignored"},
                        ]
                    }
                },
                Path(raw_dir),
            )
        self.assertEqual(
            captions, [{"start": 0.0, "end": 1.2, "text": "Hello"}]
        )

    def test_write_srt(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            path = self.processor._write_srt(
                [{"start": 0.0, "end": 1.5, "text": "Hello\nworld"}],
                Path(raw_dir) / "captions.srt",
            )
            self.assertIn(
                "00:00:00,000 --> 00:00:01,500",
                path.read_text(encoding="utf-8"),
            )


class QueueTests(unittest.TestCase):
    def test_depth_parser(self) -> None:
        self.assertEqual(
            pipeline_worker.QueueClient._parse_depth({"stats": {"queued": 7}}),
            7,
        )
        self.assertEqual(
            pipeline_worker.QueueClient._parse_depth({"pending_jobs": 3}),
            3,
        )
        self.assertIsNone(pipeline_worker.QueueClient._parse_depth([]))

    def test_claim_lists_then_atomically_claims_supported_job(self) -> None:
        client = pipeline_worker.QueueClient(pipeline_worker.Config.from_env())
        claimed = {
            "id": "job-1",
            "type": "frame",
            "status": "claimed",
            "claim_token": "secret",
        }
        with mock.patch.object(
            pipeline_worker,
            "json_request",
            side_effect=[
                (
                    200,
                    [
                        {"id": "skip", "type": "unsupported"},
                        {"id": "job-1", "type": "frame"},
                    ],
                ),
                (200, claimed),
            ],
        ) as request:
            self.assertEqual(client.claim(), claimed)
        self.assertIn("status=pending", request.call_args_list[0].args[1])
        self.assertTrue(request.call_args_list[1].args[1].endswith("/jobs/job-1/claim"))
        self.assertEqual(
            request.call_args_list[1].args[2],
            {"worker_id": client.config.worker_id},
        )

    def test_completion_uploads_file_with_claim_token(self) -> None:
        client = pipeline_worker.QueueClient(pipeline_worker.Config.from_env())
        with tempfile.TemporaryDirectory() as raw_dir:
            artifact = Path(raw_dir) / "frame.png"
            artifact.write_bytes(b"fixture")
            with mock.patch.object(
                pipeline_worker, "run_command"
            ) as command:
                client.complete(
                    "job-1",
                    artifact,
                    {"type": "frame", "media_type": "image/png"},
                    "claim-secret",
                )
        args = command.call_args.args[0]
        self.assertIn(f"file=@{artifact};type=image/png", args)
        self.assertIn("claim_token=claim-secret", args)
        self.assertIn("status=done", args)


class VoiceJobTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        self.config = dataclasses.replace(
            pipeline_worker.Config.from_env(),
            output_dir=root / "outputs",
        )
        self.config.output_dir.mkdir()
        self.processor = pipeline_worker.JobProcessor(self.config)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_voice_clone_requires_both_contract_fields(self) -> None:
        valid_ref = pipeline_worker.VOICE_REFERENCE_ROOT / "alireza.wav"
        for payload, message in (
            ({"text": "Hello", "voice_ref": str(valid_ref)}, "voice_ref_text"),
            ({"text": "Hello", "voice_ref_text": "Transcript"}, "voice_ref"),
        ):
            with self.subTest(payload=payload):
                with self.assertRaisesRegex(pipeline_worker.PipelineError, message):
                    self.processor.tts(payload, Path(self.temporary.name))

    def test_voice_clone_rejects_paths_outside_repository_assets(self) -> None:
        outside = Path(self.temporary.name) / "outside.wav"
        outside.write_bytes(b"audio")
        with self.assertRaisesRegex(
            pipeline_worker.PipelineError,
            "Local path must be under",
        ):
            self.processor.tts(
                {
                    "text": "Hello",
                    "voice_ref": str(outside),
                    "voice_ref_text": "Transcript",
                },
                Path(self.temporary.name),
            )

    def test_voice_clone_stages_valid_reference_and_passes_transcript(self) -> None:
        output = self.config.output_dir / "clone.wav"
        output.write_bytes(b"audio")
        source = pipeline_worker.VOICE_REFERENCE_ROOT / "alireza.wav"
        staged = Path("/srv/ai/assets/pipeline-worker/voice-references/clone.wav")
        with (
            mock.patch.object(
                self.processor,
                "_stage_voice_reference",
                return_value=staged,
            ) as stage,
            mock.patch.object(
                self.processor.f5tts,
                "ensure_started",
                return_value="http://f5tts.test:8000",
            ),
            mock.patch.object(
                pipeline_worker,
                "json_request",
                return_value=(200, {"output_path": output.name}),
            ) as request,
        ):
            artifact, _ = self.processor.tts(
                {
                    "text": "Clone this voice.",
                    "voice_ref": str(source),
                    "voice_ref_text": "Reference transcript.",
                },
                Path(self.temporary.name),
            )
        self.assertEqual(artifact, output)
        stage.assert_called_once_with(source.resolve())
        self.assertEqual(
            request.call_args.args[2],
            {
                "text": "Clone this voice.",
                "ref_audio_path": (
                    "/app/data/assets/pipeline-worker/"
                    "voice-references/clone.wav"
                ),
                "ref_text": "Reference transcript.",
                "speed": 1.0,
            },
        )

    def test_voice_contract_absence_keeps_default_voice(self) -> None:
        output = self.config.output_dir / "default.wav"
        output.write_bytes(b"audio")
        default_ref = Path(self.config.default_ref_audio)
        with (
            mock.patch.object(
                self.processor,
                "_safe_existing_path",
                return_value=default_ref,
            ),
            mock.patch.object(
                self.processor.f5tts,
                "ensure_started",
                return_value="http://f5tts.test:8000",
            ),
            mock.patch.object(
                pipeline_worker,
                "json_request",
                return_value=(200, {"output_path": output.name}),
            ) as request,
        ):
            self.processor.tts(
                {"text": "Use the default voice."},
                Path(self.temporary.name),
            )
        self.assertEqual(request.call_args.args[2]["ref_text"], self.config.default_ref_text)
        self.assertEqual(
            request.call_args.args[2]["ref_audio_path"],
            self.processor._container_asset_path(default_ref),
        )


class VisualJobTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        root = Path(self.temporary.name)
        workflow_dir = Path(pipeline_worker.__file__).resolve().parent / "workflows"
        self.config = dataclasses.replace(
            pipeline_worker.Config.from_env(),
            comfyui_input_dir=root / "input",
            comfyui_output_dir=root / "output",
            comfyui_workflow_dir=workflow_dir,
        )
        self.config.comfyui_input_dir.mkdir()
        self.config.comfyui_output_dir.mkdir()
        self.processor = pipeline_worker.JobProcessor(self.config)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_comfyui_free_requests_model_unload_and_memory_release(self) -> None:
        with mock.patch.object(
            pipeline_worker,
            "json_request",
            return_value=(200, None),
        ) as request:
            self.processor.comfyui.free_memory()
        request.assert_called_once_with(
            "POST",
            f"{self.config.comfyui_url}/free",
            {"unload_models": True, "free_memory": True},
            timeout=self.config.request_timeout,
        )

    def test_visual_memory_gate_frees_comfyui_and_retries(self) -> None:
        with (
            mock.patch.object(
                self.processor,
                "_mem_available_gb",
                side_effect=[39.0, 42.5],
            ) as available,
            mock.patch.object(
                self.processor.comfyui,
                "free_memory",
            ) as free_memory,
            mock.patch.object(pipeline_worker.time, "sleep") as sleep,
        ):
            self.assertEqual(self.processor._require_visual_headroom(), 42.5)
        self.assertEqual(available.call_count, 2)
        free_memory.assert_called_once_with()
        sleep.assert_called_once_with(pipeline_worker.COMFYUI_FREE_SETTLE_SECONDS)

    def test_process_leaves_visual_model_cleanup_to_worker_cache(self) -> None:
        artifact = Path(self.temporary.name) / "artifact"
        for job_type in ("frame", "video"):
            with (
                self.subTest(job_type=job_type),
                mock.patch.object(
                    self.processor,
                    job_type,
                    return_value=(artifact, {"type": job_type}),
                ),
                mock.patch.object(
                    self.processor.comfyui,
                    "free_memory",
                ) as free_memory,
            ):
                self.processor.process(
                    {"type": job_type, "payload": {}},
                    Path(self.temporary.name),
                )
                free_memory.assert_not_called()

    def test_video_accepts_one_frame_aliases(self) -> None:
        for payload in (
            {"frame": "https://example.test/one.png"},
            {"frames": ["https://example.test/one.png"]},
            {"frame_url": "https://example.test/one.png"},
            {"start_frame_url": "https://example.test/one.png"},
        ):
            with self.subTest(payload=payload):
                self.assertEqual(
                    self.processor._video_frame_urls(payload),
                    ["https://example.test/one.png"],
                )

    def test_queue_input_files_supply_video_frames(self) -> None:
        payload: dict[str, object] = {"prompt": "Motion"}
        self.processor._inject_queue_inputs(
            "video",
            payload,
            [
                {
                    "role": "start_frame",
                    "download_url": "https://queue.test/start",
                },
                {
                    "role": "end_frame",
                    "download_url": "https://queue.test/end",
                },
            ],
        )
        self.assertEqual(
            payload["frames"],
            ["https://queue.test/start", "https://queue.test/end"],
        )

    def test_queue_input_files_follow_declared_assemble_clip_roles(self) -> None:
        payload: dict[str, object] = {
            "clip_roles": ["clip_2", "clip_1"],
            "voiceover_role": "narration",
            "captions_role": "subtitles",
        }
        self.processor._inject_queue_inputs(
            "assemble",
            payload,
            [
                {"role": "clip_1", "download_url": "https://queue.test/one"},
                {"role": "subtitles", "download_url": "https://queue.test/srt"},
                {"role": "clip_2", "download_url": "https://queue.test/two"},
                {
                    "role": "narration",
                    "download_url": "https://queue.test/voice",
                },
            ],
        )
        self.assertEqual(
            payload["clips"],
            ["https://queue.test/two", "https://queue.test/one"],
        )
        self.assertEqual(payload["voiceover_url"], "https://queue.test/voice")
        self.assertEqual(payload["captions_url"], "https://queue.test/srt")

    def test_video_timing_accepts_narration_frame_count(self) -> None:
        self.assertEqual(
            self.processor._video_timing(
                {
                    "duration_seconds": 3.0,
                    "frame_count": 49,
                    "fps": 16,
                }
            ),
            (49, 3.0),
        )
        self.assertEqual(
            self.processor._video_timing(
                {
                    "duration_seconds": 10.0,
                    "frame_count": 129,
                    "fps": 16,
                }
            ),
            (129, 10.0),
        )

    def test_video_timing_rejects_invalid_frame_counts(self) -> None:
        for frame_count, message in (
            (48, "4n\\+1"),
            (0, "4n\\+1"),
            (133, "8s cap"),
            (49.0, "integer"),
            (True, "integer"),
        ):
            with self.subTest(frame_count=frame_count):
                with self.assertRaisesRegex(
                    pipeline_worker.PipelineError,
                    message,
                ):
                    self.processor._video_timing(
                        {
                            "duration_seconds": 3.0,
                            "frame_count": frame_count,
                            "fps": 16,
                        }
                    )

    def test_video_timing_requires_16_fps(self) -> None:
        with self.assertRaisesRegex(
            pipeline_worker.PipelineError,
            "fps must be 16",
        ):
            self.processor._video_timing(
                {
                    "duration_seconds": 3.0,
                    "frame_count": 49,
                    "fps": 24,
                }
            )

    def test_assemble_clone_pads_video_so_narration_is_not_trimmed(self) -> None:
        root = Path(self.temporary.name)
        clip = root / "clip.mp4"
        voiceover = root / "voiceover.wav"
        with (
            mock.patch.object(
                self.processor,
                "download",
                side_effect=[clip, voiceover],
            ),
            mock.patch.object(
                pipeline_worker,
                "run_command",
            ) as run_command,
            mock.patch.object(
                self.processor,
                "_load_captions",
                return_value=[],
            ),
            mock.patch.object(
                self.processor,
                "_duration",
                return_value=10.0,
            ),
        ):
            self.processor.assemble(
                {
                    "clips": ["https://queue.test/clip"],
                    "voiceover_url": "https://queue.test/voiceover",
                },
                root,
            )
        mux_command = run_command.call_args_list[-1].args[0]
        filter_index = mux_command.index("-vf")
        self.assertEqual(
            mux_command[filter_index + 1],
            "tpad=stop_mode=clone",
        )
        self.assertIn("-shortest", mux_command)

    def test_video_accepts_two_frames_and_selects_first_last_workflow(self) -> None:
        cases = (
            {
                "frame": [
                    "https://example.test/start.png",
                    "https://example.test/end.png",
                ]
            },
            {
                "frames": [
                    "https://example.test/start.png",
                    "https://example.test/end.png",
                ]
            },
            {
                "start_frame": "https://example.test/start.png",
                "end_frame": "https://example.test/end.png",
            },
            {
                "frame_url": "https://example.test/start.png",
                "end_frame_url": "https://example.test/end.png",
            },
        )
        for payload in cases:
            with self.subTest(payload=payload):
                self.assertEqual(
                    self.processor._video_frame_urls(payload),
                    [
                        "https://example.test/start.png",
                        "https://example.test/end.png",
                    ],
                )

        output = self.config.comfyui_output_dir / "video.mp4"
        output.write_bytes(b"fixture")
        staged = [
            self.config.comfyui_input_dir / "start.png",
            self.config.comfyui_input_dir / "end.png",
        ]
        for path in staged:
            path.write_bytes(b"fixture")
        payload = {
            "prompt": "A smooth transition",
            "frames": [
                "https://example.test/start.png",
                "https://example.test/end.png",
            ],
            "duration_seconds": 3,
            "frame_count": 49,
            "fps": 16,
            "seed": 7,
        }
        with (
            mock.patch.object(
                self.processor,
                "_stage_comfy_frame",
                side_effect=[
                    (staged[0], staged[0].name),
                    (staged[1], staged[1].name),
                ],
            ),
            mock.patch.object(
                self.processor, "_require_visual_headroom", return_value=90.0
            ),
            mock.patch.object(
                self.processor.comfyui,
                "run",
                return_value=(output, "prompt-id"),
            ) as run,
            mock.patch.object(
                self.processor, "_dimensions", return_value=(576, 1024)
            ),
        ):
            artifact, result = self.processor.video(
                payload, Path(self.temporary.name)
            )
        graph = run.call_args.args[0]
        self.assertEqual(graph["7"]["class_type"], "WanFirstLastFrameToVideo")
        self.assertEqual(graph["7"]["inputs"]["length"], 49)
        self.assertEqual(artifact, output)
        self.assertEqual(result["workflow_variant"], "first_last")
        self.assertEqual(result["input_frame_count"], 2)
        self.assertEqual(result["frame_count"], 49)
        self.assertEqual(result["requested_seconds"], 3.0)
        self.assertEqual(result["duration"], 3.0625)

    def test_video_selects_plain_i2v_workflow_for_one_frame(self) -> None:
        output = self.config.comfyui_output_dir / "video.mp4"
        output.write_bytes(b"fixture")
        staged = self.config.comfyui_input_dir / "start.png"
        staged.write_bytes(b"fixture")
        payload = {
            "prompt": "Gentle camera motion",
            "frame": "https://example.test/start.png",
            "seconds": 5,
            "seed": 9,
        }
        with (
            mock.patch.object(
                self.processor,
                "_stage_comfy_frame",
                return_value=(staged, staged.name),
            ),
            mock.patch.object(
                self.processor, "_require_visual_headroom", return_value=90.0
            ),
            mock.patch.object(
                self.processor.comfyui,
                "run",
                return_value=(output, "prompt-id"),
            ) as run,
            mock.patch.object(
                self.processor, "_dimensions", return_value=(576, 1024)
            ),
        ):
            _, result = self.processor.video(payload, Path(self.temporary.name))
        graph = run.call_args.args[0]
        self.assertEqual(graph["6"]["class_type"], "WanImageToVideo")
        self.assertEqual(result["workflow_variant"], "i2v")
        self.assertEqual(result["input_frame_count"], 1)

    def test_video_rejects_zero_or_more_than_two_frames(self) -> None:
        for payload in (
            {},
            {"frames": []},
            {
                "frames": [
                    "https://example.test/1.png",
                    "https://example.test/2.png",
                    "https://example.test/3.png",
                ]
            },
        ):
            with self.subTest(payload=payload):
                with self.assertRaisesRegex(
                    pipeline_worker.PipelineError, "one or two"
                ):
                    self.processor._video_frame_urls(payload)

    def test_checked_in_workflows_are_api_graphs(self) -> None:
        expected_nodes = {
            "flux2_klein_frame_api.json": "EmptyFlux2LatentImage",
            "wan22_i2v_api.json": "WanImageToVideo",
            "wan22_first_last_api.json": "WanFirstLastFrameToVideo",
        }
        for filename, expected in expected_nodes.items():
            with self.subTest(filename=filename):
                graph = self.processor.comfyui.load_workflow(filename)
                self.assertIn(expected, {node["class_type"] for node in graph.values()})


class WorkerModelCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.worker = pipeline_worker.Worker(pipeline_worker.Config.from_env())

    def test_same_family_is_cached_without_free(self) -> None:
        with (
            mock.patch.object(
                self.worker.processor,
                "_free_comfyui_memory",
            ) as free_memory,
            self.assertLogs("pipeline-worker", level="INFO") as logs,
        ):
            self.assertEqual(
                self.worker._prepare_model_cache("frame-1", "frame"),
                "load",
            )
            self.assertEqual(
                self.worker._prepare_model_cache("frame-2", "frame"),
                "cached",
            )
        free_memory.assert_not_called()
        self.assertIn("family=flux-frame state=load", logs.output[0])
        self.assertIn("family=flux-frame state=cached", logs.output[1])

    def test_family_transitions_free_cached_model(self) -> None:
        with mock.patch.object(
            self.worker.processor,
            "_free_comfyui_memory",
        ) as free_memory:
            self.worker._prepare_model_cache("frame-1", "frame")
            self.worker._prepare_model_cache("video-1", "video")
            self.worker._prepare_model_cache("tts-1", "tts")
        self.assertEqual(free_memory.call_count, 2)
        self.assertIn(
            "flux-frame to wan-video",
            free_memory.call_args_list[0].args[0],
        )
        self.assertIn(
            "wan-video to non-visual",
            free_memory.call_args_list[1].args[0],
        )
        self.assertIsNone(self.worker.cached_model_family)


if __name__ == "__main__":
    unittest.main()
