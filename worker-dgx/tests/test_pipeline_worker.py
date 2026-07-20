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

    def test_process_frees_comfyui_after_each_visual_job(self) -> None:
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
                free_memory.assert_called_once_with()

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
            "seconds": 5,
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
        self.assertEqual(graph["7"]["inputs"]["length"], 81)
        self.assertEqual(artifact, output)
        self.assertEqual(result["workflow_variant"], "first_last")
        self.assertEqual(result["input_frame_count"], 2)
        self.assertEqual(result["duration"], 5.0625)

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


if __name__ == "__main__":
    unittest.main()
