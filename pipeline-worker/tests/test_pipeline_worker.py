from __future__ import annotations

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
        self.assertIsNone(pipeline_worker.QueueClient._parse_depth([]))


if __name__ == "__main__":
    unittest.main()
