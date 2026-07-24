from __future__ import annotations

import re
import shlex
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PLAYBOOK_PATH = REPO_ROOT / "HERMES_PLAYBOOK.md"


def _playbook_probe_command() -> list[str]:
    playbook = PLAYBOOK_PATH.read_text(encoding="utf-8")
    match = re.search(
        r"^### Probe Grok visuals\s+.*?^```bash\n(?P<block>.*?)^```$",
        playbook,
        flags=re.MULTILINE | re.DOTALL,
    )
    assert match is not None, "HERMES_PLAYBOOK.md has no Grok probe command"

    lines = match.group("block").strip().splitlines()
    assert lines[0] == "cd /home/alireza/content-factory"
    command_text = "\n".join(lines[1:]).replace("\\\n", " ")
    command = shlex.split(command_text)
    assert command[-5:] == [
        "--probe-visuals",
        "--frames-provider",
        "grok",
        "--video-provider",
        "grok",
    ]
    return command


def test_factory_ops_probe_entrypoint_help_from_repo_root() -> None:
    assert (REPO_ROOT / "pipeline" / "__init__.py").is_file()
    assert (REPO_ROOT / "pipeline" / "providers" / "__init__.py").is_file()
    command = _playbook_probe_command()
    result = subprocess.run(
        [*command, "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "--probe-visuals" in result.stdout
    assert "--frames-provider" in result.stdout
    assert "--video-provider" in result.stdout


def test_python_module_help_guards_real_pipeline_entrypoint() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "pipeline.news_pipeline", "--help"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "Create a crash-resumable, narration-timed vertical explainer" in (
        result.stdout
    )
    assert "--config" in result.stdout
