"""Read-only xAI bearer resolution shared with the local Hermes install.

Hermes resolves its active auth store from ``HERMES_HOME/auth.json`` and, in
profile mode, can read the global Hermes root as a fallback.  Framegate mirrors
that path selection here but deliberately does not import Hermes' runtime
credential pool: selecting from that pool may refresh and persist a rotating
xAI OAuth grant, while this integration must remain read-only.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


XAI_BASE_URL = "https://api.x.ai/v1"


class XAIAuthError(RuntimeError):
    """Raised when no usable read-only xAI bearer can be resolved."""


@dataclass(frozen=True)
class XAICredentials:
    """Resolved xAI request credentials without a token-bearing repr."""

    bearer: str = field(repr=False)
    source: str
    base_url: str = XAI_BASE_URL
    auth_path: Path | None = field(default=None, repr=False)


def _platform_default_hermes_home() -> Path:
    if sys.platform == "win32":
        local_appdata = os.environ.get("LOCALAPPDATA", "").strip()
        base = Path(local_appdata) if local_appdata else Path.home() / "AppData" / "Local"
        return base / "hermes"
    return Path.home() / ".hermes"


def _default_hermes_root(hermes_home: Path) -> Path:
    """Mirror Hermes' root resolution for profile read fallback."""
    native_home = _platform_default_hermes_home()
    try:
        hermes_home.resolve(strict=False).relative_to(native_home.resolve(strict=False))
    except ValueError:
        if hermes_home.parent.name == "profiles":
            return hermes_home.parent.parent
        return hermes_home
    return native_home


def hermes_auth_paths(environment: Mapping[str, str] | None = None) -> list[Path]:
    """Return Hermes auth-store candidates in the same profile-first order."""
    env = os.environ if environment is None else environment
    explicit = str(env.get("HERMES_AUTH_PATH") or "").strip()
    if explicit:
        return [Path(explicit).expanduser()]

    configured_home = str(env.get("HERMES_HOME") or "").strip()
    hermes_home = (
        Path(configured_home).expanduser()
        if configured_home
        else _platform_default_hermes_home()
    )
    paths = [hermes_home / "auth.json"]
    global_path = _default_hermes_root(hermes_home) / "auth.json"
    try:
        duplicate = global_path.resolve(strict=False) == paths[0].resolve(strict=False)
    except OSError:
        duplicate = global_path == paths[0]
    if not duplicate:
        paths.append(global_path)
    return paths


def _jwt_expiry(token: str) -> float | None:
    parts = token.split(".")
    if len(parts) < 2:
        return None
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    expiry = decoded.get("exp") if isinstance(decoded, Mapping) else None
    return float(expiry) if isinstance(expiry, (int, float)) else None


def _explicit_expiry(values: Mapping[str, Any]) -> float | None:
    for key in ("expires_at_ms", "expiry_date"):
        value = values.get(key)
        if isinstance(value, (int, float)):
            return float(value) / 1000.0
    value = values.get("expires_at")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def _is_expired(token: str, metadata: Mapping[str, Any]) -> bool:
    expiry = _jwt_expiry(token)
    if expiry is None:
        expiry = _explicit_expiry(metadata)
    return expiry is not None and expiry <= time.time()


def _oauth_candidates(store: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    candidates: list[tuple[str, Mapping[str, Any]]] = []
    credential_pool = store.get("credential_pool")
    entries = (
        credential_pool.get("xai-oauth")
        if isinstance(credential_pool, Mapping)
        else None
    )
    if isinstance(entries, list):
        ordered_entries = sorted(
            (entry for entry in entries if isinstance(entry, Mapping)),
            key=lambda entry: (
                entry.get("priority")
                if isinstance(entry.get("priority"), (int, float))
                else 0
            ),
        )
        for entry in ordered_entries:
            if str(entry.get("last_status") or "").lower() == "dead":
                continue
            reset_at = entry.get("last_error_reset_at")
            if isinstance(reset_at, (int, float)) and reset_at > time.time():
                continue
            candidates.append(
                (str(entry.get("access_token") or "").strip(), entry)
            )

    providers = store.get("providers")
    state = providers.get("xai-oauth") if isinstance(providers, Mapping) else None
    tokens = state.get("tokens") if isinstance(state, Mapping) else None
    if isinstance(tokens, Mapping):
        metadata = {**dict(state), **dict(tokens)} if isinstance(state, Mapping) else tokens
        candidates.append((str(tokens.get("access_token") or "").strip(), metadata))
    return candidates


def resolve_xai_credentials(
    *,
    environment: Mapping[str, str] | None = None,
    api_key_env_value: str | None = None,
) -> XAICredentials:
    """Resolve ``XAI_API_KEY`` first, then a non-expired Hermes OAuth bearer.

    The auth store is only opened for reading.  This function never refreshes,
    rotates, locks for writing, or persists any credential state.
    """
    env = os.environ if environment is None else environment
    api_key = str(api_key_env_value or "").strip()
    if not api_key:
        api_key = str(env.get("XAI_API_KEY") or "").strip()
    if api_key:
        return XAICredentials(bearer=api_key, source="XAI_API_KEY")

    paths = hermes_auth_paths(env)
    saw_expired = False
    read_errors: list[str] = []
    for path in paths:
        try:
            raw = path.read_text(encoding="utf-8")
        except FileNotFoundError:
            continue
        except OSError as exc:
            read_errors.append(f"{path}: {type(exc).__name__}")
            continue
        try:
            store = json.loads(raw)
        except json.JSONDecodeError:
            read_errors.append(f"{path}: invalid JSON")
            continue
        if not isinstance(store, Mapping):
            read_errors.append(f"{path}: invalid auth-store shape")
            continue
        for token, metadata in _oauth_candidates(store):
            if not token:
                continue
            if _is_expired(token, metadata):
                saw_expired = True
                continue
            return XAICredentials(
                bearer=token,
                source="Hermes xai-oauth",
                auth_path=path,
            )

    if saw_expired:
        raise XAIAuthError(
            "Hermes' xAI OAuth access token is expired. Re-authenticate in "
            "Hermes with `hermes auth add xai-oauth` (or `hermes model`) and "
            "start the Framegate run again. Framegate will not refresh or "
            "rewrite Hermes' auth store."
        )
    locations = ", ".join(str(path) for path in paths)
    detail = f" Read errors: {'; '.join(read_errors)}." if read_errors else ""
    raise XAIAuthError(
        "No usable xAI bearer was found in XAI_API_KEY or Hermes' xai-oauth "
        f"store ({locations}). Re-authenticate in Hermes with `hermes auth add "
        "xai-oauth` (or `hermes model`) and start the Framegate run again."
        + detail
    )
