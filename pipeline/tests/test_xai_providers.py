from __future__ import annotations

import base64
import io
import json
import time
from pathlib import Path

import httpx
import pytest
from PIL import Image

from pipeline.providers.grok_imagine import (
    GrokImagineClient,
    XAIEntitlementError,
    XAIRateLimitError,
    normalize_frame_bytes,
)
from pipeline.providers.xai_auth import (
    XAICredentials,
    XAIAuthError,
    hermes_auth_paths,
    resolve_xai_credentials,
)


def jwt_with_expiry(expiry: float) -> str:
    def encoded(value: dict[str, object]) -> str:
        raw = json.dumps(value, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    return f"{encoded({'alg': 'none'})}.{encoded({'exp': expiry})}.signature"


def png_bytes(size: tuple[int, int] = (320, 240)) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", size, "navy").save(output, format="PNG")
    return output.getvalue()


def test_xai_api_key_precedes_hermes_auth_path(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist.json"
    credentials = resolve_xai_credentials(
        environment={
            "XAI_API_KEY": "explicit-key",
            "HERMES_AUTH_PATH": str(missing),
        }
    )

    assert credentials.source == "XAI_API_KEY"
    assert credentials.bearer == "explicit-key"
    assert "explicit-key" not in repr(credentials)


def test_xai_oauth_resolver_uses_override_read_only_and_rejects_expiry(
    tmp_path: Path,
) -> None:
    auth_path = tmp_path / "hermes-auth.json"
    token = jwt_with_expiry(time.time() + 3600)
    auth_path.write_text(
        json.dumps(
            {
                "providers": {
                    "xai-oauth": {"tokens": {"access_token": token}}
                }
            }
        ),
        encoding="utf-8",
    )
    before = auth_path.read_bytes()

    credentials = resolve_xai_credentials(
        environment={"HERMES_AUTH_PATH": str(auth_path)}
    )

    assert credentials.source == "Hermes xai-oauth"
    assert credentials.auth_path == auth_path
    assert credentials.bearer == token
    assert token not in repr(credentials)
    assert auth_path.read_bytes() == before

    auth_path.write_text(
        json.dumps(
            {
                "providers": {
                    "xai-oauth": {
                        "tokens": {"access_token": jwt_with_expiry(time.time() - 1)}
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(XAIAuthError, match="expired.*Re-authenticate in Hermes"):
        resolve_xai_credentials(
            environment={"HERMES_AUTH_PATH": str(auth_path)}
        )


def test_xai_auth_profile_path_falls_back_to_global_root(tmp_path: Path) -> None:
    root = tmp_path / "hermes"
    profile = root / "profiles" / "factory-ops"
    root.mkdir(parents=True)
    token = jwt_with_expiry(time.time() + 3600)
    (root / "auth.json").write_text(
        json.dumps(
            {
                "credential_pool": {
                    "xai-oauth": [{"access_token": token, "last_status": "ok"}]
                }
            }
        ),
        encoding="utf-8",
    )

    environment = {"HERMES_HOME": str(profile)}
    assert hermes_auth_paths(environment) == [
        profile / "auth.json",
        root / "auth.json",
    ]
    credentials = resolve_xai_credentials(environment=environment)
    assert credentials.bearer == token
    assert credentials.auth_path == root / "auth.json"


def test_grok_image_and_i2v_wire_contract_matches_hermes(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []
    image_content = png_bytes()

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/images/generations":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"b64_json": base64.b64encode(image_content).decode("ascii")}
                    ]
                },
            )
        if request.url.path == "/v1/videos/generations":
            return httpx.Response(200, json={"request_id": "video-request-1"})
        if request.url.path == "/v1/videos/video-request-1":
            return httpx.Response(
                200,
                json={
                    "status": "done",
                    "video": {"url": "https://media.test/video.mp4", "duration": 5},
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    call_count = 0

    def consume() -> None:
        nonlocal call_count
        call_count += 1

    client = httpx.Client(transport=httpx.MockTransport(handler))
    grok = GrokImagineClient(
        XAICredentials(bearer="oauth-bearer", source="test"),
        client,
        poll_interval=0,
        consume_call=consume,
    )
    frame_path = tmp_path / "frame.png"
    try:
        normalize_frame_bytes(grok.generate_image("portrait prompt"), frame_path)
        request_id = grok.start_image_to_video(
            frame_path,
            "slow push-in",
            4.2,
        )
        completion = grok.poll_video(request_id)
    finally:
        client.close()

    image_request, video_request, status_request = requests
    image_payload = json.loads(image_request.content)
    video_payload = json.loads(video_request.content)
    assert image_request.method == "POST"
    assert image_payload == {
        "model": "grok-imagine-image",
        "prompt": "portrait prompt",
        "aspect_ratio": "9:16",
        "resolution": "1k",
    }
    assert video_request.method == "POST"
    assert video_payload["model"] == "grok-imagine-video-1.5"
    assert video_payload["prompt"] == "slow push-in"
    assert video_payload["duration"] == 5
    assert video_payload["aspect_ratio"] == "9:16"
    assert video_payload["resolution"] == "720p"
    assert set(video_payload["image"]) == {"url"}
    assert video_payload["image"]["url"].startswith("data:image/png;base64,")
    assert video_request.headers["Authorization"] == "Bearer oauth-bearer"
    assert video_request.headers["Content-Type"] == "application/json"
    assert video_request.headers["User-Agent"].startswith("Hermes-Agent/")
    assert video_request.headers.get("x-idempotency-key")
    assert status_request.method == "GET"
    assert status_request.url.path == "/v1/videos/video-request-1"
    assert completion.url == "https://media.test/video.mp4"
    assert completion.duration_seconds == 5
    assert call_count == 2
    with Image.open(frame_path) as frame:
        assert frame.size == (1080, 1920)


@pytest.mark.parametrize(
    ("status_code", "error_type", "expected_calls"),
    [
        (403, XAIEntitlementError, 1),
        (429, XAIRateLimitError, 3),
    ],
)
def test_grok_surfaces_entitlement_and_bounded_rate_limit_errors(
    status_code: int,
    error_type: type[Exception],
    expected_calls: int,
) -> None:
    requests = 0
    sleeps: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(status_code, json={"error": {"message": "blocked"}})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    grok = GrokImagineClient(
        XAICredentials(bearer="oauth-bearer", source="test"),
        client,
        consume_call=lambda: None,
        sleep=sleeps.append,
    )
    try:
        with pytest.raises(error_type):
            grok.generate_image("test")
    finally:
        client.close()

    assert requests == expected_calls
    assert len(sleeps) == expected_calls - 1
