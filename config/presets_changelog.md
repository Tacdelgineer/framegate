# Preset changelog

- 2026-07-20T02:12:03Z — Added `target_duration_seconds` (`unset` → `45`), `clip_padding` (`unset` → `0.4`), and `max_clip_seconds` (`unset` → `8.0`) for narration-driven timing.
- 2026-07-20T02:54:41Z — Set `video_mode` (`i2v` → `auto`).
- 2026-07-20T05:33:25Z — Set `style_block` (`Cinematic news documentary photography, realistic lighting. ` → `isometric voxel diorama, chunky cubic blocks, tilt-shift macro photography, soft studio lighting, vibrant color palette against dark clean background, highly detailed 3D render, shallow depth of field`) and appended `, photorealistic humans, realistic faces` to `negative_prompt`.
- 2026-07-20T22:27:12Z — Set `video_resolution` (`720p` → `720x1280`) and `max_clip_seconds` (`8.0` → `6.0`).
- 2026-07-20T22:35:52Z — Added `caption_style` (`unset` → `{captions_enabled: true, font_size: 72, base_color: "#FFFFFF", highlight_color: "#FFD54A", position: 20}`) and `narration_style` (`unset` → `Conversational, curious, direct, and warm.`).
- 2026-07-20T22:48:05Z — Set `style_block` (`isometric voxel diorama, chunky cubic blocks, tilt-shift macro photography, soft studio lighting, vibrant color palette against dark clean background, highly detailed 3D render, shallow depth of field` → `isometric voxel diorama, chunky cubic blocks, cutaway cross-section view, tilt-shift macro photography, soft studio lighting, vibrant color palette against dark clean background, highly detailed 3D render`).
- 2026-07-20T23:08:44Z — Added `script_provider.timeout_seconds` (`unset` → `900`) as the overall streaming-generation ceiling.
- 2026-07-21T02:24:21Z — Added `voice` (`unset` → `narrator`) to select the named cloned-voice preset; use `default` for the worker's stock voice.
