#!/bin/sh
set -eu

attempt=0
while [ "$attempt" -lt 60 ]; do
    if /usr/bin/curl --fail --silent --show-error --max-time 2 \
        http://127.0.0.1:8188/system_stats >/dev/null 2>&1; then
        exit 0
    fi
    attempt=$((attempt + 1))
    sleep 2
done

echo "ComfyUI did not become ready at 127.0.0.1:8188 within 120 seconds" >&2
exit 1
