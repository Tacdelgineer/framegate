#!/usr/bin/env bash
set -u

endpoint="http://100.103.129.82:11434/api/chat"
payload='{"model":"qwen3:8b","messages":[{"role":"user","content":"Reply OK"}],"stream":false,"think":false,"keep_alive":-1,"options":{"num_predict":2}}'

for _attempt in {1..60}; do
    if /usr/bin/curl \
        --fail \
        --silent \
        --show-error \
        --max-time 600 \
        --header "Content-Type: application/json" \
        --data-binary "$payload" \
        "$endpoint" >/dev/null; then
        exit 0
    fi
    sleep 2
done

exit 1

