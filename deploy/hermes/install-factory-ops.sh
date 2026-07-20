#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PLAYBOOK="${REPO_ROOT}/HERMES_PLAYBOOK.md"
PROFILE_NAME="factory-ops"
PROFILE_HOME="${HOME}/.hermes/profiles/${PROFILE_NAME}"
PROFILE_CONFIG="${REPO_ROOT}/deploy/hermes/factory-ops.config.yaml"

if ! command -v hermes >/dev/null 2>&1; then
  printf 'error: hermes is not on PATH\n' >&2
  exit 1
fi

if [[ ! -f "${PLAYBOOK}" ]]; then
  printf 'error: playbook not found: %s\n' "${PLAYBOOK}" >&2
  exit 1
fi

if [[ ! -d "${PROFILE_HOME}" ]]; then
  hermes profile create "${PROFILE_NAME}" --no-skills \
    --description "Operates, monitors, reports on, and performs approved recovery for the content-factory pipeline under HERMES_PLAYBOOK.md."
fi

# Keep the operator narrow even after future `hermes update` runs.
touch "${PROFILE_HOME}/.no-bundled-skills"
install -m 0600 "${PROFILE_CONFIG}" "${PROFILE_HOME}/config.yaml"

# Hermes reads $HERMES_HOME/SOUL.md once while building a new session's
# system prompt. A symlink keeps the canonical playbook live without embedding
# or regeneration; edits take effect when the next factory-ops session starts.
ln -sfn "${PLAYBOOK}" "${PROFILE_HOME}/SOUL.md"

printf 'installed %s\n' "${PROFILE_HOME}"
printf 'prompt source: %s\n' "${PLAYBOOK}"
printf 'start: hermes -p %s chat\n' "${PROFILE_NAME}"
