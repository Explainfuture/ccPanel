#!/usr/bin/env bash
# Historical staged-v10 entrypoint. Original implementation remains in Git.
set -euo pipefail
printf '%s\n' 'retired_staged_drill: pressure-live-check-remote.sh; consult scripts/pressure-sdk-window-remote.sh and docs/FAULT-DRILL.md. Do not run against the current preview.' >&2
exit 64
