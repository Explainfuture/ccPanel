#!/usr/bin/env bash
# Historical staged-v10 entrypoint. Original implementation remains in Git.
set -euo pipefail
printf '%s\n' 'retired_staged_drill: commit-crash-check-remote.sh; consult scripts/worker-commit-kill-remote.sh. Do not run against the current preview.' >&2
exit 64
