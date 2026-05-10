#!/bin/bash
# Delegate a coding task to Codex CLI in workspace-write mode.
#
# Usage:
#   ./codex-agent.sh "<task description>"            # workdir defaults to repo root
#   ./codex-agent.sh "<task description>" <workdir>  # explicit workdir
#   ./codex-agent.sh --review "<review prompt>"      # read-only sandbox for code review
#   echo "<task>" | ./codex-agent.sh -                # read prompt from stdin
#
# Notes:
# - On Windows (git-bash) this calls %APPDATA%\npm\codex.cmd; on Linux/macOS it calls `codex`.
# - approval_policy=never so codex doesn't block on prompts. workspace-write keeps it inside the repo.
# - For code review only, pass --review (or --read-only) to use the read-only sandbox.

set -e

MODE="workspace-write"
if [ "$1" = "--review" ] || [ "$1" = "--read-only" ]; then
  MODE="read-only"
  shift
fi

TASK="$1"
WORKDIR="${2:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}"

if [ "$TASK" = "-" ]; then
  TASK="$(cat)"
fi

if [ -z "$TASK" ]; then
  echo "Usage: $0 [--review] \"<task description>\" [workdir]" >&2
  exit 1
fi

if [ -n "$APPDATA" ] && [ -f "$APPDATA/npm/codex.cmd" ]; then
  CODEX="$APPDATA/npm/codex.cmd"
else
  CODEX="codex"
fi

echo "[codex:$MODE] $(echo "$TASK" | head -c 100)..." >&2

echo "$TASK" | "$CODEX" exec \
  --sandbox "$MODE" \
  -c approval_policy="never" \
  --skip-git-repo-check \
  --color never \
  -C "$WORKDIR" \
  -
