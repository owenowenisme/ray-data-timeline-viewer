#!/bin/bash
# Copy the unmerged timeline + shuffle-stats python files from the timeline
# branch checkout into <workdir>/patched/, preserving the ray/data-relative
# layout that timeline_patch.py expects.
#
#   stage_patch.sh <workdir> <timeline-branch-checkout>
#
# Get the checkout with:
#   git clone -b Ray-data-timeline-generator https://github.com/owenowenisme/ray.git
set -euo pipefail
WORKDIR="${1:?usage: stage_patch.sh <workdir> <timeline-branch-checkout>}"
WORKTREE="${2:?usage: stage_patch.sh <workdir> <timeline-branch-checkout>}"
SRC="$WORKTREE/python/ray/data"
if [[ ! -d "$SRC" ]]; then
  echo "error: $SRC not found — arg 2 must be a checkout of the timeline branch:" >&2
  echo "  git clone -b Ray-data-timeline-generator https://github.com/owenowenisme/ray.git" >&2
  exit 1
fi
DST="$WORKDIR/patched"

# Timeline instrumentation + trace export (this branch), plus the shuffle
# per-task exec-stats files from its PR #66621 base (nightly may predate them;
# overwriting with the branch versions is safe since the branch sits on #66621).
FILES=(
  block.py
  dataset.py
  _internal/timeline.py
  _internal/execution/streaming_executor.py
  _internal/execution/operators/shuffle_operators/shuffle_map_operator.py
  _internal/execution/operators/shuffle_operators/shuffle_tasks.py
  _internal/execution/operators/shuffle_operators/disk_shuffle_tasks.py
  _internal/execution/operators/shuffle_operators/disk_shuffle_map_operator.py
  _internal/execution/operators/shuffle_operators/disk_shuffle_runtime.py
)

rm -rf "$DST"
for f in "${FILES[@]}"; do
  mkdir -p "$DST/$(dirname "$f")"
  cp "$SRC/$f" "$DST/$f"
done
echo "staged $(find "$DST" -name '*.py' | wc -l | tr -d ' ') files into $DST"
