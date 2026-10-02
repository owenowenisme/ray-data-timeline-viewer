"""TEMPLATE: a profiling job that exports a Ray Data execution timeline.

Runs on stock Ray — no source patching. Copy into the job's working_dir, edit
the pipeline section, and submit with `ray-data-timeline` in --requirements.
Optionally also auto-export via env vars instead of the explicit call below:
  RAY_DATA_EXECUTION_CALLBACKS=ray_data_timeline.TimelineCallback
  RAY_DATA_TIMELINE_OUTPUT=s3://bucket/<run>/
"""

import os
import time

import ray

ray.init()

import ray.data  # noqa: E402,F401
import ray_data_timeline  # noqa: E402

# Capture the true operator DAG from the live topology (no Ray changes).
ray_data_timeline.install()

# Output: artifact storage on Anyscale, else edit to any local path or s3:// URI.
OUT = os.environ.get("ANYSCALE_ARTIFACT_STORAGE", ".").rstrip("/") + "/timeline_run"

# EDIT: your Ray Data pipeline. Keep the handle; end with .materialize().
# Example:
#   ds = ray.data.read_parquet("s3://...").map_batches(fn)
#   result = ds.materialize()
t0 = time.time()
result = ray.data.range(1000).materialize()  # EDIT
print(f"pipeline done in {time.time() - t0:.1f}s, rows={result.count()}")

# Export the timeline (Chrome trace; .gz ok). Open in the hosted viewer.
ray_data_timeline.export_timeline(result, f"{OUT}/timeline.json.gz")
print(f"TIMELINE_OUTPUT: {OUT}/timeline.json.gz")
