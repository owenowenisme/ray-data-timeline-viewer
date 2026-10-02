---
name: ray-data-timeline-profiling
description: Profile a Ray Data pipeline on an Anyscale (or any Ray) cluster and render its execution timeline. Installs the ray-data-timeline pip package (no Ray source changes) to export a per-operator timeline with task lanes, running-task counters, and failed task attempts. Use when asked to profile/benchmark a Ray Data job and see where its time went.
---

# Ray Data timeline profiling

Run a Ray Data pipeline, export its execution timeline, and render it as an
interactive page. The `ray-data-timeline` package works on **stock Ray** — no
source patching, no custom image. It reconstructs the timeline from the
per-block stats Ray already records, pulls failed attempts and shuffle-stage
spans from the Ray state API, and captures the true operator DAG via a public
`ExecutionCallback`.

Assets are in `assets/`. Key resources:
- **Package**: `ray-data-timeline`, installed from the repo
  `https://github.com/owenowenisme/ray-data-timeline-viewer` (contains the
  `ray_data_timeline/` package and `pyproject.toml`). Install on the job with
  `--requirements` (see below).
- **Hosted viewer**: https://owenowenisme.github.io/ray-data-timeline-viewer/
  — drop a trace `.json.gz` on it, nothing to install.
- **Work dir**: a scratch directory per run, e.g. `~/rayprof/<run-name>/`.

## Fidelity: stock Ray vs the instrumentation branch

The package runs on stock Ray and degrades gracefully. Everything works; two
things are approximate until the upstream fixes land:

| Capability | Stock Ray | With ray-project/ray#66621 + anchor branch |
|---|---|---|
| Per-operator task lanes, counters | ✅ | ✅ |
| Failed task attempts | ✅ | ✅ |
| True operator DAG edges | ✅ (via callback) | ✅ |
| Shuffle-stage spans (join/agg/sort) | ✅ spans only, **no row/byte counts** (state-API backfill) | ✅ with rows/bytes |
| Multi-node clock | approximate (epoch offset estimated from state API) | exact (per-block epoch anchor) |

Use stock Ray by default. Only patch in the branch files (step 5, optional) if
a report needs exact shuffle row/byte counts or exact multi-node alignment.

## Overview

1. Write a profiling script from `assets/profile_template.py`.
2. Submit the job with the package installed via `--requirements`.
3. Download the trace (presign from the cluster if local SSO is expired).
4. Render in the hosted viewer; summarize where the time went.
5. (Optional) Re-run with the instrumentation branch for exact shuffle stats.

## 1. Build the profiling script

Copy `assets/profile_template.py` into the work dir. Edit the pipeline section
and keep the dataset handle ending in `.materialize()`. The template calls:

```python
import ray_data_timeline
ray_data_timeline.install()                      # capture true DAG edges
ds = <your pipeline>.materialize()
ray_data_timeline.export_timeline(ds, OUTPUT)    # OUTPUT: a local path or s3:// URI, .gz ok
```

Alternatively, **no code changes at all**: set two env vars on the job and
every execution auto-exports its trace:
- `RAY_DATA_EXECUTION_CALLBACKS=ray_data_timeline.TimelineCallback`
- `RAY_DATA_TIMELINE_OUTPUT=s3://bucket/<run>/` (or a local dir)

Each execution writes `<prefix>/<dataset_id>.json.gz` and prints
`TIMELINE_OUTPUT: <path>`.

Pipeline gotchas:
- `Dataset.join` needs `polars`; expression UDFs (e.g. a TPC-H `to_f64`) need
  the `@udf(return_dtype=...)` decorator (a bare `pc.cast(col(...))` fails
  eagerly). `assets/requirements.txt` pins `polars` + `pyarrow`.
- Disk shuffle: add `--env RAY_DATA_ENABLE_DISK_SHUFFLE=1` and use instances
  with local NVMe (m5d.* etc.).

## 2. Submit the job

Put the package on the job via `--requirements`. The simplest line installs it
straight from the repo; add it to `assets/requirements.txt`:

```
ray-data-timeline @ git+https://github.com/owenowenisme/ray-data-timeline-viewer.git
polars
pyarrow
```

Authenticate the Anyscale CLI first. For Anyscale employees on staging:
```bash
export ANYSCALE_HOST=https://console.anyscale-staging.com
anyscale auth show   # if "Not authenticated", run `anyscale login`
```

Compute config (instance types per the user's request):
```yaml
cloud: anyscale_v2_default_cloud   # see cloud gotchas below
head_node: {instance_type: m5d.2xlarge}
worker_nodes:
  - {instance_type: m5d.4xlarge, min_nodes: 8, max_nodes: 8, market_type: ON_DEMAND}
advanced_instance_config:
  BlockDeviceMappings:
    - {DeviceName: /dev/sda1, Ebs: {DeleteOnTermination: true, VolumeSize: 512}}
```

```bash
cd "$WORK"
anyscale compute-config create -f compute_config.yaml -n <cfg-name>
anyscale job submit --name <job-name> \
  --image-uri anyscale/ray:nightly-py310 \
  --compute-config <cfg-name> \
  --working-dir . \
  --requirements requirements.txt \
  [--env RAY_DATA_ENABLE_DISK_SHUFFLE=1] \
  --max-retries 0 --wait \
  -- python profile.py 2>&1 | tee submit.log
```

Watch `submit.log` for `transitioned`, `FAILED`, and the `TIMELINE_OUTPUT:`
lines. On FAILED, pull `anyscale job logs --id <prodjob_...>`.

Locally (no Anyscale): `pip install git+https://github.com/owenowenisme/ray-data-timeline-viewer.git`
then run the profile script against any running Ray.

Cloud gotchas (staging, time-dependent — try the other if one stalls):
- `anyscale_v2_default_cloud`: the shared default; quota is often saturated by
  release tests, so a job can sit in STARTING 10–40 min. Resubmit or switch.
- `anyscale_staging_default_cloud_clone`: a second us-west cloud; often empty
  (fast) but may be hibernated — the first `compute-config create` wakes it
  (~5 min), then clusters come up in ~3 min.

## 3. Download the trace

If `aws s3 cp` locally fails with expired SSO, presign from the cluster:
```bash
TIMELINE_BASE=<s3-dir/> anyscale job submit --name presign-<run> \
  --image-uri anyscale/ray:nightly-py310 --compute-config <tiny-head-cfg> \
  --working-dir . --max-retries 0 --wait -- python presign.py 2>&1 | tee presign.log
JOB=$(grep -oE 'prodjob_[a-z0-9]+' presign.log | head -1)
anyscale job logs --id "$JOB" | grep '^PRESIGNED::'
```
Then `curl -s -o timeline.json.gz "<url>"` per line. A tiny head-only compute
config (`head_node: {instance_type: m5.large}`, `worker_nodes: []`) suffices.

Note: Anyscale's job runner mangles `&` in shell-quoted URLs — presign inside
`presign.py` (python), not inline bash.

## 4. Render and report

Open https://owenowenisme.github.io/ray-data-timeline-viewer/ and drop the
`timeline.json.gz` — the interactive timeline renders in the browser. (Or load
the `.json.gz` in https://ui.perfetto.dev.)

For a terminal readout, a per-operator table (count, peak concurrency, span,
busy, rows/bytes) is computable from the trace's `ph:"X", cat:"task"` events
grouped by `pid`; the `cat:"operator"` summary slices carry `cpu_s`,
`input_ops`, rows and bytes.

Report: wall clock, per-operator busy time and peak concurrency, where the
pipeline serialized or starved, failed attempts (red lanes), and (for
shuffle-heavy jobs) that on stock Ray the shuffle stages show spans but no
row/byte counts — note that explicitly, and if the numbers matter, re-run per
step 5.

## 5. (Optional) Exact shuffle stats via the instrumentation branch

For exact shuffle row/byte counts and exact multi-node clock alignment, patch
the timeline branch's Ray files onto the nightly image at job startup — the
package still does the export, but now reads native per-task shuffle stats. See
`assets/stage_patch.sh` and `assets/timeline_patch.py`: stage the branch's
`ray/data` files into `$WORK/patched/`, ship them in the working dir, and have
the job apply them per node before importing `ray.data`. Only needed when the
stock-Ray approximations in the table above aren't good enough.
