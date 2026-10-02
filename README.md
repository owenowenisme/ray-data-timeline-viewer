# Ray Data timeline

Profile a Ray Data pipeline and see where its time went — **without modifying
Ray**. This repo has two pieces:

1. **`ray_data_timeline/`** — a pip-installable package that exports a Ray Data
   execution timeline as a Chrome trace. Works on stock Ray.
2. **`index.html`** — a single-file, dependency-free viewer for those traces,
   hosted at https://owenowenisme.github.io/ray-data-timeline-viewer/.

## Exporting a timeline

```bash
pip install git+https://github.com/owenowenisme/ray-data-timeline-viewer.git
```

```python
import ray, ray_data_timeline

ray_data_timeline.install()               # capture the true operator DAG
ds = ray.data.range(10_000).map_batches(fn).groupby("id").count().materialize()
ray_data_timeline.export_timeline(ds, "timeline.json.gz")   # local path or s3:// URI
```

Or hands-off, with no code changes — set on the job and every execution
auto-exports its trace:

```bash
export RAY_DATA_EXECUTION_CALLBACKS=ray_data_timeline.TimelineCallback
export RAY_DATA_TIMELINE_OUTPUT=s3://bucket/my-run    # or a local directory
```

### What you get, and the stock-Ray caveats

- Per-operator task lanes (lane count = peak concurrency), running-task
  counters, and a combined all-operators view. ✅ always
- FAILED / retried task attempts in red lanes, from the Ray state API. ✅ always
- The true operator DAG (e.g. a join reduce's two inputs). ✅ via `install()`
- Shuffle stages (join / aggregate / sort): on stock Ray their task **spans**
  are backfilled from the state API, but **row/byte counts are unavailable**;
  with [ray-project/ray#66621](https://github.com/ray-project/ray/pull/66621)
  the counts are exact.
- Multi-node clock: approximate on stock Ray (epoch offset estimated from the
  state API), exact with the per-block epoch-anchor branch.

Failed attempts and shuffle backfill need the cluster that ran the dataset to
still be up (they query the live state API), and GCS task-event retention is
bounded, so very large runs may evict old attempts.

## Viewing a trace

Open https://owenowenisme.github.io/ray-data-timeline-viewer/, then **Open
trace…** (or drag a file onto the chart) and pick a `timeline.json` /
`timeline.json.gz`. Everything renders locally in your browser; the trace never
leaves your machine, so the viewer can be hosted publicly while the data stays
private.

Features: per-operator rows with running-task sparklines, expandable task
lanes, failed-attempt lanes, operator DAG indentation, hover details,
click-to-focus with a copyable inspector, time zoom (pinch / ctrl+scroll),
vertical zoom (alt+scroll), pan, and a hover crosshair.

The same trace JSON also opens in [Perfetto](https://ui.perfetto.dev).
