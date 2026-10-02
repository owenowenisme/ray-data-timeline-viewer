"""Execution-timeline exporter for Ray Data, no Ray modifications required.

Quick start::

    import ray_data_timeline

    ray_data_timeline.install()          # optional: true DAG edges
    ds = <pipeline>.materialize()
    ray_data_timeline.export_timeline(ds, "trace.json.gz")

Or fully hands-off (no code changes), set on the job:

- ``RAY_DATA_EXECUTION_CALLBACKS=ray_data_timeline.TimelineCallback``
- ``RAY_DATA_TIMELINE_OUTPUT=s3://bucket/run`` (or a local directory)

and every execution writes ``<prefix>/<dataset_id>.json.gz``. Open traces in
the hosted viewer (https://owenowenisme.github.io/ray-data-timeline-viewer/)
or https://ui.perfetto.dev.
"""

from ray_data_timeline.callback import TimelineCallback, install
from ray_data_timeline.timeline import build_chrome_trace, export_timeline

__all__ = [
    "TimelineCallback",
    "build_chrome_trace",
    "export_timeline",
    "install",
]
__version__ = "0.1.0"
