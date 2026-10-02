"""ExecutionCallback that captures what post-hoc stats can't provide.

The stats DAG the executor leaves behind is a linear chain over topological
order, so multi-input structure (a join reduce's two inputs) is lost, and the
executor itself is gone by the time ``materialize()`` returns. Registering
:class:`TimelineCallback` fixes that without modifying Ray: Ray Data invokes
it with the live executor, and it records the true operator edges from the
physical topology. It can also auto-export the trace after every execution.

Two ways to register:

- In code, before running the pipeline::

      import ray_data_timeline
      ray_data_timeline.install()

- With no code changes at all, via Ray Data's callback env var (plus an
  output prefix to auto-export every execution's trace)::

      RAY_DATA_EXECUTION_CALLBACKS=ray_data_timeline.TimelineCallback
      RAY_DATA_TIMELINE_OUTPUT=s3://bucket/my-run   # or a local directory
"""

import logging
import os
from collections import OrderedDict
from typing import List, Optional

from ray.data._internal.execution.execution_callback import ExecutionCallback

logger = logging.getLogger(__name__)

# Output prefix (local directory or pyarrow URI) for auto-export; each
# execution writes <prefix>/<dataset_id>.json.gz. Unset = no auto-export.
OUTPUT_ENV = "RAY_DATA_TIMELINE_OUTPUT"

# dataset_id -> operator edges, newest last. Bounded: a long-lived driver
# running many datasets shouldn't accumulate stale entries.
_MAX_ENTRIES = 32
_edges_registry: "OrderedDict[str, List[List[int]]]" = OrderedDict()


def lookup_edges(num_ops: int) -> Optional[List[List[int]]]:
    """Most recently captured edges whose operator count matches ``num_ops``.

    Manual ``export_timeline(ds, ...)`` calls can't name the execution that
    produced the stats (executions mint fresh ids), so match on the operator
    count, newest first. A same-count collision between two recent datasets
    picks the newer one; edges are cosmetic (DAG layout), so a rare mismatch
    degrades the drawing, not the data.
    """
    for ds_id in reversed(_edges_registry):
        edges = _edges_registry[ds_id]
        if len(edges) == num_ops:
            return edges
    return None


class TimelineCallback(ExecutionCallback):
    """Capture operator edges from the live topology; optionally auto-export.

    Never raises into the job: any failure here logs and is swallowed, since
    profiling must not break the pipeline it profiles.
    """

    def after_execution_succeeds(self, executor) -> None:
        try:
            from ray_data_timeline.timeline import (
                compute_op_input_indices,
                count_stats_ops,
            )

            initial = getattr(executor, "_initial_stats", None)
            num_initial = count_stats_ops(initial) if initial else 0
            edges = compute_op_input_indices(executor._topology, num_initial)
            _edges_registry[executor._dataset_id] = edges
            while len(_edges_registry) > _MAX_ENTRIES:
                _edges_registry.popitem(last=False)

            prefix = os.environ.get(OUTPUT_ENV)
            if prefix:
                from ray_data_timeline.timeline import export_timeline

                path = f"{prefix.rstrip('/')}/{executor._dataset_id}.json.gz"
                export_timeline(executor.get_stats(), path)
                # export_timeline logs at INFO, which jobs often filter; make
                # the artifact location greppable in plain job output.
                print(f"TIMELINE_OUTPUT: {path}")
        except Exception:
            logger.exception("ray_data_timeline callback failed; continuing")


def install() -> None:
    """Register :class:`TimelineCallback` on the current DataContext.

    Idempotent; call once before executing pipelines. Equivalent to setting
    ``RAY_DATA_EXECUTION_CALLBACKS=ray_data_timeline.TimelineCallback``.
    """
    from ray.data import DataContext

    classes = DataContext.get_current().custom_execution_callback_classes
    if TimelineCallback not in classes:
        classes.append(TimelineCallback)
