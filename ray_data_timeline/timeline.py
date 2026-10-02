"""Generate an execution timeline for a Ray Data run, without modifying Ray.

Reconstructs, from the per-block execution stats that Ray Data already
records, when each task of each operator ran, and emits a Chrome trace
(``chrome://tracing`` / https://ui.perfetto.dev, or the companion HTML
viewer) with:

- one process group per operator (in topological order), with tasks laid
  out in non-overlapping lanes so concurrency is visible at a glance,
- one counter track per operator ("running tasks"), plus a global counter,
  so you can see how many tasks of each operator were in flight at any time
  and where operators overlap, and
- FAILED task attempts from the Ray state API in dedicated lanes.

Works against stock Ray. Runs on a Ray that carries the timeline
instrumentation branch (per-block Unix clock anchor, shuffle per-task stats
from ray-project/ray#66621, recorded operator edges) are exact; on stock Ray
the exporter degrades gracefully:

- Clocks: blocks record only per-node monotonic timestamps, so the exporter
  estimates each node's epoch offset from the state API's task events and
  logs that the alignment is approximate.
- Shuffle operators: their tasks produce no per-task block stats, so their
  spans are backfilled from state-API task events, grouped by task name
  (e.g. ``_shuffle_reduce_task``) rather than attributed to operators.
- Operator edges: recovered live by :class:`TimelineCallback` (register it
  before running); otherwise the trace falls back to a linear chain.

Example:
    >>> import ray
    >>> from ray_data_timeline import export_timeline
    >>> ds = ray.data.range(10_000_000).map_batches(fn).materialize()
    >>> export_timeline(ds, "/tmp/ray_data_timeline.json")  # doctest: +SKIP
"""

import gzip
import heapq
import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

if TYPE_CHECKING:
    from ray.data import Dataset
    from ray.data._internal.stats import DatasetStats

logger = logging.getLogger(__name__)


@dataclass
class TaskInterval:
    """The execution span of one task of one operator.

    Derived from the blocks the task produced: the span runs from the start
    of its first block to the end of its last block, so time the task spent
    before producing its first block (e.g. fetching inputs) is not included.
    """

    operator: str
    task_idx: int
    node_id: str
    start_s: float
    end_s: float
    # Measured process CPU seconds summed over the task's blocks (not
    # reserved CPUs x wall time); 0 when the stats carry no CPU figures.
    cpu_s: float
    num_blocks: int
    num_rows: int
    size_bytes: int


def collect_task_intervals(
    stats: "DatasetStats",
) -> Tuple[List[Tuple[str, List[TaskInterval]]], bool]:
    """Walk the stats DAG and return per-operator task intervals.

    Returns ``(per_op, used_unix_clock)``. ``per_op`` is a list of
    ``(operator_name, intervals)`` in topological (execution) order;
    operators whose blocks carry no exec stats (e.g. input operators that
    only pass through metadata) get an empty interval list so they still
    appear in the timeline. ``used_unix_clock`` is True when every block had
    a Unix timestamp, so all intervals are on the cross-node-comparable
    clock; False means the per-node monotonic fallback was used.
    """
    # Dedup by object identity: with multiple downstream branches the same
    # parent stats object can be reachable twice.
    visited: Dict[int, "DatasetStats"] = {}
    ordered: List["DatasetStats"] = []

    def visit(s: "DatasetStats") -> None:
        if id(s) in visited:
            return
        visited[id(s)] = s
        for p in s.parents:
            visit(p)
        ordered.append(s)

    visit(stats)

    # First pass: group blocks per operator per task, and pick the clock.
    # The Unix clock is usable only if EVERY block carries it; a mix of
    # clocks on one timeline would be worse than a consistently shifted one.
    grouped: List[Tuple[str, Dict[Tuple[str, int], List[Any]]]] = []
    use_unix = True
    for s in ordered:
        for op_name, block_stats in s.metadata.items():
            by_task: Dict[Tuple[str, int], List[Any]] = {}
            for bs in block_stats:
                es = bs.exec_stats
                if (
                    es is None
                    or es.start_time_s is None
                    or es.end_time_s is None
                    or es.task_idx is None
                ):
                    continue
                # getattr: stock Ray's BlockExecStats predates the anchor field.
                if getattr(es, "start_unix_time_s", None) is None:
                    use_unix = False
                by_task.setdefault((es.node_id, es.task_idx), []).append(bs)
            grouped.append((op_name, by_task))

    def block_span(es: Any) -> Tuple[float, float]:
        if use_unix:
            return (
                es.start_unix_time_s,  # all blocks carry it when use_unix
                es.start_unix_time_s + (es.end_time_s - es.start_time_s),
            )
        return es.start_time_s, es.end_time_s

    result: List[Tuple[str, List[TaskInterval]]] = []
    for op_name, by_task in grouped:
        intervals = []
        for (node_id, task_idx), blocks in by_task.items():
            spans = [block_span(b.exec_stats) for b in blocks]
            intervals.append(
                TaskInterval(
                    operator=op_name,
                    task_idx=task_idx,
                    node_id=node_id,
                    start_s=min(s for s, _ in spans),
                    end_s=max(e for _, e in spans),
                    cpu_s=sum(b.exec_stats.cpu_time_s or 0 for b in blocks),
                    num_blocks=len(blocks),
                    num_rows=sum(b.num_rows or 0 for b in blocks),
                    size_bytes=sum(b.size_bytes or 0 for b in blocks),
                )
            )
        intervals.sort(key=lambda iv: iv.start_s)
        result.append((op_name, intervals))
    return result, use_unix


def collect_op_inputs(stats: "DatasetStats") -> List[List[int]]:
    """Return, per operator, the indices of the operators feeding it.

    Indices refer to positions in ``collect_task_intervals``'s ``per_op``
    list: both walk the stats DAG identically, so they stay aligned. Within
    one stats node, metadata entries are sequential sub-operators (upstream
    first); across nodes, a parent's last operator feeds the child's first.
    Stats nodes with no metadata pass their parents' outputs through.
    """
    visited: Dict[int, "DatasetStats"] = {}
    ordered: List["DatasetStats"] = []

    def visit(s: "DatasetStats") -> None:
        if id(s) in visited:
            return
        visited[id(s)] = s
        for p in s.parents:
            visit(p)
        ordered.append(s)

    visit(stats)

    inputs: List[List[int]] = []
    # For each stats node, the operator indices downstream consumers attach
    # to: its last op, or (for a metadata-less node) its parents' tails.
    tails: Dict[int, List[int]] = {}
    next_idx = 0
    for s in ordered:
        parent_tails = [t for p in s.parents for t in tails[id(p)]]
        op_count = len(s.metadata)
        for i in range(op_count):
            inputs.append(parent_tails if i == 0 else [next_idx + i - 1])
        tails[id(s)] = [next_idx + op_count - 1] if op_count else parent_tails
        next_idx += op_count
    return inputs


def compute_op_input_indices(topology: Any, num_initial_ops: int) -> List[List[int]]:
    """Compute true operator edges from a live physical topology.

    The stats DAG the executor builds is a linear chain over topological
    order, so it loses multi-input structure (a join reduce would appear to
    have one input). This derives each operator's real inputs from the
    physical operators' ``input_dependencies`` while the topology is alive,
    indexed to match the stats chain (one entry per ``get_stats()`` key, in
    walk order, after ``num_initial_ops`` entries contributed by an input
    dataset's stats, which keep their chain edges).
    """
    from ray.data._internal.execution.operators.input_data_buffer import (
        InputDataBuffer,
    )

    phys_ops = [op for op in topology if not isinstance(op, InputDataBuffer)]
    counts = [len(op.get_stats()) for op in phys_ops]

    first_idx: Dict[Any, int] = {}
    last_idx: Dict[Any, int] = {}
    idx = num_initial_ops
    for op, count in zip(phys_ops, counts):
        if count:
            first_idx[op] = idx
            last_idx[op] = idx + count - 1
        idx += count

    edges: List[List[int]] = [[] for _ in range(idx)]
    for i in range(1, num_initial_ops):
        edges[i] = [i - 1]
    # An InputDataBuffer feeds from the input dataset's stats chain (e.g. a
    # materialized upstream execution), whose tail is the last initial op.
    initial_tail = num_initial_ops - 1 if num_initial_ops > 0 else None

    def tails(dep: Any) -> List[int]:
        if isinstance(dep, InputDataBuffer):
            return [initial_tail] if initial_tail is not None else []
        if dep in last_idx:
            return [last_idx[dep]]
        # A physical op that contributed no stats entries: pass through.
        return [t for d in dep.input_dependencies for t in tails(d)]

    for op, count in zip(phys_ops, counts):
        if not count:
            continue
        start = first_idx[op]
        for j in range(1, count):
            edges[start + j] = [start + j - 1]
        ins: List[int] = []
        for dep in op.input_dependencies:
            for t in tails(dep):
                if t not in ins:
                    ins.append(t)
        edges[start] = ins
    return edges


def bridge_op_inputs(inputs: List[List[int]], keep: List[bool]) -> List[List[int]]:
    """Rewrite edges so they only reference kept operators.

    An operator that recorded no tasks (e.g. ``ReadFiles``, ``SortSample``)
    is invisible in the timeline, so its consumers inherit its inputs
    instead, transitively. Entries for dropped operators are still returned
    (bridged) to keep indices aligned with the full operator list.
    """
    resolved: Dict[int, List[int]] = {}

    def resolve(idx: int) -> List[int]:
        if idx in resolved:
            return resolved[idx]
        resolved[idx] = []  # cycle guard; the DAG shouldn't have any
        out: List[int] = []
        for dep in inputs[idx]:
            targets = [dep] if keep[dep] else resolve(dep)
            for t in targets:
                if t not in out:
                    out.append(t)
        resolved[idx] = out
        return out

    return [resolve(i) for i in range(len(inputs))]


@dataclass
class FailedTaskAttempt:
    """A FAILED task attempt from the Ray state API.

    Block exec stats only exist for tasks that produced blocks, so failed and
    retried attempts are invisible to them; the raylet-reported task events
    are the only record. ``operator`` is the matched operator name, or None
    when the task name matches no operator (e.g. shuffle implementation
    functions, which aren't named after their operator).
    """

    operator: Optional[str]
    name: str
    start_s: float
    end_s: float
    node_id: Optional[str]
    error_type: Optional[str]
    attempt_number: int


def _fetch_job_task_events() -> Optional[List[Any]]:
    """All of this job's task-attempt records from the Ray state API.

    One fetch powers three stock-Ray fallbacks: failed attempts, monotonic
    clock alignment, and shuffle-span backfill. Requires a live cluster with
    the dashboard/state API; returns None (with a warning) when unavailable.
    Task events in GCS are bounded (``RAY_task_events_max_num_task_in_gcs``),
    so on very large runs old attempts may already be evicted.
    """
    try:
        import ray
        from ray.util.state.api import list_tasks

        job_id = ray.runtime_context.get_runtime_context().get_job_id()
        return list_tasks(
            detail=True,
            limit=10_000,
            raise_on_missing_output=False,
            filters=[("job_id", "=", job_id)],
        )
    except Exception as e:
        logger.warning(
            "Ray state API unavailable; failed task attempts, stock-Ray clock "
            "alignment, and shuffle-span backfill are disabled. Error: %s",
            e,
        )
        return None


def _failed_attempts_from_events(
    tasks: List[Any], op_names: List[str]
) -> List[FailedTaskAttempt]:
    """Extract FAILED attempts from state-API task rows."""
    attempts = []
    for t in tasks:
        if t.state != "FAILED":
            continue
        # A worker that dies early can be gone before it flushes the RUNNING
        # transition, leaving start_time_ms unset; fall back to the creation
        # time (includes queueing) rather than dropping the attempt.
        start_ms = t.start_time_ms or t.creation_time_ms or t.end_time_ms
        if start_ms is None:
            continue
        end_ms = t.end_time_ms or start_ms
        name = t.name or ""
        # Longest operator name contained in the task name wins; map tasks are
        # named exactly after their (possibly fused) operator.
        operator = max(
            (op for op in op_names if op and op in name), key=len, default=None
        )
        attempts.append(
            FailedTaskAttempt(
                operator=operator,
                name=name,
                start_s=start_ms / 1000,
                end_s=end_ms / 1000,
                node_id=t.node_id,
                error_type=t.error_type,
                attempt_number=t.attempt_number,
            )
        )
    return attempts


def _estimate_node_offsets(
    tasks: List[Any], intervals: List[TaskInterval], op_names: List[str]
) -> Dict[str, float]:
    """Estimate each node's monotonic-to-epoch clock offset.

    Stock Ray's block stats carry only per-node monotonic timestamps, but the
    state API records the same tasks' starts in epoch time. Per node, the
    earliest matched FINISHED task's epoch start and the earliest block-stat
    monotonic start describe roughly the same moment; their difference is the
    node's offset. Approximate (the block clock starts after input prep), but
    good enough to put all nodes and the state-API events on one axis.
    """
    mono_min: Dict[str, float] = {}
    for iv in intervals:
        if iv.node_id is not None:
            mono_min[iv.node_id] = min(
                mono_min.get(iv.node_id, float("inf")), iv.start_s
            )
    epoch_min: Dict[str, float] = {}
    for t in tasks:
        if t.state != "FINISHED" or not t.node_id or t.start_time_ms is None:
            continue
        if not any(op and op in (t.name or "") for op in op_names):
            continue
        epoch_min[t.node_id] = min(
            epoch_min.get(t.node_id, float("inf")), t.start_time_ms / 1000
        )
    return {n: epoch_min[n] - m for n, m in mono_min.items() if n in epoch_min}


def _backfill_empty_operators(
    per_op: List[Tuple[str, List[TaskInterval]]], tasks: List[Any]
) -> int:
    """Fill operators that have no block-stat intervals from state-API events.

    On stock Ray, shuffle map/reduce tasks (and the HashAggregate / Join /
    Sort operators built on them) produce no per-task block stats, so those
    operators show up as empty rows (fixed by ray-project/ray#66621). Their
    FINISHED task events still exist and are named exactly after the operator,
    so synthesize spans for each empty operator from its matching tasks.
    Mutates ``per_op`` in place; returns the number of operators filled.
    Spans only: rows/bytes/CPU are unknown to the state API.

    Longest-matching-operator assignment prevents a fused parent name
    ("A->B") from also claiming tasks named after its sub-operator "B".
    """
    empty = {name for name, ivs in per_op if not ivs}
    if not empty:
        return 0
    matched: Dict[str, List[Any]] = {name: [] for name in empty}
    for t in tasks:
        name = t.name or ""
        if (
            t.state != "FINISHED"
            or getattr(t, "type", "NORMAL_TASK") != "NORMAL_TASK"
            or t.start_time_ms is None
            or t.end_time_ms is None
        ):
            continue
        hits = [op for op in empty if op and op in name]
        if hits:
            matched[max(hits, key=len)].append(t)

    filled = 0
    for i, (name, ivs) in enumerate(per_op):
        rows = matched.get(name)
        if ivs or not rows:
            continue
        rows.sort(key=lambda t: t.start_time_ms)
        per_op[i] = (
            name,
            [
                TaskInterval(
                    operator=name,
                    task_idx=idx,
                    node_id=t.node_id,
                    start_s=t.start_time_ms / 1000,
                    end_s=t.end_time_ms / 1000,
                    cpu_s=0.0,
                    num_blocks=0,
                    num_rows=0,
                    size_bytes=0,
                )
                for idx, t in enumerate(rows)
            ],
        )
        filled += 1
    return filled


def _assign_lanes(intervals: List[Any]) -> List[int]:
    """Assign each interval a lane such that intervals in a lane don't overlap.

    Accepts any objects with ``start_s``/``end_s`` (``TaskInterval``,
    ``FailedTaskAttempt``), already sorted by ``start_s``.

    Chrome trace slices within one thread row must nest, so concurrent tasks
    have to land on separate rows. Greedy interval scheduling: reuse the lane
    that freed up earliest, else open a new one. The number of lanes therefore
    equals the operator's peak concurrency.
    """
    lanes: List[int] = []
    free: List[Tuple[float, int]] = []  # (end_s, lane_id) heap
    next_lane = 0
    for iv in intervals:  # already sorted by start_s
        if free and free[0][0] <= iv.start_s:
            _, lane = heapq.heappop(free)
        else:
            lane = next_lane
            next_lane += 1
        heapq.heappush(free, (iv.end_s, lane))
        lanes.append(lane)
    return lanes


def _concurrency_counter(intervals: List[TaskInterval]) -> List[Tuple[float, int]]:
    """Sweep-line over intervals: (timestamp, running task count) steps."""
    events: List[Tuple[float, int]] = []
    for iv in intervals:
        events.append((iv.start_s, 1))
        events.append((iv.end_s, -1))
    events.sort()
    steps = []
    running = 0
    for t, delta in events:
        running += delta
        # Collapse simultaneous events into the final value at that time.
        if steps and steps[-1][0] == t:
            steps[-1] = (t, running)
        else:
            steps.append((t, running))
    return steps


def _match_attempts_to_pids(
    attempts: List[FailedTaskAttempt],
    per_op: List[Tuple[str, List[TaskInterval]]],
) -> Dict[int, List[FailedTaskAttempt]]:
    """Assign each failed attempt to one operator pid.

    Operator names repeat (e.g. several ``JoinShuffleReduce`` ops in one
    query), so among same-named operators the attempt goes to the one whose
    successful-task window overlaps it most (nearest window on no overlap).
    Attempts whose name matched no operator land under pid -1.
    """
    windows: Dict[str, List[Tuple[int, float, float]]] = {}
    for pid, (op_name, intervals) in enumerate(per_op):
        if intervals:
            windows.setdefault(op_name, []).append(
                (
                    pid,
                    min(iv.start_s for iv in intervals),
                    max(iv.end_s for iv in intervals),
                )
            )
        else:
            windows.setdefault(op_name, []).append((pid, None, None))

    by_pid: Dict[int, List[FailedTaskAttempt]] = {}
    for a in attempts:
        candidates = windows.get(a.operator) if a.operator is not None else None
        if not candidates:
            by_pid.setdefault(-1, []).append(a)
            continue

        def score(c):
            pid, start, end = c
            if start is None:
                return float("-inf")
            overlap = min(a.end_s, end) - max(a.start_s, start)
            # Positive overlap wins; otherwise prefer the nearest window.
            return overlap

        pid = max(candidates, key=score)[0]
        by_pid.setdefault(pid, []).append(a)
    return by_pid


def _emit_failed_lanes(
    events: List[Dict[str, Any]],
    pid: int,
    base_tid: int,
    attempts: List[FailedTaskAttempt],
    us,
) -> None:
    """Emit FAILED-attempt slices in dedicated lanes below a pid's task lanes."""
    if not attempts:
        return
    lanes = _assign_lanes(attempts)
    for lane in range(max(lanes) + 1):
        events.append(
            {
                "name": "thread_name",
                "ph": "M",
                "pid": pid,
                "tid": base_tid + lane,
                "args": {"name": f"failed {lane}"},
            }
        )
    for a, lane in zip(attempts, lanes):
        events.append(
            {
                "name": f"FAILED attempt {a.attempt_number}",
                "cat": "failed_task",
                "ph": "X",
                "pid": pid,
                "tid": base_tid + lane,
                "ts": us(a.start_s),
                "dur": max(int((a.end_s - a.start_s) * 1e6), 1),
                "args": {
                    "task_name": a.name,
                    "error_type": a.error_type,
                    "node_id": a.node_id,
                },
            }
        )


def build_chrome_trace(
    stats: "DatasetStats",
    op_inputs: Optional[List[List[int]]] = None,
    include_failed_tasks: bool = True,
    include_state_api_tasks: bool = True,
) -> List[Dict[str, Any]]:
    """Build Chrome trace events for the given dataset stats.

    The returned list is the ``traceEvents`` array; load it in
    https://ui.perfetto.dev or chrome://tracing. ``op_inputs`` overrides the
    operator edges; by default they're derived from the stats DAG, which is a
    linear chain. With ``include_failed_tasks`` (the default), FAILED task
    attempts from the Ray state API are drawn in separate per-operator lanes.
    With ``include_state_api_tasks`` (the default), FINISHED tasks invisible
    to block stats (stock-Ray shuffle stages) get extra state-API-sourced
    rows. Both need the cluster that ran the dataset to still be up.
    """
    per_op, used_unix_clock = collect_task_intervals(stats)
    # Raw per-operator edges; bridging (dropping edge-less operators) is
    # deferred until after the state-API backfill, so shuffle operators the
    # backfill revives aren't bridged away as if they never ran.
    raw_op_inputs = op_inputs if op_inputs is not None else collect_op_inputs(stats)

    all_intervals = [iv for _, ivs in per_op for iv in ivs]
    if not all_intervals:
        logger.warning("No block execution stats found; timeline will be empty.")
        return []

    op_names = [name for name, _ in per_op]
    task_events = None
    if include_failed_tasks or include_state_api_tasks or not used_unix_clock:
        task_events = _fetch_job_task_events()

    # Stock Ray records only per-node monotonic block timestamps; estimate
    # each node's epoch offset from the state API so everything shares one
    # (approximate) epoch axis. Without the estimate, multi-node traces stay
    # shifted and the epoch-stamped state-API extras can't be drawn.
    if not used_unix_clock and task_events:
        offsets = _estimate_node_offsets(task_events, all_intervals, op_names)
        nodes = {iv.node_id for iv in all_intervals}
        if nodes <= set(offsets):
            for iv in all_intervals:
                iv.start_s += offsets[iv.node_id]
                iv.end_s += offsets[iv.node_id]
            used_unix_clock = True
            logger.info(
                "Aligned %d node clock(s) to epoch time via state-API task "
                "events; the alignment is approximate (this Ray records no "
                "per-block epoch anchor).",
                len(offsets),
            )
    if not used_unix_clock:
        nodes = {iv.node_id for iv in all_intervals}
        if len(nodes) > 1:
            logger.warning(
                "Blocks carry only per-node monotonic timestamps and no "
                "state-API alignment was possible; blocks came from %d "
                "nodes, so cross-node alignment is approximate.",
                len(nodes),
            )

    if include_state_api_tasks and task_events and used_unix_clock:
        filled = _backfill_empty_operators(per_op, task_events)
        if filled:
            logger.info(
                "Backfilled %d operator(s) with no block stats from state-API "
                "task events (shuffle stages on a Ray without "
                "ray-project/ray#66621); spans only, no row/byte counts.",
                filled,
            )
            all_intervals = [iv for _, ivs in per_op for iv in ivs]

    # Bridge now, with the post-backfill keep mask, so edges route through
    # revived shuffle operators instead of hopping over them.
    op_inputs = bridge_op_inputs(
        raw_op_inputs, [bool(ivs) for _, ivs in per_op]
    )

    failed_by_pid: Dict[int, List[FailedTaskAttempt]] = {}
    if include_failed_tasks and task_events:
        if used_unix_clock:
            attempts = _failed_attempts_from_events(task_events, op_names)
            failed_by_pid = _match_attempts_to_pids(attempts, per_op)
            if attempts:
                logger.info(
                    "Including %d failed task attempt(s) in the timeline.",
                    len(attempts),
                )
        else:
            logger.warning(
                "Timeline is on the monotonic-clock fallback, which can't be "
                "aligned with the state API's Unix timestamps; failed task "
                "attempts are not shown."
            )

    all_failed = [a for attempts in failed_by_pid.values() for a in attempts]
    t0 = min([iv.start_s for iv in all_intervals] + [a.start_s for a in all_failed])

    def us(t: float) -> int:
        return int((t - t0) * 1e6)

    events: List[Dict[str, Any]] = []
    for pid, (op_name, intervals) in enumerate(per_op):
        events.append(
            {
                "name": "process_name",
                "ph": "M",
                "pid": pid,
                "args": {"name": f"{pid}: {op_name}"},
            }
        )
        events.append(
            {
                "name": "process_sort_index",
                "ph": "M",
                "pid": pid,
                "args": {"sort_index": pid},
            }
        )
        op_failed = sorted(failed_by_pid.get(pid, []), key=lambda a: a.start_s)
        if not intervals:
            _emit_failed_lanes(events, pid, 0, op_failed, us)
            continue

        # One summary slice spanning operator start to finish, pinned above
        # the task lanes, so a collapsed operator group still reads as the
        # operator's active window.
        summary_tid = 1_000_000
        events.append(
            {
                "name": "thread_name",
                "ph": "M",
                "pid": pid,
                "tid": summary_tid,
                "args": {"name": "operator"},
            }
        )
        events.append(
            {
                "name": "thread_sort_index",
                "ph": "M",
                "pid": pid,
                "tid": summary_tid,
                "args": {"sort_index": -1},
            }
        )
        op_start = min(iv.start_s for iv in intervals)
        op_end = max(iv.end_s for iv in intervals)
        events.append(
            {
                "name": op_name,
                "cat": "operator",
                "ph": "X",
                "pid": pid,
                "tid": summary_tid,
                "ts": us(op_start),
                "dur": max(int((op_end - op_start) * 1e6), 1),
                "args": {
                    "tasks": len(intervals),
                    # Upstream operators (as pids), bridged over operators
                    # that recorded no tasks, so viewers can draw the DAG.
                    "input_ops": op_inputs[pid],
                    "busy_s": round(sum(iv.end_s - iv.start_s for iv in intervals), 3),
                    "cpu_s": round(sum(iv.cpu_s for iv in intervals), 3),
                    "rows": sum(iv.num_rows for iv in intervals),
                    "size_bytes": sum(iv.size_bytes for iv in intervals),
                },
            }
        )

        lanes = _assign_lanes(intervals)
        for lane in range(max(lanes) + 1):
            events.append(
                {
                    "name": "thread_name",
                    "ph": "M",
                    "pid": pid,
                    "tid": lane,
                    "args": {"name": f"lane {lane}"},
                }
            )
        for iv, lane in zip(intervals, lanes):
            events.append(
                {
                    "name": f"task {iv.task_idx}",
                    "cat": "task",
                    "ph": "X",
                    "pid": pid,
                    "tid": lane,
                    "ts": us(iv.start_s),
                    "dur": max(int((iv.end_s - iv.start_s) * 1e6), 1),
                    "args": {
                        "node_id": iv.node_id,
                        "cpu_s": round(iv.cpu_s, 3),
                        "blocks": iv.num_blocks,
                        "rows": iv.num_rows,
                        "size_bytes": iv.size_bytes,
                    },
                }
            )
        for t, running in _concurrency_counter(intervals):
            events.append(
                {
                    "name": "running tasks",
                    "ph": "C",
                    "pid": pid,
                    "ts": us(t),
                    "args": {"tasks": running},
                }
            )
        _emit_failed_lanes(events, pid, max(lanes) + 1, op_failed, us)

    # Global concurrency across all operators, on its own process row.
    # Sub-operators of one physical op (e.g. RepartitionSplit fused with the
    # upstream map) reuse the producing task's exec stats, so the same
    # physical task can appear under several operator names; dedupe by exact
    # interval identity so the total reflects physical tasks.
    seen = set()
    unique_intervals = []
    for iv in all_intervals:
        key = (iv.node_id, iv.task_idx, iv.start_s, iv.end_s)
        if key not in seen:
            seen.add(key)
            unique_intervals.append(iv)
    global_pid = len(per_op)
    events.append(
        {
            "name": "process_name",
            "ph": "M",
            "pid": global_pid,
            "args": {"name": "all operators"},
        }
    )
    events.append(
        {
            "name": "process_sort_index",
            "ph": "M",
            "pid": global_pid,
            "args": {"sort_index": -1},
        }
    )
    for t, running in _concurrency_counter(unique_intervals):
        events.append(
            {
                "name": "total running tasks",
                "ph": "C",
                "pid": global_pid,
                "ts": us(t),
                "args": {"tasks": running},
            }
        )

    # Every task in one set of shared lanes, slices named by operator, so the
    # whole execution is visible in a single group without expanding each
    # operator: lane count is the peak physical concurrency, and each lane
    # shows which operator occupied that execution slot over time.
    combined = sorted(unique_intervals, key=lambda iv: iv.start_s)
    combined_lanes = _assign_lanes(combined)
    for lane in range(max(combined_lanes) + 1 if combined_lanes else 0):
        events.append(
            {
                "name": "thread_name",
                "ph": "M",
                "pid": global_pid,
                "tid": lane,
                "args": {"name": f"slot {lane}"},
            }
        )
    for iv, lane in zip(combined, combined_lanes):
        events.append(
            {
                "name": iv.operator,
                "cat": "task",
                "ph": "X",
                "pid": global_pid,
                "tid": lane,
                "ts": us(iv.start_s),
                "dur": max(int((iv.end_s - iv.start_s) * 1e6), 1),
                "args": {
                    "task_idx": iv.task_idx,
                    "node_id": iv.node_id,
                    "cpu_s": round(iv.cpu_s, 3),
                    "blocks": iv.num_blocks,
                    "rows": iv.num_rows,
                    "size_bytes": iv.size_bytes,
                },
            }
        )

    # Failed attempts whose task name matched no operator (e.g. shuffle
    # implementation functions, which aren't named after their operator).
    unmatched = sorted(failed_by_pid.get(-1, []), key=lambda a: a.start_s)
    if unmatched:
        unmatched_pid = len(per_op) + 1
        events.append(
            {
                "name": "process_name",
                "ph": "M",
                "pid": unmatched_pid,
                "args": {"name": "failed tasks (unmatched)"},
            }
        )
        events.append(
            {
                "name": "process_sort_index",
                "ph": "M",
                "pid": unmatched_pid,
                "args": {"sort_index": len(per_op) + 1},
            }
        )
        _emit_failed_lanes(events, unmatched_pid, 0, unmatched, us)
    return events


def count_stats_ops(stats: "DatasetStats") -> int:
    """Number of operator entries in a stats DAG (dedup by object id)."""
    seen = set()
    count = 0
    frontier = [stats]
    while frontier:
        s = frontier.pop()
        if id(s) in seen:
            continue
        seen.add(id(s))
        count += len(s.metadata)
        frontier.extend(s.parents)
    return count


def _resolve_stats(
    dataset_or_stats: Union["Dataset", "DatasetStats"]
) -> "DatasetStats":
    """Return the raw DatasetStats, unwrapping a Dataset if given one.

    Mirrors ``Dataset.get_stats_summary``: after streaming execution the
    stats live on the executor, and a written dataset delegates to the
    dataset that performed the write.
    """
    from ray.data.dataset import Dataset

    ds = dataset_or_stats
    if not isinstance(ds, Dataset):
        return ds
    if ds._current_executor:
        return ds._current_executor.get_stats()
    if ds._write_ds is not None and ds._write_ds._has_computed_output():
        return _resolve_stats(ds._write_ds)
    return ds._raw_stats()


def _resolve_edges(stats: "DatasetStats") -> Optional[List[List[int]]]:
    """True operator edges for the stats, from whichever source has them.

    Priority: the ``op_input_indices`` attribute the instrumented Ray branch
    attaches at stats-build time, then edges captured live by
    :class:`TimelineCallback` (stock Ray). Falls back to None (chain-derived
    edges) when neither matches this stats object's operator count.
    """
    per_op, _ = collect_task_intervals(stats)
    edges = getattr(stats, "op_input_indices", None)
    if edges is not None and len(edges) == len(per_op):
        return edges
    from ray_data_timeline.callback import lookup_edges

    return lookup_edges(num_ops=len(per_op))


def export_timeline(
    dataset_or_stats: Union["Dataset", "DatasetStats"],
    path: str,
    include_failed_tasks: bool = True,
    include_state_api_tasks: bool = True,
) -> None:
    """Write a Chrome trace of the dataset's execution timeline to ``path``.

    Accepts an executed :class:`~ray.data.Dataset` (e.g. the result of
    ``materialize()``) or a ``DatasetStats`` object. Open the resulting JSON
    in https://ui.perfetto.dev, chrome://tracing, or the companion viewer.

    With ``include_failed_tasks`` (the default), FAILED task attempts from the
    Ray state API are drawn in dedicated per-operator lanes; with
    ``include_state_api_tasks`` (the default), FINISHED tasks invisible to
    block stats (stock-Ray shuffle stages) get extra rows. Both require the
    cluster that ran the dataset to still be up, and degrade gracefully (with
    a warning) when the state API is unavailable.

    ``path`` may be a local path or any pyarrow-supported URI (e.g.
    ``s3://bucket/run/trace.json``), so a job running on a remote cluster can
    export straight to cloud storage for local viewing. A ``.gz`` suffix
    gzip-compresses the trace.
    """
    stats = _resolve_stats(dataset_or_stats)
    op_inputs = _resolve_edges(stats)
    payload = json.dumps(
        {
            "traceEvents": build_chrome_trace(
                stats,
                op_inputs=op_inputs,
                include_failed_tasks=include_failed_tasks,
                include_state_api_tasks=include_state_api_tasks,
            )
        }
    ).encode()
    if path.endswith(".gz"):
        payload = gzip.compress(payload)
    if "://" in path:
        import pyarrow.fs as pa_fs

        filesystem, fs_path = pa_fs.FileSystem.from_uri(path)
        # compression=None: the payload is already gzipped when asked for;
        # pyarrow would otherwise re-compress based on the .gz extension.
        with filesystem.open_output_stream(fs_path, compression=None) as f:
            f.write(payload)
    else:
        with open(path, "wb") as f:
            f.write(payload)
    logger.info("Wrote Ray Data execution timeline to %s", path)
