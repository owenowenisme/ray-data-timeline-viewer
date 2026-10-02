"""Multi-node clock alignment without the per-block epoch anchor (stock Ray).

A local 2-node Ray cluster shares one physical monotonic clock, so it can't
exercise the cross-node offset problem. These synthetic tests inject known
per-node monotonic bases and assert the state-API-derived offsets pull every
node onto one epoch axis.
"""
from dataclasses import dataclass
from typing import Optional

from ray_data_timeline.timeline import TaskInterval, _estimate_node_offsets


@dataclass
class FakeTask:
    name: str
    state: str = "FINISHED"
    node_id: Optional[str] = "A"
    start_time_ms: Optional[int] = None
    type: str = "NORMAL_TASK"


def iv(node, start, end):
    return TaskInterval(
        operator="Map(fn)",
        task_idx=0,
        node_id=node,
        start_s=start,
        end_s=end,
        cpu_s=0.0,
        num_blocks=1,
        num_rows=1,
        size_bytes=1,
    )


def test_two_nodes_with_divergent_monotonic_clocks_align_to_epoch():
    # Node A's perf_counter base ~100s; node B's ~50000s (different boot
    # times). Their true epoch starts are ~1s apart. The block clock starts a
    # little after the task's RUNNING start (input prep), modeled as 50ms.
    intervals = [
        iv("A", 100.0, 100.5),  # node A first block at mono 100.0
        iv("A", 100.6, 101.0),
        iv("B", 50000.0, 50000.4),  # node B first block at mono 50000.0
    ]
    tasks = [
        # epoch RUNNING starts 50ms before each node's first block.
        FakeTask(name="Map(fn)", node_id="A", start_time_ms=int((1000.0 - 0.05) * 1000)),
        FakeTask(name="Map(fn)", node_id="B", start_time_ms=int((1001.0 - 0.05) * 1000)),
    ]
    offsets = _estimate_node_offsets(tasks, intervals, ["Map(fn)"])
    assert set(offsets) == {"A", "B"}

    # Apply the offsets as build_chrome_trace would.
    for i in intervals:
        i.start_s += offsets[i.node_id]
        i.end_s += offsets[i.node_id]

    a_start = min(i.start_s for i in intervals if i.node_id == "A")
    b_start = min(i.start_s for i in intervals if i.node_id == "B")
    # Both land on the shared epoch axis, ~1s apart, matching the true starts.
    assert abs(a_start - 1000.0) < 0.1
    assert abs(b_start - 1001.0) < 0.1
    assert abs((b_start - a_start) - 1.0) < 0.1  # cross-node gap preserved


def test_node_without_matched_task_event_gets_no_offset():
    # A node whose task events were evicted (huge run) can't be aligned; the
    # caller then falls back to raw monotonic with a warning.
    intervals = [iv("A", 100.0, 100.5), iv("B", 50000.0, 50000.4)]
    tasks = [FakeTask(name="Map(fn)", node_id="A", start_time_ms=1_000_000)]
    offsets = _estimate_node_offsets(tasks, intervals, ["Map(fn)"])
    assert set(offsets) == {"A"}  # B is absent -> gate `nodes <= offsets` fails


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-vv", __file__]))
