"""Unit tests for failed-attempt extraction (no Ray cluster needed)."""
from dataclasses import dataclass
from typing import Optional

from ray_data_timeline.timeline import _failed_attempts_from_events


@dataclass
class FakeTask:
    """Minimal stand-in for ray.util.state TaskState."""

    name: str
    state: str
    type: str = "NORMAL_TASK"
    attempt_number: int = 0
    start_time_ms: Optional[int] = 1000
    creation_time_ms: Optional[int] = 900
    end_time_ms: Optional[int] = 2000
    node_id: Optional[str] = "node1"
    error_type: Optional[str] = "WORKER_DIED"


def test_genuine_normal_task_failure_is_kept():
    tasks = [FakeTask(name="MapBatches(fn)", state="FAILED")]
    attempts = _failed_attempts_from_events(tasks, ["MapBatches(fn)"])
    assert len(attempts) == 1
    assert attempts[0].operator == "MapBatches(fn)"
    assert attempts[0].error_type == "WORKER_DIED"


def test_actor_method_failure_is_kept():
    # An actor-pool map UDF crash is a real failure worth showing.
    tasks = [FakeTask(name="MapWorker.map", state="FAILED", type="ACTOR_TASK")]
    attempts = _failed_attempts_from_events(tasks, ["MapWorker"])
    assert len(attempts) == 1


def test_actor_creation_teardown_is_dropped():
    # Ray re-stamps a torn-down actor's completed __init__ as FAILED; that is
    # teardown, not a retriable failure, and must not appear.
    tasks = [
        FakeTask(
            name="FooterReader.__init__",
            state="FAILED",
            type="ACTOR_CREATION_TASK",
            start_time_ms=None,
            node_id=None,
        )
    ]
    attempts = _failed_attempts_from_events(tasks, ["ReadFiles"])
    assert attempts == []


def test_non_failed_rows_ignored():
    tasks = [FakeTask(name="MapBatches(fn)", state="FINISHED")]
    assert _failed_attempts_from_events(tasks, ["MapBatches(fn)"]) == []


def test_missing_start_falls_back_to_creation_time():
    tasks = [
        FakeTask(name="MapBatches(fn)", state="FAILED", start_time_ms=None)
    ]
    attempts = _failed_attempts_from_events(tasks, ["MapBatches(fn)"])
    assert attempts[0].start_s == 0.9  # creation_time_ms / 1000


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-vv", __file__]))
