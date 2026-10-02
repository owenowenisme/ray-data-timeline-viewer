"""Unit tests for state-API backfill of operators without block stats."""
from dataclasses import dataclass
from typing import Optional

from ray_data_timeline.timeline import (
    TaskInterval,
    _backfill_empty_operators,
    _split_by_time_gaps,
)


@dataclass
class FT:
    name: str
    state: str = "FINISHED"
    type: str = "NORMAL_TASK"
    start_time_ms: Optional[int] = 0
    end_time_ms: Optional[int] = 50
    node_id: Optional[str] = "n"


def nonempty(name):
    return (name, [TaskInterval(name, 0, "n", 0.0, 1.0, 0.0, 1, 1, 1)])


def test_exact_match_phantom_does_not_steal_substring_tasks():
    # "ReadFiles" must not claim "ReadFilesParquetV2->Project" tasks.
    per_op = [("ReadFiles", []), nonempty("ReadFilesParquetV2->Project")]
    tasks = [FT("ReadFilesParquetV2->Project")]
    _backfill_empty_operators(per_op, tasks)
    assert per_op[0][1] == []  # ReadFiles stays empty


def test_repeated_name_splits_tasks_across_instances():
    # Two reduce ops with identical names, tasks in two time clusters.
    per_op = [("Reduce", []), nonempty("Map"), ("Reduce", [])]
    tasks = [FT("Reduce", start_time_ms=s, end_time_ms=s + 10) for s in (0, 5, 1000, 1005)]
    filled = _backfill_empty_operators(per_op, tasks)
    assert filled == 2
    assert len(per_op[0][1]) == 2 and len(per_op[2][1]) == 2
    # First instance gets the early cluster, second the late one.
    assert max(iv.start_s for iv in per_op[0][1]) < min(
        iv.start_s for iv in per_op[2][1]
    )


def test_split_by_time_gaps_always_returns_k_groups():
    tasks = [FT(name="x", start_time_ms=s) for s in (0, 1, 100, 101, 200)]
    assert [len(g) for g in _split_by_time_gaps(tasks, 3)] == [2, 2, 1]
    assert len(_split_by_time_gaps(tasks, 7)) == 7  # more ops than tasks
    assert _split_by_time_gaps(tasks, 1) == [tasks]


def test_actor_and_failed_tasks_are_not_backfilled():
    per_op = [("Reduce", [])]
    tasks = [
        FT("Reduce", state="FAILED"),
        FT("Reduce", type="ACTOR_CREATION_TASK"),
        FT("Reduce", start_time_ms=None),
    ]
    assert _backfill_empty_operators(per_op, tasks) == 0


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-vv", __file__]))
