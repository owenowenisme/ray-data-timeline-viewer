"""Preamble for Anyscale profiling jobs that need the unmerged timeline code.

The timeline feature (Dataset.export_timeline + its executor/stats
instrumentation, branch owenowenisme/Ray-data-timeline-generator) isn't in the
nightly image, so before any Ray Data code is imported anywhere, overwrite the
affected python files in site-packages on EVERY node. Files are shipped via the
job's working_dir in a sibling ``patched/`` directory; each node applies them
with one pinned, zero-CPU task. Workers import ray.data lazily on their first
task, so patching before the pipeline runs wins the race.

Usage in a profiling script::

    import ray
    ray.init()
    from timeline_patch import patch_all_nodes
    patch_all_nodes(expected_nodes=9)   # workers + head
    # ... only now import ray.data and build the pipeline ...
    result = pipeline.materialize()
    result.export_timeline(OUT + "/timeline.json.gz")
"""

import importlib.util
import os
import shutil
import time

import ray

PATCH_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "patched")


def _apply_patch_locally() -> str:
    """Overwrite ray/data files in this node's site-packages (atomically)."""
    spec = importlib.util.find_spec("ray.data")
    data_root = os.path.dirname(spec.origin)
    copied = []
    for dirpath, _, files in os.walk(PATCH_DIR):
        for fname in files:
            if not fname.endswith(".py"):
                continue
            src = os.path.join(dirpath, fname)
            rel = os.path.relpath(src, PATCH_DIR)
            dst = os.path.join(data_root, rel)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            tmp = dst + ".tmp"
            shutil.copyfile(src, tmp)
            os.replace(tmp, dst)  # atomic: no torn reads by concurrent workers
            copied.append(rel)
    return f"{ray.get_runtime_context().get_node_id()[:12]}: {len(copied)} files"


@ray.remote(num_cpus=0, max_calls=1)
def _patch_node() -> str:
    return _apply_patch_locally()


def _wait_for_nodes(n: int, timeout_s: float = 1200) -> list:
    start = time.time()
    while True:
        alive = [node for node in ray.nodes() if node["Alive"]]
        if len(alive) >= n:
            return alive
        if time.time() - start > timeout_s:
            raise TimeoutError(f"only {len(alive)}/{n} nodes after {timeout_s}s")
        print(f"waiting for nodes: {len(alive)}/{n}")
        time.sleep(10)


def patch_all_nodes(expected_nodes: int) -> None:
    """Patch the driver, wait for the full cluster, then patch every node."""
    from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

    print(_apply_patch_locally() + " (driver node)")
    nodes = _wait_for_nodes(expected_nodes)
    ray.get(
        [
            _patch_node.options(
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    node_id=node["NodeID"], soft=False
                )
            ).remote()
            for node in nodes
        ]
    )
    print(f"patched {len(nodes)} nodes")
