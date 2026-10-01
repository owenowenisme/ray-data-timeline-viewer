# Ray Data timeline viewer

A single-file, dependency-free viewer for Ray Data execution timelines.

Open the page, click **Open trace…** (or drag a file onto the chart), and pick a
`timeline.json` / `timeline.json.gz` exported by `Dataset.export_timeline`.
Everything renders locally in your browser; the trace never leaves your machine,
so the viewer can be hosted publicly while the data stays private.

Features: per-operator rows with running-task sparklines, expandable task
lanes, operator DAG indentation, hover details, click-to-focus with a copyable
inspector (including measured CPU time), time zoom (pinch / ctrl+scroll),
vertical zoom (alt+scroll), pan, and a hover crosshair.

The same trace JSON also opens in [Perfetto](https://ui.perfetto.dev).
To regenerate this page from the Ray source of truth:

    python -m ray.data.timeline trace.json.gz -o index.html
