# ray-data-timeline-profiling plugin

A Claude Code plugin that profiles a Ray Data pipeline on an Anyscale cluster
and renders its execution timeline — while `Dataset.export_timeline` is still
an unmerged branch. It patches the branch's python files onto every node of a
stock `anyscale/ray:nightly` image at job startup, so no custom image build is
needed, then renders the exported trace in the hosted viewer at
https://owenowenisme.github.io/ray-data-timeline-viewer/.

## Install

Inside Claude Code:

```
/plugin marketplace add owenowenisme/ray-data-timeline-viewer
/plugin install ray-data-timeline-profiling@ray-data-timeline-tools
```

You also need a checkout of the timeline branch — it is the source of the
files patched onto the cluster (the skill will clone it for you if asked, or):

```bash
git clone -b Ray-data-timeline-generator --depth 1 https://github.com/owenowenisme/ray.git
```

## Use

Ask Claude Code to profile a Ray Data job, e.g.:

> Profile this pipeline on an 8x m5d.4xlarge Anyscale cluster and show me
> where the time went: ray.data.read_parquet("s3://...").groupby("k").count()

or invoke the skill directly with `/ray-data-timeline-profiling`. Claude will
build the profiling script, submit the Anyscale job with the timeline patch,
download the trace, and hand you a `timeline.json.gz` to drop onto
https://owenowenisme.github.io/ray-data-timeline-viewer/.

Prerequisites: the `anyscale` CLI authenticated against your org's console,
and (for the S3 presign fallback) nothing — the trace is presigned from the
cluster, so no local AWS credentials are needed.
