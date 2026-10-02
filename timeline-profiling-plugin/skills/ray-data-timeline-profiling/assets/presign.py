"""Presign the timeline artifacts for local download.

Run as a tiny head-only Anyscale job (the cluster has the bucket credentials;
presigned URLs then need none locally, which sidesteps expired local AWS SSO).
Set BASE to the ANYSCALE_ARTIFACT_STORAGE subdir the profiling job wrote to;
the job logs print it after 'TIMELINE_OUTPUT:'.
"""

import os
import subprocess

# EDIT to the s3:// prefix your profiling job wrote to (the directory, with a
# trailing slash), or pass it via the TIMELINE_BASE env var.
BASE = os.environ.get("TIMELINE_BASE", "s3://REPLACE_ME/timeline_run/")

for fname in ["timeline.json.gz"]:
    url = (
        subprocess.check_output(
            ["aws", "s3", "presign", BASE.rstrip("/") + "/" + fname, "--expires-in", "3600"]
        )
        .decode()
        .strip()
    )
    print("PRESIGNED::" + fname + "::" + url)
