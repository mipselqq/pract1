"""Параметры компьютера и образы Docker для замеров."""

import hashlib
import json
import os
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from run import ROOT, RESULTS

cpu = next(
    (
        line.split(":", 1)[1].strip()
        for line in Path("/proc/cpuinfo").read_text().splitlines()
        if line.startswith("model name")
    ),
    platform.processor(),
)
meminfo = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
images = {}
for tag in (
    "pgsty/silo:latest",
    "pgsty/mc:latest",
    "quay.io/lakekeeper/catalog:latest-main",
    "postgres:17",
    "trinodb/trino:476",
    "apache/spark:3.5.3",
):
    p = subprocess.run(
        ["docker", "image", "inspect", tag, "--format", "{{json .RepoDigests}}"],
        check=True,
        capture_output=True,
        text=True,
    )
    images[tag] = json.loads(p.stdout)
jar_checksums = {
    p.name: hashlib.sha256(p.read_bytes()).hexdigest()
    for p in sorted((ROOT / "spark/jars").glob("*.jar"))
}
out = {
    "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    "cpu_model": cpu,
    "logical_cpus": os.cpu_count(),
    "ram_total_bytes": int(meminfo["MemTotal"].strip().split()[0]) * 1024,
    "platform": platform.platform(),
    "python": platform.python_version(),
    "scale_factor": "sf1",
    "docker_images": images,
    "jar_sha256": jar_checksums,
    "baseline": "one CTAS and cold/warm CLI wall time for eight variants",
    "repeated": "five warm runs per variant; server timings and query IDs in repeated_runs.csv",
}
p = subprocess.run(
    ["docker", "compose", "ps", "--format", "json"],
    cwd=ROOT,
    check=True,
    text=True,
    capture_output=True,
)
services = (
    json.loads(p.stdout)
    if p.stdout.lstrip().startswith("[")
    else [json.loads(line) for line in p.stdout.splitlines() if line.strip()]
)
out["services"] = [
    {k: row.get(k) for k in ("Service", "State", "Health", "Image")} for row in services
]
(RESULTS / "environment_details.json").write_text(json.dumps(out, indent=2) + "\n")
print(json.dumps(out, indent=2))
