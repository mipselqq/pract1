"""Создание бакетов MinIO и warehouse Lakekeeper."""

import json
import subprocess
import time
import urllib.error
import urllib.request


def post(path, payload, ok=(200, 201, 204)):
    req = urllib.request.Request(
        "http://127.0.0.1:8181" + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=90) as response:
            print(path, response.status, response.read().decode())
            if response.status not in ok:
                raise RuntimeError(f"Unexpected status: {response.status}")
    except urllib.error.HTTPError as error:
        body = error.read().decode()
        if path.endswith("/bootstrap") and "CatalogAlreadyBootstrapped" in body:
            print("Lakekeeper already bootstrapped")
        else:
            raise RuntimeError(f"{path}: {error.code} {body}") from error


for _ in range(60):
    try:
        urllib.request.urlopen("http://127.0.0.1:8181/health", timeout=2).close()
        break
    except Exception:
        time.sleep(2)
else:
    raise RuntimeError("Lakekeeper did not become healthy")

subprocess.run(
    [
        "docker",
        "run",
        "--rm",
        "--network",
        "pract1_default",
        "--entrypoint",
        "/bin/sh",
        "pgsty/mc:latest",
        "-c",
        "set -e; mc alias set local http://minio:9000 minioadmin minioadmin; "
        "for b in bronze silver gold bench; do mc mb --ignore-existing local/$b; done",
    ],
    check=True,
)
post("/management/v1/bootstrap", {"accept-terms-of-use": True})
with urllib.request.urlopen(
    "http://127.0.0.1:8181/management/v1/warehouse", timeout=30
) as response:
    existing = {w["name"]: w for w in json.load(response)["warehouses"]}
for name in ("bronze", "silver", "gold", "bench"):
    if name in existing:
        profile = existing[name]["storage-profile"]
        if profile["bucket"] != name or profile["endpoint"].rstrip("/") != "http://minio:9000":
            raise RuntimeError(f"Existing warehouse {name} has a different storage profile")
        print(f"Warehouse {name}: existing storage profile verified")
        continue
    post(
        "/management/v1/warehouse",
        {
            "warehouse-name": name,
            "project-id": "00000000-0000-0000-0000-000000000000",
            "storage-profile": {
                "type": "s3",
                "bucket": name,
                "key-prefix": "",
                "endpoint": "http://minio:9000",
                "region": "us-east-1",
                "path-style-access": True,
                "flavor": "s3-compat",
                "sts-enabled": False,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "access-key-id": "minioadmin",
                "secret-access-key": "minioadmin",
            },
        },
    )
