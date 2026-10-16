"""Чтение Bronze и выполнение Q18 на Spark."""

import json
import subprocess
from run import ROOT, RESULTS, query, ready
from repeat_benchmark import exact_q18, timed

ready()
sql = {
    "customer_sample": "SELECT custkey,name,CAST(acctbal AS DECIMAL(18,2)) AS acctbal "
    "FROM bronze.lakehouse.raw_customer ORDER BY custkey LIMIT 5",
    "customer_scan": "SELECT count(*),sum(CAST(acctbal AS DECIMAL(18,2))),min(custkey),max(custkey) "
    "FROM bronze.lakehouse.raw_customer",
    "q18_exact": exact_q18("raw_lineitem_zstd"),
}
expected = {}
for name, text in sql.items():
    rows, metrics = timed(text)
    expected[name] = {"rows": rows, **metrics}
cmd = [
    "docker",
    "run",
    "--rm",
    "--network",
    "pract1_default",
    "-v",
    f"{ROOT}/spark/conf:/opt/spark/conf:ro",
    "-v",
    f"{ROOT}/spark/jars:/opt/spark/iceberg-jars:ro",
    "-v",
    f"{ROOT}:/opt/spark/task:ro",
    "apache/spark:3.5.3",
    "/opt/spark/bin/spark-submit",
    "--master",
    "local[2]",
    "/opt/spark/task/spark/verify_bronze.py",
]
p = subprocess.run(cmd, text=True, capture_output=True, cwd=ROOT, timeout=600)
(RESULTS / "spark_bronze.txt").write_text(
    "\n".join(line.rstrip() for line in (p.stdout + "\n" + p.stderr).splitlines()) + "\n"
)
if p.returncode:
    raise RuntimeError(f"Spark failed; inspect results/spark_bronze.txt: {p.stderr[-2000:]}")
payload = next(
    line.split("=", 1)[1]
    for line in p.stdout.splitlines()
    if line.startswith("BRONZE_VERIFICATION_JSON=")
)
out = json.loads(payload)
for name, result in out["results"].items():
    result["trino_metrics"] = {k: v for k, v in expected[name].items() if k != "rows"}
    result["matches_trino"] = result["rows"] == expected[name]["rows"]
    if not result["matches_trino"]:
        raise AssertionError(f"Spark and Trino differ for {name}")
gold, _ = query(
    "SELECT name,custkey,orderkey,orderdate,totalprice,total_quantity "
    "FROM gold.lakehouse.q18_top100 ORDER BY totalprice DESC,orderdate,orderkey"
)
out["q18_matches_gold"] = out["results"]["q18_exact"]["rows"] == gold
if not out["q18_matches_gold"]:
    raise AssertionError("Spark Q18 and Gold differ")
(RESULTS / "spark_verification.json").write_text(json.dumps(out, indent=2) + "\n")
print(
    json.dumps(
        {
            "q18_matches_gold": out["q18_matches_gold"],
            "checks": {
                k: {
                    "rows": len(v["rows"]),
                    "matches_trino": v["matches_trino"],
                    "spark_seconds": v["seconds"],
                }
                for k, v in out["results"].items()
            },
        },
        indent=2,
    )
)
