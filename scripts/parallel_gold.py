"""Один и десять одновременных запросов к Gold."""

import json
import time
from concurrent.futures import ThreadPoolExecutor
from run import RESULTS, query, ready

ready()
sql = (
    "SELECT name,custkey,orderkey,orderdate,totalprice,total_quantity "
    "FROM gold.lakehouse.q18_top100 ORDER BY totalprice DESC,orderdate,orderkey"
)
reference, _ = query(sql)
_, single_seconds = query(sql)
start = time.perf_counter()
with ThreadPoolExecutor(max_workers=10) as pool:
    runs = list(pool.map(lambda _: query(sql), range(10)))
batch_seconds = time.perf_counter() - start
valid = all(rows == reference for rows, _ in runs)
out = {
    "query": sql,
    "streams": 10,
    "executions": 10,
    "ok": sum(rows == reference for rows, _ in runs),
    "errors": 0,
    "single_seconds": single_seconds,
    "batch_seconds": batch_seconds,
    "single_requests_per_second": 1 / single_seconds,
    "batch_requests_per_second": 10 / batch_seconds,
    "throughput_ratio": 10 * single_seconds / batch_seconds,
    "individual_seconds": [seconds for _, seconds in runs],
    "all_results_verified": valid,
    "measurement": "end-to-end CLI wall time; materialized Gold has 57 rows, so startup/network dominate",
}
(RESULTS / "parallel_gold.json").write_text(json.dumps(out, indent=2) + "\n")
print(json.dumps(out, indent=2))
assert valid
