"""Загрузка слоёв и сравнение форматов хранения."""

import argparse
import csv
import io
import json
import math
import platform
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
RESULTS.mkdir(exist_ok=True)


def query(sql, session=None):
    cmd = ["docker", "compose", "exec", "-T", "trino", "trino", "--output-format", "CSV"]
    if session:
        cmd += ["--session", session]
    cmd += ["--execute", sql]
    start = time.perf_counter()
    p = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True)
    seconds = time.perf_counter() - start
    if p.returncode:
        raise RuntimeError(f"SQL failed ({seconds:.2f}s): {sql}\n{p.stderr}\n{p.stdout}")
    return list(csv.reader(io.StringIO(p.stdout))), seconds


def execute(sql, session=None):
    rows, seconds = query(sql, session)
    print(f"{seconds:.3f}s | {sql[:100]}", flush=True)
    return rows, seconds


def scalar(sql):
    return query(sql)[0][0][0]


def table_exists(catalog, name):
    return any(row[0] == name for row in query(f"SHOW TABLES FROM {catalog}.lakehouse")[0])


def pipeline():
    ready()
    for catalog in ("bronze", "silver", "gold", "bench"):
        execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.lakehouse")

    raw = {
        "raw_lineitem": "SELECT * FROM tpch.sf1.lineitem",
        "raw_orders": "SELECT * FROM tpch.sf1.orders",
        "raw_customer": "SELECT * FROM tpch.sf1.customer",
    }
    for name, source in raw.items():
        if not table_exists("bronze", name):
            execute(
                f"CREATE TABLE bronze.lakehouse.{name} WITH (format='PARQUET') AS {source}",
                "bronze.compression_codec=ZSTD",
            )

    # Контрольные дубль и строка с отрицательным количеством.
    if not table_exists("bronze", "injected_anomalies"):
        execute(
            "INSERT INTO bronze.lakehouse.raw_lineitem SELECT * FROM bronze.lakehouse.raw_lineitem LIMIT 1"
        )
        execute(
            "INSERT INTO bronze.lakehouse.raw_lineitem SELECT CAST(-1 AS bigint), partkey, suppkey, linenumber, "
            "CAST(-1 AS double), extendedprice, discount, tax, returnflag, linestatus, "
            "shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment "
            "FROM bronze.lakehouse.raw_lineitem LIMIT 1"
        )
        execute("CREATE TABLE bronze.lakehouse.injected_anomalies AS SELECT 1 AS batch_id")

    if not table_exists("silver", "clean_lineitem"):
        execute(
            """CREATE TABLE silver.lakehouse.clean_lineitem WITH (format='PARQUET') AS
        SELECT orderkey, linenumber, quantity
        FROM (
          SELECT orderkey, linenumber, quantity,
                 ROW_NUMBER() OVER (PARTITION BY orderkey, linenumber ORDER BY orderkey) AS rn
          FROM bronze.lakehouse.raw_lineitem
          WHERE orderkey > 0 AND linenumber > 0 AND quantity > 0
        ) WHERE rn = 1""",
            "silver.compression_codec=ZSTD",
        )
    if not table_exists("silver", "large_orders"):
        execute(
            """CREATE TABLE silver.lakehouse.large_orders WITH (format='PARQUET') AS
        SELECT orderkey, sum(quantity) AS total_quantity
        FROM silver.lakehouse.clean_lineitem
        GROUP BY orderkey HAVING sum(quantity) > 300""",
            "silver.compression_codec=ZSTD",
        )
    if not table_exists("silver", "large_order_facts"):
        execute(
            """CREATE TABLE silver.lakehouse.large_order_facts WITH (format='PARQUET') AS
        SELECT trim(c.name) AS name, c.custkey, o.orderkey,
               o.orderdate, o.totalprice, l.total_quantity
        FROM silver.lakehouse.large_orders l
        JOIN bronze.lakehouse.raw_orders o ON l.orderkey = o.orderkey
        JOIN bronze.lakehouse.raw_customer c ON o.custkey = c.custkey""",
            "silver.compression_codec=ZSTD",
        )
    if not table_exists("gold", "q18_top100"):
        execute(
            """CREATE TABLE gold.lakehouse.q18_top100 WITH (format='PARQUET') AS
        SELECT name, custkey, orderkey, orderdate, totalprice, total_quantity
        FROM silver.lakehouse.large_order_facts
        ORDER BY totalprice DESC, orderdate, orderkey LIMIT 100""",
            "gold.compression_codec=ZSTD",
        )

    counts = {
        name: int(scalar(f"SELECT count(*) FROM {catalog}.lakehouse.{name}"))
        for catalog, name in [
            ("bronze", "raw_lineitem"),
            ("bronze", "raw_orders"),
            ("bronze", "raw_customer"),
            ("silver", "clean_lineitem"),
            ("silver", "large_orders"),
            ("silver", "large_order_facts"),
            ("gold", "q18_top100"),
        ]
    }
    counts["invalid_rows"] = int(
        scalar(
            "SELECT count(*) FROM bronze.lakehouse.raw_lineitem WHERE NOT coalesce(orderkey > 0 AND linenumber > 0 AND quantity > 0, false)"
        )
    )
    counts["duplicate_keys"] = int(
        scalar(
            "SELECT count(*) - count(DISTINCT (orderkey, linenumber)) FROM bronze.lakehouse.raw_lineitem WHERE orderkey > 0 AND linenumber > 0 AND quantity > 0"
        )
    )
    counts["silver_unique_keys"] = int(
        scalar("SELECT count(DISTINCT (orderkey, linenumber)) FROM silver.lakehouse.clean_lineitem")
    )
    counts["gold_min_quantity"] = scalar(
        "SELECT min(total_quantity) FROM gold.lakehouse.q18_top100"
    )
    (RESULTS / "quality.json").write_text(json.dumps(counts, indent=2, ensure_ascii=False) + "\n")
    print(json.dumps(counts, indent=2, ensure_ascii=False), flush=True)
    assert counts["clean_lineitem"] == counts["silver_unique_keys"]
    assert (
        counts["raw_lineitem"]
        == counts["clean_lineitem"] + counts["invalid_rows"] + counts["duplicate_keys"]
    )
    assert counts["clean_lineitem"] == 6001215  # deterministic TPC-H sf1
    assert counts["large_orders"] == counts["large_order_facts"]
    assert counts["q18_top100"] == min(100, counts["large_order_facts"])
    assert float(counts["gold_min_quantity"]) > 300


AGG = """SELECT returnflag, linestatus, count(*) AS cnt,
 sum(quantity) AS sum_qty, sum(extendedprice) AS sum_price,
 sum(discount) AS sum_disc, sum(extendedprice * (1 - discount)) AS sum_revenue
 FROM bench.lakehouse.{table} GROUP BY returnflag, linestatus ORDER BY returnflag, linestatus"""

Q18 = """SELECT c.name, c.custkey, o.orderkey, o.orderdate, o.totalprice,
 l.total_quantity
 FROM (SELECT orderkey, sum(quantity) AS total_quantity
       FROM {catalog}.lakehouse.{table}
       GROUP BY orderkey HAVING sum(quantity) > 300) l
 JOIN bronze.lakehouse.raw_orders o ON l.orderkey = o.orderkey
 JOIN bronze.lakehouse.raw_customer c ON o.custkey = c.custkey
 ORDER BY o.totalprice DESC, o.orderdate, o.orderkey LIMIT 100"""


def ready():
    for _ in range(90):
        try:
            if scalar("SELECT sum(nationkey) FROM tpch.tiny.nation"):
                return
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("Trino did not become ready")


def same_aggregation(a, b):
    if len(a) != len(b):
        return False
    return all(
        x[:2] == y[:2]
        and all(
            math.isclose(float(m), float(n), rel_tol=1e-10, abs_tol=1e-4)
            for m, n in zip(x[2:], y[2:])
        )
        for x, y in zip(a, b)
    )


def benchmark(resume=False):
    ready()
    variants = [
        (fmt, codec) for fmt in ("parquet", "orc") for codec in ("none", "snappy", "gzip", "zstd")
    ]
    rows_out = (
        json.loads((RESULTS / "benchmark.json").read_text())
        if resume and (RESULTS / "benchmark.json").exists()
        else []
    )
    reference = None
    write_path = RESULTS / "write_times.json"
    write_times = json.loads(write_path.read_text()) if write_path.exists() else {}
    for fmt, codec in variants:
        if resume and any(x["variant"] == f"{fmt}_{codec}" for x in rows_out):
            continue
        table = f"sales_{fmt}_{codec}"
        if not table_exists("bench", table):
            _, write_s = execute(
                f"CREATE TABLE bench.lakehouse.{table} WITH (format='{fmt.upper()}') "
                "AS SELECT * FROM tpch.sf1.lineitem",
                f"bench.compression_codec={codec.upper()}",
            )
            write_times[table] = round(write_s, 3)
            write_path.write_text(json.dumps(write_times, indent=2) + "\n")
        else:
            write_s = write_times.get(table)
        files = query(
            f'SELECT count(*), sum(file_size_in_bytes) FROM bench.lakehouse."{table}$files"'
        )[0][0]
        manifests = query(f'SELECT count(*), sum(length) FROM bench.lakehouse."{table}$manifests"')[
            0
        ][0]
        props = query(f'SELECT key, value FROM bench.lakehouse."{table}$properties"')[0]
        subprocess.run(
            ["docker", "compose", "restart", "trino", "minio"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        ready()
        result, cold_s = execute(AGG.format(table=table))
        again, warm_s = execute(AGG.format(table=table))
        q18_result, q18_s = execute(Q18.format(catalog="bench", table=table))
        assert same_aggregation(result, again), table
        if reference is None:
            reference = result
        assert same_aggregation(result, reference), f"Aggregation differs for {table}"
        prop = dict(props)
        row = {
            "variant": f"{fmt}_{codec}",
            "format": fmt.upper(),
            "codec": codec.upper(),
            "rows": int(scalar(f"SELECT count(*) FROM bench.lakehouse.{table}")),
            "files": int(files[0]),
            "data_bytes": int(files[1]),
            "write_seconds": round(write_s, 3) if write_s is not None else None,
            "cold_seconds": round(cold_s, 3),
            "warm_seconds": round(warm_s, 3),
            "q18_seconds": round(q18_s, 3),
            "q18_first_orderkey": q18_result[0][2],
            "manifests": int(manifests[0]),
            "manifest_bytes": int(manifests[1]),
            "iceberg_codec_property": prop.get(f"write.{fmt}.compression-codec", ""),
        }
        rows_out.append(row)
        (RESULTS / "benchmark.json").write_text(json.dumps(rows_out, indent=2) + "\n")
        with (RESULTS / "benchmark.csv").open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(row), lineterminator="\n")
            writer.writeheader()
            writer.writerows(rows_out)
        print(json.dumps(row), flush=True)
    # Сравнение GZIP и ZSTD на Bronze.
    bronze_variants = []
    previous_bronze = (
        {x["codec"]: x for x in json.loads((RESULTS / "bronze_compression.json").read_text())}
        if (RESULTS / "bronze_compression.json").exists()
        else {}
    )
    for codec in ("gzip", "zstd"):
        table = f"raw_lineitem_{codec}"
        if not table_exists("bronze", table):
            _, write_s = execute(
                f"CREATE TABLE bronze.lakehouse.{table} WITH (format='PARQUET') "
                "AS SELECT * FROM tpch.sf1.lineitem",
                f"bronze.compression_codec={codec.upper()}",
            )
        else:
            write_s = previous_bronze.get(codec.upper(), {}).get("write_seconds")
        files = query(f'SELECT sum(file_size_in_bytes) FROM bronze.lakehouse."{table}$files"')[0][
            0
        ][0]
        props = dict(query(f'SELECT key, value FROM bronze.lakehouse."{table}$properties"')[0])
        _, q18_s = execute(Q18.format(catalog="bronze", table=table))
        bronze_variants.append(
            {
                "codec": codec.upper(),
                "bytes": int(files),
                "write_seconds": write_s,
                "q18_seconds": q18_s,
                "iceberg_codec_property": props.get("write.parquet.compression-codec", ""),
            }
        )
    (RESULTS / "bronze_compression.json").write_text(json.dumps(bronze_variants, indent=2) + "\n")
    env = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "scale_factor": "sf1",
        "variants": len(variants),
        "measurement": "end-to-end wall time including Trino CLI startup; one CTAS and cold/warm aggregation per variant",
    }
    (RESULTS / "environment.json").write_text(json.dumps(env, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=["pipeline", "benchmark"])
    parser.add_argument("--resume", action="store_true", help="continue an interrupted benchmark")
    args = parser.parse_args()
    pipeline() if args.stage == "pipeline" else benchmark(args.resume)
