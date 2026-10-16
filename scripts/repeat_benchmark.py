"""Повторные замеры агрегаций и Q18."""

import argparse
import csv
import json
import random
import statistics
import uuid
from datetime import datetime, timezone
from run import ROOT, RESULTS, AGG, query, ready, same_aggregation


def exact_q18(table):
    codec = table.rsplit("_", 1)[-1]
    sql = (ROOT / "sql/q18_reference.sql").read_text().strip().rstrip(";")
    return (
        sql.replace("tpch.sf1.customer", f"bronze.lakehouse.raw_customer_{codec}")
        .replace("tpch.sf1.orders", f"bronze.lakehouse.raw_orders_{codec}")
        .replace("tpch.sf1.lineitem", f"bronze.lakehouse.{table}")
    )


def timed(sql):
    token = "pract1_audit_" + uuid.uuid4().hex
    rows, wall = query(f"/* {token} */\n{sql}")
    metrics, _ = query(f"""SELECT query_id, state,
        date_diff('millisecond', started, "end"),
        queued_time_ms, analysis_time_ms, planning_time_ms
        FROM system.runtime.queries
        WHERE query LIKE '%{token}%'
          AND query NOT LIKE '%system.runtime.queries%'
        ORDER BY created DESC LIMIT 1""")
    if len(metrics) != 1 or metrics[0][1] != "FINISHED":
        raise RuntimeError(f"Missing completed server query: {metrics}")
    m = metrics[0]
    return rows, {
        "query_id": m[0],
        "wall_seconds": round(wall, 6),
        "server_elapsed_ms": int(m[2]),
        "queued_ms": int(m[3]),
        "analysis_ms": int(m[4]),
        "planning_ms": int(m[5]),
    }


def save(records):
    (RESULTS / "repeated_runs.json").write_text(json.dumps(records, indent=2) + "\n")
    with (RESULTS / "repeated_runs.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(records[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(records)


def main(rounds, q18_only=False):
    ready()
    gold, _ = query(
        "SELECT name,custkey,orderkey,orderdate,totalprice,total_quantity "
        "FROM gold.lakehouse.q18_top100 ORDER BY totalprice DESC,orderdate,orderkey"
    )
    variants = [x["variant"] for x in json.loads((RESULTS / "benchmark.json").read_text())]
    reference = None
    # Прогрев каждой таблицы перед замерами.
    for variant in [] if q18_only else variants:
        result, _ = query(AGG.format(table=f"sales_{variant}"))
        if reference is None:
            reference = result
        if not same_aggregation(result, reference):
            raise AssertionError(f"Warm-up aggregate differs: {variant}")
    for codec in ("gzip", "zstd"):
        result, _ = query(exact_q18("raw_lineitem_" + codec))
        if result != gold:
            raise AssertionError(f"Exact Q18 differs: {codec}")
    if reference is not None:
        (RESULTS / "aggregation_reference.json").write_text(json.dumps(reference, indent=2) + "\n")
    rng = random.Random(42)
    records = (
        [
            r
            for r in json.loads((RESULTS / "repeated_runs.json").read_text())
            if r["kind"] == "aggregation"
        ]
        if q18_only
        else []
    )
    experiments = (
        [("q18_exact_bronze", ["gzip", "zstd"])]
        if q18_only
        else [("aggregation", variants), ("q18_exact_bronze", ["gzip", "zstd"])]
    )
    for kind, names in experiments:
        for round_index in range(1, rounds + 1):
            order = list(names)
            rng.shuffle(order)
            for position, name in enumerate(order, 1):
                sql = (
                    AGG.format(table=f"sales_{name}")
                    if kind == "aggregation"
                    else exact_q18("raw_lineitem_" + name)
                )
                result, metrics = timed(sql)
                valid = (
                    same_aggregation(result, reference) if kind == "aggregation" else result == gold
                )
                if not valid:
                    raise AssertionError(f"Query result differs: {kind}/{name}")
                record = {
                    "kind": kind,
                    "variant": name,
                    "round": round_index,
                    "position_in_round": position,
                    "phase": "warm",
                    "result_rows": len(result),
                    "result_verified": valid,
                    **metrics,
                }
                records.append(record)
                save(records)
                print(
                    f"{kind}/{name} round {round_index}: {metrics['server_elapsed_ms']} ms server, "
                    f"{metrics['wall_seconds']:.3f} s CLI",
                    flush=True,
                )
    summaries = []
    for kind, names in (("aggregation", variants), ("q18_exact_bronze", ["gzip", "zstd"])):
        for name in names:
            group = [x for x in records if x["kind"] == kind and x["variant"] == name]
            times = [x["server_elapsed_ms"] for x in group]
            summaries.append(
                {
                    "kind": kind,
                    "variant": name,
                    "runs": len(group),
                    "server_median_ms": statistics.median(times),
                    "server_min_ms": min(times),
                    "server_max_ms": max(times),
                    "server_stdev_ms": round(statistics.stdev(times), 3),
                    "wall_median_seconds": round(
                        statistics.median(x["wall_seconds"] for x in group), 6
                    ),
                    "all_results_verified": all(x["result_verified"] for x in group),
                }
            )
    (RESULTS / "repeated_summary.json").write_text(
        json.dumps(
            {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "seed": 42,
                "rounds": rounds,
                "warmup_per_variant": 1,
                "server_timing": "date_diff millisecond between started and end in system.runtime.queries; queue/planning recorded separately",
                "summaries": summaries,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument(
        "--q18-only",
        action="store_true",
        help="refresh only Q18; preserve existing aggregation trials",
    )
    args = parser.parse_args()
    if args.rounds < 3:
        parser.error("At least three rounds are required")
    main(args.rounds, args.q18_only)
