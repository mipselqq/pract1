"""Изменение схемы Silver и восстановление из снапшота."""

import json
from pathlib import Path
from run import query, execute, scalar, table_exists, ready

root = Path(__file__).resolve().parents[1]
ready()
files_before = sorted(
    query('SELECT file_path, file_size_in_bytes FROM silver.lakehouse."large_orders$files"')[0]
)
snapshots_before = query(
    'SELECT snapshot_id FROM silver.lakehouse."large_orders$snapshots" ORDER BY committed_at'
)[0]
columns = [r[0] for r in query("SHOW COLUMNS FROM silver.lakehouse.large_orders")[0]]
before = int(scalar("SELECT count(*) FROM silver.lakehouse.large_orders"))
if "audit_note" in columns and "review_note" not in columns:
    execute("ALTER TABLE silver.lakehouse.large_orders RENAME COLUMN audit_note TO review_note")
    columns.append("review_note")
if "review_note" not in columns:
    execute("ALTER TABLE silver.lakehouse.large_orders ADD COLUMN review_note varchar")
execute("ALTER TABLE silver.lakehouse.large_orders RENAME COLUMN review_note TO audit_note")
execute("ALTER TABLE silver.lakehouse.large_orders RENAME COLUMN audit_note TO review_note")
after = int(scalar("SELECT count(*) FROM silver.lakehouse.large_orders"))
nulls = int(scalar("SELECT count(*) FROM silver.lakehouse.large_orders WHERE review_note IS NULL"))
files_after = sorted(
    query('SELECT file_path, file_size_in_bytes FROM silver.lakehouse."large_orders$files"')[0]
)
snapshots_after = query(
    'SELECT snapshot_id FROM silver.lakehouse."large_orders$snapshots" ORDER BY committed_at'
)[0]
snaps = query(
    'SELECT snapshot_id, operation FROM bronze.lakehouse."raw_lineitem$snapshots" ORDER BY committed_at'
)[0]
first = int(snaps[0][0])
old_rows = int(
    scalar(f"SELECT count(*) FROM bronze.lakehouse.raw_lineitem FOR VERSION AS OF {first}")
)
new_rows = int(scalar("SELECT count(*) FROM bronze.lakehouse.raw_lineitem"))
recovery_sql = (
    "CREATE TABLE bench.lakehouse.restored_lineitem WITH (format='PARQUET') AS "
    f"SELECT * FROM bronze.lakehouse.raw_lineitem FOR VERSION AS OF {first}"
)
(root / "results" / "recovery.sql").write_text(recovery_sql + ";\n")
previous_path = root / "results" / "evolution.json"
previous = json.loads(previous_path.read_text()) if previous_path.exists() else {}
restore_seconds = previous.get("restore_seconds")
if not table_exists("bench", "restored_lineitem"):
    _, restore_seconds = execute(recovery_sql, "bench.compression_codec=ZSTD")
checks = "count(*), sum(orderkey), sum(quantity)"
old_checks = query(f"SELECT {checks} FROM bronze.lakehouse.raw_lineitem FOR VERSION AS OF {first}")[
    0
]
restored_checks = query(f"SELECT {checks} FROM bench.lakehouse.restored_lineitem")[0]
result = {
    "silver_rows_before": before,
    "silver_rows_after": after,
    "new_column_nulls": nulls,
    "bronze_first_snapshot_id": first,
    "bronze_old_rows": old_rows,
    "bronze_current_rows": new_rows,
    "bronze_snapshot_count": len(snaps),
    "silver_data_files_unchanged": files_before == files_after,
    "silver_snapshots_unchanged": snapshots_before == snapshots_after,
    "restored_table": "bench.lakehouse.restored_lineitem",
    "restored_checks": restored_checks,
    "source_snapshot_checks": old_checks,
    "restore_seconds": restore_seconds,
    "restore_verified": old_checks == restored_checks,
}
assert before == after == nulls
assert new_rows == old_rows + 2
assert files_before == files_after and snapshots_before == snapshots_after
assert old_checks == restored_checks
(root / "results" / "evolution.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))
