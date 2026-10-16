"""Сравнение Gold с исходным Q18 и выгрузка результата."""

import json
import csv
from pathlib import Path
from run import query

root = Path(__file__).resolve().parents[1]
reference_sql = (root / "sql" / "q18_reference.sql").read_text().strip().rstrip(";")
reference, reference_seconds = query(reference_sql)
gold, gold_seconds = query(
    "SELECT name, custkey, orderkey, orderdate, totalprice, total_quantity "
    "FROM gold.lakehouse.q18_top100 "
    "ORDER BY totalprice DESC, orderdate, orderkey"
)
result = {
    "reference_rows": len(reference),
    "gold_rows": len(gold),
    "identical": reference == gold,
    "reference_seconds": round(reference_seconds, 3),
    "gold_seconds": round(gold_seconds, 3),
    "first_orderkey": reference[0][2] if reference else None,
}
(root / "results" / "q18_validation.json").write_text(json.dumps(result, indent=2) + "\n")
with (root / "results" / "q18_gold.csv").open("w", newline="") as output:
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["name", "custkey", "orderkey", "orderdate", "totalprice", "total_quantity"])
    writer.writerows(gold)
print(json.dumps(result, indent=2))
assert result["identical"]
