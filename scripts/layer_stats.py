"""Размеры, схемы и параметры таблиц Iceberg."""

import json
from pathlib import Path
from run import query, scalar

root = Path(__file__).resolve().parents[1]
tables = {
    "bronze": [
        "raw_lineitem",
        "raw_orders",
        "raw_customer",
        "injected_anomalies",
        "raw_lineitem_gzip",
        "raw_lineitem_zstd",
        "raw_orders_gzip",
        "raw_orders_zstd",
        "raw_customer_gzip",
        "raw_customer_zstd",
    ],
    "silver": ["clean_lineitem", "large_orders", "large_order_facts"],
    "gold": ["q18_top100"],
}
out = []
for catalog, names in tables.items():
    for name in names:
        base = f"{catalog}.lakehouse.{name}"
        files = query(
            f'SELECT count(*), sum(file_size_in_bytes) FROM {catalog}.lakehouse."{name}$files"'
        )[0][0]
        props = dict(query(f'SELECT key, value FROM {catalog}.lakehouse."{name}$properties"')[0])
        ddl = query(f"SHOW CREATE TABLE {base}")[0][0][0]
        out.append(
            {
                "layer": catalog,
                "table": name,
                "rows": int(scalar(f"SELECT count(*) FROM {base}")),
                "files": int(files[0]),
                "bytes": int(files[1]),
                "format": props.get("write.format.default", ""),
                "parquet_codec": props.get("write.parquet.compression-codec", ""),
                "orc_codec": props.get("write.orc.compression-codec", ""),
                "columns": query(f"SHOW COLUMNS FROM {base}")[0],
                "ddl": ddl,
            }
        )
(root / "results" / "layers.json").write_text(json.dumps(out, indent=2) + "\n")
print(json.dumps(out, indent=2))
