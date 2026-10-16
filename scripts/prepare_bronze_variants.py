"""Копии трёх таблиц Bronze со сжатием GZIP и ZSTD."""

import json
from run import RESULTS, execute, query, scalar, table_exists, ready

ready()
legacy = (
    {x["codec"]: x for x in json.loads((RESULTS / "bronze_compression.json").read_text())}
    if (RESULTS / "bronze_compression.json").exists()
    else {}
)
path = RESULTS / "bronze_variants.json"
previous = json.loads(path.read_text()) if path.exists() else []
old = {(x["codec"], t["table"]): t for x in previous for t in x["tables"]}
out = []
for codec in ("GZIP", "ZSTD"):
    tables = []
    for source, expected_count in (
        ("lineitem", 6001215),
        ("orders", 1500000),
        ("customer", 150000),
    ):
        table = f"raw_{source}_{codec.lower()}"
        write_seconds = old.get((codec, table), {}).get("write_seconds")
        if source == "lineitem" and write_seconds is None:
            write_seconds = legacy.get(codec, {}).get("write_seconds")
        if not table_exists("bronze", table):
            _, write_seconds = execute(
                f"CREATE TABLE bronze.lakehouse.{table} WITH (format='PARQUET') "
                f"AS SELECT * FROM tpch.sf1.{source}",
                f"bronze.compression_codec={codec}",
            )
        stats = query(
            f"SELECT count(*),sum(file_size_in_bytes),sum(record_count) "
            f'FROM bronze.lakehouse."{table}$files"'
        )[0][0]
        properties = dict(query(f'SELECT key,value FROM bronze.lakehouse."{table}$properties"')[0])
        count = int(scalar(f"SELECT count(*) FROM bronze.lakehouse.{table}"))
        assert count == int(stats[2]) == expected_count
        assert properties["write.parquet.compression-codec"] == codec.lower()
        tables.append(
            {
                "table": table,
                "source": f"tpch.sf1.{source}",
                "rows": count,
                "files": int(stats[0]),
                "bytes": int(stats[1]),
                "write_seconds": write_seconds,
                "iceberg_codec_property": properties["write.parquet.compression-codec"],
            }
        )
    out.append(
        {"codec": codec, "tables": tables, "bronze_total_bytes": sum(t["bytes"] for t in tables)}
    )
    path.write_text(json.dumps(out, indent=2) + "\n")
print(json.dumps(out, indent=2))
