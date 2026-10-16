"""Запросы к Bronze, выполняемые через spark-submit."""

import json
import time
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pract1-bronze-verification").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
queries = {
    "customer_sample": "SELECT custkey,name,CAST(acctbal AS DECIMAL(18,2)) AS acctbal "
    "FROM bronze.lakehouse.raw_customer ORDER BY custkey LIMIT 5",
    "customer_scan": "SELECT count(*),sum(CAST(acctbal AS DECIMAL(18,2))),min(custkey),max(custkey) "
    "FROM bronze.lakehouse.raw_customer",
}
q18 = Path("/opt/spark/task/sql/q18_reference.sql").read_text().strip().rstrip(";")
queries["q18_exact"] = (
    q18.replace("tpch.sf1.customer", "bronze.lakehouse.raw_customer_zstd")
    .replace("tpch.sf1.orders", "bronze.lakehouse.raw_orders_zstd")
    .replace("tpch.sf1.lineitem", "bronze.lakehouse.raw_lineitem_zstd")
)
out = {
    "spark_version": spark.version,
    "bronze_tables": [r.tableName for r in spark.sql("SHOW TABLES IN bronze.lakehouse").collect()],
    "results": {},
}
for name, sql in queries.items():
    start = time.perf_counter()
    rows = spark.sql(sql).collect()
    out["results"][name] = {
        "seconds": round(time.perf_counter() - start, 6),
        "rows": [[str(v) for v in r] for r in rows],
    }
print("BRONZE_VERIFICATION_JSON=" + json.dumps(out), flush=True)
spark.stop()
