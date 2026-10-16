-- Эти запросы позволяют проверить медальон вручную в Trino/DBeaver.
SELECT count(*) FROM bronze.lakehouse.raw_lineitem;
SELECT count(*) FROM silver.lakehouse.clean_lineitem;
SELECT count(*) FROM silver.lakehouse.large_orders;
SELECT count(*) FROM gold.lakehouse.q18_top100;
SELECT min(total_quantity) FROM gold.lakehouse.q18_top100;
SELECT * FROM gold.lakehouse.q18_top100
ORDER BY totalprice DESC, orderdate, orderkey;
SELECT snapshot_id, operation, committed_at
FROM bronze.lakehouse."raw_lineitem$snapshots"
ORDER BY committed_at;
