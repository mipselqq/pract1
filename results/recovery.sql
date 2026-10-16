CREATE TABLE bench.lakehouse.restored_lineitem WITH (format='PARQUET') AS SELECT * FROM bronze.lakehouse.raw_lineitem FOR VERSION AS OF 8918826211794721819;
