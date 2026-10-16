# Дополнительный движок: Spark 3.5.3

Из корня репозитория скачать JAR-файлы Iceberg 1.7.1 (исключены из Git) и проверить их SHA-256, затем запустить проверку Bronze:

```bash
bash scripts/download_spark.sh
python3 scripts/prepare_bronze_variants.py
python3 scripts/verify_spark.py
```

`verify_spark.py` запускает `apache/spark:3.5.3` с `spark-submit --master local[2]`, монтирует конфигурацию и JAR в отдельную папку `/opt/spark/iceberg-jars`. Библиотеки самого Spark в `/opt/spark/jars` сохраняются. Iceberg runtime соответствует Spark 3.5 и Scala 2.12; AWS bundle предоставляет S3FileIO.

Выполняются SHOW TABLES, выборка пяти клиентов, суммирование acctbal с чтением колонки и точный Q18 на Bronze/ZSTD. Результаты сравниваются с Trino, Q18 дополнительно сравнивается с Gold. Вывод - `results/spark_bronze.txt`, результаты проверок и времена - `results/spark_verification.json`. Оба движка используют один REST-каталог Lakekeeper и те же объекты MinIO. Каталог Bronze и таблицы готовятся основным пайплайном и `prepare_bronze_variants.py`.
