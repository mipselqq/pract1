#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p spark/jars
curl -fL --retry 3 -o spark/jars/iceberg-spark-runtime-3.5_2.12-1.7.1.jar \
  https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.1/iceberg-spark-runtime-3.5_2.12-1.7.1.jar
curl -fL --retry 3 -o spark/jars/iceberg-aws-bundle-1.7.1.jar \
  https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.7.1/iceberg-aws-bundle-1.7.1.jar
(cd spark/jars && sha256sum -c ../jars.sha256)
