#!/bin/bash
spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  --driver-class-path /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  --conf "spark.sql.shuffle.partitions=5" \
  /opt/spark/apps/batch.py
