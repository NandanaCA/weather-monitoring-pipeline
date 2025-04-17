#!/bin/bash
docker exec spark spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  --driver-class-path /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  /opt/spark/apps/streaming.py
