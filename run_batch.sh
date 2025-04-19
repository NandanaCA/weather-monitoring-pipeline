#!/bin/bash
docker exec -it spark spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  --driver-class-path /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  /opt/spark/apps/batch.py
