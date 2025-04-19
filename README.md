# weather-monitoring-pipeline

## Starting Containers
```bash
# startup
docker compose up -d

# if there are changes
docker compose up -d --build
```
## Dependencies for python scripts
```bash
pip install confluent-kafka
pip install pymysql
pip install cryptography
```

## Access MySQL
```bash
docker exec -it mysql-container mysql -u root -p
```

## Run/Process batch processing data:
```bash
./rbatch.sh

```

## Run Stream processing data:
```bash
./rstream.sh

```
## Run Stream VS Batch comaparison:
```bash
./run_comparison.sh

```

## if you lose batch data run this
```bash
cat > read_parquet.py << EOF
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ReadParquet").getOrCreate()
df = spark.read.parquet("/opt/spark/data/avg_temp")
print("Data from parquet file:")
df.show()
spark.stop()
EOF

docker cp read_parquet.py spark:/tmp/read_parquet.py

docker exec -it spark spark-submit --master "local[*]" /tmp/read_parquet.py
```
## Shell scripts not working, use these commands
```bash
# Create run_stream.sh
cat > run_stream.sh << 'EOF'
#!/bin/bash
docker exec -it spark spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
  --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  --driver-class-path /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  /opt/spark/apps/streaming.py
EOF

# Create run_batch.sh
cat > run_batch.sh << 'EOF'
#!/bin/bash
docker exec -it spark spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  --driver-class-path /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  /opt/spark/apps/batch.py
EOF

# Create run_comaprison.sh
cat > run_comparison.sh << 'EOF'
#!/bin/bash
docker exec -it spark spark-submit \
  --master local[*] \
  --jars /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  --driver-class-path /opt/spark/jars/mysql-connector-j-8.0.33.jar \
  /opt/spark/apps/batch_streaming.py
EOF

# Make them executable
chmod +x run_stream.sh run_batch.sh run_comparison.sh

```
