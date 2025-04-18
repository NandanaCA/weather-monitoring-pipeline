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
./batch.sh

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
