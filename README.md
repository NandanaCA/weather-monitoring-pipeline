# weather-monitoring-pipeline

docker run -d --name=kafka_dbt -p 9092:9092 apache/kafka

docker build -t json-server-weather .

docker run -d -p 3000:3000 --name json-server-dbt_container json-server-weather

docker run --name mysql-dbt_container \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=weather_monitoring \
  -p 3307:3306 \
  -v mysql_dbt_data:/var/lib/mysql \
  -v "$(pwd)"/init.sql:/docker-entrypoint-initdb.d/init.sql \
  -d mysql:latest