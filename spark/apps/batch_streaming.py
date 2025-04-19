from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, avg, max, min, lit
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
import time
import json
import os

# Define the metrics file path - independent of Docker
METRICS_DIR = "/tmp/metrics"  # Using /tmp to ensure it's writable
STREAMING_METRICS_FILE = os.path.join(METRICS_DIR, "streaming_metrics.json")
BATCH_METRICS_FILE = os.path.join(METRICS_DIR, "batch_metrics.json")

def log_performance_metrics(batch_id, processing_time, num_records, window_size="5 minutes", mode="streaming"):
    """Log performance metrics to a file."""
    metrics = {
        "timestamp": time.time(),
        "batch_id": batch_id,
        "processing_time_seconds": processing_time,
        "records_processed": num_records,
        "records_per_second": num_records / processing_time if processing_time > 0 else 0,
        "window_size": window_size,
        "mode": mode
    }
    
    # Create directory if it doesn't exist
    if not os.path.exists(METRICS_DIR):
        try:
            os.makedirs(METRICS_DIR)
            print(f"Created metrics directory at {METRICS_DIR}")
        except Exception as e:
            print(f"Error creating metrics directory: {e}")
            return None
    
    # Determine which file to write to
    metrics_file = STREAMING_METRICS_FILE if mode == "streaming" else BATCH_METRICS_FILE
    
    # Write metrics to file with error handling
    try:
        with open(metrics_file, 'a') as f:
            f.write(json.dumps(metrics) + '\n')
        print(f"Metrics written to {metrics_file}")
    except Exception as e:
        print(f"Error writing metrics to file: {e}")
    
    # Print metrics to console
    print(f"\n===== PERFORMANCE METRICS: {mode.upper()} Batch {batch_id} =====")
    print(f"Processing time: {processing_time:.2f} seconds")
    print(f"Records processed: {num_records}")
    print(f"Records per second: {metrics['records_per_second']:.2f}")
    print(f"Window size: {window_size}")
    print(f"==========================================\n")
    
    return metrics

def process_batch(batch_df, batch_id):
    """Process each batch of streaming data and log metrics."""
    start_time = time.time()
    
    # Count records in the batch
    count = batch_df.count()
    
    # Calculate processing time
    processing_time = time.time() - start_time
    
    # Log metrics
    log_performance_metrics(batch_id, processing_time, count)
    
    # Return the dataframe to continue the pipeline
    return batch_df

def run_batch_processing(spark, weather_schema, alert_schema):
    """Execute identical queries on stored data in batch mode."""
    print("\n\n===== STARTING BATCH PROCESSING =====\n")
    
    # Load saved streaming results for batch processing
    # Assuming data has been stored in parquet format
    batch_start_time = time.time()
    
    try:
        # Read data from Kafka but don't use streaming
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "weather_data")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        
        # Parse JSON from Kafka
        weather_df = (
            kafka_df.selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), weather_schema).alias("data"))
            .select("data.*")
        )
        
        # Extract nested data fields
        weather_data = weather_df.select(
            col("location"),
            col("data.timestamp").cast(TimestampType()).alias("timestamp"),
            col("data.temperature").alias("temperature"),
            col("data.humidity").alias("humidity"),
            col("data.wind_speed").alias("wind_speed"),
            col("data.precipitation").alias("precipitation")
        )
        
        # Count records for metrics
        weather_count = weather_data.count()
        
        # Process with same window logic as streaming
        window_duration = "5 minutes"
        batch_windowed_data = weather_data \
            .groupBy(window("timestamp", window_duration), "location") \
            .agg(
                count("*").alias("reading_count"),
                avg("temperature").alias("avg_temperature"),
                max("temperature").alias("max_temperature"),
                min("temperature").alias("min_temperature"),
                avg("humidity").alias("avg_humidity"),
                avg("wind_speed").alias("avg_wind_speed"),
                avg("precipitation").alias("avg_precipitation")
            )
        
        # Show results
        print("BATCH WINDOWED DATA RESULTS:")
        batch_windowed_data.show(truncate=False)
        
        # Extreme conditions in batch mode
        batch_extreme_conditions = weather_data \
            .filter("temperature > 30 OR wind_speed > 15 OR precipitation > 3") \
            .groupBy(window("timestamp", window_duration), "location") \
            .agg(
                count("*").alias("extreme_count"),
                avg("temperature").alias("avg_extreme_temp"),
                max("wind_speed").alias("max_wind_speed"),
                max("precipitation").alias("max_precipitation")
            )
        
        # Show extreme conditions
        print("BATCH EXTREME CONDITIONS RESULTS:")
        batch_extreme_conditions.show(truncate=False)
        
        # Read alerts from Kafka in batch mode
        alerts_kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "weather_alerts")
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        
        # Parse alert JSON from Kafka
        alerts_df = (
            alerts_kafka_df.selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), alert_schema).alias("data"))
            .select("data.*")
        )
        
        # Extract and process alert data
        alerts = alerts_df.select(
            col("location"),
            col("alert.alert_type").alias("alert_type"),
            col("alert.severity").alias("severity"),
            col("alert.start_time").cast(TimestampType()).alias("start_time")
        )
        
        # Count alerts in batch
        alerts_count = alerts.count()
        
        # Process alerts in batch mode
        batch_alert_counts = alerts \
            .groupBy(
                window("start_time", window_duration),
                "location",
                "alert_type",
                "severity"
            ) \
            .count() \
            .orderBy("window", "count", ascending=False)
        
        # Show alert counts
        print("BATCH ALERT COUNTS RESULTS:")
        batch_alert_counts.show(truncate=False)
        
        # Calculate batch execution time
        batch_execution_time = time.time() - batch_start_time
        
        # Log batch metrics
        total_records = weather_count + alerts_count
        log_performance_metrics(1, batch_execution_time, total_records, window_duration, "batch")
        
        print(f"\nBatch processing completed in {batch_execution_time:.2f} seconds")
        print(f"Processed {total_records} records")
        print(f"Processing rate: {total_records / batch_execution_time:.2f} records/second")
        
    except Exception as e:
        print(f"Error in batch processing: {e}")
    
    print("\n===== BATCH PROCESSING COMPLETE =====\n")

def compare_performance():
    """Compare performance between streaming and batch processing."""
    try:
        # Read the metrics files
        streaming_metrics = []
        batch_metrics = []
        
        if os.path.exists(STREAMING_METRICS_FILE):
            with open(STREAMING_METRICS_FILE, 'r') as f:
                for line in f:
                    streaming_metrics.append(json.loads(line))
        
        if os.path.exists(BATCH_METRICS_FILE):
            with open(BATCH_METRICS_FILE, 'r') as f:
                for line in f:
                    batch_metrics.append(json.loads(line))
        
        if not streaming_metrics or not batch_metrics:
            print("Not enough data to compare performance")
            return
        
        print("\n===== PERFORMANCE COMPARISON =====")
        
        # Calculate streaming metrics
        streaming_avg_time = sum(m["processing_time_seconds"] for m in streaming_metrics) / len(streaming_metrics)
        streaming_total_records = sum(m["records_processed"] for m in streaming_metrics)
        streaming_avg_records = streaming_total_records / len(streaming_metrics)
        streaming_avg_rate = sum(m["records_per_second"] for m in streaming_metrics) / len(streaming_metrics)
        
        # Get batch metrics
        batch_time = batch_metrics[0]["processing_time_seconds"]
        batch_records = batch_metrics[0]["records_processed"]
        batch_rate = batch_metrics[0]["records_per_second"]
        
        print(f"Streaming mode:")
        print(f"  - Average processing time per batch: {streaming_avg_time:.2f} seconds")
        print(f"  - Average records per batch: {streaming_avg_records:.2f}")
        print(f"  - Total records processed: {streaming_total_records}")
        print(f"  - Average processing rate: {streaming_avg_rate:.2f} records/sec")
        print(f"  - Number of micro-batches: {len(streaming_metrics)}")
        
        print(f"\nBatch mode:")
        print(f"  - Processing time: {batch_time:.2f} seconds")
        print(f"  - Records processed: {batch_records}")
        print(f"  - Processing rate: {batch_rate:.2f} records/sec")
                
        print("=================================")
    except Exception as e:
        print(f"Error comparing performance: {e}")

def main():
    """Main function to run the streaming job"""
    # Create Spark session
    spark = SparkSession.builder.appName("WeatherStreaming").getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Reset metrics files at start
        for file_path in [STREAMING_METRICS_FILE, BATCH_METRICS_FILE]:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"Removed existing metrics file: {file_path}")
                except Exception as e:
                    print(f"Could not remove metrics file {file_path}: {e}")
            
        # Define schema for weather data
        weather_schema = StructType(
            [
                StructField("event", StringType(), True),
                StructField("station_id", IntegerType(), True),
                StructField("location", StringType(), True),
                StructField(
                    "data",
                    StructType(
                        [
                            StructField("timestamp", StringType(), True),
                            StructField("temperature", DoubleType(), True),
                            StructField("humidity", IntegerType(), True),
                            StructField("pressure", DoubleType(), True),
                            StructField("wind_speed", DoubleType(), True),
                            StructField("wind_direction", StringType(), True),
                            StructField("precipitation", DoubleType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

        # Define schema for weather alerts
        alert_schema = StructType([
            StructField("event", StringType(), True),
            StructField("location_id", IntegerType(), True),
            StructField("location", StringType(), True),
            StructField("alert", StructType([
                StructField("locationId", IntegerType(), True),
                StructField("alert_type", StringType(), True),
                StructField("severity", StringType(), True),
                StructField("message", StringType(), True),
                StructField("start_time", StringType(), True),
                StructField("end_time", StringType(), True)
            ]), True)
        ])

        # Read from Kafka for weather data
        kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "weather_data")
            .option("startingOffsets", "earliest")
            # .option("startingOffsets", "latest")
            .load()
        )

        # Parse JSON from Kafka
        weather_df = (
            kafka_df.selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), weather_schema).alias("data"))
            .select("data.*")
        )

        # Extract nested data fields
        weather_data = weather_df.select(
            col("location"),
            col("data.timestamp").cast(TimestampType()).alias("timestamp"),
            col("data.temperature").alias("temperature"),
            col("data.humidity").alias("humidity"),
            col("data.wind_speed").alias("wind_speed"),
            col("data.precipitation").alias("precipitation")
        )
        
        # Process data with tumbling window (5 minutes - SAME AS BATCH)
        window_duration = "5 minutes"
        windowed_data = weather_data.withWatermark("timestamp", "5 minutes") \
            .groupBy(window("timestamp", window_duration), "location") \
            .agg(
                count("*").alias("reading_count"),
                avg("temperature").alias("avg_temperature"),
                max("temperature").alias("max_temperature"),
                min("temperature").alias("min_temperature"),
                avg("humidity").alias("avg_humidity"),
                avg("wind_speed").alias("avg_wind_speed"),
                avg("precipitation").alias("avg_precipitation")
            )
        
        # Start the streaming query to console for windowed data with foreachBatch
        query1 = windowed_data.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .foreachBatch(process_batch) \
            .start()
        
        # Detect extreme weather conditions (5 minute window)
        extreme_conditions = weather_data \
            .filter("temperature > 30 OR wind_speed > 15 OR precipitation > 3") \
            .withWatermark("timestamp", "5 minutes") \
            .groupBy(window("timestamp", window_duration), "location") \
            .agg(
                count("*").alias("extreme_count"),
                avg("temperature").alias("avg_extreme_temp"),
                max("wind_speed").alias("max_wind_speed"),
                max("precipitation").alias("max_precipitation")
            )
        
        # Start the streaming query for extreme conditions
        query2 = extreme_conditions.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .start()

        # Read alerts from Kafka
        alerts_kafka_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", "weather_alerts")
            .option("startingOffsets", "latest")
            .load()
        )

        # Parse alert JSON from Kafka
        alerts_df = (
            alerts_kafka_df.selectExpr("CAST(value AS STRING) as json")
            .select(from_json(col("json"), alert_schema).alias("data"))
            .select("data.*")
        )

        # Extract and process alert data
        alerts = alerts_df.select(
            col("location"),
            col("alert.alert_type").alias("alert_type"),
            col("alert.severity").alias("severity"),
            col("alert.start_time").cast(TimestampType()).alias("start_time")
        )
        
        # Count alerts by type and severity in 5-minute windows (SAME AS BATCH)
        alert_counts = alerts.withWatermark("start_time", "5 minutes") \
            .groupBy(
                window("start_time", window_duration),
                "location",
                "alert_type",
                "severity"
            ) \
            .count() \
            .orderBy("window", "count", ascending=False)
        
        # Write alerts to console
        query3 = alert_counts.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", "false") \
            .start()

        print("\n===== STREAMING JOB STARTED =====")
        print("Streaming mode will run for 2 minutes before batch processing starts...\n")
        
        # Run streaming for a fixed duration (2 minutes)
        time.sleep(120)  # 2 minutes
        
        # Stop streaming queries
        for query in [query1, query2, query3]:
            query.stop()
        
        print("\n===== STREAMING JOB COMPLETED =====")
        
        # Run batch processing on the same data
        run_batch_processing(spark, weather_schema, alert_schema)
        
        # Compare performance
        compare_performance()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()