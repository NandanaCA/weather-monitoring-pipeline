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

def log_performance_metrics(batch_id, processing_time, num_records, window_size="20 minutes"):
    """Log performance metrics to a file.
    
    For streaming mode, each entry represents a separate micro-batch.
    The comparison script will calculate the average processing time across all micro-batches.
    """
    metrics = {
        "timestamp": time.time(),
        "batch_id": batch_id,
        "processing_time_seconds": processing_time,  # Time for this specific micro-batch only
        "records_processed": num_records,            # Records in this specific micro-batch
        "records_per_second": num_records / processing_time if processing_time > 0 else 0,
        "window_size": window_size,
        "mode": "streaming"
    }
    
    # Print metrics to console
    print(f"\n===== PERFORMANCE METRICS: Batch {batch_id} =====")
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
    
    return batch_df

def main():
    """Main function to run the streaming job"""
    # Create Spark session
    spark = SparkSession.builder.appName("WeatherStreaming").getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    try:
            
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
            .option("startingOffsets", "latest")
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
        
        # Process data with tumbling window (20 minutes - SAME AS BATCH)
        window_duration = "20 minutes"
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
        
        # Start the streaming query to console for windowed data
        query1 = windowed_data.writeStream \
            .outputMode("update") \
            .format("console") \
            .option("truncate", "false") \
            .foreachBatch(lambda df, id: process_batch(df, id)) \
            .start()
        
        # Detect extreme weather conditions (20 minute window)
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
        
        # Count alerts by type and severity in 20-minute windows (SAME AS BATCH)
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

        # Wait for all queries to terminate
        query1.awaitTermination()
        query2.awaitTermination()
        query3.awaitTermination()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()