from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min, window, expr
import time
import json
import os


def log_performance_metrics(func_name, duration, record_count, window_size="20 minutes"):
    """Log performance metrics to a file."""
    metrics = {
        "timestamp": time.time(),
        "function": func_name,
        "processing_time_seconds": duration,
        "records_processed": record_count,
        "records_per_second": record_count/duration if duration > 0 else 0,
        "window_size": window_size,
        "mode": "batch"
    }
    
    # Print metrics to console
    print(f"\n===== PERFORMANCE METRICS: {func_name} =====")
    print(f"Processing time: {duration:.2f} seconds")
    print(f"Records processed: {record_count}")
    print(f"Records per second: {metrics['records_per_second']:.2f}")
    print(f"Window size: {window_size}")
    print(f"==========================================\n")
    
    return metrics


def measure_performance(func):
    """Decorator to measure performance of functions."""
    def wrapper(*args, **kwargs):
        # Record start time
        start_time = time.time()
        
        # Execute the function
        result = func(*args, **kwargs)
        
        # Record end time
        end_time = time.time()
        duration = end_time - start_time
        
        # Get the record count if result is a DataFrame
        record_count = 0
        if hasattr(result, 'count'):
            record_count = result.count()
        
        # Log performance metrics
        log_performance_metrics(func.__name__, duration, record_count)
        
        return result
    return wrapper


@measure_performance
def process_window_aggregation(df):
    """Process weather data with 20-minute windows by location."""
    print("\n--- 20-Minute Window Aggregations by Location ---")
    
    # Create timestamp windows with 20-minute duration
    df_with_window = df.withColumn(
        "window", 
        expr("window(timestamp, '20 minutes')")
    )
    
    windowed_data = df_with_window.groupBy("window", "location") \
        .agg(
            count("*").alias("reading_count"),
            avg("temperature").alias("avg_temperature"),
            max("temperature").alias("max_temperature"),
            min("temperature").alias("min_temperature"),
            avg("humidity").alias("avg_humidity"),
            avg("wind_speed").alias("avg_wind_speed"),
            avg("precipitation").alias("avg_precipitation")
        )
    
    windowed_data.show(10, truncate=False)
    return windowed_data


@measure_performance
def detect_extreme_weather(df):
    """Detect extreme weather conditions."""
    print("\n--- Extreme Weather Conditions ---")
    
    # Filter for extreme conditions (same logic as streaming)
    extreme_conditions = df.filter("temperature > 30 OR wind_speed > 15 OR precipitation > 3")
    
    # Group by windows and location with 20-minute duration
    df_with_window = extreme_conditions.withColumn(
        "window", 
        expr("window(timestamp, '20 minutes')")
    )
    
    extreme_summary = df_with_window.groupBy("window", "location") \
        .agg(
            count("*").alias("extreme_count"),
            avg("temperature").alias("avg_extreme_temp"),
            max("wind_speed").alias("max_wind_speed"),
            max("precipitation").alias("max_precipitation")
        )
    
    extreme_summary.show(truncate=False)
    return extreme_summary


@measure_performance
def process_alerts(spark, jdbc_url):
    """Process weather alerts in batch mode."""
    # Read alerts from MySQL
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "weather_alerts")
        .option("user", "root")
        .option("password", "password")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .load()
    )
    
    print("\n=== Weather Alerts Analysis ===")
    print("Total alerts:", df.count())
    
    # Register as temp view for SQL queries
    df.createOrReplaceTempView("weather_alerts")
    
    # Count alerts by type and severity using 20-minute windows
    alert_counts = spark.sql("""
        SELECT 
            window(start_time, '20 minutes') as window,
            location, 
            alert_type, 
            severity, 
            COUNT(*) as count
        FROM weather_alerts
        GROUP BY 
            window(start_time, '20 minutes'),
            location, 
            alert_type, 
            severity
        ORDER BY window, count DESC
    """)
    
    alert_counts.show(truncate=False)
    return alert_counts


def main():
    """Main function to run the batch job"""
    # Record overall execution start time
    overall_start_time = time.time()

    # Create Spark session
    spark = SparkSession.builder.appName("WeatherBatch").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:

        # Read data from MySQL
        jdbc_url = "jdbc:mysql://mysql-container:3306/weather_monitoring"

        # Weather readings
        weather_df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "weather_readings")
            .option("user", "root")
            .option("password", "password")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )
        
        # Additional processing from spark_batch_py
        print("\nAdditional analytics processing:")
        
        # Run window aggregation
        window_results = process_window_aggregation(weather_df)
        
        # Detect extreme weather
        extreme_results = detect_extreme_weather(weather_df)
        
        # Process alerts
        alert_results = process_alerts(spark, jdbc_url)
        
        # Store results in a dictionary
        batch_results = {
            "window_data": window_results,
            "extreme_data": extreme_results,
            "alert_data": alert_results
        }
        
        # Calculate overall execution time
        overall_execution_time = time.time() - overall_start_time
        
        # Log overall metrics
        overall_metrics = {
            "timestamp": time.time(),
            "function": "overall_batch_processing",
            "processing_time_seconds": overall_execution_time,
            "window_size": "20 minutes",
            "mode": "batch"
        }
                
        print(f"\n===== OVERALL BATCH PROCESSING PERFORMANCE =====")
        print(f"Total execution time: {overall_execution_time:.2f} seconds")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()