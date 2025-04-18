from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


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

        # Read from Kafka
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

        # Output to console
        query = weather_df.writeStream.outputMode("append").format("console").start()

        query.awaitTermination()

    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
