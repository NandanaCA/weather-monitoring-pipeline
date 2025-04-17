from pyspark.sql import SparkSession


def main():
    """Main function to run the batch job"""

    # Create Spark session
    spark = SparkSession.builder.appName("WeatherBatch").getOrCreate()

    try:
        # Read data from MySQL
        jdbc_url = "jdbc:mysql://mysql:3306/weather_monitoring"

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

        # Register as temp view
        weather_df.createOrReplaceTempView("weather_readings")

        # Run analytics query
        avg_temp = spark.sql(
            """
            SELECT 
                location, 
                AVG(temperature) as avg_temperature,
                MIN(temperature) as min_temperature,
                MAX(temperature) as max_temperature
            FROM weather_readings
            GROUP BY location
        """
        )

        # Show results
        print("Average Temperature by Location:")
        avg_temp.show()

        # Save results
        avg_temp.write.mode("overwrite").parquet("/opt/spark/data/avg_temp")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
