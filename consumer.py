import json
import pymysql
from datetime import datetime
from confluent_kafka import Consumer, KafkaException

KAFKA_BROKER = "localhost:9092"
TOPICS = ["weather_data", "weather_alerts", "weather_forecast"]
GROUP_ID = "weather-consumer-group"

# MySQL Connection
db = pymysql.connect(
    host="localhost",
    port=3307,
    user="root",
    password="password",
    database="weather_monitoring",
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True
)

def create_tables():
    with db.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS weather_readings")
        cursor.execute("DROP TABLE IF EXISTS weather_alerts")
        cursor.execute("DROP TABLE IF EXISTS weather_forecasts")
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_readings (
            id INT AUTO_INCREMENT PRIMARY KEY,
            station_id INT NOT NULL,
            location VARCHAR(100) NOT NULL,
            timestamp DATETIME NOT NULL,
            temperature DECIMAL(5,2),
            humidity INT,
            pressure DECIMAL(6,1),
            wind_speed DECIMAL(5,1),
            wind_direction VARCHAR(2),
            precipitation DECIMAL(5,1),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_alerts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location_id INT NOT NULL,
            location VARCHAR(100) NOT NULL,
            alert_type VARCHAR(50) NOT NULL,
            severity VARCHAR(20) NOT NULL,
            message TEXT,
            start_time DATETIME NOT NULL,
            end_time DATETIME NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_forecasts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            location_id INT NOT NULL,
            location VARCHAR(100) NOT NULL,
            forecast_date DATE NOT NULL,
            high_temp DECIMAL(5,2),
            low_temp DECIMAL(5,2),
            humidity INT,
            conditions VARCHAR(50),
            precipitation_chance INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

# Run table creation before consuming
create_tables()

consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe(TOPICS)

print(f"🔍 Listening to Kafka topics: {TOPICS}...")

def insert_weather_data(weather_data):
    try:
        with db.cursor() as cursor:
            query = """
            INSERT INTO weather_readings (
                station_id, location, timestamp, temperature, 
                humidity, pressure, wind_speed, wind_direction,
                precipitation
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                weather_data['station_id'],
                weather_data['location'],
                weather_data['timestamp'],
                weather_data['temperature'],
                weather_data['humidity'],
                weather_data['pressure'],
                weather_data['wind_speed'],
                weather_data['wind_direction'],
                weather_data['precipitation']
            ))
    except Exception as e:
        print(f"MySQL insert error (weather data): {e}")

def insert_alert(alert):
    try:
        with db.cursor() as cursor:
            query = """
            INSERT INTO weather_alerts (
                location_id, location, alert_type, severity,
                message, start_time, end_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                alert['location_id'],
                alert['location'],
                alert['alert_type'],
                alert['severity'],
                alert['message'],
                alert['start_time'],
                alert['end_time']
            ))
    except Exception as e:
        print(f"MySQL insert error (alert): {e}")

def insert_forecast(forecast):
    try:
        with db.cursor() as cursor:
            query = """
            INSERT INTO weather_forecasts (
                location_id, location, forecast_date, high_temp,
                low_temp, humidity, conditions, precipitation_chance
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, (
                forecast['location_id'],
                forecast['location'],
                forecast['date'],
                forecast['high_temp'],
                forecast['low_temp'],
                forecast['humidity'],
                forecast['conditions'],
                forecast['precipitation_chance']
            ))
    except Exception as e:
        print(f"MySQL insert error (forecast): {e}")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        topic = msg.topic()
        data = json.loads(msg.value().decode("utf-8"))

        print(f"\nReceived message from {topic}:")
        
        if topic == "weather_data":
            if data.get("event") == "Weather Reading":
                print(f"Weather data from {data.get('location')} - Station: {data.get('station_id')}")
                weather_data = {
                    'station_id': data.get('station_id'),
                    'location': data.get('location'),
                    'timestamp': data.get('data', {}).get('timestamp', datetime.now().isoformat()),
                    'temperature': data.get('data', {}).get('temperature'),
                    'humidity': data.get('data', {}).get('humidity'),
                    'pressure': data.get('data', {}).get('pressure'),
                    'wind_speed': data.get('data', {}).get('wind_speed'),
                    'wind_direction': data.get('data', {}).get('wind_direction'),
                    'precipitation': data.get('data', {}).get('precipitation')
                }
                insert_weather_data(weather_data)
                
        elif topic == "weather_alerts":
            if data.get("event") == "Weather Alert":
                print(f"Weather alert for {data.get('location')}: {data.get('alert', {}).get('alert_type')}")
                alert = {
                    'location_id': data.get('location_id'),
                    'location': data.get('location'),
                    'alert_type': data.get('alert', {}).get('alert_type'),
                    'severity': data.get('alert', {}).get('severity'),
                    'message': data.get('alert', {}).get('message'),
                    'start_time': data.get('alert', {}).get('start_time'),
                    'end_time': data.get('alert', {}).get('end_time')
                }
                insert_alert(alert)
                
        elif topic == "weather_forecast":
            if data.get("event") == "Weather Forecast":
                print(f"Weather forecast for {data.get('location')} on {data.get('forecast', {}).get('date')}")
                forecast = {
                    'location_id': data.get('location_id'),
                    'location': data.get('location'),
                    'date': data.get('forecast', {}).get('date'),
                    'high_temp': data.get('forecast', {}).get('high_temp'),
                    'low_temp': data.get('forecast', {}).get('low_temp'),
                    'humidity': data.get('forecast', {}).get('humidity'),
                    'conditions': data.get('forecast', {}).get('conditions'),
                    'precipitation_chance': data.get('forecast', {}).get('precipitation_chance')
                }
                insert_forecast(forecast)

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()