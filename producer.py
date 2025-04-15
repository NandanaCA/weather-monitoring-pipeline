import json
from confluent_kafka import Producer

KAFKA_BROKER = "localhost:9092"
TOPIC_WEATHER_DATA = "weather_data"
TOPIC_ALERTS = "weather_alerts"
TOPIC_FORECAST = "weather_forecast"

producer = Producer({"bootstrap.servers": KAFKA_BROKER})

def log_weather_data(station_id, location_name, data):
    """Log weather data readings to Kafka."""
    log_data = {
        "event": "Weather Reading",
        "station_id": station_id,
        "location": location_name,
        "data": data
    }
    message = json.dumps(log_data).encode("utf-8")
    producer.produce(TOPIC_WEATHER_DATA, value=message)
    producer.flush()
    print(f"Sent to Kafka ({TOPIC_WEATHER_DATA}): Weather reading from {location_name}")

def log_weather_alert(location_id, location_name, alert_data):
    """Log weather alerts to Kafka."""
    log_data = {
        "event": "Weather Alert",
        "location_id": location_id,
        "location": location_name,
        "alert": alert_data
    }
    message = json.dumps(log_data).encode("utf-8")
    producer.produce(TOPIC_ALERTS, value=message)
    producer.flush()
    print(f"Sent to Kafka ({TOPIC_ALERTS}): Alert for {location_name}")

def log_weather_forecast(location_id, location_name, forecast_data):
    """Log weather forecasts to Kafka."""
    log_data = {
        "event": "Weather Forecast",
        "location_id": location_id,
        "location": location_name,
        "forecast": forecast_data
    }
    message = json.dumps(log_data).encode("utf-8")
    producer.produce(TOPIC_FORECAST, value=message)
    producer.flush()
    print(f"Sent to Kafka ({TOPIC_FORECAST}): Forecast for {location_name}")