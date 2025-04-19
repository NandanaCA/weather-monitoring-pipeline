import threading
import random
import time
import json
from datetime import datetime, timedelta
from producer import log_weather_data, log_weather_alert, log_weather_forecast

# Basic data for simulation
LOCATIONS = [
    {"id": 1, "name": "New York", "country": "USA"},
    {"id": 2, "name": "London", "country": "UK"},
    {"id": 3, "name": "Tokyo", "country": "Japan"},
    {"id": 4, "name": "Sydney", "country": "Australia"},
    {"id": 5, "name": "Rio de Janeiro", "country": "Brazil"}
]

STATIONS = [
    {"id": 1, "locationId": 1, "name": "NYC Central Park"},
    {"id": 2, "locationId": 1, "name": "NYC JFK Airport"},
    {"id": 3, "locationId": 2, "name": "London Heathrow"},
    {"id": 4, "locationId": 3, "name": "Tokyo Shinjuku"},
    {"id": 5, "locationId": 4, "name": "Sydney Observatory"},
    {"id": 6, "locationId": 5, "name": "Rio Downtown"}
]

# Counter for total readings
READING_COUNT = 0
LOCK = threading.Lock()

def get_location_for_station(station_id):
    """Get location details for a given station."""
    station = next((s for s in STATIONS if s["id"] == station_id), None)
    if station:
        location = next((l for l in LOCATIONS if l["id"] == station["locationId"]), None)
        return location
    return None

def generate_weather_reading():
    """Generate a random weather reading for a random station."""
    global READING_COUNT
    
    # Select a random station
    station = random.choice(STATIONS)
    station_id = station["id"]
    
    # Get location for this station
    location = get_location_for_station(station_id)
    location_name = location["name"] if location else "Unknown"
    
    # Generate weather data
    weather_data = {
        "stationId": station_id,
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(10.0, 35.0), 1),
        "humidity": random.randint(30, 90),
        "pressure": round(random.uniform(990.0, 1020.0), 1),
        "wind_speed": round(random.uniform(0, 20.0), 1),
        "wind_direction": random.choice(["N", "NE", "E", "SE", "S", "SW", "W", "NW"]),
        "precipitation": round(random.uniform(0, 5.0), 1)
    }
    
    # Log to Kafka
    log_weather_data(station_id, location_name, weather_data)
    
    with LOCK:
        READING_COUNT += 1
        
    print(f"Generated weather reading for {location_name}, Station #{station_id}")
    return weather_data

def generate_weather_alert():
    """Generate a random weather alert for a random location."""
    location = random.choice(LOCATIONS)
    location_id = location["id"]
    location_name = location["name"]
    
    # Alert types with corresponding severities
    alert_types = {
        "high_temperature": ["moderate", "severe"],
        "low_temperature": ["moderate", "severe"],
        "heavy_rain": ["moderate", "severe", "extreme"],
        "thunderstorm": ["moderate", "severe"],
        "high_wind": ["moderate", "severe"],
        "fog": ["moderate"]
    }
    
    alert_type = random.choice(list(alert_types.keys()))
    severity = random.choice(alert_types[alert_type])
    
    # Create alert messages based on type
    messages = {
        "high_temperature": f"Unusually high temperatures expected in {location_name}",
        "low_temperature": f"Cold weather warning for {location_name}",
        "heavy_rain": f"Heavy rainfall warning for {location_name}",
        "thunderstorm": f"Thunderstorm warning for {location_name}",
        "high_wind": f"High wind warning for {location_name}",
        "fog": f"Dense fog expected in {location_name}"
    }
    
    # Random duration between 3-12 hours
    start_time = datetime.now()
    end_time = start_time + timedelta(hours=random.randint(3, 12))
    
    alert_data = {
        "locationId": location_id,
        "alert_type": alert_type,
        "severity": severity,
        "message": messages[alert_type],
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat()
    }
    
    # Log to Kafka
    log_weather_alert(location_id, location_name, alert_data)
    print(f"Generated weather alert for {location_name}: {alert_type} ({severity})")
    return alert_data

def generate_weather_forecast():
    """Generate a random weather forecast for a random location."""
    location = random.choice(LOCATIONS)
    location_id = location["id"]
    location_name = location["name"]
    
    # Weather conditions with associated precipitation chances
    conditions = {
        "sunny": (0, 10),
        "partly cloudy": (10, 30),
        "cloudy": (30, 50),
        "light rain": (50, 70),
        "heavy rain": (70, 90),
        "thunderstorm": (80, 100)
    }
    
    condition = random.choice(list(conditions.keys()))
    precip_range = conditions[condition]
    
    # Generate forecast for the next day
    forecast_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    
    forecast_data = {
        "locationId": location_id,
        "date": forecast_date,
        "high_temp": round(random.uniform(15.0, 35.0), 1),
        "low_temp": round(random.uniform(5.0, 20.0), 1),
        "humidity": random.randint(30, 90),
        "conditions": condition,
        "precipitation_chance": random.randint(precip_range[0], precip_range[1])
    }
    
    # Log to Kafka
    log_weather_forecast(location_id, location_name, forecast_data)
    print(f"Generated forecast for {location_name} on {forecast_date}: {condition}")
    return forecast_data

def simulate_weather_system():
    """Continuously generate random weather data and send to Kafka topics."""
    global READING_COUNT
    
    print("Starting weather simulation...")
    
while True:
    # Generate 5 weather readings in parallel
    for _ in range(5):
        threading.Thread(target=generate_weather_reading).start()

    # Generate alerts and forecasts based on count
    with LOCK:
        if READING_COUNT % 5 == 0 and READING_COUNT > 0:
            threading.Thread(target=generate_weather_alert).start()
        if READING_COUNT % 10 == 0 and READING_COUNT > 0:
            threading.Thread(target=generate_weather_forecast).start()
        
        # Random delay between generations
        time.sleep(random.uniform(0.005, 0.01))

if __name__ == "__main__":
    simulate_weather_system()