-- Create the database
CREATE DATABASE IF NOT EXISTS weather_monitoring;
USE weather_monitoring;

-- Weather readings table
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
);

-- Weather alerts table
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
);

-- Weather forecasts table
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
);