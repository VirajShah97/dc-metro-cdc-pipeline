CREATE TABLE train_predictions (
    id SERIAL PRIMARY KEY,
    station_code VARCHAR(10),
    station_name VARCHAR(100),
    line_code VARCHAR(5),
    car_count INTEGER,
    destination_code VARCHAR(10),
    destination_name VARCHAR(100),
    platform_group INTEGER,
    minutes VARCHAR(20),
    ingested_at TIMESTAMP DEFAULT NOW()
);