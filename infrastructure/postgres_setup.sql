CREATE TABLE IF NOT EXISTS train_predictions (
    id SERIAL PRIMARY KEY,
    station_code VARCHAR(10),
    station_name VARCHAR(100),
    line_code VARCHAR(5),
    car_count VARCHAR(10),
    destination_code VARCHAR(10),
    destination_name VARCHAR(100),
    platform_group INTEGER,
    minutes VARCHAR(20),
    ingested_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE train_predictions REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS train_positions (
    id                       SERIAL PRIMARY KEY,
    train_id                 VARCHAR(10),
    car_count                INTEGER,
    direction_num            INTEGER,
    circuit_id               INTEGER,
    destination_station_code VARCHAR(10),
    line_code                VARCHAR(5),
    seconds_at_location      INTEGER,
    service_type             VARCHAR(20),
    ingested_at              TIMESTAMP DEFAULT NOW()
);

ALTER TABLE train_positions REPLICA IDENTITY FULL;