-- Enable PostGIS and TimescaleDB extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create core train telemetry hypertable schema
CREATE TABLE IF NOT EXISTS train_telemetry (
    train_id VARCHAR(20) NOT NULL,
    route_id VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    location GEOMETRY(Point, 4326),
    current_speed_kmph DOUBLE PRECISION,
    last_station_passed VARCHAR(50),
    signal_aspect VARCHAR(10),
    current_delay_minutes INT,
    PRIMARY KEY (train_id, timestamp)
);

-- Convert table into a TimescaleDB hypertable
SELECT create_hypertable('train_telemetry', 'timestamp', if_not_exists => TRUE);

-- Spatial and Temporal Indexes
CREATE INDEX IF NOT EXISTS idx_telemetry_location ON train_telemetry USING GIST (location);
CREATE INDEX IF NOT EXISTS idx_train_time ON train_telemetry (train_id, timestamp DESC);

-- Trigger Function: Convert Lat/Lon to PostGIS Point Geometry on write
CREATE OR REPLACE FUNCTION set_postgis_geometry()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.longitude IS NOT NULL AND NEW.latitude IS NOT NULL THEN
        NEW.location := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_set_geometry
BEFORE INSERT OR UPDATE ON train_telemetry
FOR EACH ROW
EXECUTE FUNCTION set_postgis_geometry();

-- Continuous Aggregate View: 15-minute tumbling summary per train
CREATE MATERIALIZED VIEW IF NOT EXISTS train_15min_summary
WITH (timescaledb.continuous) AS
SELECT 
    train_id,
    time_bucket('15 minutes', timestamp) AS bucket,
    AVG(current_speed_kmph) AS avg_speed,
    MAX(current_delay_minutes) AS max_delay,
    COUNT(*) AS ping_count
FROM train_telemetry
GROUP BY train_id, bucket;

-- Continuous aggregate refresh policy
SELECT add_continuous_aggregate_policy('train_15min_summary',
    start_offset => INTERVAL '1 day',
    end_offset => INTERVAL '15 minutes',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => TRUE
);
