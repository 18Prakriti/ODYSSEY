-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 1. Create Raw Telemetry Time-Series Table
CREATE TABLE IF NOT EXISTS train_telemetry (
    timestamp TIMESTAMPTZ NOT NULL,
    train_id VARCHAR(16) NOT NULL,
    route_id VARCHAR(32) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    current_speed_kmph DOUBLE PRECISION NOT NULL,
    last_station_passed VARCHAR(16) NOT NULL,
    signal_aspect VARCHAR(16) NOT NULL,
    current_delay_minutes DOUBLE PRECISION NOT NULL,
    calculated_deviation_minutes DOUBLE PRECISION DEFAULT 0.0
);

-- 2. Convert to TimescaleDB Hypertable partitioned by timestamp (1-day chunks)
SELECT create_hypertable('train_telemetry', 'timestamp', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- 3. High-Throughput Spatial-Temporal Indices
CREATE INDEX IF NOT EXISTS idx_train_telemetry_train_time 
    ON train_telemetry (train_id, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_train_telemetry_spatial 
    ON train_telemetry (last_station_passed, timestamp DESC);

-- 4. Enable Native Timescale Compression for records older than 7 days
ALTER TABLE train_telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'train_id, route_id'
);

SELECT add_compression_policy('train_telemetry', INTERVAL '7 days', if_not_exists => TRUE);
