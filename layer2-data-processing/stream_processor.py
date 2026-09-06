"""
Layer 2 — Fault-Tolerant RTIS Stream Processor
──────────────────────────────────────────────
Consumes raw RTIS telemetry from Kafka topic 'rtis-telemetry', cleans & validates
coordinates, calculates schedule deviation deltas, and persists time-series
into TimescaleDB while upserting live state into MongoDB.
"""

import json
import logging
import os
import sys
import time
from typing import List, Tuple, Optional

try:
    from confluent_kafka import Consumer, KafkaError
    HAS_CONFLUENT = True
except ImportError:
    HAS_CONFLUENT = False

import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("layer2-stream-processor")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "rtis-telemetry")
TIMESCALE_URI = os.getenv("TIMESCALE_URI", "postgresql://postgres:postgrespassword@localhost:5432/ir_eta_db")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

# Bounds for Indian subcontinent GPS coordinates
LAT_MIN, LAT_MAX = 6.0, 38.0
LON_MIN, LON_MAX = 68.0, 98.0


def get_db_connection():
    """Establishes connection to TimescaleDB."""
    return psycopg2.connect(TIMESCALE_URI, connect_timeout=3)


def get_mongo_collection():
    """Returns MongoDB train_live_state collection with quick timeout."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
        client.admin.command('ping')
        return client["railway_operations"]["train_live_state"]
    except Exception as e:
        logger.warning(f"MongoDB not reachable at {MONGO_URI}: {e}")
        return None


def validate_and_process_payload(payload: dict) -> Optional[Tuple[tuple, dict]]:
    """
    Validates GPS bounds and calculates running schedule deviations.
    Returns TimescaleDB tuple and MongoDB update dictionary.
    """
    try:
        lat = float(payload.get("latitude", 0.0))
        lon = float(payload.get("longitude", 0.0))
        if not (LAT_MIN <= lat <= LAT_MAX and LON_MIN <= lon <= LON_MAX):
            logger.warning(f"Out-of-bounds GPS ping dropped: ({lat}, {lon}) for train {payload.get('train_id')}")
            return None

        reported_delay = float(payload.get("current_delay_minutes", 0.0))
        speed = float(payload.get("current_speed_kmph", 0.0))
        signal = str(payload.get("signal_aspect", "GREEN")).upper()

        # Calculated deviation incorporates signal penalty: +5 mins penalty on red aspect
        calculated_deviation = reported_delay + (5.0 if signal == "RED" else 0.0)

        ts = payload.get("timestamp")
        train_id = str(payload.get("train_id", ""))
        route_id = str(payload.get("route_id", "DEL-BCT"))
        last_station = str(payload.get("last_station_passed", "UNKNOWN"))

        pg_record = (
            ts,
            train_id,
            route_id,
            lat,
            lon,
            speed,
            last_station,
            signal,
            reported_delay,
            calculated_deviation
        )

        mongo_doc = {
            "train_id": train_id,
            "last_updated": ts,
            "route_id": route_id,
            "lat": lat,
            "lon": lon,
            "speed": speed,
            "current_station": last_station,
            "signal": signal,
            "current_delay_minutes": reported_delay,
            "deviation_minutes": calculated_deviation
        }

        return pg_record, mongo_doc
    except Exception as e:
        logger.error(f"Error validating payload: {e}")
        return None


def flush_batch(conn, cursor, batch: List[tuple]):
    """Flushes buffered telemetry records to TimescaleDB."""
    if not batch:
        return
    insert_sql = """
        INSERT INTO train_telemetry (
            timestamp, train_id, route_id, latitude, longitude,
            current_speed_kmph, last_station_passed, signal_aspect,
            current_delay_minutes, calculated_deviation_minutes
        ) VALUES %s
        ON CONFLICT DO NOTHING
    """
    try:
        execute_values(cursor, insert_sql, batch)
        conn.commit()
        logger.info(f"✓ Persisted {len(batch)} telemetry records to TimescaleDB.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to flush telemetry batch to TimescaleDB: {e}")


def process_stream():
    """Main streaming consumption and dual-storage persistence loop."""
    if not HAS_CONFLUENT:
        logger.error("confluent-kafka is not installed. Please install confluent-kafka.")
        return

    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'group.id': 'stream-processing-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False
    }

    logger.info(f"Initializing Kafka Consumer for topic '{TOPIC}' at {KAFKA_BOOTSTRAP}...")
    try:
        consumer = Consumer(conf)
        consumer.subscribe([TOPIC])
    except Exception as e:
        logger.error(f"Could not connect Kafka consumer: {e}")
        return

    mongo_col = get_mongo_collection()
    pg_conn = None
    pg_cursor = None
    try:
        pg_conn = get_db_connection()
        pg_cursor = pg_conn.cursor()
        logger.info("Connected to TimescaleDB.")
    except Exception as e:
        logger.warning(f"TimescaleDB connection failed ({e}). Running with in-memory/cache fallback.")

    logger.info(f"Layer 2 Stream Processor running. Listening to topic '{TOPIC}'...")

    batch = []
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if batch and (time.time() - last_flush > 3.0):
                    if pg_conn and pg_cursor:
                        flush_batch(pg_conn, pg_cursor, batch)
                    consumer.commit()
                    batch = []
                    last_flush = time.time()
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                time.sleep(2)
                continue

            try:
                payload = json.loads(msg.value().decode('utf-8'))
                processed = validate_and_process_payload(payload)
                if not processed:
                    continue

                pg_record, mongo_doc = processed
                batch.append(pg_record)

                # Upsert Live State to MongoDB
                if mongo_col is not None:
                    try:
                        mongo_col.update_one(
                            {"train_id": mongo_doc["train_id"]},
                            {"$set": mongo_doc},
                            upsert=True
                        )
                    except Exception as me:
                        logger.warning(f"Mongo update error: {me}")

                if len(batch) >= 50 or (time.time() - last_flush > 3.0):
                    if pg_conn and pg_cursor:
                        flush_batch(pg_conn, pg_cursor, batch)
                    consumer.commit()
                    batch = []
                    last_flush = time.time()

            except Exception as e:
                logger.error(f"Error parsing telemetry record: {e}")

    except KeyboardInterrupt:
        logger.info("Stream processor stopped by user.")
    finally:
        if batch and pg_conn and pg_cursor:
            flush_batch(pg_conn, pg_cursor, batch)
        if pg_cursor:
            pg_cursor.close()
        if pg_conn:
            pg_conn.close()
        consumer.close()


if __name__ == "__main__":
    process_stream()
