"""
Layer 2 — MongoDB Static Metadata Seeding Script
─────────────────────────────────────────────────
Populates MongoDB collections with static train schedules, station waypoints,
and operational track metadata for the Indian Railways ETA Prediction System.
"""

import os
import sys
from pymongo import MongoClient, ReplaceOne
import pymongo

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")

def get_mongo_client():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)

def seed_static_metadata():
    print(f"Connecting to MongoDB at {MONGO_URI}...")
    try:
        client = get_mongo_client()
        # Verify connectivity
        client.admin.command('ping')
        db = client["railway_operations"]

        # Collections
        trains_col = db["trains"]
        stations_col = db["stations"]

        # Sample Stations on the Rajdhani & Golden Quadrilateral Corridors
        stations_data = [
            {"station_code": "NDLS", "name": "New Delhi", "lat": 28.6139, "lon": 77.2090, "zone": "NR"},
            {"station_code": "MTJ", "name": "Mathura Junction", "lat": 27.4924, "lon": 77.6737, "zone": "NCR"},
            {"station_code": "AGC", "name": "Agra Cantt", "lat": 27.1767, "lon": 78.0081, "zone": "NCR"},
            {"station_code": "JP", "name": "Jaipur Junction", "lat": 26.9124, "lon": 75.7873, "zone": "NWR"},
            {"station_code": "KOTA", "name": "Kota Junction", "lat": 25.1825, "lon": 75.8340, "zone": "WCR"},
            {"station_code": "RTM", "name": "Ratlam Junction", "lat": 23.4733, "lon": 75.1326, "zone": "WR"},
            {"station_code": "BRC", "name": "Vadodara Junction", "lat": 22.3072, "lon": 73.1812, "zone": "WR"},
            {"station_code": "ST", "name": "Surat", "lat": 21.1702, "lon": 72.8311, "zone": "WR"},
            {"station_code": "BCT", "name": "Mumbai Central", "lat": 18.9696, "lon": 72.8193, "zone": "WR"},
            {"station_code": "HWH", "name": "Howrah Junction", "lat": 22.5958, "lon": 88.3426, "zone": "ER"},
            {"station_code": "PRYJ", "name": "Prayagraj Junction", "lat": 25.4358, "lon": 81.8463, "zone": "NCR"},
            {"station_code": "CNB", "name": "Kanpur Central", "lat": 26.4499, "lon": 80.3319, "zone": "NCR"},
            {"station_code": "MAS", "name": "Chennai Central", "lat": 13.0827, "lon": 80.2707, "zone": "SR"},
            {"station_code": "SBC", "name": "KSR Bengaluru", "lat": 12.9784, "lon": 77.5684, "zone": "SWR"}
        ]
        stations_col.create_index("station_code", unique=True)
        stations_col.bulk_write([
            ReplaceOne({"station_code": s["station_code"]}, s, upsert=True) for s in stations_data
        ])
        print(f"✓ Seeded {len(stations_data)} station records.")

        train_schedules = [
            {
                "train_id": "12951",
                "name": "Mumbai Rajdhani Express",
                "route_id": "DEL-BCT",
                "waypoints": [
                    {"station_code": "NDLS", "distance_km": 0, "scheduled_dep_min": 0},
                    {"station_code": "MTJ", "distance_km": 141, "scheduled_dep_min": 95},
                    {"station_code": "KOTA", "distance_km": 465, "scheduled_dep_min": 310},
                    {"station_code": "RTM", "distance_km": 731, "scheduled_dep_min": 490},
                    {"station_code": "BRC", "distance_km": 992, "scheduled_dep_min": 670},
                    {"station_code": "BCT", "distance_km": 1384, "scheduled_dep_min": 980}
                ]
            },
            {
                "train_id": "12301",
                "name": "Howrah Rajdhani Express",
                "route_id": "HWH-NDLS",
                "waypoints": [
                    {"station_code": "HWH", "distance_km": 0, "scheduled_dep_min": 0},
                    {"station_code": "PRYJ", "distance_km": 820, "scheduled_dep_min": 520},
                    {"station_code": "CNB", "distance_km": 1014, "scheduled_dep_min": 645},
                    {"station_code": "NDLS", "distance_km": 1447, "scheduled_dep_min": 950}
                ]
            }
        ]
        trains_col.create_index("train_id", unique=True)
        for ts in train_schedules:
            trains_col.replace_one({"train_id": ts["train_id"]}, ts, upsert=True)

        print(f"✓ Seeded {len(train_schedules)} train route schedules.")
        print("✓ MongoDB successfully seeded with station schedules and metadata.")
        return True
    except Exception as e:
        print(f"Notice: MongoDB not available at {MONGO_URI} ({e}). Seed skipped.")
        return False

if __name__ == "__main__":
    seed_static_metadata()
