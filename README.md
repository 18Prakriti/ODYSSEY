# Indian Railways Real-Time Train ETA Prediction System

This project contains the complete end-to-end architecture (Layers 1-5) for ingesting, processing, predicting, and displaying real-time ETA for Indian Railways.

## Architecture

* **Layer 1 & 2: Ingestion & Storage** (Kafka, Zookeeper, Flink, TimescaleDB, PostGIS)
* **Layer 3: ML Engine** (PyTorch Spatial-Temporal LSTM Forecaster)
* **Layer 4: API Gateway** (FastAPI + WebSocket Broadcast)
* **Layer 5: Presentation Layer** (Next.js React Dashboard & Passenger App)

## Quickstart Guide (Run Everything)

### 1. Boot Infrastructure (Docker Compose)
This command starts Zookeeper, Kafka, TimescaleDB, Layer 3 ML Engine, Layer 4 API Gateway, and Layer 5 Presentation UI:

```bash
docker-compose up -d --build
```

Wait a minute or two for all services to become healthy.

### 2. Stream Ingestion (Optional)
To process the real-time telemetry stream from Kafka into TimescaleDB, you need to run the PyFlink job.

First, download the necessary JAR dependencies:
```bash
chmod +x scripts/download_jars.sh
./scripts/download_jars.sh
```

Then, run the PyFlink job in the background:
```bash
python src/flink_job.py
```

### 3. Accessing the System
* **Presentation UI (Station Desk & Passenger App)**: http://localhost:3000
* **API Gateway**: http://localhost:8080
  * Swagger UI: http://localhost:8080/docs
* **ML Engine API**: http://localhost:8000
  * Swagger UI: http://localhost:8000/docs
* **Database (TimescaleDB)**: `localhost:5432` (User: `postgres`, Password: `postgrespassword`, DB: `ir_eta_db`)
