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

# SIH26028: Network-Aware Dynamic ETA & Cascading Delay Propagation Engine

> **Technical Positioning:** An enterprise-grade, feed-agnostic railway intelligence platform that synthesizes real-time telemetry, network occupancy, historical corridor dynamics, and operational disruptions to generate explainable, confidence-bounded arrival forecasts across complex rail networks.

---

## Executive Overview

Static timetable models and simplistic distance-to-speed regressions fail in real-world railway networks because trains do not operate in isolation. A single unscheduled stop triggers cascading headway interventions, route conflicts, and platform bottlenecks across multiple downstream sections.

This platform solves **SIH26028** by abandoning monolithic destination-level ETA predictions in favor of **Recursive Section-Level Forecasting** combined with a **Dynamic Delay Propagation Graph**. Built with a feed-agnostic data adapter layer, the system decouples machine learning inference from operational telemetry sources (e.g., RTIS, FOIS, or simulated feeds), ensuring drop-in production feasibility.

---

## System Architecture

```text
┌───────────────────────────────────────────────────────────────────────────┐
│                           1. INGESTION LAYER                              │
│  RTIS / GPS Feeds  │  Signal Interlocking  │  Weather Grid  │  Schedules  │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                   2. DATA QUALITY & INTEGRITY LAYER                       │
│  Timestamp Sync  │  Outlier Pruning  │  Map-Matching  │  Reliability Score │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                    3. STREAM INGESTION & PIPELINE                         │
│       Apache Kafka / Redpanda  ───►  Redis State Store (Sub-10ms)         │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                 4. ETA INTELLIGENCE CORE (HYBRID ENGINE)                  │
│  ┌─────────────────────────┐                   ┌────────────────────────┐ │
│  │ Section Forecast Engine │                   │ Delay Propagation GNN  │ │
│  │ (Gradient Boosted Trees)│                   │ (Cascading Graph Net)  │ │
│  └────────────┬────────────┘                   └───────────┬────────────┘ │
│               └───────────────────┬────────────────────────┘              │
│                                   ▼                                       │
│                       Operational Rule Engine                             │
│                  (TSRs, Signal Blocks, Maintenance)                       │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                 5. EXPLAINABILITY & CONFIDENCE BOUNDING                   │
│   Conformal Uncertainty Intervals  │  SHAP Feature Attribution Factor     │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                    6. MULTI-TENANT DELIVERY APIS                          │
│    Passenger Mobile App    │   Station Displays   │   Controller Console  │
│  (Window ETA + Alerts)     │   (Freshness Meta)   │   (Network Topology)  │
└───────────────────────────────────────────────────────────────────────────┘

```

---

## Core Technical Innovations

### 1. Feed-Agnostic Data Adapter & Quality Scoring

* **Decoupled Telemetry:** Normalizes incoming streams (RTIS locomotive feeds, public APIs, or simulated network telemetry) into a strict internal standard schema.
* **Map Matching & Anomaly Pruning:** Uses a Hidden Markov Model (HMM) running against digitized railway network geometries to eliminate GPS drift, ghost halts, and timestamp jumps.
* **Telemetry Reliability Index ($R_{GPS}$):** Calculates signal confidence based on update latency and sensor noise, directly modulating the output confidence interval:

$$R_{GPS} = \alpha \cdot e^{-\Delta t / \tau} + (1 - \alpha) \cdot \left(1 - \frac{\sigma_{\text{jitter}}}{\sigma_{\text{max}}}\right)$$



### 2. Section-Wise Recursive Inference

Predicting total journey ETA directly accumulates compounding errors. The engine decomposes an entire journey into granular operational sections:

* Predicts **Section Run Time (SRT)** for the immediate ahead section ($S_{i}$).
* Evaluates upstream signal clearance and downstream train density.
* Recursively aggregates $S_{i+1}, \dots, S_n$ to construct intermediate and destination ETAs, auto-correcting every time the locomotive clears an axle-counter or block boundary.

### 3. Graph-Driven Cascading Delay Propagation

* Employs a spatio-temporal directed graph where **nodes = stations/interlockings** and **edges = block sections**.
* Downstream impact modeling tracks how an unscheduled dwell time of Train $A$ forces braking curves and precedence overtakes on trailing Trains $B$ and $C$.
* Generates automatic dispatch impact reports for station operational staff.

### 4. Hybrid Machine Learning + Deterministic Event Engine

Pure statistical models fail during rare operational anomalies. The pipeline combines:

* **ML Layer (XGBoost / LightGBM):** Learns cyclical delays, zone-specific speed dynamics, time-of-day friction, and localized geospatial weather impacts.
* **Deterministic Event Engine:** Overrides model baselines when discrete operational flags are raised (e.g., Temporary Speed Restrictions [TSR], signal holds, scheduled track maintenance blocks).

### 5. Transparent & Explainable Predictions (XAI)

Eliminates the "black-box AI" problem for train operations controllers:

* **Confidence Bounds:** Outputs time intervals alongside single points (e.g., `20:42 | Expected: 20:38–20:47 | Confidence: 88%`).
* **Root-Cause Attribution:** Quantifies delay components in real time:
```text
Predicted Delay: +13 min
├── Network Congestion (Section 4B): +6 min
├── Precedence Hold (Rajdhani Crossing): +4 min
└── Severe Weather Speed Restriction: +3 min

```



---

## Strategic Challenge Matrix

| Challenge | Failure Mode in Naive Solutions | Our Architecture's Defense |
| --- | --- | --- |
| **Telemetry Access** | Hard-coding prototype to a specific mock API fails judge scrutiny. | **Feed-Agnostic Adapter:** Plugs into RTIS, CRIS schemas, or simulated real-time data generators without engine code alterations. |
| **Noisy / Missing GPS** | Missing 10-minute GPS updates cause wild ETA swings or crashes. | **Data Quality Pipeline:** Dead-reckoning fallback, HMM map matching, and telemetry reliability scoring. |
| **Compounding Error** | Small errors at intermediate stops scale into massive destination deviations. | **Recursive Sectional Recalculation:** Re-anchors baseline ground truth at every block section. |
| **Cascading Delays** | Isolated train delay prediction misses ripple effects on crossing traffic. | **Delay Propagation Engine:** Graph-based topological monitoring of downstream block occupancy. |
| **False Alerts** | Notification fatigue occurs when minor variations dispatch critical alerts. | **Hysteresis Alert Filter:** Configurable operational bands (under 3 min: silent; 3–10 min: info; 10+ min: actionable alert). |
| **High Concurrency** | Inference crashes under thousands of concurrent train event streams. | **CQRS + In-Memory Caching:** Heavy ML features run asynchronously; UI reads pre-calculated state from Redis. |

---

## Evaluation & Validation Protocol

To guarantee mathematical rigour and prevent model leakage, the training pipeline avoids random train/test splits:

* **Time-Based Validation:** Trained strictly on historical temporal partitions ($T < t_0$) and evaluated on forward windows ($T \ge t_0$) across peak, off-peak, and monsoon seasons.
* **Stratified Delay Buckets:** Models are independently benchmarked across distinct disruption scales:
* Minor Delay ($0 - 5 \text{ min}$)
* Moderate Delay ($5 - 15 \text{ min}$)
* Severe Disruption ($15 - 30 \text{ min}$)
* Extreme Network Disruption ($> 30 \text{ min}$)


* **Target Metrics:** Evaluated against Mean Absolute Error (MAE), Root Mean Squared Error (RMSE), and Percentage Accuracy within $\pm 5$ and $\pm 10$-minute operational bands.

---

## Persona-Driven Interface Layer

```text
                     ┌───────────────────────────┐
                     │   UNIFIED PREDICTION CORE │
                     └─────────────┬─────────────┘
                                   │
      ┌────────────────────────────┼────────────────────────────┐
      ▼                            ▼                            ▼
┌───────────────┐          ┌───────────────┐          ┌───────────────────┐
│ Passenger App │          │Station Display│          │  Control Dashboard│
└───────┬───────┘          └───────┬───────┘          └─────────┬─────────┘
        │                          │                            │
• Dynamic Arrival Time     • Next 3 Trains            • Section Occupancy
• Uncertainty Band         • Platform Indicators      • Propagation Impact
• Contextual Push Alerts   • Freshness Timestamp      • Conflict Alerts

```

---

## Scalability & Production Rollout Strategy

1. **Phase 1 (Proof-of-Concept / SIH Prototype):**
* High-density simulated corridor (e.g., New Delhi – Kanpur Central).
* Verified against static historical datasets + event injection engine.


2. **Phase 2 (Authorized Pilot Integration):**
* Connect Adapter to authorized RTIS/FOIS streams via secure messaging pipelines.
* Run in shadow evaluation mode alongside existing NTES systems.


3. **Phase 3 (Zonal Corridors):**
* Deploy to 2 dedicated operational railway zones (e.g., Northern & Western Railways) to train zone-specific dispatch priors.


4. **Phase 4 (Pan-India Production):**
* Scale distributed inference horizontally across Kubernetes clusters backed by geo-replicated data caches.



---

## Tech Stack

* **Streaming & Messaging:** Apache Kafka / Redpanda, WebSockets
* **In-Memory Cache & Storage:** Redis, PostgreSQL / TimescaleDB (Spatial Time-Series)
* **ML & Graph Compute:** LightGBM, PyTorch Geometric (GNNs), NetworkX, Scikit-learn
* **Backend Framework:** Python (FastAPI), Go (High-throughput ingestion worker)
* **Frontend Dashboards:** React, TailwindCSS, Mapbox GL / Deck.gl (GIS section overlays)

---

## Quickstart (Local Prototype)

```bash
# Clone repository
git clone https://github.com/organization/sih26028-rail-eta-engine.git
cd sih26028-rail-eta-engine

# Configure environment
cp .env.example .env

# Spin up Redis, Kafka, and Database instances
docker-compose up -d

# Install dependencies
pip install -r requirements.txt

# Run real-time feed simulator (Corridor: NDLS -> CNB)
python -m simulator.feed_generator --speed=1.0 --corridor=NDLS-CNB &

# Start ETA Prediction Service
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

```
