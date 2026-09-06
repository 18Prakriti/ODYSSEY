# RTIS Machine Learning Engine (Layer 3 — Forecaster)

This module trains an XGBoost Regressor model on historical train telemetry and runs a continuous inference loop to predict dynamic ETAs for active Indian Railways trains.

## Quickstart

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Train the Model
*(Pulls historical telemetry from TimescaleDB or uses synthetic telemetry fallback)*
```bash
python train.py
```
This generates `xgb_eta_model.joblib`.

### 3. Run the Inference Engine
```bash
# Single pass
python predict.py --once

# Continuous stream monitoring
python predict.py
```

### 4. Run Executable Server
```bash
./server.py --port 8000
# or
python server.py --host 0.0.0.0 --port 8000
```
Interactive API docs will be available at `http://localhost:8000/docs`.

### 5. Docker Containerization
```bash
docker build -t rtis-layer3-xgboost .
docker run -p 8000:8000 rtis-layer3-xgboost
```
