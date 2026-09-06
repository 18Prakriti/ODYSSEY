#!/usr/bin/env python3
"""
RTIS Machine Learning Engine — Executable Inference Server
─────────────────────────────────────────────────────────
Production FastAPI server hosting the XGBoost Dynamic ETA model.
Seamlessly integrates with Layer 4 (API Gateway) and Layer 5 (Presentation Dashboard).

Usage:
    ./src/server.py
    or
    python src/server.py --port 8000
    or
    uvicorn src.server:app --host 0.0.0.0 --port 8000
"""

import argparse
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

# Ensure project root is in sys.path
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.predict import (
    load_model_bundle,
    predict_single,
    fetch_active_telemetry_from_db,
    generate_mock_active_fleet,
)
from src.train import train_model, MODEL_PATH


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Ensures model bundle is loaded or trained immediately upon server boot."""
    try:
        bundle = load_model_bundle()
        print(f"✓ RTIS ML Server: XGBoost model bundle loaded successfully (Trained: {bundle.get('trained_at')})")
    except Exception as e:
        print(f"Notice: Loading failed ({e}), initiating fresh model training...")
        train_model()
        load_model_bundle()
    yield


app = FastAPI(
    title="Indian Railways RTIS Machine Learning Engine",
    description="Layer 3 ML Engine: Real-time dynamic train ETA forecasting using XGBoost and spatial-temporal telemetry.",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Enable CORS for Layer 5 frontend dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Pydantic Request / Response Models ─────────────────────────

class TelemetryPoint(BaseModel):
    section_id: Optional[int] = Field(0, description="Track section identifier")
    current_speed_kmph: float = Field(..., description="Current train speed in km/h")
    current_delay_minutes: float = Field(..., description="Observed delay in minutes")
    signal_aspect_code: Optional[int] = Field(0, description="0: Green, 1: Yellow, 2: Red")
    signal_aspect: Optional[str] = Field(None, description="GREEN, YELLOW, RED")
    weather_severity: Optional[int] = Field(0, description="Weather severity index (0-5)")
    scheduled_section_time_min: Optional[float] = Field(45.0, description="Scheduled time for section in minutes")
    latitude: Optional[float] = Field(None, description="GPS Latitude")
    longitude: Optional[float] = Field(None, description="GPS Longitude")


class ETAPredictionRequest(BaseModel):
    train_id: str = Field(..., json_schema_extra={"example": "12951"})
    route_id: str = Field("DEL-BCT", json_schema_extra={"example": "DEL-BCT"})
    telemetry_sequence: List[TelemetryPoint]


class ETAPredictionResponse(BaseModel):
    train_id: str
    predicted_delay_minutes: float
    adjusted_eta_status: str
    estimated_arrival_iso: Optional[str] = None
    model_type: str = "XGBoost Regressor"


class DirectTelemetryInput(BaseModel):
    train_id: str = "12951"
    route_id: str = "DEL-BCT"
    current_speed_kmph: float = 90.0
    current_delay_minutes: float = 5.0
    signal_aspect: str = "GREEN"
    latitude: float = 28.6139
    longitude: float = 77.2090
    timestamp: Optional[str] = None


# ── REST API Endpoints ─────────────────────────────────────────

@app.get("/", tags=["System"])
def root():
    return {
        "service": "RTIS Dynamic ETA Machine Learning Engine",
        "model": "XGBoost Regressor",
        "version": "2.0.0",
        "documentation": "/docs",
        "endpoints": {
            "health": "/health",
            "predict_eta": "POST /predict-eta (Layer 4 compatible)",
            "predict_telemetry": "POST /predict-telemetry",
            "active_etas": "GET /api/v1/active-etas",
            "retrain": "POST /train",
        },
    }


@app.get("/health", tags=["System"])
def health_check():
    """Health check validating ML model readiness and operational status."""
    is_loaded = os.path.exists(MODEL_PATH)
    metrics = {}
    trained_at = None
    if is_loaded:
        try:
            bundle = load_model_bundle()
            metrics = bundle.get("metrics", {})
            trained_at = bundle.get("trained_at")
        except Exception:
            pass

    return {
        "status": "healthy",
        "service": "rtis_ml_engine",
        "engine": "XGBoost",
        "model_loaded": is_loaded,
        "model_metrics": metrics,
        "trained_at": trained_at,
    }


@app.post("/predict-eta", response_model=ETAPredictionResponse, tags=["Inference"])
def predict_eta(payload: ETAPredictionRequest):
    """
    Primary inference endpoint consumed by Layer 4 API Gateway.
    Consumes telemetry sequence, processes latest state, and forecasts delay.
    """
    if not payload.telemetry_sequence:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Empty telemetry_sequence provided.",
        )

    # Use the most recent telemetry point in sequence
    latest_pt = payload.telemetry_sequence[-1]
    rec = {
        "train_id": payload.train_id,
        "route_id": payload.route_id,
        "current_speed_kmph": latest_pt.current_speed_kmph,
        "current_delay_minutes": latest_pt.current_delay_minutes,
        "signal_aspect_code": latest_pt.signal_aspect_code if latest_pt.signal_aspect_code is not None else 0,
        "signal_aspect": latest_pt.signal_aspect or ("GREEN" if latest_pt.signal_aspect_code == 0 else "YELLOW"),
        "scheduled_section_time_min": latest_pt.scheduled_section_time_min or 45.0,
        "latitude": latest_pt.latitude if latest_pt.latitude is not None else 28.6139,
        "longitude": latest_pt.longitude if latest_pt.longitude is not None else 77.2090,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        pred = predict_single(rec)
        return ETAPredictionResponse(
            train_id=pred["train_id"],
            predicted_delay_minutes=pred["predicted_delay_minutes"],
            adjusted_eta_status=pred["adjusted_eta_status"],
            estimated_arrival_iso=pred.get("estimated_arrival_iso"),
            model_type=pred.get("model_type", "XGBoost Regressor"),
        )
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Inference error: {str(err)}",
        )


@app.post("/predict-telemetry", tags=["Inference"])
def predict_telemetry(payload: DirectTelemetryInput):
    """Direct single-point telemetry prediction endpoint."""
    try:
        result = predict_single(payload.dict())
        return result
    except Exception as err:
        raise HTTPException(status_code=500, detail=str(err))


@app.get("/api/v1/active-etas", tags=["Active Fleet"])
def get_active_fleet_etas():
    """Returns dynamic ETA forecasts for all active trains on the network."""
    active_records = fetch_active_telemetry_from_db()
    if not active_records:
        active_records = generate_mock_active_fleet()

    results = []
    for rec in active_records:
        try:
            pred = predict_single(rec)
            results.append({
                **pred,
                "current_speed_kmph": rec.get("current_speed_kmph"),
                "signal_aspect": rec.get("signal_aspect"),
                "last_station": rec.get("last_station_passed", "NDLS"),
            })
        except Exception:
            continue

    return {
        "count": len(results),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "trains": results,
    }


@app.post("/train", tags=["Model Lifecycle"])
def trigger_training(background_tasks: BackgroundTasks):
    """Triggers background retraining of the XGBoost model."""
    background_tasks.add_task(train_model)
    return {
        "status": "training_scheduled",
        "message": "XGBoost model retraining has been initiated in background.",
    }


# ── Executable Server Entrypoint ───────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="RTIS Dynamic ETA ML Executable Server")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Host interface to bind (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=int(os.getenv("ML_PORT", "8000")), help="Port number (default: 8000)")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload on code change")
    args = parser.parse_args()

    print("=" * 65)
    print(f"Starting RTIS Machine Learning Server on http://{args.host}:{args.port}")
    print(f"Interactive API Docs: http://{args.host}:{args.port}/docs")
    print("=" * 65)

    uvicorn.run(
        "src.server:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level="info",
    )


if __name__ == "__main__":
    main()
