import os
import logging
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any

# Import local modules
from db.mongo_connection import get_database
from vendor_analysis.queries import (
    get_vendor_purchase_summary,
    get_profit_analysis,
    get_monthly_vendor_trends,
    get_vendor_product_breakdown
)

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="F2B Sales Analytics API")

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Health Check ──────────────────────────────────────────────────────────────
@app.get("/health")
def health_check():
    return {"status": "online", "message": "F2B Sales Analytics API is running"}

# ── Data Endpoints (CSV based) ────────────────────────────────────────────────
@app.get("/data/demand")
def get_demand_data():
    try:
        df = pd.read_csv("latest_demand_intelligence.csv")
        return df.fillna(0).to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading demand data: {e}")
        raise HTTPException(status_code=404, detail="Demand data not found")

@app.get("/data/historical")
def get_historical_sales():
    try:
        df = pd.read_csv("historical_sales.csv")
        return df.fillna(0).to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading historical sales: {e}")
        raise HTTPException(status_code=404, detail="Historical sales data not found")

# ── Vendor Analysis Endpoints (MongoDB based) ─────────────────────────────────
@app.get("/vendors/summary")
def get_vendors_summary():
    try:
        db = get_database()
        records = get_vendor_purchase_summary(db)
        df = pd.DataFrame(records).fillna(0)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error fetching vendor summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vendors/profit")
def get_vendors_profit():
    try:
        db = get_database()
        records = get_profit_analysis(db)
        df = pd.DataFrame(records).fillna(0)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error fetching profit analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vendors/trends")
def get_vendors_trends():
    try:
        db = get_database()
        records = get_monthly_vendor_trends(db)
        df = pd.DataFrame(records).fillna(0)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error fetching vendor trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/vendors/{vendor_id}/products")
def get_vendor_products(vendor_id: str):
    try:
        db = get_database()
        records = get_vendor_product_breakdown(db, vendor_id)
        df = pd.DataFrame(records).fillna(0)
        return df.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error fetching product breakdown for {vendor_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
