import os
import logging
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import List, Dict, Any

# Import local modules
from db.mongo_connection import get_database
from vendor_analysis.queries import (
    get_vendor_purchase_summary,
    get_profit_analysis,
    get_monthly_vendor_trends,
    get_vendor_product_breakdown
)
from processing.live_pipeline import get_live_demand_intelligence, get_live_historical_sales

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

# ── Data Endpoints (Live MongoDB based, March 2026 to Present) ────────────────
@app.get("/data/demand")
def get_demand_data():
    try:
        return get_live_demand_intelligence()
    except Exception as e:
        logger.error(f"Error generating live demand data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/data/historical")
def get_historical_sales():
    try:
        return get_live_historical_sales()
    except Exception as e:
        logger.error(f"Error generating live historical sales: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
# ── Serve Frontend Static Files ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dist_path = os.path.join(BASE_DIR, "dist")
dist_assets_path = os.path.join(dist_path, "assets")
dist_index_path = os.path.join(dist_path, "index.html")

if os.path.exists(dist_path):
    app.mount("/assets", StaticFiles(directory=dist_assets_path), name="assets")

    @app.get("/")
    def read_root():
        return FileResponse(dist_index_path)

    @app.get("/{catchall:path}")
    def read_index(catchall: str):
        # Do not capture API routes to avoid returning HTML for bad API calls
        if catchall.startswith(("data/", "vendors/", "health")):
            raise HTTPException(status_code=404, detail="Not Found")
        return FileResponse(dist_index_path)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
