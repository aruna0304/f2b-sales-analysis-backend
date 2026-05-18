import os
import glob
import logging
from datetime import datetime
import pandas as pd

# Import local data-fetching and cleaning utilities
from db.fetch_data import fetch_order_data, fetch_products
from processing.clean_data import parse_and_clean_data
from processing.aggregate import aggregate_daily_sales

logger = logging.getLogger(__name__)

# Start date for filtering: March 1st, 2026
START_DATE = datetime(2026, 3, 1)

def get_live_historical_sales_df():
    """
    Fetches, cleans, and builds the historical sales DataFrame in-memory,
    filtering all orders from March 1st, 2026 to the present.
    """
    logger.info("Starting live historical sales processing...")
    
    # 1. Fetch from MongoDB starting from March 1st, 2026
    orderdetails, retailorders = fetch_order_data(start_date=START_DATE)
    
    # 2. Clean and structure the raw records
    dfRaw = parse_and_clean_data(orderdetails, retailorders)
    if dfRaw.empty:
        logger.warning("No raw order data found since March 1st, 2026.")
        return pd.DataFrame(columns=["productId", "date", "date_str", "final_quantity", "unit", "productName", "month"])
        
    # --- HISTORICAL SALES DATASET CONSTRUCTION ---
    df_sales = dfRaw.copy()
    if "sales" in df_sales.columns:
        df_sales.rename(columns={"sales": "final_quantity"}, inplace=True)
        
    df_sales["date"] = pd.to_datetime(df_sales["date"])
    df_sales["date_str"] = df_sales["date"].dt.strftime("%d-%m-%Y")
    
    if "unit" not in df_sales.columns:
        df_sales["unit"] = ""
    else:
        df_sales["unit"] = df_sales["unit"].fillna("").astype(str)

    df_sales = df_sales[["productId", "date", "date_str", "final_quantity", "unit"]]
    
    # Merge with product catalog to resolve productName
    products_data = fetch_products()
    if products_data:
        products_df = pd.DataFrame(products_data)
        products_df["_id"] = products_df["_id"].astype(str)
        df_sales["productId"] = df_sales["productId"].astype(str)
        
        df_sales = df_sales.merge(
            products_df[["_id", "productName"]],
            left_on="productId",
            right_on="_id",
            how="left"
        )
        if "_id" in df_sales.columns:
            df_sales.drop(columns=["_id"], inplace=True)
        if "productName" not in df_sales.columns:
            df_sales["productName"] = "UNKNOWN PRODUCT"
        else:
            df_sales["productName"] = df_sales["productName"].fillna("UNKNOWN PRODUCT")
    else:
        df_sales["productName"] = "UNKNOWN PRODUCT"

    df_sales["month"] = df_sales["date"].dt.to_period("M").astype(str)
    
    return df_sales

def get_live_historical_sales():
    """
    Returns the real-time historical sales as a clean JSON-serializable list of dicts.
    """
    df = get_live_historical_sales_df()
    if df.empty:
        return []
        
    # Standardize datetime objects to strings before returning to ensure 100% JSON compatibility
    df_serialized = df.copy()
    df_serialized["date"] = df_serialized["date"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df_serialized.fillna(0).to_dict(orient="records")

def get_live_demand_intelligence():
    """
    Returns the real-time demand intelligence as a clean JSON-serializable list of dicts.
    Runs in-memory aggregation, scoring, and classification.
    """
    logger.info("Starting live demand intelligence processing...")
    
    # 1. Fetch live historical sales
    df_sales = get_live_historical_sales_df()
    
    # 2. Fetch all products from product catalog
    products_data = fetch_products()
    if not products_data:
        logger.error("No products found in MongoDB!")
        return []
        
    all_products_df = pd.DataFrame(products_data)
    all_products_df["_id"] = all_products_df["_id"].astype(str)
    all_products_df.rename(columns={"_id": "productId"}, inplace=True)
    
    if df_sales.empty:
        # If no sales exist in this range, initialize blank sales columns for all products
        df_final = all_products_df.copy()
        df_final["date"] = 0
        df_final["final_quantity"] = 0.0
        df_final["avg_7"] = 0.0
        df_final["recent_sales"] = 0.0
        today = pd.to_datetime(datetime.now())
    else:
        # Sort sales for rolling features calculation
        df_sales = df_sales.sort_values(["productId", "date"])
        today = df_sales["date"].max()
        
        # Calculate 7-day rolling averages and sums on sales data
        df_sales["avg_7"] = df_sales.groupby("productId")["final_quantity"].transform(
            lambda x: x.rolling(7, min_periods=1).mean()
        )
        df_sales["recent_sales"] = df_sales.groupby("productId")["final_quantity"].transform(
            lambda x: x.rolling(7, min_periods=1).sum()
        )
        
        # Extract the latest sales record per product that has sales
        latest_sales = df_sales.groupby("productId").tail(1).copy()
        
        # Merge product catalog with latest sales statistics
        df_final = all_products_df.merge(
            latest_sales[["productId", "date", "final_quantity", "avg_7", "recent_sales"]],
            on="productId",
            how="left"
        )
        
        df_final["recent_sales"] = df_final["recent_sales"].fillna(0)
        df_final["avg_7"] = df_final["avg_7"].fillna(0)
        df_final["final_quantity"] = df_final["final_quantity"].fillna(0)

    # --- DEMAND CLASSIFICATION LOGIC ---
    def classify_demand(row):
        if row["recent_sales"] == 0:
            return "NO DEMAND"
        elif row["recent_sales"] <= 5:
            return "LOW"
        elif row["recent_sales"] <= 20:
            return "NORMAL"
        else:
            return "HIGH"

    df_final["demand_level"] = df_final.apply(classify_demand, axis=1)

    # --- WILL_SELL LOGIC ---
    # Lookup any predictions file generated by ML models in backend folder
    backend_dir = "c:\\Users\\aruna\\f2b-sales-forecasting\\backend"
    pred_files = glob.glob(os.path.join(backend_dir, "daily_predictions_*.csv"))
    if pred_files:
        latest_pred_file = max(pred_files)
        try:
            df_preds = pd.read_csv(latest_pred_file)
            df_final = df_final.merge(df_preds[["productId", "predicted_sales"]], on="productId", how="left")
            df_final["predicted_sales"] = df_final["predicted_sales"].fillna(0)
            df_final["will_sell"] = (df_final["predicted_sales"] > 0).astype(int)
        except Exception as e:
            logger.error(f"Error merging prediction file: {e}")
            df_final["predicted_sales"] = 0
            df_final["will_sell"] = 0
    else:
        # Fallback: Sold in the last 7 calendar days
        df_final["predicted_sales"] = 0
        df_final["days_since"] = (pd.to_datetime(today) - pd.to_datetime(df_final["date"])).dt.days
        df_final["will_sell"] = (df_final["days_since"] <= 7).astype(int)

    # --- ACTIVITY LOGIC ---
    def activity(row):
        if pd.isna(row["date"]) or row["date"] == 0:
            return "INACTIVE"
        days_since = (today - pd.to_datetime(row["date"])).days
        if days_since > 30:
            return "INACTIVE"
        elif row["avg_7"] > 2:
            return "ACTIVE"
        else:
            return "LOW ACTIVITY"

    df_final["activity"] = df_final.apply(activity, axis=1)

    # --- TREND LOGIC ---
    def trend(row):
        if row["avg_7"] == 0:
            return "STABLE"
        if row["final_quantity"] > row["avg_7"] * 1.5:
            return "INCREASING"
        elif row["final_quantity"] < row["avg_7"] * 0.5:
            return "DECREASING"
        else:
            return "STABLE"

    df_final["trend"] = df_final.apply(trend, axis=1)

    # --- PRIORITY SCORING LOGIC ---
    def priority_score(row):
        score = 0
        
        # Demand level points
        if row["demand_level"] == "HIGH":
            score += 3
        elif row["demand_level"] == "NORMAL":
            score += 2

        # Activity points
        if row["activity"] == "ACTIVE":
            score += 2
        elif row["activity"] == "LOW ACTIVITY":
            score += 1

        # Trend points
        if row["trend"] == "INCREASING":
            score += 2
        elif row["trend"] == "STABLE":
            score += 1

        # Recency/Will sell points
        if row["recent_sales"] > 0:
            score += 1
            
        return score

    df_final["priority_score"] = df_final.apply(priority_score, axis=1)
    
    # Sort by priority score descending
    df_final = df_final.sort_values(by="priority_score", ascending=False)
    
    # Format dates to string for clean JSON serialization
    df_final_serialized = df_final.copy()
    # Check if 'days_since' is in columns and fill NaN
    if "days_since" in df_final_serialized.columns:
        df_final_serialized["days_since"] = df_final_serialized["days_since"].fillna(999).astype(int)
    
    # Convert Timestamp objects to string
    def format_date(val):
        if pd.isna(val) or val == 0:
            return ""
        return pd.to_datetime(val).strftime("%Y-%m-%d %H:%M:%S")
        
    df_final_serialized["date"] = df_final_serialized["date"].apply(format_date)
    
    return df_final_serialized.fillna(0).to_dict(orient="records")
