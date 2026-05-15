import logging
from db.fetch_data import fetch_order_data, fetch_products
from processing.clean_data import parse_and_clean_data
from processing.aggregate import aggregate_daily_sales
# ML models and unused feature engineering removed
# ML models completely removed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def main():
    import sys
    if sys.stdout.encoding != 'utf-8':
        sys.stdout.reconfigure(encoding='utf-8')
    logger.info("Starting Demand Forecasting Pipeline...")

    # 1. Fetch
    logger.info("--- Step 1: Fetching Data ---")
    orderdetails, retailorders = fetch_order_data()

    # 2. Process
    logger.info("--- Step 2: Processing Data ---")
    dfRaw = parse_and_clean_data(orderdetails, retailorders)
    logger.info(f"Combined Raw DataFrame shape: {dfRaw.shape}")
    
    dfAgg = aggregate_daily_sales(dfRaw)
    logger.info(f"Aggregated DataFrame shape: {dfAgg.shape}")

    # --- HISTORICAL SALES DATASET ---
    import pandas as pd
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
    df_sales.to_csv("historical_sales.csv", index=False)
    logger.info("Generated historical_sales.csv")

    # --- STEP 3: PREPARE ALL PRODUCTS ---
    products_data = fetch_products()
    if not products_data:
        logger.error("No products found in MongoDB!")
        return
        
    all_products_df = pd.DataFrame(products_data)
    all_products_df["_id"] = all_products_df["_id"].astype(str)
    all_products_df.rename(columns={"_id": "productId"}, inplace=True)
    
    # --- STEP 4: COMPUTE FEATURES ON ALL PRODUCTS WITH SALES ---
    today = df_sales["date"].max()
    df_sales = df_sales.sort_values(["productId", "date"])
    
    # Calculate rolling averages and sums on the sales data
    df_sales["avg_7"] = df_sales.groupby("productId")["final_quantity"].transform(
        lambda x: x.rolling(7, min_periods=1).mean()
    )
    df_sales["recent_sales"] = df_sales.groupby("productId")["final_quantity"].transform(
        lambda x: x.rolling(7, min_periods=1).sum()
    )
    
    # Get the latest sales record for each product that has sales
    latest_sales = df_sales.groupby("productId").tail(1).copy()
    
    # --- STEP 5: MERGE ALL PRODUCTS WITH LATEST SALES STATS ---
    df_final = all_products_df.merge(
        latest_sales[["productId", "date", "final_quantity", "avg_7", "recent_sales"]],
        on="productId",
        how="left"
    )
    
    # Fill values for products with no sales
    df_final["recent_sales"] = df_final["recent_sales"].fillna(0)
    df_final["avg_7"] = df_final["avg_7"].fillna(0)
    df_final["final_quantity"] = df_final["final_quantity"].fillna(0)
    
    # --- STEP 6: DEMAND CLASSIFICATION ---
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

    # --- STEP 7: WILL_SELL LOGIC ---
    import glob
    pred_files = glob.glob("daily_predictions_*.csv")
    if pred_files:
        latest_pred_file = max(pred_files)
        df_preds = pd.read_csv(latest_pred_file)
        df_final = df_final.merge(df_preds[["productId", "predicted_sales"]], on="productId", how="left")
        df_final["predicted_sales"] = df_final["predicted_sales"].fillna(0)
        df_final["will_sell"] = (df_final["predicted_sales"] > 0).astype(int)
    else:
        # Fallback: Sold in the last 7 calendar days
        df_final["predicted_sales"] = 0
        df_final["days_since"] = (pd.to_datetime(today) - pd.to_datetime(df_final["date"])).dt.days
        df_final["will_sell"] = (df_final["days_since"] <= 7).astype(int)

    # Activity and Trend Logic
    def activity(row):
        if pd.isna(row["date"]):
            return "INACTIVE"
        days_since = (today - row["date"]).days
        if days_since > 30:
            return "INACTIVE"
        elif row["avg_7"] > 2:
            return "ACTIVE"
        else:
            return "LOW ACTIVITY"

    df_final["activity"] = df_final.apply(activity, axis=1)

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

    # PRIORITY SCORING
    def priority_score(row):
        score = 0
        
        # Demand
        if row["demand_level"] == "HIGH":
            score += 3
        elif row["demand_level"] == "NORMAL":
            score += 2

        # Activity
        if row["activity"] == "ACTIVE":
            score += 2
        elif row["activity"] == "LOW ACTIVITY":
            score += 1

        # Trend
        if row["trend"] == "INCREASING":
            score += 2
        elif row["trend"] == "STABLE":
            score += 1

        # Will sell
        if row["recent_sales"] > 0:
            score += 1
            
        return score

    df_final["priority_score"] = df_final.apply(priority_score, axis=1)
    
    # SORTING AND TOP PRODUCTS
    df_final = df_final.sort_values(by="priority_score", ascending=False)
    
    threshold = df_final['priority_score'].quantile(0.75)
    priority_products = df_final[df_final['priority_score'] >= threshold]
    
    top_products = df_final.head(10)

    # SAVE OUTPUTS
    df_final.to_csv("latest_demand_intelligence.csv", index=False)

    # MAINTAIN HISTORY
    import os
    from datetime import datetime
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    df_final["run_date"] = today_str
    
    history_file = "demand_history.csv"
    if os.path.exists(history_file):
        existing = pd.read_csv(history_file)
        combined = pd.concat([existing, df_final], ignore_index=True)
    else:
        combined = df_final.copy()
        
    combined.to_csv(history_file, index=False)

    # CONSOLE OUTPUT
    print("\n--- TOP DEMAND PRODUCTS ---")
    print(top_products[[
        "productName",
        "priority_score",
        "demand_level",
        "activity",
        "trend"
    ]])

    print("\n--- SCORE DISTRIBUTION ---")
    print(df_final['priority_score'].value_counts().sort_index(ascending=False))

    print("\n--- BUSINESS SUMMARY ---")
    print(f"Top 25% Priority Products (Score >= {threshold}): {len(priority_products)}")
    print(f"Active Products: {(df_final['activity'] == 'ACTIVE').sum()}")
    print(f"No Demand Products: {(df_final['demand_level'] == 'NO DEMAND').sum()}")
    print("----------------------")

    print("\n--- COCONUT VALIDATION ---")
    target_product = "Coconut (thengai)"
    validation_df = df_sales[df_sales["productName"] == target_product]
    if not validation_df.empty:
        total_qty = validation_df["final_quantity"].sum()
        print(f"Total packets sold for {target_product}: {total_qty}")
    else:
        print("Product not found.")
    print("------------------------------")
    
    logger.info("Pipeline Execution Completed Successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[ERROR] Pipeline failed: {e}")
