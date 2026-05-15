import pandas as pd
import logging

logger = logging.getLogger(__name__)

def aggregate_daily_sales(df):
    """
    Groups data by productId and date, computing total sales per product per day.
    Filters for recent data (>= 2025-01-01) and selects top 50 active products.
    """
    if df.empty:
        return df
        
    # Aggregate data: total sales per product per day
    df_agg = df.groupby(["productId", "date"], as_index=False)["final_quantity"].sum()
    
    # Smart Active Product Filtering
    df_agg["date"] = pd.to_datetime(df_agg["date"])
    recent_cutoff = df_agg["date"].max() - pd.Timedelta(days=60)
    recent_df = df_agg[df_agg["date"] >= recent_cutoff]
    
    active_products = recent_df.groupby("productId")["final_quantity"].sum()
    active_products = active_products[active_products > 5].index
    
    df_agg = df_agg[df_agg["productId"].isin(active_products)]
    
    logger.info(f"Filtered to {len(active_products)} active products (final_quantity > 5 in last 60 days).")
    
    # Sort to ensure chronological order per product
    df_agg = df_agg.sort_values(["productId", "date"]).reset_index(drop=True)
    
    return df_agg
