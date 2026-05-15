import pandas as pd
import logging

logger = logging.getLogger(__name__)

def parse_and_clean_data(orderdetails_data, retailorders_data):
    """
    Converts raw dict lists to DataFrames, homogenizes columns.
    Converts createdAt -> date, quantity -> sales.
    """
    df_online = pd.DataFrame(orderdetails_data)
    df_offline = pd.DataFrame(retailorders_data)
    
    # Normalize offline createdAt to date before concat
    if not df_offline.empty and "createdAt" in df_offline.columns:
        df_offline["date"] = df_offline["createdAt"]
        df_offline = df_offline.drop(columns=["createdAt"])

    if not df_online.empty and "createdAt" in df_online.columns:
        df_online["date"] = df_online["createdAt"]
        df_online = df_online.drop(columns=["createdAt"])

    # Combine datasets
    df = pd.concat([df_online, df_offline], ignore_index=True)
    
    if df.empty:
        logger.warning("No data found in both collections.")
        return df

    # Normalize columns
    if "date" in df.columns:
        # Convert to datetime and strip time to just keep date
        df["date"] = pd.to_datetime(df["date"], errors='coerce').dt.normalize()
        
        # Keep only data from January 1, 2025 onwards (including historical baseline)
        df = df[df["date"] >= pd.to_datetime("2025-01-01")]
        
        if df.empty:
            logger.warning("No data found after date filtering.")
            return df
    
    def extract_quantity(row):
        # 1. Check if subUnits (array) exists (Online Orders)
        subunits = row.get("subUnits")
        if isinstance(subunits, list) and len(subunits) > 0:
            return sum(item.get("quantity", 0) for item in subunits)
        
        # 2. Check if quantity field exists directly (Retail Details)
        qty = row.get("quantity")
        if pd.notnull(qty) and isinstance(qty, (int, float)):
            return qty
            
        return 1  # fallback

    df["final_quantity"] = df.apply(extract_quantity, axis=1)

    # Clean up redundant columns
    cols_to_drop = ["quantity", "subUnits"]
    df = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
        
    return df
