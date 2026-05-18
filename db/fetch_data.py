from db.mongo_connection import get_database
import logging

logger = logging.getLogger(__name__)

def fetch_order_data(start_date=None):
    """
    Fetches raw order data from the 'orderdetails' and 'retaildetails' collections.
    Strictly performs read-only operations.
    Extracts only specific required fields to minimize memory usage.
    """
    db = get_database()
    
    # 1. Fetch Online Orders (orderdetails)
    # Note: We join with 'orders' to get the creation date
    pipeline_online = [
        {
            "$lookup": {
                "from": "orders",
                "localField": "orderId",
                "foreignField": "_id",
                "as": "order_info"
            }
        },
        {"$unwind": "$order_info"},
    ]
    
    if start_date is not None:
        pipeline_online.append({
            "$match": {
                "$or": [
                    {"order_info.createdOn": {"$gte": start_date}},
                    {"createdAt": {"$gte": start_date}}
                ]
            }
        })
        
    pipeline_online.append({
        "$project": {
            "_id": 0,
            "productId": "$farmProductId",
            "quantity": 1,
            "subUnits": 1,
            "unit": "$unitValue",
            "date": {"$ifNull": ["$order_info.createdOn", "$createdAt"]}
        }
    })
    
    logger.info("Fetching data from 'orderdetails' collection...")
    orderdetails_data = list(db.orderdetails.aggregate(pipeline_online))
    logger.info(f"Fetched {len(orderdetails_data)} records from online orders.")
    
    # 2. Fetch Retail Orders (retaildetails)
    # Field names in retaildetails: farmProductId, quantity, createdOn
    projection_offline = {
        "_id": 0,
        "productId": "$farmProductId",
        "quantity": 1,
        "unit": "$unitValue",
        "date": "$createdOn"
    }
    
    filter_offline = {}
    if start_date is not None:
        filter_offline["createdOn"] = {"$gte": start_date}
    
    logger.info("Fetching data from 'retaildetails' collection...")
    retaildetails_data = list(db.retaildetails.find(filter_offline, projection_offline))
    logger.info(f"Fetched {len(retaildetails_data)} records from offline orders.")
    
    return orderdetails_data, retaildetails_data

def fetch_products():
    """
    Fetches product details from the 'farmproducts' collection.
    """
    db = get_database()
    logger.info("Fetching data from 'farmproducts' collection...")
    products_cursor = db["farmproducts"].find(
        {"isDeleted": False},
        {"_id": 1, "productName": 1}
    )
    return list(products_cursor)
