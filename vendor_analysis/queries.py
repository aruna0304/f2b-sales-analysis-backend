"""
MongoDB Aggregation Pipelines for Vendor-Based Sales Analysis.

Schema confirmed from live database inspection:
  - Collection: warehousepurchases  (NOTE: plural)
  - productDetails is an ARRAY of sub-documents (must $unwind before grouping)
  - GST is stored as gstValue (percentage) per productDetails item
  - castingscreens linked via productId (no direct vendorId)
"""

import logging
from pymongo.database import Database
from datetime import datetime

logger = logging.getLogger(__name__)

# ── Confirmed Collection Names ────────────────────────────────────────────────
COL_WAREHOUSE_PURCHASES = "warehousepurchases"
COL_VENDORS             = "vendors"
COL_CASTINGSCREENS      = "castingscreens"

# ── warehousepurchases top-level fields ───────────────────────────────────────
WP_VENDOR_ID      = "vendorId"
WP_PURCHASE_AMT   = "totalPurchaseCost"    # total for the whole purchase doc
WP_PURCHASE_DATE  = "purchaseDate"         # stored as string "YYYY-MM-DD"
WP_STATUS         = "status"               # e.g. "Completed"
WP_PAYMENT_STATUS = "paymentStatus"

# ── productDetails array sub-document fields ──────────────────────────────────
PD_PRODUCT_ID             = "productDetails.productId"
PD_PRODUCT_NAME           = "productDetails.productName"
PD_BASE_UNIT_QUANTITY     = "productDetails.totalquantity"      # qty received
PD_PURCHASE_COST_PER_UNIT = "productDetails.pricePerUnitWithTax"
PD_PURCHASE_COST          = "productDetails.purchaseCost"       # line-level cost
PD_GST_VALUE              = "productDetails.gstValue"           # GST % (e.g. 5)
PD_UNIT                   = "productDetails.unitValue"

# ── vendors fields ────────────────────────────────────────────────────────────
V_NAME            = "vendorName"
V_CONTACT         = "mobile"
V_LOCATION        = "address"
V_COMPANY         = "vendorName"           # no separate company field

# ── castingscreens fields ─────────────────────────────────────────────────────
CS_PRODUCT_ID     = "productId"
CS_SELLING_PRICE  = "sellingPricePerUnit"
CS_NUM_PACKS      = "numberOfPacks"
CS_PROFIT_PCT     = "profitPercentage"
CS_PROFIT_VAL     = "profitValue"
CS_SELLING_COST_PER_UNIT = "sellingCost"          # total selling value per base unit
CS_COST_PER_UNIT  = "costPerUnit"
CS_PACK_SIZE      = "packSize"
CS_CREATED_ON     = "createdOn"


# ── Pipeline Helpers ──────────────────────────────────────────────────────────

def _get_fy_start_date():
    """Returns April 1st of the current financial year as YYYY-MM-DD string."""
    now = datetime.now()
    year = now.year if now.month >= 4 else now.year - 1
    return f"{year}-04-01"

def _vendor_lookup_on_id():
    """$lookup vendors after a $group where _id = vendorId."""
    return {
        "$lookup": {
            "from": COL_VENDORS,
            "localField": "_id",
            "foreignField": "_id",
            "as": "vendorInfo"
        }
    }

def _vendor_name_project():
    """Project fields to extract vendor name/contact/location safely."""
    return {
        "vendorName":    {"$ifNull": [f"$vendorInfo.{V_NAME}",    "Unknown Vendor"]},
        "contactNumber": {"$ifNull": [f"$vendorInfo.{V_CONTACT}", "N/A"]},
        "location":      {"$ifNull": [f"$vendorInfo.{V_LOCATION}","N/A"]},
    }


# ── Pipeline 1: Vendor Purchase Summary ──────────────────────────────────────

def get_purchases_summary(db: Database):
    """Aggregates vendor totals from the purchases collection."""
    pipeline = [
        {"$match": {
            "purchaseStatus": {"$in": ["Completed", "Partially Fulfilled"]},
            "isDeleted": {"$ne": True},
            "grandTotal": {"$gt": 0}
        }},
        {"$unwind": {"path": "$purchaseDetails", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {
            "cleanedProductId": {
                "$cond": [
                    {"$eq": [{"$type": "$purchaseDetails.farmProductId"}, "object"]},
                    "$purchaseDetails.farmProductId._id",
                    {"$ifNull": ["$purchaseDetails.farmProductId", None]}
                ]
            }
        }},
        {"$group": {
            "_id": "$_id",
            "vendorId": {"$first": "$vendorId"},
            "vendorName": {"$first": "$vendorName"},
            "purchaseDate": {"$first": "$purchaseDate"},
            "grandTotal": {"$first": "$grandTotal"},
            "gstAmount": {"$first": "$gstAmount"},
            "totalQuantity": {"$sum": {"$ifNull": ["$purchaseDetails.quantity", 0]}},
            "uniqueProducts": {"$addToSet": "$cleanedProductId"},
        }},
        {"$addFields": {
            "parsedDate": {
                "$dateFromString": {
                    "dateString": "$purchaseDate",
                    "format": "%d-%m-%Y",
                    "onError": None,
                    "onNull": None
                }
            }
        }},
        {"$addFields": {
            "formattedDate": {
                "$cond": [
                    {"$ne": ["$parsedDate", None]},
                    {"$dateToString": {"format": "%Y-%m-%d", "date": "$parsedDate"}},
                    "$purchaseDate"
                ]
            }
        }},
        {"$group": {
            "_id": "$vendorId",
            "totalQuantity": {"$sum": "$totalQuantity"},
            "totalPurchaseAmt": {"$sum": "$grandTotal"},
            "totalGST": {"$sum": "$gstAmount"},
            "transactionCount": {"$sum": 1},
            "uniqueProducts": {"$push": "$uniqueProducts"},
            "firstDate": {"$min": "$formattedDate"},
            "lastDate": {"$max": "$formattedDate"},
        }},
        {
            "$lookup": {
                "from": COL_VENDORS,
                "localField": "_id",
                "foreignField": "_id",
                "as": "vendorInfo"
            }
        },
        {"$unwind": {"path": "$vendorInfo", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "vendorId": {"$toString": "$_id"},
            "vendorName": {"$ifNull": ["$vendorName", {"$ifNull": [f"$vendorInfo.{V_NAME}", "Unknown Vendor"]}]},
            "contactNumber": {"$ifNull": [f"$vendorInfo.{V_CONTACT}", "N/A"]},
            "location": {"$ifNull": [f"$vendorInfo.{V_LOCATION}", "N/A"]},
            "totalQuantity": 1,
            "totalPurchaseAmt": 1,
            "totalGST": 1,
            "uniqueProductCount": {"$size": {"$reduce": {
                "input": "$uniqueProducts",
                "initialValue": [],
                "in": {"$setUnion": ["$$value", "$$this"]}
            }}},
            "transactionCount": 1,
            "firstDate": 1,
            "lastDate": 1,
        }}
    ]
    try:
        return list(db["purchases"].aggregate(pipeline))
    except Exception as e:
        logger.error(f"Error in purchases summary aggregation: {e}")
        return []

def get_vendor_purchase_summary(db: Database):
    """
    Vendor-wise totals from both warehousepurchases and purchases collections.
    """
    # 1. Warehouse Purchases Summary
    wp_pipeline = [
        {"$match": {
            WP_PURCHASE_AMT: {"$gt": 0},
            "isDeleted": {"$ne": True},
            WP_PURCHASE_DATE: {"$gte": _get_fy_start_date()}
        }},
        {"$unwind": {"path": "$productDetails", "preserveNullAndEmptyArrays": False}},
        {"$match": {"productDetails.isDeleted": {"$ne": True}}},
        {"$addFields": {
            "lineGST": {
                "$multiply": [
                    {"$ifNull": ["$productDetails.purchaseCost", 0]},
                    {"$divide": [{"$ifNull": ["$productDetails.gstValue", 0]}, 100]}
                ]
            },
            "lineCostWithoutGST": {
                "$divide": [
                    {"$ifNull": ["$productDetails.purchaseCost", 0]},
                    {"$add": [1, {"$divide": [{"$ifNull": ["$productDetails.gstValue", 0]}, 100]}]}
                ]
            }
        }},
        {"$group": {
            "_id":              f"${WP_VENDOR_ID}",
            "totalQuantity":    {"$sum": {"$ifNull": [f"${PD_BASE_UNIT_QUANTITY}", 0]}},
            "totalPurchaseAmt": {"$sum": {"$ifNull": [f"${PD_PURCHASE_COST}", 0]}},
            "totalGST":         {"$sum": "$lineGST"},
            "transactionCount": {"$sum": 1},
            "uniqueProducts":   {"$addToSet": f"${PD_PRODUCT_ID}"},
            "firstDate":        {"$min": f"${WP_PURCHASE_DATE}"},
            "lastDate":         {"$max": f"${WP_PURCHASE_DATE}"},
        }},
        {"$addFields": {
            "uniqueProductCount": {"$size": "$uniqueProducts"}
        }},
        _vendor_lookup_on_id(),
        {"$unwind": {"path": "$vendorInfo", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "vendorId":          {"$toString": "$_id"},
            "vendorName":        {"$ifNull": [f"$vendorInfo.{V_NAME}",    "Unknown Vendor"]},
            "contactNumber":     {"$ifNull": [f"$vendorInfo.{V_CONTACT}", "N/A"]},
            "location":          {"$ifNull": [f"$vendorInfo.{V_LOCATION}","N/A"]},
            "totalQuantity":     1,
            "totalPurchaseAmt":  1,
            "totalGST":          1,
            "uniqueProductCount":1,
            "transactionCount":  1,
            "firstDate":         1,
            "lastDate":          1,
        }}
    ]

    wp_results = []
    try:
        wp_results = list(db[COL_WAREHOUSE_PURCHASES].aggregate(wp_pipeline, allowDiskUse=True))
    except Exception as e:
        logger.error(f"Error in warehouse purchases summary: {e}")

    # 2. Purchases Summary
    p_results = get_purchases_summary(db)

    # 3. Merge results
    merged = {}
    for r in wp_results:
        vid = r["vendorId"]
        merged[vid] = r

    for r in p_results:
        vid = r["vendorId"]
        if vid in merged:
            existing = merged[vid]
            existing["totalQuantity"] = float(existing["totalQuantity"] or 0) + float(r["totalQuantity"] or 0)
            existing["totalPurchaseAmt"] = float(existing["totalPurchaseAmt"] or 0) + float(r["totalPurchaseAmt"] or 0)
            existing["totalGST"] = float(existing["totalGST"] or 0) + float(r["totalGST"] or 0)
            existing["transactionCount"] = int(existing["transactionCount"] or 0) + int(r["transactionCount"] or 0)
            existing["uniqueProductCount"] = int(existing["uniqueProductCount"] or 0) + int(r["uniqueProductCount"] or 0)
            
            # Date handling
            if r["firstDate"] and (not existing["firstDate"] or existing["firstDate"] == "N/A" or r["firstDate"] < existing["firstDate"]):
                existing["firstDate"] = r["firstDate"]
            if r["lastDate"] and (not existing["lastDate"] or existing["lastDate"] == "N/A" or r["lastDate"] > existing["lastDate"]):
                existing["lastDate"] = r["lastDate"]
        else:
            merged[vid] = r

    result = list(merged.values())
    result.sort(key=lambda x: x.get("totalPurchaseAmt", 0), reverse=True)
    return result


# ── Pipeline 2: Profit Analysis ───────────────────────────────────────────────

def get_purchases_profit_analysis(db: Database):
    """Profit analysis aggregation from the purchases collection."""
    pipeline = [
        {"$match": {
            "purchaseStatus": {"$in": ["Completed", "Partially Fulfilled"]},
            "isDeleted": {"$ne": True},
            "grandTotal": {"$gt": 0}
        }},
        {"$unwind": {"path": "$purchaseDetails", "preserveNullAndEmptyArrays": False}},
        {"$addFields": {
            "cleanedProductId": {
                "$cond": [
                    {"$eq": [{"$type": "$purchaseDetails.farmProductId"}, "object"]},
                    "$purchaseDetails.farmProductId._id",
                    "$purchaseDetails.farmProductId"
                ]
            }
        }},
        {"$addFields": {
            "prodIdObj": {
                "$cond": [
                    {"$eq": [{"$type": "$cleanedProductId"}, "string"]},
                    {"$toObjectId": "$cleanedProductId"},
                    "$cleanedProductId"
                ]
            }
        }},
        {"$lookup": {
            "from": COL_CASTINGSCREENS,
            "let": {"pid": "$prodIdObj"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": [f"${CS_PRODUCT_ID}", "$$pid"]},
                    "isDeleted": {"$ne": True}
                }},
                {"$sort": {CS_CREATED_ON: -1}},
                {"$limit": 1}
            ],
            "as": "casting"
        }},
        {"$unwind": {"path": "$casting", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "productpricehistory",
            "let": {"pid": "$prodIdObj"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$farmProductId", "$$pid"]}
                }},
                {"$sort": {"recordedOn": -1}},
                {"$limit": 1}
            ],
            "as": "priceHistory"
        }},
        {"$unwind": {"path": "$priceHistory", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "farmproducts",
            "localField": "prodIdObj",
            "foreignField": "_id",
            "as": "farmProduct"
        }},
        {"$unwind": {"path": "$farmProduct", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "inwardentries",
            "let": {"pid": "$prodIdObj"},
            "pipeline": [
                {"$unwind": "$items"},
                {"$match": {
                    "$expr": {
                        "$or": [
                            {"$eq": ["$items.productId", {"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}]},
                            {"$eq": [{"$convert": {"input": "$items.productId", "to": "objectId", "onError": None, "onNull": None}}, "$$pid"]},
                            {"$and": [
                                {"$eq": [{"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}, "6943611b328c87a9656c0a9e"]},
                                {"$eq": ["$items.productId", "611a572055c5cb38895fcc30"]}
                            ]}
                        ]
                    },
                    "items.storeSellingPrice": {"$gt": 0}
                }},
                {"$sort": {"createdOn": -1}},
                {"$limit": 1}
            ],
            "as": "inward"
        }},
        {"$unwind": {"path": "$inward", "preserveNullAndEmptyArrays": True}},
        # ── Fallback price: use actual price if >0, else productpricehistory, inward, or casting cost ──
        {"$addFields": {
            "fallbackUnitPrice": {
                "$cond": [
                    {"$gt": [{"$ifNull": ["$purchaseDetails.price", 0]}, 0]},
                    "$purchaseDetails.price",
                    {"$cond": [
                        {"$gt": [{"$ifNull": ["$priceHistory.buyingPrice", 0]}, 0]},
                        "$priceHistory.buyingPrice",
                        {"$cond": [
                            {"$gt": [{"$ifNull": ["$inward.items.price", 0]}, 0]},
                            "$inward.items.price",
                            {"$cond": [
                                {"$gt": [{"$ifNull": [f"$casting.{CS_COST_PER_UNIT}", 0]}, 0]},
                                f"$casting.{CS_COST_PER_UNIT}",
                                0.0
                            ]}
                        ]}
                    ]}
                ]
            }
        }},
        {"$addFields": {
            "lineCost": {
                "$ifNull": [
                    "$purchaseDetails.totalAmount",
                    {"$multiply": ["$fallbackUnitPrice", {"$ifNull": ["$purchaseDetails.quantity", 0]}]}
                ]
            },
            "profitPct": {
                "$cond": [
                    {"$and": [
                        {"$ne": [{"$ifNull": [f"$casting.{CS_PROFIT_PCT}", None]}, None]},
                        {"$gt": [f"$casting.{CS_PROFIT_PCT}", 0]}
                    ]},
                    f"$casting.{CS_PROFIT_PCT}",
                    {"$cond": [
                        {"$and": [
                            {"$ne": [{"$ifNull": ["$priceHistory.marginPercent", None]}, None]},
                            {"$gt": ["$priceHistory.marginPercent", 0]}
                        ]},
                        "$priceHistory.marginPercent",
                        {"$cond": [
                            {"$and": [
                                {"$ne": [{"$ifNull": ["$priceHistory.sellingPrice", None]}, None]},
                                {"$ne": [{"$ifNull": ["$priceHistory.buyingPrice", None]}, None]},
                                {"$gt": ["$priceHistory.buyingPrice", 0]}
                            ]},
                            {"$multiply": [
                                {"$divide": [
                                    {"$subtract": ["$priceHistory.sellingPrice", "$priceHistory.buyingPrice"]},
                                    "$priceHistory.buyingPrice"
                                ]},
                                100
                            ]},
                            {"$cond": [
                                {"$and": [
                                    {"$ne": [{"$ifNull": ["$inward.items.marginPercent", None]}, None]},
                                    {"$gt": ["$inward.items.marginPercent", 0]}
                                ]},
                                "$inward.items.marginPercent",
                                {"$cond": [
                                    {"$and": [
                                        {"$ne": [{"$ifNull": ["$inward.items.storeSellingPrice", None]}, None]},
                                        {"$ne": [{"$ifNull": ["$inward.items.price", None]}, None]},
                                        {"$gt": ["$inward.items.price", 0]}
                                    ]},
                                    {"$multiply": [
                                        {"$divide": [
                                            {"$subtract": ["$inward.items.storeSellingPrice", "$inward.items.price"]},
                                            "$inward.items.price"
                                        ]},
                                        100
                                    ]},
                                    0.0
                                ]}
                            ]}
                        ]}
                    ]}
                ]
            }
        }},
        {"$addFields": {
            "lineProfit": {
                "$multiply": [
                    "$lineCost",
                    {"$divide": ["$profitPct", 100]}
                ]
            }
        }},
        {"$addFields": {
            "lineRevenue": {"$add": ["$lineCost", "$lineProfit"]}
        }},
        {"$addFields": {
            "isAnomaly": {
                "$cond": [
                    {"$gt": ["$lineRevenue", {"$multiply": ["$lineCost", 10]}]},
                    True,
                    False
                ]
            }
        }},
        {"$group": {
            "_id": "$_id",
            "vendorId": {"$first": "$vendorId"},
            "vendorName": {"$first": "$vendorName"},
            "gstAmount": {"$first": "$gstAmount"},
            "docRevenue": {"$sum": "$lineRevenue"},
            "docCost": {"$sum": "$lineCost"},
            "docProfit": {"$sum": "$lineProfit"},
            "docQuantity": {"$sum": {"$ifNull": ["$purchaseDetails.quantity", 0]}},
            "docAnomaly": {"$sum": {"$cond": ["$isAnomaly", 1, 0]}}
        }},
        {"$group": {
            "_id": "$vendorId",
            "vendorName": {"$first": "$vendorName"},
            "totalRevenue": {"$sum": "$docRevenue"},
            "totalPurchaseAmt": {"$sum": "$docCost"},
            "totalGST": {"$sum": "$gstAmount"},
            "estimatedProfit": {"$sum": "$docProfit"},
            "totalQuantity": {"$sum": "$docQuantity"},
            "transactionCount": {"$sum": 1},
            "anomalyCount": {"$sum": "$docAnomaly"}
        }},
        {
            "$lookup": {
                "from": COL_VENDORS,
                "localField": "_id",
                "foreignField": "_id",
                "as": "vendorInfo"
            }
        },
        {"$unwind": {"path": "$vendorInfo", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "vendorId": {"$toString": "$_id"},
            "vendorName": {"$ifNull": ["$vendorName", {"$ifNull": [f"$vendorInfo.{V_NAME}", "Unknown Vendor"]}]},
            "contactNumber": {"$ifNull": [f"$vendorInfo.{V_CONTACT}", "N/A"]},
            "location": {"$ifNull": [f"$vendorInfo.{V_LOCATION}", "N/A"]},
            "totalRevenue": 1,
            "totalPurchaseAmt": 1,
            "totalGST": 1,
            "estimatedProfit": 1,
            "totalQuantity": 1,
            "transactionCount": 1,
            "anomalyCount": 1,
        }}
    ]
    try:
        return list(db["purchases"].aggregate(pipeline))
    except Exception as e:
        logger.error(f"Error in purchases profit analysis: {e}")
        return []

def get_profit_analysis(db: Database):
    """
    Profit analysis merging warehousepurchases and purchases data.
    """
    # 1. Warehouse Purchases Profit Analysis
    wp_pipeline = [
        {"$match": {
            WP_PURCHASE_AMT: {"$gt": 0},
            "isDeleted": {"$ne": True},
            WP_PURCHASE_DATE: {"$gte": _get_fy_start_date()}
        }},
        {"$unwind": {"path": "$productDetails", "preserveNullAndEmptyArrays": False}},
        {"$match": {"productDetails.isDeleted": {"$ne": True}}},
        {"$lookup": {
            "from": COL_CASTINGSCREENS,
            "let": {"lineProductId": "$productDetails.productId"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": [f"${CS_PRODUCT_ID}", "$$lineProductId"]},
                    "isDeleted": {"$ne": True}
                }},
                {"$sort": {CS_CREATED_ON: -1}},
                {"$limit": 1}
            ],
            "as": "casting"
        }},
        {"$unwind": {"path": "$casting", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "productpricehistory",
            "let": {"lineProductId": "$productDetails.productId"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$farmProductId", "$$lineProductId"]}
                }},
                {"$sort": {"recordedOn": -1}},
                {"$limit": 1}
            ],
            "as": "priceHistory"
        }},
        {"$unwind": {"path": "$priceHistory", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "farmproducts",
            "localField": "productDetails.productId",
            "foreignField": "_id",
            "as": "farmProduct"
        }},
        {"$unwind": {"path": "$farmProduct", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "inwardentries",
            "let": {"pid": "$productDetails.productId"},
            "pipeline": [
                {"$unwind": "$items"},
                {"$match": {
                    "$expr": {
                        "$or": [
                            {"$eq": ["$items.productId", {"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}]},
                            {"$eq": [{"$convert": {"input": "$items.productId", "to": "objectId", "onError": None, "onNull": None}}, "$$pid"]},
                            {"$and": [
                                {"$eq": [{"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}, "6943611b328c87a9656c0a9e"]},
                                {"$eq": ["$items.productId", "611a572055c5cb38895fcc30"]}
                            ]}
                        ]
                    },
                    "items.storeSellingPrice": {"$gt": 0}
                }},
                {"$sort": {"createdOn": -1}},
                {"$limit": 1}
            ],
            "as": "inward"
        }},
        {"$unwind": {"path": "$inward", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {
            "baseUnitQuantity": {"$ifNull": [f"${PD_BASE_UNIT_QUANTITY}", 0]},
            "purchaseCost":     {"$ifNull": [f"${PD_PURCHASE_COST}", 0]},
            "profitPct": {
                "$cond": [
                    {"$and": [
                        {"$ne": [{"$ifNull": [f"$casting.{CS_PROFIT_PCT}", None]}, None]},
                        {"$gt": [f"$casting.{CS_PROFIT_PCT}", 0]}
                    ]},
                    f"$casting.{CS_PROFIT_PCT}",
                    {"$cond": [
                        {"$and": [
                            {"$ne": [{"$ifNull": ["$priceHistory.marginPercent", None]}, None]},
                            {"$gt": ["$priceHistory.marginPercent", 0]}
                        ]},
                        "$priceHistory.marginPercent",
                        {"$cond": [
                            {"$and": [
                                {"$ne": [{"$ifNull": ["$priceHistory.sellingPrice", None]}, None]},
                                {"$ne": [{"$ifNull": ["$priceHistory.buyingPrice", None]}, None]},
                                {"$gt": ["$priceHistory.buyingPrice", 0]}
                            ]},
                            {"$multiply": [
                                {"$divide": [
                                    {"$subtract": ["$priceHistory.sellingPrice", "$priceHistory.buyingPrice"]},
                                    "$priceHistory.buyingPrice"
                                ]},
                                100
                            ]},
                            {"$cond": [
                                {"$and": [
                                    {"$ne": [{"$ifNull": ["$inward.items.marginPercent", None]}, None]},
                                    {"$gt": ["$inward.items.marginPercent", 0]}
                                ]},
                                "$inward.items.marginPercent",
                                {"$cond": [
                                    {"$and": [
                                        {"$ne": [{"$ifNull": ["$inward.items.storeSellingPrice", None]}, None]},
                                        {"$ne": [{"$ifNull": ["$inward.items.price", None]}, None]},
                                        {"$gt": ["$inward.items.price", 0]}
                                    ]},
                                    {"$multiply": [
                                        {"$divide": [
                                            {"$subtract": ["$inward.items.storeSellingPrice", "$inward.items.price"]},
                                            "$inward.items.price"
                                        ]},
                                        100
                                    ]},
                                    0.0
                                ]}
                            ]}
                        ]}
                    ]}
                ]
            },
            "lineGST": {
                "$multiply": [
                    {"$ifNull": [f"${PD_PURCHASE_COST}", 0]},
                    {"$divide": [{"$ifNull": [f"${PD_GST_VALUE}", 0]}, 100]}
                ]
            }
        }},
        {"$addFields": {
            "lineProfit": {
                "$multiply": [
                    "$purchaseCost",
                    {"$divide": ["$profitPct", 100]}
                ]
            }
        }},
        {"$addFields": {
            "lineRevenue": {"$add": ["$purchaseCost", "$lineProfit"]}
        }},
        {"$addFields": {
            "isAnomaly": {
                "$cond": [
                    {"$gt": ["$lineRevenue", {"$multiply": ["$purchaseCost", 10]}]},
                    True,
                    False
                ]
            }
        }},
        {"$group": {
            "_id":              f"${WP_VENDOR_ID}",
            "totalRevenue":     {"$sum": "$lineRevenue"},
            "totalPurchaseAmt": {"$sum": "$purchaseCost"},
            "totalGST":         {"$sum": "$lineGST"},
            "estimatedProfit":  {"$sum": "$lineProfit"},
            "totalQuantity":    {"$sum": "$baseUnitQuantity"},
            "transactionCount": {"$sum": 1},
            "anomalyCount":     {"$sum": {"$cond": ["$isAnomaly", 1, 0]}}
        }},
        _vendor_lookup_on_id(),
        {"$unwind": {"path": "$vendorInfo", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "vendorId":         {"$toString": "$_id"},
            "vendorName":       {"$ifNull": [f"$vendorInfo.{V_NAME}",    "Unknown Vendor"]},
            "contactNumber":    {"$ifNull": [f"$vendorInfo.{V_CONTACT}", "N/A"]},
            "location":         {"$ifNull": [f"$vendorInfo.{V_LOCATION}","N/A"]},
            "totalRevenue":     1,
            "totalPurchaseAmt": 1,
            "totalGST":         1,
            "estimatedProfit":  1,
            "totalQuantity":    1,
            "transactionCount": 1,
            "anomalyCount":     1,
        }}
    ]

    wp_results = []
    try:
        wp_results = list(db[COL_WAREHOUSE_PURCHASES].aggregate(wp_pipeline, allowDiskUse=True))
    except Exception as e:
        logger.error(f"Error in warehouse profit analysis: {e}")

    # 2. Purchases Profit Analysis
    p_results = get_purchases_profit_analysis(db)

    # 3. Merge Results
    merged = {}
    for r in wp_results:
        vid = r["vendorId"]
        merged[vid] = r

    for r in p_results:
        vid = r["vendorId"]
        if vid in merged:
            existing = merged[vid]
            existing["totalRevenue"] = float(existing["totalRevenue"] or 0) + float(r["totalRevenue"] or 0)
            existing["totalPurchaseAmt"] = float(existing["totalPurchaseAmt"] or 0) + float(r["totalPurchaseAmt"] or 0)
            existing["totalGST"] = float(existing["totalGST"] or 0) + float(r["totalGST"] or 0)
            existing["estimatedProfit"] = float(existing["estimatedProfit"] or 0) + float(r["estimatedProfit"] or 0)
            existing["totalQuantity"] = float(existing["totalQuantity"] or 0) + float(r["totalQuantity"] or 0)
            existing["transactionCount"] = int(existing["transactionCount"] or 0) + int(r["transactionCount"] or 0)
            existing["anomalyCount"] = int(existing["anomalyCount"] or 0) + int(r["anomalyCount"] or 0)
        else:
            merged[vid] = r

    # Recalculate profitMarginPct and totalCost
    for vid, existing in merged.items():
        existing["totalCost"] = existing["totalPurchaseAmt"]
        rev = existing["totalRevenue"] or 0
        prof = existing["estimatedProfit"] or 0
        existing["profitMarginPct"] = (prof / rev * 100.0) if rev > 0 else 0.0

    result = list(merged.values())
    result.sort(key=lambda x: x.get("estimatedProfit", 0), reverse=True)
    return result


# ── Pipeline 3: Monthly Vendor Trends ────────────────────────────────────────

def get_purchases_trends(db: Database):
    """Aggregates monthly vendor trends from the purchases collection."""
    pipeline = [
        {"$match": {
            "purchaseStatus": {"$in": ["Completed", "Partially Fulfilled"]},
            "isDeleted": {"$ne": True},
            "grandTotal": {"$gt": 0}
        }},
        {"$unwind": {"path": "$purchaseDetails", "preserveNullAndEmptyArrays": False}},
        {"$addFields": {
            "cleanedProductId": {
                "$cond": [
                    {"$eq": [{"$type": "$purchaseDetails.farmProductId"}, "object"]},
                    "$purchaseDetails.farmProductId._id",
                    "$purchaseDetails.farmProductId"
                ]
            }
        }},
        {"$addFields": {
            "prodIdObj": {
                "$cond": [
                    {"$eq": [{"$type": "$cleanedProductId"}, "string"]},
                    {"$toObjectId": "$cleanedProductId"},
                    "$cleanedProductId"
                ]
            }
        }},
        {"$lookup": {
            "from": COL_CASTINGSCREENS,
            "let": {"pid": "$prodIdObj"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": [f"${CS_PRODUCT_ID}", "$$pid"]},
                    "isDeleted": {"$ne": True}
                }},
                {"$sort": {CS_CREATED_ON: -1}},
                {"$limit": 1}
            ],
            "as": "casting"
        }},
        {"$unwind": {"path": "$casting", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "productpricehistory",
            "let": {"pid": "$prodIdObj"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$farmProductId", "$$pid"]}
                }},
                {"$sort": {"recordedOn": -1}},
                {"$limit": 1}
            ],
            "as": "priceHistory"
        }},
        {"$unwind": {"path": "$priceHistory", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "farmproducts",
            "localField": "prodIdObj",
            "foreignField": "_id",
            "as": "farmProduct"
        }},
        {"$unwind": {"path": "$farmProduct", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "inwardentries",
            "let": {"pid": "$prodIdObj"},
            "pipeline": [
                {"$unwind": "$items"},
                {"$match": {
                    "$expr": {
                        "$or": [
                            {"$eq": ["$items.productId", {"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}]},
                            {"$eq": [{"$convert": {"input": "$items.productId", "to": "objectId", "onError": None, "onNull": None}}, "$$pid"]},
                            {"$and": [
                                {"$eq": [{"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}, "6943611b328c87a9656c0a9e"]},
                                {"$eq": ["$items.productId", "611a572055c5cb38895fcc30"]}
                            ]}
                        ]
                    },
                    "items.storeSellingPrice": {"$gt": 0}
                }},
                {"$sort": {"createdOn": -1}},
                {"$limit": 1}
            ],
            "as": "inward"
        }},
        {"$unwind": {"path": "$inward", "preserveNullAndEmptyArrays": True}},
        # ── Fallback price: use actual price if >0, else productpricehistory, inward, or casting cost ──
        {"$addFields": {
            "fallbackUnitPrice": {
                "$cond": [
                    {"$gt": [{"$ifNull": ["$purchaseDetails.price", 0]}, 0]},
                    "$purchaseDetails.price",
                    {"$cond": [
                        {"$gt": [{"$ifNull": ["$priceHistory.buyingPrice", 0]}, 0]},
                        "$priceHistory.buyingPrice",
                        {"$cond": [
                            {"$gt": [{"$ifNull": ["$inward.items.price", 0]}, 0]},
                            "$inward.items.price",
                            {"$cond": [
                                {"$gt": [{"$ifNull": [f"$casting.{CS_COST_PER_UNIT}", 0]}, 0]},
                                f"$casting.{CS_COST_PER_UNIT}",
                                0.0
                            ]}
                        ]}
                    ]}
                ]
            }
        }},
        {"$addFields": {
            "lineCost": {
                "$ifNull": [
                    "$purchaseDetails.totalAmount",
                    {"$multiply": ["$fallbackUnitPrice", {"$ifNull": ["$purchaseDetails.quantity", 0]}]}
                ]
            },
            "profitPct": {
                "$cond": [
                    {"$and": [
                        {"$ne": [{"$ifNull": [f"$casting.{CS_PROFIT_PCT}", None]}, None]},
                        {"$gt": [f"$casting.{CS_PROFIT_PCT}", 0]}
                    ]},
                    f"$casting.{CS_PROFIT_PCT}",
                    {"$cond": [
                        {"$and": [
                            {"$ne": [{"$ifNull": ["$priceHistory.marginPercent", None]}, None]},
                            {"$gt": ["$priceHistory.marginPercent", 0]}
                        ]},
                        "$priceHistory.marginPercent",
                        {"$cond": [
                            {"$and": [
                                {"$ne": [{"$ifNull": ["$priceHistory.sellingPrice", None]}, None]},
                                {"$ne": [{"$ifNull": ["$priceHistory.buyingPrice", None]}, None]},
                                {"$gt": ["$priceHistory.buyingPrice", 0]}
                            ]},
                            {"$multiply": [
                                {"$divide": [
                                    {"$subtract": ["$priceHistory.sellingPrice", "$priceHistory.buyingPrice"]},
                                    "$priceHistory.buyingPrice"
                                ]},
                                100
                            ]},
                            {"$cond": [
                                {"$and": [
                                    {"$ne": [{"$ifNull": ["$inward.items.marginPercent", None]}, None]},
                                    {"$gt": ["$inward.items.marginPercent", 0]}
                                ]},
                                "$inward.items.marginPercent",
                                {"$cond": [
                                    {"$and": [
                                        {"$ne": [{"$ifNull": ["$inward.items.storeSellingPrice", None]}, None]},
                                        {"$ne": [{"$ifNull": ["$inward.items.price", None]}, None]},
                                        {"$gt": ["$inward.items.price", 0]}
                                    ]},
                                    {"$multiply": [
                                        {"$divide": [
                                            {"$subtract": ["$inward.items.storeSellingPrice", "$inward.items.price"]},
                                            "$inward.items.price"
                                        ]},
                                        100
                                    ]},
                                    0.0
                                ]}
                            ]}
                        ]}
                    ]}
                ]
            }
        }},
        {"$addFields": {
            "lineProfit": {
                "$multiply": [
                    "$lineCost",
                    {"$divide": ["$profitPct", 100]}
                ]
            }
        }},
        {"$addFields": {
            "lineRevenue": {"$add": ["$lineCost", "$lineProfit"]}
        }},
        {"$addFields": {
            "parsedDate": {
                "$dateFromString": {
                    "dateString": "$purchaseDate",
                    "format": "%d-%m-%Y",
                    "onError": None,
                    "onNull": None
                }
            }
        }},
        {"$match": {"parsedDate": {"$ne": None}}},
        {"$addFields": {
            "year": {"$year": "$parsedDate"},
            "month": {"$month": "$parsedDate"},
            "monthStr": {"$dateToString": {"format": "%Y-%m", "date": "$parsedDate"}}
        }},
        {"$group": {
            "_id": "$_id",
            "vendorId": {"$first": "$vendorId"},
            "vendorName": {"$first": "$vendorName"},
            "year": {"$first": "$year"},
            "month": {"$first": "$month"},
            "monthStr": {"$first": "$monthStr"},
            "gstAmount": {"$first": "$gstAmount"},
            "docRevenue": {"$sum": "$lineRevenue"},
            "docCost": {"$sum": "$lineCost"},
            "docProfit": {"$sum": "$lineProfit"},
            "docQuantity": {"$sum": {"$ifNull": ["$purchaseDetails.quantity", 0]}},
            "products": {"$addToSet": "$purchaseDetails.productName"}
        }},
        {"$group": {
            "_id": {
                "vendorId": "$vendorId",
                "year": "$year",
                "month": "$month",
                "monthStr": "$monthStr"
            },
            "totalQuantity": {"$sum": "$docQuantity"},
            "totalPurchaseAmt": {"$sum": "$docCost"},
            "totalGST": {"$sum": "$gstAmount"},
            "estimatedProfit": {"$sum": "$docProfit"},
            "totalRevenue": {"$sum": "$docRevenue"},
            "transactionCount": {"$sum": 1},
            "products": {"$push": "$products"},
            "vendorName": {"$first": "$vendorName"}
        }},
        {
            "$lookup": {
                "from": COL_VENDORS,
                "localField": "_id.vendorId",
                "foreignField": "_id",
                "as": "vendorInfo"
            }
        },
        {"$unwind": {"path": "$vendorInfo", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "vendorId": {"$toString": "$_id.vendorId"},
            "vendorName": {"$ifNull": ["$vendorName", {"$ifNull": [f"$vendorInfo.{V_NAME}", "Unknown Vendor"]}]},
            "year": "$_id.year",
            "month": "$_id.month",
            "monthStr": "$_id.monthStr",
            "totalQuantity": 1,
            "totalPurchaseAmt": 1,
            "totalGST": 1,
            "estimatedProfit": 1,
            "totalRevenue": 1,
            "transactionCount": 1,
            "products": {"$reduce": {
                "input": "$products",
                "initialValue": [],
                "in": {"$setUnion": ["$$value", "$$this"]}
            }}
        }},
        {"$sort": {"year": 1, "month": 1}}
    ]
    try:
        return list(db["purchases"].aggregate(pipeline))
    except Exception as e:
        logger.error(f"Error in purchases trends aggregation: {e}")
        return []

def get_monthly_vendor_trends(db: Database):
    """
    Monthly time-series merging warehousepurchases and purchases trends.
    """
    # 1. Warehouse Purchases Trends
    wp_pipeline = [
        {"$match": {
            WP_PURCHASE_AMT: {"$gt": 0},
            WP_PURCHASE_DATE: {"$exists": True, "$ne": None, "$gte": _get_fy_start_date()},
            "isDeleted": {"$ne": True}
        }},
        {"$unwind": {"path": "$productDetails", "preserveNullAndEmptyArrays": False}},
        {"$match": {"productDetails.isDeleted": {"$ne": True}}},
        {"$lookup": {
            "from": COL_CASTINGSCREENS,
            "let": {"lineProductId": "$productDetails.productId"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": [f"${CS_PRODUCT_ID}", "$$lineProductId"]},
                    "isDeleted": {"$ne": True}
                }},
                {"$sort": {CS_CREATED_ON: -1}},
                {"$limit": 1}
            ],
            "as": "casting"
        }},
        {"$unwind": {"path": "$casting", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "productpricehistory",
            "let": {"lineProductId": "$productDetails.productId"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$farmProductId", "$$lineProductId"]}
                }},
                {"$sort": {"recordedOn": -1}},
                {"$limit": 1}
            ],
            "as": "priceHistory"
        }},
        {"$unwind": {"path": "$priceHistory", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "farmproducts",
            "localField": "productDetails.productId",
            "foreignField": "_id",
            "as": "farmProduct"
        }},
        {"$unwind": {"path": "$farmProduct", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "inwardentries",
            "let": {"pid": "$productDetails.productId"},
            "pipeline": [
                {"$unwind": "$items"},
                {"$match": {
                    "$expr": {
                        "$or": [
                            {"$eq": ["$items.productId", {"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}]},
                            {"$eq": [{"$convert": {"input": "$items.productId", "to": "objectId", "onError": None, "onNull": None}}, "$$pid"]},
                            {"$and": [
                                {"$eq": [{"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}, "6943611b328c87a9656c0a9e"]},
                                {"$eq": ["$items.productId", "611a572055c5cb38895fcc30"]}
                            ]}
                        ]
                    },
                    "items.storeSellingPrice": {"$gt": 0}
                }},
                {"$sort": {"createdOn": -1}},
                {"$limit": 1}
            ],
            "as": "inward"
        }},
        {"$unwind": {"path": "$inward", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {
            "purchaseCost":     {"$ifNull": [f"${PD_PURCHASE_COST}", 0]},
            "profitPct": {
                "$cond": [
                    {"$and": [
                        {"$ne": [{"$ifNull": [f"$casting.{CS_PROFIT_PCT}", None]}, None]},
                        {"$gt": [f"$casting.{CS_PROFIT_PCT}", 0]}
                    ]},
                    f"$casting.{CS_PROFIT_PCT}",
                    {"$cond": [
                        {"$and": [
                            {"$ne": [{"$ifNull": ["$priceHistory.marginPercent", None]}, None]},
                            {"$gt": ["$priceHistory.marginPercent", 0]}
                        ]},
                        "$priceHistory.marginPercent",
                        {"$cond": [
                            {"$and": [
                                {"$ne": [{"$ifNull": ["$priceHistory.sellingPrice", None]}, None]},
                                {"$ne": [{"$ifNull": ["$priceHistory.buyingPrice", None]}, None]},
                                {"$gt": ["$priceHistory.buyingPrice", 0]}
                            ]},
                            {"$multiply": [
                                {"$divide": [
                                    {"$subtract": ["$priceHistory.sellingPrice", "$priceHistory.buyingPrice"]},
                                    "$priceHistory.buyingPrice"
                                ]},
                                100
                            ]},
                            {"$cond": [
                                {"$and": [
                                    {"$ne": [{"$ifNull": ["$inward.items.marginPercent", None]}, None]},
                                    {"$gt": ["$inward.items.marginPercent", 0]}
                                ]},
                                "$inward.items.marginPercent",
                                {"$cond": [
                                    {"$and": [
                                        {"$ne": [{"$ifNull": ["$inward.items.storeSellingPrice", None]}, None]},
                                        {"$ne": [{"$ifNull": ["$inward.items.price", None]}, None]},
                                        {"$gt": ["$inward.items.price", 0]}
                                    ]},
                                    {"$multiply": [
                                        {"$divide": [
                                            {"$subtract": ["$inward.items.storeSellingPrice", "$inward.items.price"]},
                                            "$inward.items.price"
                                        ]},
                                        100
                                    ]},
                                    0.0
                                ]}
                            ]}
                        ]}
                    ]}
                ]
            },
            "lineGST": {
                "$multiply": [
                    {"$ifNull": [f"${PD_PURCHASE_COST}", 0]},
                    {"$divide": [{"$ifNull": [f"${PD_GST_VALUE}", 0]}, 100]}
                ]
            }
        }},
        {"$addFields": {
            "lineProfit": {
                "$multiply": [
                    "$purchaseCost",
                    {"$divide": ["$profitPct", 100]}
                ]
            }
        }},
        {"$addFields": {
            "lineRevenue": {"$add": ["$purchaseCost", "$lineProfit"]}
        }},
        {"$addFields": {
            "parsedDate": {
                "$dateFromString": {
                    "dateString": f"${WP_PURCHASE_DATE}",
                    "onError": None,
                    "onNull": None
                }
            }
        }},
        {"$match": {"parsedDate": {"$ne": None}}},
        {"$addFields": {
            "year":     {"$year":  "$parsedDate"},
            "month":    {"$month": "$parsedDate"},
            "monthStr": {"$dateToString": {"format": "%Y-%m", "date": "$parsedDate"}}
        }},
        {"$group": {
            "_id": {
                "vendorId": f"${WP_VENDOR_ID}",
                "year":     "$year",
                "month":    "$month",
                "monthStr": "$monthStr"
            },
            "totalQuantity":    {"$sum": {"$ifNull": [f"${PD_BASE_UNIT_QUANTITY}", 0]}},
            "totalPurchaseAmt": {"$sum": "$purchaseCost"},
            "totalGST":         {"$sum": "$lineGST"},
            "estimatedProfit":  {"$sum": "$lineProfit"},
            "totalRevenue":     {"$sum": "$lineRevenue"},
            "transactionCount": {"$sum": 1},
            "products":         {"$addToSet": f"${PD_PRODUCT_NAME}"}
        }},
        {"$lookup": {
            "from": COL_VENDORS,
            "localField": "_id.vendorId",
            "foreignField": "_id",
            "as": "vendorInfo"
        }},
        {"$unwind": {"path": "$vendorInfo", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 0,
            "vendorId":         {"$toString": "$_id.vendorId"},
            "vendorName":       {"$ifNull": [f"$vendorInfo.{V_NAME}", "Unknown Vendor"]},
            "year":             "$_id.year",
            "month":            "$_id.month",
            "monthStr":         "$_id.monthStr",
            "totalQuantity":    1,
            "totalPurchaseAmt": 1,
            "totalGST":         1,
            "estimatedProfit":  1,
            "totalRevenue":     1,
            "transactionCount": 1,
            "products":         1,
        }}
    ]

    wp_results = []
    try:
        wp_results = list(db[COL_WAREHOUSE_PURCHASES].aggregate(wp_pipeline, allowDiskUse=True))
    except Exception as e:
        logger.error(f"Error in warehouse trends: {e}")

    # 2. Purchases Trends
    p_results = get_purchases_trends(db)

    # 3. Combine and sort
    results = wp_results + p_results
    results.sort(key=lambda x: (x.get("year", 0), x.get("month", 0)))
    return results


# ── Pipeline 4: Top Vendors ───────────────────────────────────────────────────

def get_top_vendors_by_revenue(db: Database, limit: int = 10):
    results = get_profit_analysis(db)
    return sorted(results, key=lambda x: x.get("totalRevenue", 0), reverse=True)[:limit]


def get_top_vendors_by_profit(db: Database, limit: int = 10):
    results = get_profit_analysis(db)
    return sorted(results, key=lambda x: x.get("estimatedProfit", 0), reverse=True)[:limit]


# ── Pipeline 5: Product Breakdown per Vendor ─────────────────────────────────

def get_purchases_vendor_product_breakdown(db: Database, vendor_id: str):
    """For a specific vendor, details products purchased from purchases collection.
    
    Uses a fallback hierarchy for unit buying price when purchaseDetails.price is 0:
      1. purchaseDetails.price (actual purchase price, if > 0)
      2. productpricehistory.buyingPrice
      3. inwardentries item price
      4. castingscreens costPerUnit
    This ensures Partially Fulfilled purchases (which often have price=0) still
    produce correct totalPurchaseCost values in the vendor product drill-down.
    """
    from bson import ObjectId
    try:
        vid = ObjectId(vendor_id)
    except Exception:
        vid = vendor_id

    pipeline = [
        {"$match": {
            "vendorId": {"$in": [vendor_id, vid]},
            "purchaseStatus": {"$in": ["Completed", "Partially Fulfilled"]},
            "isDeleted": {"$ne": True},
            "grandTotal": {"$gt": 0}
        }},
        {"$unwind": {"path": "$purchaseDetails", "preserveNullAndEmptyArrays": False}},
        {"$addFields": {
            "cleanedProductId": {
                "$cond": [
                    {"$eq": [{"$type": "$purchaseDetails.farmProductId"}, "object"]},
                    "$purchaseDetails.farmProductId._id",
                    "$purchaseDetails.farmProductId"
                ]
            }
        }},
        # ── Convert cleanedProductId to ObjectId for lookups ──
        {"$addFields": {
            "prodIdObj": {
                "$cond": [
                    {"$eq": [{"$type": "$cleanedProductId"}, "string"]},
                    {"$toObjectId": "$cleanedProductId"},
                    "$cleanedProductId"
                ]
            }
        }},
        # ── Lookups performed BEFORE $group so fallback prices are available per line ──
        {"$lookup": {
            "from": COL_CASTINGSCREENS,
            "let": {"pid": "$prodIdObj"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": [f"${CS_PRODUCT_ID}", "$$pid"]},
                    "isDeleted": {"$ne": True}
                }},
                {"$sort": {CS_CREATED_ON: -1}},
                {"$limit": 1}
            ],
            "as": "casting"
        }},
        {"$unwind": {"path": "$casting", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "productpricehistory",
            "let": {"pid": "$prodIdObj"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$farmProductId", "$$pid"]}
                }},
                {"$sort": {"recordedOn": -1}},
                {"$limit": 1}
            ],
            "as": "priceHistory"
        }},
        {"$unwind": {"path": "$priceHistory", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "farmproducts",
            "localField": "prodIdObj",
            "foreignField": "_id",
            "as": "farmProduct"
        }},
        {"$unwind": {"path": "$farmProduct", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "inwardentries",
            "let": {"pid": "$prodIdObj"},
            "pipeline": [
                {"$unwind": "$items"},
                {"$match": {
                    "$expr": {
                        "$or": [
                            {"$eq": ["$items.productId", {"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}]},
                            {"$eq": [{"$convert": {"input": "$items.productId", "to": "objectId", "onError": None, "onNull": None}}, "$$pid"]},
                            {"$and": [
                                {"$eq": [{"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}, "6943611b328c87a9656c0a9e"]},
                                {"$eq": ["$items.productId", "611a572055c5cb38895fcc30"]}
                            ]}
                        ]
                    },
                    "items.storeSellingPrice": {"$gt": 0}
                }},
                {"$sort": {"createdOn": -1}},
                {"$limit": 1}
            ],
            "as": "inward"
        }},
        {"$unwind": {"path": "$inward", "preserveNullAndEmptyArrays": True}},
        # ── Determine the best available unit buying price per line item ──
        {"$addFields": {
            "fallbackUnitPrice": {
                "$cond": [
                    {"$gt": [{"$ifNull": ["$purchaseDetails.price", 0]}, 0]},
                    "$purchaseDetails.price",
                    {"$cond": [
                        {"$gt": [{"$ifNull": ["$priceHistory.buyingPrice", 0]}, 0]},
                        "$priceHistory.buyingPrice",
                        {"$cond": [
                            {"$gt": [{"$ifNull": ["$inward.items.price", 0]}, 0]},
                            "$inward.items.price",
                            {"$cond": [
                                {"$gt": [{"$ifNull": [f"$casting.{CS_COST_PER_UNIT}", 0]}, 0]},
                                f"$casting.{CS_COST_PER_UNIT}",
                                0.0
                            ]}
                        ]}
                    ]}
                ]
            }
        }},
        # ── Compute line cost: use totalAmount if present, else fallback price × qty ──
        {"$addFields": {
            "lineCost": {
                "$ifNull": [
                    "$purchaseDetails.totalAmount",
                    {"$multiply": [
                        "$fallbackUnitPrice",
                        {"$ifNull": ["$purchaseDetails.quantity", 0]}
                    ]}
                ]
            }
        }},
        # ── Group by product, preserving the first joined lookup docs for downstream use ──
        {"$group": {
            "_id":              "$cleanedProductId",
            "productName":      {"$first": "$purchaseDetails.productName"},
            "unitValue":        {"$first": "$purchaseDetails.unitValue"},
            "totalQuantity":    {"$sum": {"$ifNull": ["$purchaseDetails.quantity", 0]}},
            "totalPurchaseCost":{"$sum": "$lineCost"},
            "totalGST":         {"$sum": 0},
            "avgPricePerUnit":  {"$avg": "$fallbackUnitPrice"},
            "transactionCount": {"$sum": 1},
            "casting":          {"$first": "$casting"},
            "priceHistory":     {"$first": "$priceHistory"},
            "farmProduct":      {"$first": "$farmProduct"},
            "inward":           {"$first": "$inward"},
        }},
        {"$addFields": {
            "prodIdObj": {
                "$cond": [
                    {"$eq": [{"$type": "$_id"}, "string"]},
                    {"$toObjectId": "$_id"},
                    "$_id"
                ]
            }
        }},
        {"$addFields": {
            "profitPercentage": {
                "$cond": [
                    {"$and": [
                        {"$ne": [{"$ifNull": [f"$casting.{CS_PROFIT_PCT}", None]}, None]},
                        {"$gt": [f"$casting.{CS_PROFIT_PCT}", 0]}
                    ]},
                    f"$casting.{CS_PROFIT_PCT}",
                    {"$cond": [
                        {"$and": [
                            {"$ne": [{"$ifNull": ["$priceHistory.marginPercent", None]}, None]},
                            {"$gt": ["$priceHistory.marginPercent", 0]}
                        ]},
                        "$priceHistory.marginPercent",
                        {"$cond": [
                            {"$and": [
                                {"$ne": [{"$ifNull": ["$priceHistory.sellingPrice", None]}, None]},
                                {"$ne": [{"$ifNull": ["$priceHistory.buyingPrice", None]}, None]},
                                {"$gt": ["$priceHistory.buyingPrice", 0]}
                            ]},
                            {"$multiply": [
                                {"$divide": [
                                    {"$subtract": ["$priceHistory.sellingPrice", "$priceHistory.buyingPrice"]},
                                    "$priceHistory.buyingPrice"
                                ]},
                                100
                            ]},
                            {"$cond": [
                                {"$and": [
                                    {"$ne": [{"$ifNull": ["$inward.items.marginPercent", None]}, None]},
                                    {"$gt": ["$inward.items.marginPercent", 0]}
                                ]},
                                "$inward.items.marginPercent",
                                {"$cond": [
                                    {"$and": [
                                        {"$ne": [{"$ifNull": ["$inward.items.storeSellingPrice", None]}, None]},
                                        {"$ne": [{"$ifNull": ["$inward.items.price", None]}, None]},
                                        {"$gt": ["$inward.items.price", 0]}
                                    ]},
                                    {"$multiply": [
                                        {"$divide": [
                                            {"$subtract": ["$inward.items.storeSellingPrice", "$inward.items.price"]},
                                            "$inward.items.price"
                                        ]},
                                        100
                                    ]},
                                    0.0
                                ]}
                            ]}
                        ]}
                    ]}
                ]
            },
            "packSize": {"$ifNull": [f"$casting.{CS_PACK_SIZE}", {"$ifNull": ["$priceHistory.packSize", {"$ifNull": ["$farmProduct.unitValue", ""]}]}]}
        }},
        {"$addFields": {
            "estimatedProfit": {
                "$multiply": [
                    "$totalPurchaseCost",
                    {"$divide": ["$profitPercentage", 100]}
                ]
            }
        }},
        {"$addFields": {
            "totalRevenue": {"$add": ["$totalPurchaseCost", "$estimatedProfit"]}
        }},
        {"$addFields": {
            "isAnomaly": {
                "$cond": [
                    {"$gt": ["$totalRevenue", {"$multiply": ["$totalPurchaseCost", 10]}]},
                    True,
                    False
                ]
            }
        }},
        {"$project": {
            "_id": 0,
            "productId": {"$toString": "$_id"},
            "productName": 1,
            "unitValue": 1,
            "totalQuantity": 1,
            "totalPurchaseCost": 1,
            "totalGST": 1,
            "avgPricePerUnit": 1,
            "sellingPricePerUnit": {
                "$cond": [
                    {"$and": [
                        {"$ne": [{"$ifNull": [f"$casting.{CS_SELLING_PRICE}", None]}, None]},
                        {"$gt": [f"$casting.{CS_SELLING_PRICE}", 0]}
                    ]},
                    f"$casting.{CS_SELLING_PRICE}",
                    {"$cond": [
                        {"$and": [
                            {"$ne": [{"$ifNull": ["$priceHistory.sellingPrice", None]}, None]},
                            {"$gt": ["$priceHistory.sellingPrice", 0]}
                        ]},
                        "$priceHistory.sellingPrice",
                        {"$cond": [
                            {"$and": [
                                {"$ne": [{"$ifNull": ["$farmProduct.mrp", None]}, None]},
                                {"$gt": ["$farmProduct.mrp", 0]}
                            ]},
                            "$farmProduct.mrp",
                            {"$cond": [
                                {"$and": [
                                    {"$ne": [{"$ifNull": ["$farmProduct.perUnitRate", None]}, None]},
                                    {"$gt": ["$farmProduct.perUnitRate", 0]}
                                ]},
                                "$farmProduct.perUnitRate",
                                {"$cond": [
                                    {"$and": [
                                        {"$ne": [{"$ifNull": ["$farmProduct.perRate", None]}, None]},
                                        {"$gt": ["$farmProduct.perRate", 0]}
                                    ]},
                                    "$farmProduct.perRate",
                                    {"$cond": [
                                        {"$and": [
                                            {"$ne": [{"$ifNull": ["$inward.items.storeSellingPrice", None]}, None]},
                                            {"$gt": ["$inward.items.storeSellingPrice", 0]}
                                        ]},
                                        "$inward.items.storeSellingPrice",
                                        0.0
                                    ]}
                                ]}
                            ]}
                        ]}
                    ]}
                ]
            },
            "packSize": 1,
            "profitPercentage": 1,
            "totalRevenue": 1,
            "estimatedProfit": 1,
            "transactionCount": 1,
            "isAnomaly": 1
        }}
    ]
    try:
        return list(db["purchases"].aggregate(pipeline))
    except Exception as e:
        logger.error(f"Error in purchases vendor product breakdown: {e}")
        return []

def get_vendor_product_breakdown(db: Database, vendor_id: str):
    """
    For a specific vendor: product-level details from both collections.
    """
    from bson import ObjectId
    try:
        vid = ObjectId(vendor_id)
    except Exception:
        vid = vendor_id

    # 1. Warehouse Purchases breakdown
    wp_pipeline = [
        {"$match": {
            WP_VENDOR_ID: {"$in": [vendor_id, vid]},
            WP_PURCHASE_AMT: {"$gt": 0},
            "isDeleted": {"$ne": True},
            WP_PURCHASE_DATE: {"$gte": _get_fy_start_date()}
        }},
        {"$unwind": {"path": "$productDetails", "preserveNullAndEmptyArrays": False}},
        {"$match": {"productDetails.isDeleted": {"$ne": True}}},
        {"$addFields": {
            "lineGST": {
                "$multiply": [
                    {"$ifNull": ["$productDetails.purchaseCost", 0]},
                    {"$divide": [{"$ifNull": ["$productDetails.gstValue", 0]}, 100]}
                ]
            }
        }},
        {"$group": {
            "_id":              f"${PD_PRODUCT_ID}",
            "productName":      {"$first": f"${PD_PRODUCT_NAME}"},
            "unitValue":        {"$first": f"${PD_UNIT}"},
            "totalQuantity":    {"$sum": {"$ifNull": [f"${PD_BASE_UNIT_QUANTITY}", 0]}},
            "totalPurchaseCost":{"$sum": {"$ifNull": [f"${PD_PURCHASE_COST}", 0]}},
            "totalGST":         {"$sum": "$lineGST"},
            "avgPricePerUnit":  {"$avg": {"$ifNull": [f"${PD_PURCHASE_COST_PER_UNIT}", 0]}},
            "transactionCount": {"$sum": 1},
        }},
        {"$lookup": {
            "from": COL_CASTINGSCREENS,
            "let": {"productId": "$_id"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": [f"${CS_PRODUCT_ID}", "$$productId"]},
                    "isDeleted": {"$ne": True}
                }},
                {"$sort": {CS_CREATED_ON: -1}},
                {"$limit": 1}
            ],
            "as": "casting"
        }},
        {"$unwind": {"path": "$casting", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "productpricehistory",
            "let": {"productId": "$_id"},
            "pipeline": [
                {"$match": {
                    "$expr": {"$eq": ["$farmProductId", "$$productId"]}
                }},
                {"$sort": {"recordedOn": -1}},
                {"$limit": 1}
            ],
            "as": "priceHistory"
        }},
        {"$unwind": {"path": "$priceHistory", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "farmproducts",
            "localField": "_id",
            "foreignField": "_id",
            "as": "farmProduct"
        }},
        {"$unwind": {"path": "$farmProduct", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "inwardentries",
            "let": {"pid": "$_id"},
            "pipeline": [
                {"$unwind": "$items"},
                {"$match": {
                    "$expr": {
                        "$or": [
                            {"$eq": ["$items.productId", {"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}]},
                            {"$eq": [{"$convert": {"input": "$items.productId", "to": "objectId", "onError": None, "onNull": None}}, "$$pid"]},
                            {"$and": [
                                {"$eq": [{"$convert": {"input": "$$pid", "to": "string", "onError": "", "onNull": ""}}, "6943611b328c87a9656c0a9e"]},
                                {"$eq": ["$items.productId", "611a572055c5cb38895fcc30"]}
                            ]}
                        ]
                    },
                    "items.storeSellingPrice": {"$gt": 0}
                }},
                {"$sort": {"createdOn": -1}},
                {"$limit": 1}
            ],
            "as": "inward"
        }},
        {"$unwind": {"path": "$inward", "preserveNullAndEmptyArrays": True}},
        {"$addFields": {
            "profitPercentage": {
                "$cond": [
                    {"$and": [
                        {"$ne": [{"$ifNull": [f"$casting.{CS_PROFIT_PCT}", None]}, None]},
                        {"$gt": [f"$casting.{CS_PROFIT_PCT}", 0]}
                    ]},
                    f"$casting.{CS_PROFIT_PCT}",
                    {"$cond": [
                        {"$and": [
                            {"$ne": [{"$ifNull": ["$priceHistory.marginPercent", None]}, None]},
                            {"$gt": ["$priceHistory.marginPercent", 0]}
                        ]},
                        "$priceHistory.marginPercent",
                        {"$cond": [
                            {"$and": [
                                {"$ne": [{"$ifNull": ["$priceHistory.sellingPrice", None]}, None]},
                                {"$ne": [{"$ifNull": ["$priceHistory.buyingPrice", None]}, None]},
                                {"$gt": ["$priceHistory.buyingPrice", 0]}
                            ]},
                            {"$multiply": [
                                {"$divide": [
                                    {"$subtract": ["$priceHistory.sellingPrice", "$priceHistory.buyingPrice"]},
                                    "$priceHistory.buyingPrice"
                                ]},
                                100
                            ]},
                            {"$cond": [
                                {"$and": [
                                    {"$ne": [{"$ifNull": ["$inward.items.marginPercent", None]}, None]},
                                    {"$gt": ["$inward.items.marginPercent", 0]}
                                ]},
                                "$inward.items.marginPercent",
                                {"$cond": [
                                    {"$and": [
                                        {"$ne": [{"$ifNull": ["$inward.items.storeSellingPrice", None]}, None]},
                                        {"$ne": [{"$ifNull": ["$inward.items.price", None]}, None]},
                                        {"$gt": ["$inward.items.price", 0]}
                                    ]},
                                    {"$multiply": [
                                        {"$divide": [
                                            {"$subtract": ["$inward.items.storeSellingPrice", "$inward.items.price"]},
                                            "$inward.items.price"
                                        ]},
                                        100
                                    ]},
                                    0.0
                                ]}
                            ]}
                        ]}
                    ]}
                ]
            },
            "packSize": {"$ifNull": [f"$casting.{CS_PACK_SIZE}", {"$ifNull": ["$priceHistory.packSize", {"$ifNull": ["$farmProduct.unitValue", ""]}]}]}
        }},
        {"$addFields": {
            "estimatedProfit": {
                "$multiply": [
                    "$totalPurchaseCost",
                    {"$divide": ["$profitPercentage", 100]}
                ]
            }
        }},
        {"$addFields": {
            "totalRevenue": {"$add": ["$totalPurchaseCost", "$estimatedProfit"]}
        }},
        {"$addFields": {
            "isAnomaly": {
                "$cond": [
                    {"$gt": ["$totalRevenue", {"$multiply": ["$totalPurchaseCost", 10]}]},
                    True,
                    False
                ]
            }
        }},
        {"$project": {
            "_id": 0,
            "productId":          {"$toString": "$_id"},
            "productName":        1,
            "unitValue":          1,
            "totalQuantity":      1,
            "totalPurchaseCost":  1,
            "totalGST":           1,
            "avgPricePerUnit":    1,
            "sellingPricePerUnit": {
                "$cond": [
                    {"$and": [
                        {"$ne": [{"$ifNull": [f"$casting.{CS_SELLING_PRICE}", None]}, None]},
                        {"$gt": [f"$casting.{CS_SELLING_PRICE}", 0]}
                    ]},
                    f"$casting.{CS_SELLING_PRICE}",
                    {"$cond": [
                        {"$and": [
                            {"$ne": [{"$ifNull": ["$priceHistory.sellingPrice", None]}, None]},
                            {"$gt": ["$priceHistory.sellingPrice", 0]}
                        ]},
                        "$priceHistory.sellingPrice",
                        {"$cond": [
                            {"$and": [
                                {"$ne": [{"$ifNull": ["$farmProduct.mrp", None]}, None]},
                                {"$gt": ["$farmProduct.mrp", 0]}
                            ]},
                            "$farmProduct.mrp",
                            {"$cond": [
                                {"$and": [
                                    {"$ne": [{"$ifNull": ["$farmProduct.perUnitRate", None]}, None]},
                                    {"$gt": ["$farmProduct.perUnitRate", 0]}
                                ]},
                                "$farmProduct.perUnitRate",
                                {"$cond": [
                                    {"$and": [
                                        {"$ne": [{"$ifNull": ["$farmProduct.perRate", None]}, None]},
                                        {"$gt": ["$farmProduct.perRate", 0]}
                                    ]},
                                    "$farmProduct.perRate",
                                    {"$cond": [
                                        {"$and": [
                                            {"$ne": [{"$ifNull": ["$inward.items.storeSellingPrice", None]}, None]},
                                            {"$gt": ["$inward.items.storeSellingPrice", 0]}
                                        ]},
                                        "$inward.items.storeSellingPrice",
                                        0.0
                                    ]}
                                ]}
                            ]}
                        ]}
                    ]}
                ]
            },
            "packSize":           1,
            "profitPercentage":   1,
            "totalRevenue":       1,
            "estimatedProfit":    1,
            "transactionCount":   1,
            "isAnomaly":          1,
        }}
    ]

    wp_results = []
    try:
        wp_results = list(db[COL_WAREHOUSE_PURCHASES].aggregate(wp_pipeline, allowDiskUse=True))
    except Exception as e:
        logger.error(f"Error in warehouse vendor product breakdown: {e}")

    # 2. Purchases Breakdown
    p_results = get_purchases_vendor_product_breakdown(db, vendor_id)

    # 3. Merge products by productId
    merged = {}
    for r in wp_results:
        pid = r["productId"]
        merged[pid] = r

    for r in p_results:
        pid = r["productId"]
        if pid in merged:
            existing = merged[pid]
            existing["totalQuantity"] = float(existing["totalQuantity"] or 0) + float(r["totalQuantity"] or 0)
            existing["totalPurchaseCost"] = float(existing["totalPurchaseCost"] or 0) + float(r["totalPurchaseCost"] or 0)
            existing["totalGST"] = float(existing["totalGST"] or 0) + float(r["totalGST"] or 0)
            existing["avgPricePerUnit"] = (float(existing["avgPricePerUnit"] or 0) + float(r["avgPricePerUnit"] or 0)) / 2.0
            existing["transactionCount"] = int(existing["transactionCount"] or 0) + int(r["transactionCount"] or 0)
            existing["estimatedProfit"] = float(existing["estimatedProfit"] or 0) + float(r["estimatedProfit"] or 0)
            existing["totalRevenue"] = float(existing["totalRevenue"] or 0) + float(r["totalRevenue"] or 0)
            existing["isAnomaly"] = existing["isAnomaly"] or r["isAnomaly"]
        else:
            merged[pid] = r

    result = list(merged.values())
    result.sort(key=lambda x: x.get("totalPurchaseCost", 0), reverse=True)
    return result


# ── Indexing Recommendations ─────────────────────────────────────────────────

INDEX_RECOMMENDATIONS = """
Run these in MongoDB shell for best performance:

db.warehousepurchases.createIndex({ vendorId: 1 })
db.warehousepurchases.createIndex({ purchaseDate: 1 })
db.warehousepurchases.createIndex({ "productDetails.productId": 1 })
db.warehousepurchases.createIndex({ vendorId: 1, purchaseDate: 1 })
db.warehousepurchases.createIndex({ totalPurchaseCost: 1 })
db.castingscreens.createIndex({ productId: 1 })
db.productpricehistory.createIndex({ farmProductId: 1 })
db.productpricehistory.createIndex({ farmProductId: 1, recordedOn: -1 })
"""
