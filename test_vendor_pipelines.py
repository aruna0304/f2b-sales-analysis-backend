import sys
sys.path.insert(0, '.')
from db.mongo_connection import get_database
from vendor_analysis.queries import (
    get_vendor_purchase_summary,
    get_profit_analysis,
    get_monthly_vendor_trends,
    get_vendor_product_breakdown
)

db = get_database()

print("=== Purchase Summary ===")
summary = get_vendor_purchase_summary(db)
print(f"  {len(summary)} vendors found")
for r in summary[:5]:
    print(f"  {r['vendorName']} | Qty:{r['totalQuantity']} | Cost:Rs.{r['totalPurchaseAmt']:.0f} | GST:Rs.{r['totalGST']:.0f} | SKUs:{r['uniqueProductCount']}")

print()
print("=== Profit Analysis ===")
profit = get_profit_analysis(db)
print(f"  {len(profit)} vendors found")
for r in profit[:5]:
    print(f"  {r['vendorName']} | Revenue:Rs.{r['totalRevenue']:.0f} | Profit:Rs.{r['estimatedProfit']:.0f} | Margin:{r['profitMarginPct']:.1f}%")

print()
print("=== Monthly Trends ===")
trends = get_monthly_vendor_trends(db)
print(f"  {len(trends)} monthly records")
for r in trends[:5]:
    print(f"  {r['vendorName']} | {r['monthStr']} | Qty:{r['totalQuantity']} | Cost:Rs.{r['totalPurchaseAmt']:.0f}")

if summary:
    print()
    print("=== Product Breakdown (first vendor) ===")
    bd = get_vendor_product_breakdown(db, summary[0]['vendorId'])
    print(f"  {len(bd)} products")
    for r in bd[:5]:
        print(f"  {r['productName']} | Qty:{r['totalQuantity']} | Cost:Rs.{r['totalPurchaseCost']:.0f} | SellPrice:Rs.{r['sellingPricePerUnit']:.0f}")
