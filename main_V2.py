from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd

# MongoDB setup
connection_string = "mongodb+srv://mongoReadOnly-ofp:Blc7wmwKql6ENjD5@ofp-cluster-mongo.o7qum.mongodb.net/admin"
client = MongoClient(connection_string)
db = client["overview"]
tracking_db = client["tracking"]

# Date setup
start_days = 7
end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(days=start_days)

# Utility to run aggregation and return DataFrame
def run_query(collection, pipeline):
    result = list(collection.aggregate(pipeline))
    df = pd.DataFrame(result)
    if df.empty:
        df = pd.DataFrame({"date": pd.date_range(start=start_date, end=end_date - timedelta(days=1)).strftime('%Y-%m-%d')})
        metric_name = pipeline[-2]['$group'].get('_id', '') if '$group' in pipeline[-2] else 'metric'
        if isinstance(metric_name, dict):
            for key in metric_name.keys():
                if key != 'customer_id' and key != 'date':
                    df[key] = 0
        else:
            df[metric_name] = 0
    return df

# Metric functions

def new_signups():
    pipeline = [
        {"$match": {"verified": True, "created_at": {"$gte": start_date, "$lte": end_date}}},
        {"$addFields": {"day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}}}},
        {"$group": {"_id": "$day", "newSignupsInDashBoard": {"$sum": 1}}},
        {"$project": {"date": "$_id", "newSignupsInDashBoard": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.customers, pipeline)

def total_orders():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$addFields": {"day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
        {"$group": {"_id": "$day", "totalOrders": {"$sum": 1}}},
        {"$project": {"date": "$_id", "totalOrders": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def overal_revenue():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$addFields": {"day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
        {"$group": {"_id": "$day", "overalRevenue": {"$sum": "$total"}}},
        {"$project": {"date": "$_id", "overalRevenue": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def number_of_new_activated_accounts():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "plan": {"$ne": None}}},
        {"$addFields": {"day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
        {"$group": {"_id": "$day", "numberOfNewActivatedAccounts": {"$sum": 1}}},
        {"$project": {"date": "$_id", "numberOfNewActivatedAccounts": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def number_of_active_paid_users():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$group": {"_id": {"customer_id": "$customer_id", "day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}}},
        {"$group": {"_id": "$_id.day", "numberOfActivePaidUsers": {"$sum": 1}}},
        {"$project": {"date": "$_id", "numberOfActivePaidUsers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def revenue_from_existing_customers():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$lookup": {
            "from": "customers",
            "localField": "customer_id",
            "foreignField": "_id",
            "as": "customer"
        }},
        {"$unwind": "$customer"},
        {"$match": {"customer.created_at": {"$lt": start_date}}},
        {"$addFields": {"day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
        {"$group": {"_id": "$day", "revenueFromExistingCustomers": {"$sum": "$total"}}},
        {"$project": {"date": "$_id", "revenueFromExistingCustomers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def number_of_new_paid_users():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$addFields": {"orderDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
        {"$group": {
            "_id": {"customer_id": "$customer_id", "orderDay": "$orderDay"},
            "orderDate": {"$min": "$createdAt"}
        }},
        {"$lookup": {
            "from": "orders_ofps",
            "let": {"customerId": "$_id.customer_id", "currentDate": "$orderDate"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$customer_id", "$$customerId"]},
                    {"$lt": ["$createdAt", "$$currentDate"]}
                ]}}},
                {"$limit": 1}
            ],
            "as": "previousOrder"
        }},
        {"$match": {"previousOrder": {"$size": 0}}},
        {"$group": {"_id": "$_id.orderDay", "numberOfNewPaidUsers": {"$sum": 1}}},
        {"$project": {"date": "$_id", "numberOfNewPaidUsers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def number_of_purchases_by_new_customers():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$addFields": {"orderDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
        {"$group": {
            "_id": {"customer_id": "$customer_id", "orderDay": "$orderDay"},
            "orderDate": {"$min": "$createdAt"}
        }},
        {"$lookup": {
            "from": "orders_ofps",
            "let": {"customerId": "$_id.customer_id", "currentDate": "$orderDate"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$customer_id", "$$customerId"]},
                    {"$lt": ["$createdAt", "$$currentDate"]}
                ]}}},
                {"$limit": 1}
            ],
            "as": "previousOrder"
        }},
        {"$match": {"previousOrder": {"$size": 0}}},
        {"$group": {"_id": "$_id.orderDay", "numberOfPurchasesByNewCustomers": {"$sum": 1}}},
        {"$project": {"date": "$_id", "numberOfPurchasesByNewCustomers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def revenue_from_new_customers():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$lookup": {
            "from": "customers",
            "localField": "customer_id",
            "foreignField": "_id",
            "as": "customer"
        }},
        {"$unwind": "$customer"},
        {"$match": {"customer.created_at": {"$gte": start_date, "$lt": end_date}}},
        {"$addFields": {"purchaseDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
        {"$group": {"_id": "$purchaseDay", "revenueFromNewCustomers": {"$sum": "$total"}}},
        {"$project": {"date": "$_id", "revenueFromNewCustomers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def number_of_retained_customers():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$group": {
            "_id": {"customer_id": "$customer_id", "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}},
            "orderDate": {"$min": "$createdAt"}
        }},
        {"$lookup": {
            "from": "orders_ofps",
            "let": {"customerId": "$_id.customer_id", "orderDate": "$orderDate"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$customer_id", "$$customerId"]},
                    {"$lt": ["$createdAt", "$$orderDate"]},
                    {"$gte": ["$createdAt", {"$subtract": ["$$orderDate", 60 * 24 * 60 * 60000]}]}
                ]}}}
            ],
            "as": "priorOrders"
        }},
        {"$match": {"priorOrders.0": {"$exists": True}}},
        {"$group": {"_id": "$_id.date", "numberOfRetainedCustomers": {"$sum": 1}}},
        {"$project": {"date": "$_id", "numberOfRetainedCustomers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def number_of_orders_by_retained_customers():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$group": {
            "_id": {"customer_id": "$customer_id", "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}},
            "orderDate": {"$min": "$createdAt"},
            "orders": {"$push": "$total"}
        }},
        {"$lookup": {
            "from": "orders_ofps",
            "let": {"customerId": "$_id.customer_id", "orderDate": "$orderDate"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$customer_id", "$$customerId"]},
                    {"$lt": ["$createdAt", "$$orderDate"]},
                    {"$gte": ["$createdAt", {"$subtract": ["$$orderDate", 60 * 24 * 60 * 60000]}]}
                ]}}}
            ],
            "as": "priorOrders"
        }},
        {"$match": {"priorOrders.0": {"$exists": True}}},
        {"$group": {"_id": "$_id.date", "numberOfOrdersByRetainedCustomers": {"$sum": {"$size": "$orders"}}}},
        {"$project": {"date": "$_id", "numberOfOrdersByRetainedCustomers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def revenue_from_retained_customers():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$group": {
            "_id": {"customer_id": "$customer_id", "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}},
            "orderDate": {"$min": "$createdAt"},
            "revenue": {"$sum": "$total"}
        }},
        {"$lookup": {
            "from": "orders_ofps",
            "let": {"customerId": "$_id.customer_id", "orderDate": "$orderDate"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$customer_id", "$$customerId"]},
                    {"$lt": ["$createdAt", "$$orderDate"]},
                    {"$gte": ["$createdAt", {"$subtract": ["$$orderDate", 60 * 24 * 60 * 60000]}]}
                ]}}}
            ],
            "as": "priorOrders"
        }},
        {"$match": {"priorOrders.0": {"$exists": True}}},
        {"$group": {"_id": "$_id.date", "revenueFromRetainedCustomers": {"$sum": "$revenue"}}},
        {"$project": {"date": "$_id", "revenueFromRetainedCustomers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def number_of_resurrected_customers():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$group": {
            "_id": {"customer_id": "$customer_id", "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}},
            "orderDate": {"$min": "$createdAt"}
        }},
        {"$lookup": {
            "from": "orders_ofps",
            "let": {"customerId": "$_id.customer_id", "orderDate": "$orderDate"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$customer_id", "$$customerId"]},
                    {"$lt": ["$createdAt", {"$subtract": ["$$orderDate", 60 * 24 * 60 * 60000]}]}
                ]}}}
            ],
            "as": "priorOrders"
        }},
        {"$match": {"priorOrders.0": {"$exists": True}}},
        {"$group": {"_id": "$_id.date", "numberOfResurrectedCustomers": {"$sum": 1}}},
        {"$project": {"date": "$_id", "numberOfResurrectedCustomers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def number_of_orders_by_resurrected_customers():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$group": {
            "_id": {"customer_id": "$customer_id", "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}},
            "orderDate": {"$min": "$createdAt"},
            "orders": {"$push": "$total"}
        }},
        {"$lookup": {
            "from": "orders_ofps",
            "let": {"customerId": "$_id.customer_id", "orderDate": "$orderDate"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$customer_id", "$$customerId"]},
                    {"$lt": ["$createdAt", {"$subtract": ["$$orderDate", 60 * 24 * 60 * 60000]}]}
                ]}}}
            ],
            "as": "priorOrders"
        }},
        {"$match": {"priorOrders.0": {"$exists": True}}},
        {"$group": {"_id": "$_id.date", "numberOfOrdersByResurrectedCustomers": {"$sum": {"$size": "$orders"}}}},
        {"$project": {"date": "$_id", "numberOfOrdersByResurrectedCustomers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

def revenue_from_resurrected_customers():
    pipeline = [
        {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
        {"$group": {
            "_id": {"customer_id": "$customer_id", "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}},
            "orderDate": {"$min": "$createdAt"},
            "revenue": {"$sum": "$total"}
        }},
        {"$lookup": {
            "from": "orders_ofps",
            "let": {"customerId": "$_id.customer_id", "orderDate": "$orderDate"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$customer_id", "$$customerId"]},
                    {"$lt": ["$createdAt", {"$subtract": ["$$orderDate", 60 * 24 * 60 * 60000]}]}
                ]}}}
            ],
            "as": "priorOrders"
        }},
        {"$match": {"priorOrders.0": {"$exists": True}}},
        {"$group": {"_id": "$_id.date", "revenueFromResurrectedCustomers": {"$sum": "$revenue"}}},
        {"$project": {"date": "$_id", "revenueFromResurrectedCustomers": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]
    return run_query(db.orders_ofps, pipeline)

metric_functions = [
    new_signups,
    total_orders,
    overal_revenue,
    number_of_new_activated_accounts,
    number_of_active_paid_users,
    revenue_from_existing_customers,
    number_of_new_paid_users,
    number_of_purchases_by_new_customers,
    revenue_from_new_customers,
    number_of_retained_customers,
    number_of_orders_by_retained_customers,
    revenue_from_retained_customers,
    number_of_resurrected_customers,
    number_of_orders_by_resurrected_customers,
    revenue_from_resurrected_customers
]

# Build base DataFrame with date range
base_dates = pd.DataFrame({
    "date": pd.date_range(start=start_date, end=end_date - timedelta(days=1)).strftime('%Y-%m-%d')
})

# Merge all metrics
for func in metric_functions:
    df = func()
    base_dates = base_dates.merge(df, on="date", how="left")

# Fill missing with 0
base_dates.fillna(0, inplace=True)

# Save to CSV
base_dates.to_csv("marketing_report.csv", index=False)
print("Marketing report saved as 'marketing_report.csv'")
