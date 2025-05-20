from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import gspread
import gspread_dataframe as gd
from google.oauth2.service_account import Credentials

# MongoDB Atlas connection string
connection_string = "mongodb+srv://mongoReadOnly-ofp:Blc7wmwKql6ENjD5@ofp-cluster-mongo.o7qum.mongodb.net/admin"

# Connect to MongoDB Atlas
client = MongoClient(connection_string)

# Choose the database you want to use
db = client["overview"]

# Set date range
start_number_of_days = 7
end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(days=start_number_of_days)

# Helper to convert aggregation results to DataFrame
def agg_to_df(collection, pipeline):
    return pd.DataFrame(list(collection.aggregate(pipeline)))

# 1. Total new signups
signups_pipeline = [
    {"$match": {"verified": True, "created_at": {"$gt": start_date, "$lt": end_date}}},
    {"$addFields": {"day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}}}},
    {"$group": {"_id": "$day", "totalNewAccountSignups": {"$sum": 1}}},
    {"$project": {"date": "$_id", "totalNewAccountSignups": 1, "_id": 0}},
    {"$sort": {"date": 1}},
]
total_new_signups = agg_to_df(db.customers, signups_pipeline)

# 2. Number of complete orders
orders_pipeline = [
    {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
    {"$addFields": {"day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$group": {"_id": "$day", "totalOrders": {"$sum": 1}}},
    {"$project": {"date": "$_id", "totalOrders": 1, "_id": 0}},
    {"$sort": {"date": 1}},
]
number_of_complete_orders = agg_to_df(db.orders_ofps, orders_pipeline)

# 3. Total order revenue
revenue_pipeline = [
    {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
    {"$addFields": {"day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$group": {"_id": "$day", "totalRevenue": {"$sum": "$total"}}},
    {"$project": {"date": "$_id", "totalRevenue": 1, "_id": 0}},
    {"$sort": {"date": 1}},
]
total_order_revenue = agg_to_df(db.orders_ofps, revenue_pipeline)

# 4. totalNewCustomerOrderRevenue
new_customer_order_revenue_pipeline = [
    {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
    {"$addFields": {"purchaseDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$group": {
        "_id": {"customerId": "$customer_id", "purchaseDay": "$purchaseDay"},
        "totalRevenue": {"$sum": "$total"},
        "dayDate": {"$first": "$createdAt"}
    }},
    {"$lookup": {
        "from": "orders_ofps", "localField": "_id.customerId", "foreignField": "customer_id", "as": "allPurchases"
    }},
    {"$addFields": {"firstPurchaseDate": {"$min": "$allPurchases.createdAt"}}},
    {"$match": {"$expr": {"$eq": [{"$dateToString": {"format": "%Y-%m-%d", "date": "$firstPurchaseDate"}}, "$_id.purchaseDay"]}}},
    {"$group": {"_id": "$_id.purchaseDay", "totalRevenueFromNewCustomers": {"$sum": "$totalRevenue"}}},
    {"$project": {"date": "$_id", "totalRevenueFromNewCustomers": 1, "_id": 0}},
    {"$sort": {"date": 1}}
]

total_new_customer_order_revenue = agg_to_df(db.orders_ofps, new_customer_order_revenue_pipeline)


#5 totalPurchasesByNewCustomers
purchases_by_new_customers_pipeline = [
    {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
    {"$addFields": {"purchaseDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$group": {
        "_id": {"customerId": "$customer_id", "purchaseDay": "$purchaseDay"},
        "dayDate": {"$first": "$createdAt"},
        "accountIds": {"$addToSet": "$customer_id"}
    }},
    {"$lookup": {
        "from": "orders_ofps", "localField": "_id.customerId", "foreignField": "customer_id", "as": "allPurchases"
    }},
    {"$addFields": {"firstPurchaseDate": {"$min": "$allPurchases.createdAt"}}},
    {"$match": {"$expr": {"$eq": [{"$dateToString": {"format": "%Y-%m-%d", "date": "$firstPurchaseDate"}}, "$_id.purchaseDay"]}}},
    {"$group": {
        "_id": "$_id.purchaseDay",
        "allAccounts": {"$addToSet": "$accountIds"}
    }},
    {"$project": {
        "date": "$_id",
        "numberOfAccounts": {"$size": {"$reduce": {
            "input": "$allAccounts",
            "initialValue": [],
            "in": {"$setUnion": ["$$value", "$$this"]}
        }}},
        "_id": 0
    }},
    {"$sort": {"date": 1}}
]
total_purchases_by_new_customers = agg_to_df(db.orders_ofps, purchases_by_new_customers_pipeline)

# Set date range
start_number_of_days = 30
end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
start_date = end_date - timedelta(days=start_number_of_days)

#6  Total Retained Customer Order Revenue
retained_pipeline = [
    {"$addFields": {"purchaseDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$match": {"createdAt": {"$gt": start_date, "$lt": end_date}, "status": "completed"}},
    {"$group": {"_id": {"customerId": "$customer_id", "purchaseDay": "$purchaseDay"}, "accountIds": {"$addToSet": "$customer_id"}, "dayDate": {"$first": "$createdAt"}}},
    {"$lookup": {"from": "orders_ofps", "localField": "_id.customerId", "foreignField": "customer_id", "as": "priorPurchases"}},
    {"$addFields": {"retained": {"$filter": {"input": "$priorPurchases", "as": "prior", "cond": {"$and": [{"$lt": ["$$prior.createdAt", "$dayDate"]}, {"$gte": ["$$prior.createdAt", {"$dateSubtract": {"startDate": start_date, "unit": "day", "amount": 30}}]}]}}}}},
    {"$match": {"retained.0": {"$exists": True}}},
    {"$group": {"_id": "$_id.purchaseDay", "allAccounts": {"$addToSet": "$accountIds"}}},
    {"$project": {"date": "$_id", "numberOfAccounts": {"$size": {"$reduce": {"input": "$allAccounts", "initialValue": [], "in": {"$setUnion": ["$$value", "$$this"]}}}}, "_id": 0}},
    {"$sort": {"date": 1}}
]
total_retained_customer_order_revenue = agg_to_df(db.orders_ofps, retained_pipeline)

#7 Total Resurrected Customer Order Revenue
resurrected_pipeline = [
    {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
    {"$addFields": {"purchaseDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$group": {"_id": {"customerId": "$customer_id", "purchaseDay": "$purchaseDay"}, "dayDate": {"$first": "$createdAt"}, "accountIds": {"$addToSet": "$customer_id"}}},
    {"$lookup": {"from": "orders_ofps", "localField": "_id.customerId", "foreignField": "customer_id", "as": "priorPurchases"}},
    {"$addFields": {
        "hasOldPurchases": {"$filter": {"input": "$priorPurchases", "as": "prior", "cond": {"$lt": ["$$prior.createdAt", "$dayDate"]}}},
        "hasRecentPurchases": {"$filter": {"input": "$priorPurchases", "as": "prior", "cond": {"$and": [{"$lt": ["$$prior.createdAt", "$dayDate"]}, {"$gte": ["$$prior.createdAt", {"$dateSubtract": {"startDate": "$dayDate", "unit": "day", "amount": 30}}]}]}}}
    }},
    {"$match": {"hasOldPurchases.0": {"$exists": True}, "hasRecentPurchases.0": {"$exists": False}}},
    {"$group": {"_id": "$_id.purchaseDay", "allAccounts": {"$addToSet": "$accountIds"}}},
    {"$project": {"date": "$_id", "numberOfAccounts": {"$size": {"$reduce": {"input": "$allAccounts", "initialValue": [], "in": {"$setUnion": ["$$value", "$$this"]}}}}, "_id": 0}},
    {"$sort": {"date": 1}}
]
total_resurrected_customer_order_revenue = agg_to_df(db.orders_ofps, resurrected_pipeline)

#8 Total Revenue from Retained and Resurrected Accounts
retained_resurrected_pipeline = [
    {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
    {"$addFields": {"orderDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$group": {"_id": {"customerId": "$customer_id", "orderDay": "$orderDay"}, "firstOrderDate": {"$min": "$createdAt"}, "totalRevenue": {"$sum": "$total"}}},
    {"$lookup": {"from": "orders_ofps", "localField": "_id.customerId", "foreignField": "customer_id", "as": "priorPurchases"}},
    {"$addFields": {
        "retained": {"$filter": {"input": "$priorPurchases", "as": "prior", "cond": {"$and": [{"$lt": ["$$prior.createdAt", "$firstOrderDate"]}, {"$gte": ["$$prior.createdAt", {"$dateSubtract": {"startDate": start_date, "unit": "day", "amount": 30}}]}]}}},
        "resurrected": {"$filter": {"input": "$priorPurchases", "as": "prior", "cond": {"$and": [{"$lt": ["$$prior.createdAt", "$firstOrderDate"]}, {"$lt": ["$$prior.createdAt", {"$dateSubtract": {"startDate": start_date, "unit": "day", "amount": 30}}]}]}}}
    }},
    {"$match": {"$or": [{"retained.0": {"$exists": True}}, {"resurrected.0": {"$exists": True}}]}},
    {"$group": {"_id": "$_id.orderDay", "dailyRevenue": {"$sum": "$totalRevenue"}}},
    {"$project": {"date": "$_id", "dailyRevenue": 1, "_id": 0}},
    {"$sort": {"date": 1}}
]
total_retained_and_resurrected_accounts = agg_to_df(db.orders_ofps, retained_resurrected_pipeline)

#9 Total Activated Accounts
activated_pipeline = [
    {"$match": {"status": "completed", "createdAt": {"$gte": start_date, "$lte": end_date}}},
    {"$group": {"_id": "$customer_id", "firstOrderDate": {"$min": "$createdAt"}}},
    {"$match": {"firstOrderDate": {"$gte": start_date, "$lte": end_date}}},
    {"$group": {"_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$firstOrderDate"}}, "numberOfNewActivatedAccounts": {"$sum": 1}}},
    {"$sort": {"_id": 1}},
    {"$project": {"date": "$_id", "numberOfNewActivatedAccounts": 1, "_id": 0}}
]
total_activated_account = agg_to_df(db.orders_ofps, activated_pipeline)

# 10. New Paid Users (distinct users making first-ever purchase)
new_paid_users_pipeline = [
    {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
    {"$addFields": {"purchaseDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$group": {"_id": {"customerId": "$customer_id", "purchaseDay": "$purchaseDay"}, "dayDate": {"$first": "$createdAt"}}},
    {"$lookup": {"from": "orders_ofps", "localField": "_id.customerId", "foreignField": "customer_id", "as": "allPurchases"}},
    {"$addFields": {"firstPurchaseDate": {"$min": "$allPurchases.createdAt"}}},
    {"$match": {"$expr": {"$eq": [{"$dateToString": {"format": "%Y-%m-%d", "date": "$firstPurchaseDate"}}, "$_id.purchaseDay"]}}},
    {"$group": {"_id": "$_id.purchaseDay", "newPaidUsers": {"$addToSet": "$_id.customerId"}}},
    {"$project": {"date": "$_id", "numberOfNewPaidUsers": {"$size": "$newPaidUsers"}, "_id": 0}},
    {"$sort": {"date": 1}}
]
new_paid_users = agg_to_df(db.orders_ofps, new_paid_users_pipeline)

# 11. Revenue from Retained Customers
retained_revenue_pipeline = [
    {"$addFields": {"purchaseDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$match": {"createdAt": {"$gt": start_date, "$lt": end_date}, "status": "completed"}},
    {"$group": {"_id": {"customerId": "$customer_id", "purchaseDay": "$purchaseDay"}, "dayDate": {"$first": "$createdAt"}, "revenue": {"$sum": "$total"}}},
    {"$lookup": {"from": "orders_ofps", "localField": "_id.customerId", "foreignField": "customer_id", "as": "priorPurchases"}},
    {"$addFields": {"retained": {"$filter": {"input": "$priorPurchases", "as": "prior", "cond": {
        "$and": [
            {"$lt": ["$$prior.createdAt", "$dayDate"]},
            {"$gte": ["$$prior.createdAt", {"$dateSubtract": {"startDate": start_date, "unit": "day", "amount": 30}}]}
        ]}}}}},
    {"$match": {"retained.0": {"$exists": True}}},
    {"$group": {"_id": "$_id.purchaseDay", "retainedCustomerRevenue": {"$sum": "$revenue"}}},
    {"$project": {"date": "$_id", "retainedCustomerRevenue": 1, "_id": 0}},
    {"$sort": {"date": 1}}
]

retained_customer_revenue = agg_to_df(db.orders_ofps, retained_revenue_pipeline)

# 12. Revenue from Resurrected Customers
resurrected_revenue_pipeline = [
    {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
    {"$addFields": {"purchaseDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$group": {"_id": {"customerId": "$customer_id", "purchaseDay": "$purchaseDay"}, "dayDate": {"$first": "$createdAt"}, "revenue": {"$sum": "$total"}}},
    {"$lookup": {"from": "orders_ofps", "localField": "_id.customerId", "foreignField": "customer_id", "as": "priorPurchases"}},
    {"$addFields": {
        "hasOldPurchases": {"$filter": {"input": "$priorPurchases", "as": "prior", "cond": {"$lt": ["$$prior.createdAt", "$dayDate"]}}},
        "hasRecentPurchases": {"$filter": {"input": "$priorPurchases", "as": "prior", "cond": {"$and": [{"$lt": ["$$prior.createdAt", "$dayDate"]}, {"$gte": ["$$prior.createdAt", {"$dateSubtract": {"startDate": "$dayDate", "unit": "day", "amount": 30}}]}]}}}
    }},
    {"$match": {"hasOldPurchases.0": {"$exists": True}, "hasRecentPurchases.0": {"$exists": False}}},
    {"$group": {"_id": "$_id.purchaseDay", "resurrectedCustomerRevenue": {"$sum": "$revenue"}}},
    {"$project": {"date": "$_id", "resurrectedCustomerRevenue": 1, "_id": 0}},
    {"$sort": {"date": 1}}
]
resurrected_customer_revenue = agg_to_df(db.orders_ofps, resurrected_revenue_pipeline)


# 13. Number of Resurrected Customers
resurrected_customers_pipeline = [
    {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
    {"$addFields": {"purchaseDay": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}}}},
    {"$group": {"_id": {"customerId": "$customer_id", "purchaseDay": "$purchaseDay"}, "dayDate": {"$first": "$createdAt"}}},
    {"$lookup": {"from": "orders_ofps", "localField": "_id.customerId", "foreignField": "customer_id", "as": "priorPurchases"}},
    {"$addFields": {
        "hasOldPurchases": {"$filter": {"input": "$priorPurchases", "as": "prior", "cond": {"$lt": ["$$prior.createdAt", "$dayDate"]}}},
        "hasRecentPurchases": {"$filter": {"input": "$priorPurchases", "as": "prior", "cond": {"$and": [{"$lt": ["$$prior.createdAt", "$dayDate"]}, {"$gte": ["$$prior.createdAt", {"$dateSubtract": {"startDate": "$dayDate", "unit": "day", "amount": 30}}]}]}}}
    }},
    {"$match": {"hasOldPurchases.0": {"$exists": True}, "hasRecentPurchases.0": {"$exists": False}}},
    {"$group": {"_id": "$_id.purchaseDay", "resurrectedCustomers": {"$addToSet": "$_id.customerId"}}},
    {"$project": {"date": "$_id", "numberOfResurrectedCustomers": {"$size": "$resurrectedCustomers"}, "_id": 0}},
    {"$sort": {"date": 1}}
]
resurrected_customers = agg_to_df(db.orders_ofps, resurrected_customers_pipeline)

# Merge dataframes
dfs = [total_new_signups, number_of_complete_orders, total_order_revenue,
       total_new_customer_order_revenue, total_purchases_by_new_customers, total_retained_customer_order_revenue,
       total_resurrected_customer_order_revenue, total_retained_and_resurrected_accounts, total_activated_account,
new_paid_users, retained_customer_revenue, resurrected_customer_revenue, resurrected_customers
       ]

df = dfs[0]
# Merge the rest
for d in dfs[1:]:
    df = pd.merge(df, d, on="date", how="outer")


df.rename(columns={
    "numberOfAccounts_x": "newCustomerPurchases",
    "numberOfAccounts_y": "retainedCustomerPurchases",
    "numberOfAccounts": "resurrectedCustomerPurchases",
    "dailyRevenue": "retainedAndResurrectedRevenue"
}, inplace=True)

# Add missing/blank columns
df["activePaidUsers"] = df.get("numberOfNewPaidUsers", 0) + df.get("retainedCustomerPurchases", 0) + df.get("resurrectedCustomerPurchases", 0)
df["existingCustomerRevenue"] = df.get("retainedCustomerRevenue", 0) + df.get("resurrectedCustomerRevenue", 0)

# Fill all NaN with 0
df = df.fillna(0)

# Define the final column order (including blanks added above)
# final_column_order = [
#     "date",
#     "totalNewAccountSignups",
#     "totalOrders",
#     "totalRevenue",
#     "activePaidUsers",
#     "existingCustomerRevenue",
#     "numberOfNewPaidUsers",
#     "newCustomerPurchases",
#     "totalRevenueFromNewCustomers",
#     "retainedCustomerPurchases",
#     "retainedCustomerPurchases",  # same as previous — if needed twice, you can alias
#     "retainedCustomerRevenue",
#     "numberOfResurrectedCustomers",
#     "resurrectedCustomerPurchases",
#     "resurrectedCustomerRevenue",
#     "numberOfNewActivatedAccounts"
# ]


# Select and rename final columns
df = df[[
    "date",
    "totalNewAccountSignups",
    "totalOrders",
    "totalRevenue",
    "totalRevenueFromNewCustomers",
    "newCustomerPurchases",
    "retainedCustomerPurchases",
    "resurrectedCustomerPurchases",
    "retainedAndResurrectedRevenue",
    "numberOfNewActivatedAccounts"
]].rename(columns={
    "totalNewAccountSignups": "totalNewSignups",
    "totalRevenueFromNewCustomers": "newCustomerRevenue",
    "numberOfNewActivatedAccounts": "totalActivatedAccount"
})
df = df.sort_values("date")

#  TO GOOGLE SHEETS
SERVICE_ACCOUNT_FILE = 'ofp_google_sheets.json'
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("ofp_google_sheets.json", scopes=SCOPES)
gc = gspread.authorize(creds)


spreadsheet = gc.open("MKT KPI - Completed ORDERS")
worksheet = spreadsheet.worksheet("customer_metrics")

# Get next empty row (assuming column A is always filled for each row)
next_row = len(worksheet.get_all_values()) + 1
gd.set_with_dataframe(worksheet, df, row=next_row, include_column_header=False)
