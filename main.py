from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import gspread
import gspread_dataframe as gd
from google.oauth2.service_account import Credentials


# def ofp_data_to_sheets(event, context):
def ofp_data_to_sheets():
    # MongoDB setup
    connection_string = "mongodb+srv://mongoReadOnly-ofp:Blc7wmwKql6ENjD5@ofp-cluster-mongo.o7qum.mongodb.net/admin"
    client = MongoClient(connection_string)
    db = client["overview"]
    tracking_db = client["tracking"]

    # Date setup
    start_days = 13
    end_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=start_days)

    # Utility to run aggregation and return DataFrame
    def run_query(collection, pipeline):
        result = list(collection.aggregate(pipeline))
        df = pd.DataFrame(result)
        if df.empty:
            df = pd.DataFrame({"date": pd.date_range(start=start_date, end=end_date - timedelta(days=1)).strftime('%Y-%m-%d')})
        return df


    def number_of_active_paid_users():
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
                    {"$sort": {"createdAt": -1}},
                    {"$limit": 1}
                ],
                "as": "previousOrder"
            }},
            {"$addFields": {
                "lastOrderDate": {"$arrayElemAt": ["$previousOrder.createdAt", 0]},
                "isNew": {"$eq": [{"$size": "$previousOrder"}, 0]},
                "isRetained": {"$cond": [{"$and": [
                    {"$gt": [{"$size": "$previousOrder"}, 0]},
                    {"$gte": [
                        {"$arrayElemAt": ["$previousOrder.createdAt", 0]},
                        {"$dateSubtract": {"startDate": "$orderDate", "unit": "day", "amount": 30}}
                    ]}
                ]}, True, False]},
                "isResurrected": {"$cond": [{"$and": [
                    {"$gt": [{"$size": "$previousOrder"}, 0]},
                    {"$lt": [
                        {"$arrayElemAt": ["$previousOrder.createdAt", 0]},
                        {"$dateSubtract": {"startDate": "$orderDate", "unit": "day", "amount": 30}}
                    ]}
                ]}, True, False]}
            }},
            {"$project": {
                "date": "$_id.orderDay",
                "isNew": 1,
                "isRetained": 1,
                "isResurrected": 1
            }},
            {"$group": {
                "_id": "$date",
                "newUsers": {"$sum": {"$cond": ["$isNew", 1, 0]}},
                "retainedUsers": {"$sum": {"$cond": ["$isRetained", 1, 0]}},
                "resurrectedUsers": {"$sum": {"$cond": ["$isResurrected", 1, 0]}}
            }},
            {"$project": {
                "date": "$_id",
                "numberOfActivePaidUsers": {"$add": ["$newUsers", "$retainedUsers", "$resurrectedUsers"]},
                "_id": 0
            }},
            {"$sort": {"date": 1}}
        ]
        return run_query(db.orders_ofps, pipeline)

    def revenue_from_existing_customers():
        pipeline = [
            {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
            {"$lookup": {
                "from": "orders_ofps",
                "let": {"customerId": "$customer_id", "currentOrderDate": "$createdAt"},
                "pipeline": [
                    {"$match": {"$expr": {"$and": [
                        {"$eq": ["$customer_id", "$$customerId"]},
                        {"$lt": ["$createdAt", "$$currentOrderDate"]}
                    ]}}},
                    {"$sort": {"createdAt": -1}},
                    {"$limit": 1}
                ],
                "as": "previousOrder"
            }},
            {"$addFields": {"lastOrderDate": {"$arrayElemAt": ["$previousOrder.createdAt", 0]}}},
            {"$addFields": {"customerType": {"$cond": [
                {"$not": ["$lastOrderDate"]},
                "new",
                {"$cond": [
                    {"$gte": ["$lastOrderDate", {"$dateSubtract": {"startDate": "$createdAt", "unit": "day", "amount": 30}}]},
                    "retained",
                    "resurrected"
                ]}
            ]}}},
            {"$project": {"createdAt": 1, "total": 1, "customerType": 1, "day": {"$dateToString": {"date": "$createdAt", "format": "%Y-%m-%d"}}}},
            {"$match": {"customerType": {"$in": ["retained", "resurrected"]}}},
            {"$group": {
                "_id": "$day",
                "revenueFromExistingCustomers": {"$sum": "$total"}
            }},
            {"$project": {"date": "$_id", "revenueFromExistingCustomers": 1, "_id": 0}},
            {"$sort": {"date": 1}}
        ]
        return run_query(db.orders_ofps, pipeline)

    def number_of_purchases_by_new_customers():
        pipeline = [
            {
                "$match": {
                    "createdAt": {"$gte": start_date, "$lt": end_date},
                    "status": "completed"
                }
            },
            {
                "$addFields": {
                    "orderDay": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}
                    },
                    "firstOrderDate": start_date  # <- inject literal date here
                }
            },
            {
                "$group": {
                    "_id": {
                        "customerId": "$customer_id",
                        "orderDay": "$orderDay"
                    },
                    "orderDay": {"$first": "$orderDay"},
                    "customerId": {"$first": "$customer_id"},
                    "firstOrderDate": {"$first": "$firstOrderDate"}
                }
            },
            {
                "$lookup": {
                    "from": "orders_ofps",
                    "let": {
                        "customerId": "$customerId",
                        "firstDate": "$firstOrderDate"
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$customer_id", "$$customerId"]},
                                        {"$lt": ["$createdAt", "$$firstDate"]}
                                    ]
                                }
                            }
                        },
                        {"$limit": 1}
                    ],
                    "as": "priorOrders"
                }
            },
            {
                "$match": {
                    "priorOrders.0": {"$exists": False}
                }
            },
            {
                "$group": {
                    "_id": "$orderDay",
                    "numberOfPurchasesByNewCustomers": {"$sum": 1}
                }
            },
            {
                "$sort": {"_id": 1}
            },
            {
                "$project": {
                    "date": "$_id",
                    "numberOfPurchasesByNewCustomers": 1,
                    "_id": 0
                }
            }
        ]
        return run_query(db.orders_ofps, pipeline)

    def number_of_new_activated_accounts():
        pipeline = [
            {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}}},
            {"$group": {"_id": "$username", "firstTradeDate": {"$min": "$createdAt"}}},
            {"$match": {"firstTradeDate": {"$gte": start_date, "$lt": end_date}}},
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$firstTradeDate"}},
                "numberOfNewActivatedAccounts": {"$sum": 1}
            }},
            {"$project": {"date": "$_id", "numberOfNewActivatedAccounts": 1, "_id": 0}},
            {"$sort": {"date": 1}}
        ]
        return run_query(tracking_db.trades, pipeline)

    def revenue_from_new_customers():
        pipeline =  [
        {
            "$match": {
                "createdAt": { "$gte": start_date, "$lt": end_date },
                "status": "completed"
            }
        },
        {
            "$addFields": {
                "customerObjectId": { "$toObjectId": "$customer_id" }
            }
        },
        {
            "$lookup": {
                "from": "customers",
                "localField": "customerObjectId",
                "foreignField": "_id",
                "as": "customer"
            }
        },
        {
            "$unwind": {
                "path": "$customer",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$match": {
                "$expr": {
                    "$lt": [ "$customer.created_at", end_date ]
                }
            }
        },
        {
            "$addFields": {
                "purchaseDay": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$createdAt"
                    }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "customerId": "$customer_id",
                    "purchaseDay": "$purchaseDay"
                },
                "totalRevenue": { "$sum": "$total" },
                "dayDate": { "$first": "$createdAt" }
            }
        },
        {
            "$lookup": {
                "from": "orders_ofps",
                "localField": "_id.customerId",
                "foreignField": "customer_id",
                "as": "allPurchases"
            }
        },
        {
            "$addFields": {
                "firstPurchaseDate": { "$min": "$allPurchases.createdAt" }
            }
        },
        {
            "$match": {
                "$expr": {
                    "$eq": [
                        { "$dateToString": { "format": "%Y-%m-%d", "date": "$firstPurchaseDate" } },
                        "$_id.purchaseDay"
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": "$_id.purchaseDay",
                "revenueFromNewCustomers": { "$sum": "$totalRevenue" }
            }
        },
        {
            "$project": {
                "date": "$_id",
                "revenueFromNewCustomers": 1,
                "_id": 0
            }
        },
        { "$sort": { "date": 1 } }
    ]
        return run_query(db.orders_ofps, pipeline)

    def number_of_retained_customers():
        pipeline = [
            {
                "$match": {
                    "createdAt": {"$gte": start_date, "$lt": end_date},
                    "status": "completed"
                }
            },
            {
                "$addFields": {
                    "orderDay": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "customer_id": "$customer_id",
                        "orderDay": "$orderDay"
                    },
                    "orders": {"$push": "$$ROOT"},
                    "orderDate": {"$min": "$createdAt"}
                }
            },
            {
                "$lookup": {
                    "from": "orders_ofps",
                    "let": {"customerId": "$_id.customer_id", "currentDate": "$orderDate"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$customer_id", "$$customerId"]},
                                        {"$lt": ["$createdAt", "$$currentDate"]}
                                    ]
                                }
                            }
                        },
                        {"$sort": {"createdAt": -1}},
                        {"$limit": 1}
                    ],
                    "as": "previousOrder"
                }
            },
            {
                "$addFields": {
                    "isRetained": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gt": [{"$size": "$previousOrder"}, 0]},
                                    {
                                        "$gte": [
                                            {"$arrayElemAt": ["$previousOrder.createdAt", 0]},
                                            {
                                                "$dateSubtract": {
                                                    "startDate": "$orderDate",
                                                    "unit": "day",
                                                    "amount": 30
                                                }
                                            }
                                        ]
                                    }
                                ]
                            },
                            True,
                            False
                        ]
                    }
                }
            },
            {"$unwind": "$orders"},
            {
                "$match": {"isRetained": True}
            },
            {
                "$group": {
                    "_id": "$_id.orderDay",
                    "retainedUsers": {"$addToSet": "$_id.customer_id"}
                }
            },
            {
                "$project": {
                    "date": "$_id",
                    "numberOfRetainedCustomers": {"$size": "$retainedUsers"},
                    "_id": 0
                }
            },
            {"$sort": {"date": 1}}
        ]
        return run_query(db.orders_ofps, pipeline)

    def number_of_orders_by_retained_customers():
        pipeline = [
            {
                "$match": {
                    "createdAt": {"$gte": start_date, "$lt": end_date},
                    "status": "completed"
                }
            },
            {
                "$addFields": {
                    "orderDay": {
                        "$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "customer_id": "$customer_id",
                        "orderDay": "$orderDay"
                    },
                    "orders": {"$push": "$$ROOT"},
                    "orderDate": {"$min": "$createdAt"}
                }
            },
            {
                "$lookup": {
                    "from": "orders_ofps",
                    "let": {"customerId": "$_id.customer_id", "currentDate": "$orderDate"},
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$customer_id", "$$customerId"]},
                                        {"$lt": ["$createdAt", "$$currentDate"]}
                                    ]
                                }
                            }
                        },
                        {"$sort": {"createdAt": -1}},
                        {"$limit": 1}
                    ],
                    "as": "previousOrder"
                }
            },
            {
                "$addFields": {
                    "isRetained": {
                        "$cond": [
                            {
                                "$and": [
                                    {"$gt": [{"$size": "$previousOrder"}, 0]},
                                    {
                                        "$gte": [
                                            {"$arrayElemAt": ["$previousOrder.createdAt", 0]},
                                            {
                                                "$dateSubtract": {
                                                    "startDate": "$orderDate",
                                                    "unit": "day",
                                                    "amount": 30
                                                }
                                            }
                                        ]
                                    }
                                ]
                            },
                            True,
                            False
                        ]
                    }
                }
            },
            {"$unwind": "$orders"},
            {
                "$match": {
                    "isRetained": True
                }
            },
            {
                "$group": {
                    "_id": "$_id.orderDay",
                    "numberOfOrdersByRetainedCustomers": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "date": "$_id",
                    "numberOfOrdersByRetainedCustomers": 1,
                    "_id": 0
                }
            },
            {"$sort": {"date": 1}}
        ]
        return run_query(db.orders_ofps, pipeline)

    def revenue_from_retained_customers():
        pipeline = [
        {
            "$match": {
                "createdAt": { "$gte": start_date, "$lt": end_date },
                "status": "completed"
            }
        },
        {
            "$lookup": {
                "from": "orders_ofps",
                "let": { "customerId": "$customer_id", "currentOrderDate": "$createdAt" },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    { "$eq": ["$customer_id", "$$customerId"] },
                                    { "$lt": ["$createdAt", "$$currentOrderDate"] }
                                ]
                            }
                        }
                    },
                    { "$sort": { "createdAt": -1 } },
                    { "$limit": 1 }
                ],
                "as": "previousOrder"
            }
        },
        {
            "$addFields": {
                "lastOrderDate": {
                    "$arrayElemAt": ["$previousOrder.createdAt", 0]
                }
            }
        },
        {
            "$addFields": {
                "customerType": {
                    "$cond": [
                        { "$not": ["$lastOrderDate"] },
                        "new",
                        {
                            "$cond": [
                                {
                                    "$gte": [
                                        "$lastOrderDate",
                                        {
                                            "$dateSubtract": {
                                                "startDate": "$createdAt",
                                                "unit": "day",
                                                "amount": 30
                                            }
                                        }
                                    ]
                                },
                                "retained",
                                "resurrected"
                            ]
                        }
                    ]
                }
            }
        },
        {
            "$project": {
                "createdAt": 1,
                "total": 1,
                "customerType": 1,
                "day": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$createdAt"
                    }
                }
            }
        },
        {
            "$match": {
                "customerType": { "$in": ["retained", "resurrected"] }
            }
        },
        {
            "$group": {
                "_id": { "day": "$day", "type": "$customerType" },
                "revenue": { "$sum": "$total" }
            }
        },
        {
            "$group": {
                "_id": "$_id.day",
                "retainedRevenue": {
                    "$sum": {
                        "$cond": [
                            { "$eq": ["$_id.type", "retained"] },
                            "$revenue",
                            0
                        ]
                    }
                }
            }
        },
        {
            "$project": {
                "date": "$_id",
                "revenueFromRetainedCustomers": "$retainedRevenue",
                "_id": 0
            }
        },
        {
            "$sort": { "date": 1 }
        }
    ]
        return run_query(db.orders_ofps, pipeline)

    def number_of_resurrected_customers():
        pipeline = [
        {
            "$match": {
                "createdAt": { "$gte": start_date, "$lt": end_date },
                "status": "completed"
            }
        },
        {
            "$addFields": {
                "orderDay": {
                    "$dateToString": { "format": "%Y-%m-%d", "date": "$createdAt" }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "customer_id": "$customer_id",
                    "orderDay": "$orderDay"
                },
                "orderDate": { "$min": "$createdAt" }
            }
        },
        {
            "$lookup": {
                "from": "orders_ofps",
                "let": { "customerId": "$_id.customer_id", "currentDate": "$orderDate" },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    { "$eq": ["$customer_id", "$$customerId"] },
                                    { "$lt": ["$createdAt", "$$currentDate"] }
                                ]
                            }
                        }
                    },
                    { "$sort": { "createdAt": -1 } },
                    { "$limit": 1 }
                ],
                "as": "previousOrder"
            }
        },
        {
            "$addFields": {
                "isResurrected": {
                    "$cond": [
                        {
                            "$and": [
                                { "$gt": [{ "$size": "$previousOrder" }, 0] },
                                {
                                    "$lt": [
                                        { "$arrayElemAt": ["$previousOrder.createdAt", 0] },
                                        {
                                            "$dateSubtract": {
                                                "startDate": "$orderDate",
                                                "unit": "day",
                                                "amount": 30
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        True,
                        False
                    ]
                }
            }
        },
        {
            "$project": {
                "date": "$_id.orderDay",
                "isResurrected": 1
            }
        },
        {
            "$group": {
                "_id": "$date",
                "resurrectedUsers": {
                    "$sum": { "$cond": ["$isResurrected", 1, 0] }
                }
            }
        },
        {
            "$project": {
                "date": "$_id",
                "numberOfResurrectedCustomers": "$resurrectedUsers",
                "_id": 0
            }
        },
        {
            "$sort": { "date": 1 }
        }
    ]
        return run_query(db.orders_ofps, pipeline)

    def number_of_orders_by_resurrected_customers():
        pipeline = [
        {
            "$match": {
                "createdAt": { "$gte": start_date, "$lt": end_date },
                "status": "completed"
            }
        },
        {
            "$addFields": {
                "orderDay": {
                    "$dateToString": { "format": "%Y-%m-%d", "date": "$createdAt" }
                }
            }
        },
        {
            "$lookup": {
                "from": "orders_ofps",
                "let": { "customerId": "$customer_id", "currentOrderDate": "$createdAt" },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    { "$eq": ["$customer_id", "$$customerId"] },
                                    { "$lt": ["$createdAt", "$$currentOrderDate"] }
                                ]
                            }
                        }
                    },
                    { "$sort": { "createdAt": -1 } },
                    { "$limit": 1 }
                ],
                "as": "previousOrder"
            }
        },
        {
            "$addFields": {
                "isResurrected": {
                    "$cond": [
                        {
                            "$and": [
                                { "$gt": [{ "$size": "$previousOrder" }, 0] },
                                {
                                    "$lt": [
                                        { "$arrayElemAt": ["$previousOrder.createdAt", 0] },
                                        {
                                            "$dateSubtract": {
                                                "startDate": "$createdAt",
                                                "unit": "day",
                                                "amount": 30
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        True,
                        False
                    ]
                }
            }
        },
        {
            "$match": {
                "isResurrected": True
            }
        },
        {
            "$group": {
                "_id": "$orderDay",
                "numberOfOrdersByResurrectedCustomers": { "$sum": 1 }
            }
        },
        {
            "$project": {
                "date": "$_id",
                "numberOfOrdersByResurrectedCustomers": 1,
                "_id": 0
            }
        },
        {
            "$sort": { "date": 1 }
        }
    ]
        return run_query(db.orders_ofps, pipeline)

    def revenue_from_resurrected_customers():
        pipeline = [
        {
            "$match": {
                "createdAt": { "$gte": start_date, "$lt": end_date },
                "status": "completed"
            }
        },
        {
            "$addFields": {
                "orderDay": {
                    "$dateToString": { "format": "%Y-%m-%d", "date": "$createdAt" }
                }
            }
        },
        {
            "$lookup": {
                "from": "orders_ofps",
                "let": {
                    "customerId": "$customer_id",
                    "currentOrderDate": "$createdAt"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    { "$eq": ["$customer_id", "$$customerId"] },
                                    { "$lt": ["$createdAt", "$$currentOrderDate"] }
                                ]
                            }
                        }
                    },
                    { "$sort": { "createdAt": -1 } },
                    { "$limit": 1 }
                ],
                "as": "previousOrder"
            }
        },
        {
            "$addFields": {
                "isResurrected": {
                    "$cond": [
                        {
                            "$and": [
                                { "$gt": [{ "$size": "$previousOrder" }, 0] },
                                {
                                    "$lt": [
                                        { "$arrayElemAt": ["$previousOrder.createdAt", 0] },
                                        {
                                            "$dateSubtract": {
                                                "startDate": "$createdAt",
                                                "unit": "day",
                                                "amount": 30
                                            }
                                        }
                                    ]
                                }
                            ]
                        },
                        True,
                        False
                    ]
                }
            }
        },
        {
            "$match": {
                "isResurrected": True
            }
        },
        {
            "$group": {
                "_id": "$orderDay",
                "revenueFromResurrectedCustomers": { "$sum": "$total" }
            }
        },
        {
            "$project": {
                "date": "$_id",
                "revenueFromResurrectedCustomers": 1,
                "_id": 0
            }
        },
        {
            "$sort": { "date": 1 }
        }
    ]
        return run_query(db.orders_ofps, pipeline)

    def new_signups_in_dashboard():
        pipeline = [
            {"$match": {
                "verified": True,
                "created_at": {"$gte": start_date, "$lte": end_date}
            }},
            {"$addFields": {
                "day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}}
            }},
            {"$group": {
                "_id": "$day",
                "newSignupsInDashBoard": {"$sum": 1}
            }},
            {"$project": {
                "date": "$_id",
                "newSignupsInDashBoard": 1,
                "_id": 0
            }},
            {"$sort": {"date": 1}}
        ]
        return run_query(db.customers, pipeline)

    def total_orders():
        pipeline = [
            {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
            {"$group": {
                "_id": {"$dateToString": {"date": "$createdAt", "format": "%Y-%m-%d"}},
                "totalOrders": {"$sum": 1}
            }},
            {"$project": {"date": "$_id", "totalOrders": 1, "_id": 0}},
            {"$sort": {"date": 1}}
        ]
        return run_query(db.orders_ofps, pipeline)

    def overal_revenue():
        pipeline = [
            {"$match": {"createdAt": {"$gte": start_date, "$lt": end_date}, "status": "completed"}},
            {"$group": {
                "_id": {"$dateToString": {"date": "$createdAt", "format": "%Y-%m-%d"}},
                "overalRevenue": {"$sum": "$total"}
            }},
            {"$project": {"date": "$_id", "overalRevenue": 1, "_id": 0}},
            {"$sort": {"date": 1}}
        ]
        return run_query(db.orders_ofps, pipeline)

    def number_of_new_paid_users():
        pipeline = [
        {
            "$match": {
                "createdAt": { "$gte": start_date, "$lt": end_date },
                "status": "completed"
            }
        },
        {
            "$addFields": {
                "orderDay": {
                    "$dateToString": { "format": "%Y-%m-%d", "date": "$createdAt" }
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "customer_id": "$customer_id",
                    "orderDay": "$orderDay"
                },
                "orderDate": { "$min": "$createdAt" }
            }
        },
        {
            "$lookup": {
                "from": "orders_ofps",
                "let": { "customerId": "$_id.customer_id", "currentDate": "$orderDate" },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    { "$eq": ["$customer_id", "$$customerId"] },
                                    { "$lt": ["$createdAt", "$$currentDate"] }
                                ]
                            }
                        }
                    },
                    { "$sort": { "createdAt": -1 } },
                    { "$limit": 1 }
                ],
                "as": "previousOrder"
            }
        },
        {
            "$addFields": {
                "isNew": { "$eq": [{ "$size": "$previousOrder" }, 0] }
            }
        },
        {
            "$project": {
                "date": "$_id.orderDay",
                "isNew": 1
            }
        },
        {
            "$group": {
                "_id": "$date",
                "numberOfNewPaidUsers": {
                    "$sum": { "$cond": ["$isNew", 1, 0] }
                }
            }
        },
        {
            "$project": {
                "date": "$_id",
                "numberOfNewPaidUsers": 1,
                "_id": 0
            }
        },
        {
            "$sort": { "date": 1 }
        }
    ]
        return run_query(db.orders_ofps, pipeline)

    # Register all metric functions
    metric_functions = [
        new_signups_in_dashboard,
        total_orders,
        overal_revenue,
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
        revenue_from_resurrected_customers,
        number_of_new_activated_accounts
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

    #  TO GOOGLE SHEETS
    SERVICE_ACCOUNT_FILE = 'ofp_google_sheets.json'
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file("ofp_google_sheets.json", scopes=SCOPES)
    gc = gspread.authorize(creds)

    spreadsheet = gc.open("MKT KPI - Completed ORDERS")
    # customer_metrics_v2
    worksheet = spreadsheet.worksheet("test")
    # Get next empty row (assuming column A is always filled for each row)
    next_row = len(worksheet.get_all_values()) + 1
    gd.set_with_dataframe(worksheet, base_dates, row=next_row, include_column_header=True)

ofp_data_to_sheets()