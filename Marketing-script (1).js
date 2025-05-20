const startNumberOfDays = 7; // Change this value depending on how many days you want the datat for
const startdate = new Date();
startdate.setHours(0, 0, 0, 0);
startdate.setDate(startdate.getDate() - startNumberOfDays);

const endDate = new Date();
endDate.setHours(0, 0, 0, 0);

print(startdate.toDateString());
print(endDate.toDateString());

const totalNewSignups = db.customers
  .aggregate([
    {
      $match: {
        verified: true,
        created_at: { $gt: startdate, $lt: endDate },
      },
    },
    {
      $addFields: {
        day: {
          $dateToString: { format: "%Y-%m-%d", date: "$created_at" },
        },
      },
    },
    {
      $group: {
        _id: "$day",
        totalNewAccountSignups: { $sum: 1 },
      },
    },
    {
      $project: {
        date: "$_id",
        totalNewAccountSignups: 1,
        _id: 0,
      },
    },
    { $sort: { date: 1 } },
  ])
  .toArray();

print(totalNewSignups);
// End of New sign-ups in dashboard

const numberOfCompleteOrders = db.orders_ofps
  .aggregate([
    {
      $match: {
        createdAt: { $gte: startdate, $lt: endDate },
        status: "completed",
      },
    },
    {
      $addFields: {
        day: {
          $dateToString: { format: "%Y-%m-%d", date: "$createdAt" },
        },
      },
    },
    {
      $group: {
        _id: "$day",
        totalOrders: { $sum: 1 },
      },
    },
    {
      $project: {
        date: "$_id",
        totalOrders: 1,
        _id: 0,
      },
    },
    { $sort: { date: 1 } },
  ])
  .toArray();

print(numberOfCompleteOrders);
// End of Total orders

const totalOrderRevenue = db.orders_ofps
  .aggregate([
    {
      $match: {
        createdAt: { $gte: startdate, $lt: endDate },
        status: "completed",
      },
    },
    {
      $addFields: {
        day: {
          $dateToString: { format: "%Y-%m-%d", date: "$createdAt" },
        },
      },
    },
    {
      $group: {
        _id: "$day",
        totalRevenue: { $sum: "$total" },
      },
    },
    {
      $project: {
        date: "$_id",
        totalRevenue: 1,
        _id: 0,
      },
    },
    { $sort: { date: 1 } },
  ])
  .toArray();

print(totalOrderRevenue);
// End of Overall revenue

const totalNewCustomerOrderRevenue = db.orders_ofps
  .aggregate([
    {
      $match: {
        createdAt: {
          $gte: startdate,
          $lt: endDate,
        },
        status: "completed",
      },
    },
    {
      $addFields: {
        purchaseDay: {
          $dateToString: { format: "%Y-%m-%d", date: "$createdAt" },
        },
      },
    },
    {
      $group: {
        _id: {
          customerId: "$customer_id",
          purchaseDay: "$purchaseDay",
        },
        totalRevenue: { $sum: "$total" },
        dayDate: { $first: "$createdAt" },
      },
    },
    {
      $lookup: {
        from: "orders_ofps",
        localField: "_id.customerId",
        foreignField: "customer_id",
        as: "allPurchases",
      },
    },
    {
      $addFields: {
        firstPurchaseDate: {
          $min: "$allPurchases.createdAt",
        },
      },
    },
    {
      $match: {
        $expr: {
          $eq: [
            {
              $dateToString: { format: "%Y-%m-%d", date: "$firstPurchaseDate" },
            },
            "$_id.purchaseDay",
          ],
        },
      },
    },
    {
      $group: {
        _id: "$_id.purchaseDay",
        totalRevenueFromNewCustomers: { $sum: "$totalRevenue" },
      },
    },
    {
      $project: {
        date: "$_id",
        totalRevenueFromNewCustomers: 1,
        _id: 0,
      },
    },
    { $sort: { date: 1 } },
  ])
  .toArray();

print(totalNewCustomerOrderRevenue);
// End of Revenue from new customers

const totalPurchasesByNewCustomers = db.orders_ofps
  .aggregate([
    {
      $match: {
        createdAt: {
          $gte: startdate,
          $lt: endDate,
        },
        status: "completed",
      },
    },
    {
      $addFields: {
        purchaseDay: {
          $dateToString: { format: "%Y-%m-%d", date: "$createdAt" },
        },
      },
    },
    {
      $group: {
        _id: {
          customerId: "$customer_id",
          purchaseDay: "$purchaseDay",
        },
        dayDate: { $first: "$createdAt" },
        accountIds: { $addToSet: "$customer_id" },
      },
    },
    {
      $lookup: {
        from: "orders_ofps",
        localField: "_id.customerId",
        foreignField: "customer_id",
        as: "allPurchases",
      },
    },
    {
      $addFields: {
        firstPurchaseDate: {
          $min: "$allPurchases.createdAt",
        },
      },
    },
    {
      $match: {
        $expr: {
          $eq: [
            {
              $dateToString: { format: "%Y-%m-%d", date: "$firstPurchaseDate" },
            },
            "$_id.purchaseDay",
          ],
        },
      },
    },
    {
      $group: {
        _id: "$_id.purchaseDay",
        allAccounts: { $addToSet: "$accountIds" },
      },
    },
    {
      $project: {
        date: "$_id",
        numberOfAccounts: {
          $size: {
            $reduce: {
              input: "$allAccounts",
              initialValue: [],
              in: { $setUnion: ["$$value", "$$this"] },
            },
          },
        },
        _id: 0,
      },
    },
    { $sort: { date: 1 } },
  ])
  .toArray();

print(totalPurchasesByNewCustomers);
// End of Purchases by new paid users

const past30Days = new Date();
past30Days.setHours(0, 0, 0, 0);
past30Days.setDate(past30Days.getDate() - 30);

const totalRetainedCustomerOrderRevenue = db.orders_ofps
  .aggregate([
    {
      $addFields: {
        purchaseDay: {
          $dateToString: { format: "%Y-%m-%d", date: "$createdAt" },
        },
      },
    },
    {
      $match: {
        createdAt: { $gt: startdate, $lt: endDate },
        status: "completed",
      },
    },
    {
      $group: {
        _id: {
          customerId: "$customer_id",
          purchaseDay: "$purchaseDay",
        },
        accountIds: { $addToSet: "$customer_id" },
        dayDate: { $first: "$createdAt" },
      },
    },
    {
      $lookup: {
        from: "orders_ofps",
        localField: "_id.customerId",
        foreignField: "customer_id",
        as: "priorPurchases",
      },
    },
    {
      $addFields: {
        retained: {
          $filter: {
            input: "$priorPurchases",
            as: "prior",
            cond: {
              $and: [
                { $lt: ["$$prior.createdAt", "$dayDate"] },
                {
                  $gte: [
                    "$$prior.createdAt",
                    {
                      $dateSubtract: {
                        startDate: startdate,
                        unit: "day",
                        amount: 30,
                      },
                    },
                  ],
                },
              ],
            },
          },
        },
      },
    },
    {
      $match: {
        "retained.0": { $exists: true },
      },
    },
    {
      $group: {
        _id: "$_id.purchaseDay",
        allAccounts: { $addToSet: "$accountIds" },
      },
    },
    {
      $project: {
        date: "$_id",
        numberOfAccounts: {
          $size: {
            $reduce: {
              input: "$allAccounts",
              initialValue: [],
              in: { $setUnion: ["$$value", "$$this"] },
            },
          },
        },
        _id: 0,
      },
    },
    { $sort: { date: 1 } },
  ])
  .toArray();

print(totalRetainedCustomerOrderRevenue);
// End of Purchases by retained customers

const totalResurrectedCustomerOrderRevenue = db.orders_ofps
  .aggregate([
    {
      $match: {
        createdAt: {
          $gte: startdate,
          $lt: endDate,
        },
        status: "completed",
      },
    },
    {
      $addFields: {
        purchaseDay: {
          $dateToString: { format: "%Y-%m-%d", date: "$createdAt" },
        },
      },
    },
    {
      $group: {
        _id: {
          customerId: "$customer_id",
          purchaseDay: "$purchaseDay",
        },
        dayDate: { $first: "$createdAt" },
        accountIds: { $addToSet: "$customer_id" },
      },
    },
    {
      $lookup: {
        from: "orders_ofps",
        localField: "_id.customerId",
        foreignField: "customer_id",
        as: "priorPurchases",
      },
    },
    {
      $addFields: {
        hasOldPurchases: {
          $filter: {
            input: "$priorPurchases",
            as: "prior",
            cond: { $lt: ["$$prior.createdAt", "$dayDate"] },
          },
        },
        hasRecentPurchases: {
          $filter: {
            input: "$priorPurchases",
            as: "prior",
            cond: {
              $and: [
                { $lt: ["$$prior.createdAt", "$dayDate"] },
                {
                  $gte: [
                    "$$prior.createdAt",
                    {
                      $dateSubtract: {
                        startDate: "$dayDate",
                        unit: "day",
                        amount: 30,
                      },
                    },
                  ],
                },
              ],
            },
          },
        },
      },
    },
    {
      $match: {
        "hasOldPurchases.0": { $exists: true },
        "hasRecentPurchases.0": { $exists: false },
      },
    },
    {
      $group: {
        _id: "$_id.purchaseDay",
        allAccounts: { $addToSet: "$accountIds" },
      },
    },
    {
      $project: {
        date: "$_id",
        numberOfAccounts: {
          $size: {
            $reduce: {
              input: "$allAccounts",
              initialValue: [],
              in: { $setUnion: ["$$value", "$$this"] },
            },
          },
        },
        _id: 0,
      },
    },
    { $sort: { date: 1 } },
  ])
  .toArray();

print(totalResurrectedCustomerOrderRevenue);
// End of Purchases by resurrected customers

const totalForRetainedAndRessurectedAccounts = db.orders_ofps
  .aggregate([
    {
      $match: {
        createdAt: { $gte: startdate, $lt: endDate },
        status: "completed", // assuming completed orders are the ones that contribute to revenue
      },
    },
    {
      $addFields: {
        orderDay: { $dateToString: { format: "%Y-%m-%d", date: "$createdAt" } },
      },
    },
    {
      $group: {
        _id: {
          customerId: "$customer_id",
          orderDay: "$orderDay",
        },
        firstOrderDate: { $min: "$createdAt" },
        totalRevenue: { $sum: "$total" }, // assuming "total" is the revenue field
      },
    },
    {
      $lookup: {
        from: "orders_ofps",
        localField: "_id.customerId",
        foreignField: "customer_id",
        as: "priorPurchases",
      },
    },
    {
      $addFields: {
        retained: {
          $filter: {
            input: "$priorPurchases",
            as: "prior",
            cond: {
              $and: [
                { $lt: ["$$prior.createdAt", "$firstOrderDate"] }, // purchase before the first order
                {
                  $gte: [
                    "$$prior.createdAt",
                    {
                      $dateSubtract: {
                        startDate: startdate,
                        unit: "day",
                        amount: 30,
                      },
                    },
                  ],
                }, // purchase within 30 days before the first order
              ],
            },
          },
        },
        resurrected: {
          $filter: {
            input: "$priorPurchases",
            as: "prior",
            cond: {
              $and: [
                { $lt: ["$$prior.createdAt", "$firstOrderDate"] }, // purchase before the first order
                {
                  $lt: [
                    "$$prior.createdAt",
                    {
                      $dateSubtract: {
                        startDate: startdate,
                        unit: "day",
                        amount: 30,
                      },
                    },
                  ],
                }, // purchase outside the 30-day window before the first order
              ],
            },
          },
        },
      },
    },
    {
      $match: {
        $or: [
          { "retained.0": { $exists: true } }, // retained
          { "resurrected.0": { $exists: true } }, // resurrected
        ],
      },
    },
    {
      $group: {
        _id: "$_id.orderDay", // Group by day
        dailyRevenue: { $sum: "$totalRevenue" },
      },
    },
    {
      $project: {
        date: "$_id",
        dailyRevenue: 1,
        _id: 0,
      },
    },
    {
      $sort: { date: 1 },
    },
  ])
  .toArray();

print(totalForRetainedAndRessurectedAccounts);
// End of Purchases by resurrected customers

const totalActivatedAccount = db.orders_ofps
  .aggregate([
    {
      $match: {
        status: "completed",
        createdAt: { $gte: startdate, $lte: endDate },
      },
    },
    {
      $group: {
        _id: "$customer_id",
        firstOrderDate: { $min: "$createdAt" },
      },
    },
    {
      $match: {
        firstOrderDate: { $gte: startdate, $lte: endDate },
      },
    },
    {
      $group: {
        _id: { $dateToString: { format: "%Y-%m-%d", date: "$firstOrderDate" } },
        numberOfNewActivatedAccounts: { $sum: 1 },
      },
    },
    {
      $sort: { _id: 1 },
    },
    {
      $project: {
        date: "$_id",
        numberOfNewActivatedAccounts: 1,
        _id: 0,
      },
    },
  ])
  .toArray();

print(totalActivatedAccount);
// End of New activated accounts

const result = {
  numberOfCompleteOrders: numberOfCompleteOrders,
  totalOrderRevenue: totalOrderRevenue,
  totalNewCustomerOrderRevenue: totalNewCustomerOrderRevenue,
  totalPurchasesByNewCustomers: totalPurchasesByNewCustomers,
  totalRetainedCustomerOrderRevenue: totalRetainedCustomerOrderRevenue,
  totalResurrectedCustomerOrderRevenue: totalResurrectedCustomerOrderRevenue,
  totalForRetainedAndRessurectedAccounts:
    totalForRetainedAndRessurectedAccounts,
  totalActivatedAccount: totalActivatedAccount,
  totalNewSignups: totalNewSignups,
};

// Mapping to CSV starts here

// Map all data into a single dictionary by date
const resultsByDate = {};

function mergeIntoResults(array, key, valueKey) {
  array.forEach((entry) => {
    const date = entry.date;
    if (!resultsByDate[date]) resultsByDate[date] = { date };
    resultsByDate[date][key] = entry[valueKey];
  });
}

// Merge each array with appropriate keys
mergeIntoResults(
  result.totalNewSignups,
  "totalNewSignups",
  "totalNewAccountSignups"
);
mergeIntoResults(result.numberOfCompleteOrders, "totalOrders", "totalOrders");
mergeIntoResults(result.totalOrderRevenue, "totalRevenue", "totalRevenue");
mergeIntoResults(
  result.totalNewCustomerOrderRevenue,
  "newCustomerRevenue",
  "totalRevenueFromNewCustomers"
);
mergeIntoResults(
  result.totalPurchasesByNewCustomers,
  "newCustomerPurchases",
  "numberOfAccounts"
);
mergeIntoResults(
  result.totalRetainedCustomerOrderRevenue,
  "retainedCustomerPurchases",
  "numberOfAccounts"
);
mergeIntoResults(
  result.totalResurrectedCustomerOrderRevenue,
  "resurrectedCustomerPurchases",
  "numberOfAccounts"
);
mergeIntoResults(
  result.totalForRetainedAndRessurectedAccounts,
  "retainedAndResurrectedRevenue",
  "dailyRevenue"
);
mergeIntoResults(
  result.totalActivatedAccount,
  "totalActivatedAccount",
  "numberOfNewActivatedAccounts"
);

// Sort by date
const sortedDates = Object.keys(resultsByDate).sort();
const finalData = sortedDates.map((date) => resultsByDate[date]);

// Define the headers for the CSV
// Create CSV output
const headers = [
  "date",
  "totalNewSignups",
  "totalOrders",
  "totalRevenue",
  "newCustomerRevenue",
  "newCustomerPurchases",
  "retainedCustomerPurchases",
  "resurrectedCustomerPurchases",
  "retainedAndResurrectedRevenue",
  "totalActivatedAccount",
];

const csvRows = [headers.join(",")];
finalData.forEach((row) => {
  const csvRow = headers
    .map((header) => (row[header] !== undefined ? row[header] : ""))
    .join(",");
  csvRows.push(csvRow);
});

const csvOutput = csvRows.join("\n");

// Print result in Mongo shell (or console.log in Node.js)
print(csvOutput);
