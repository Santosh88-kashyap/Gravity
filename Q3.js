db.sales.aggregate([
    {
        $project: {
            store: 1,
            month: { $dateToString: { format: "%Y-%m", date: "$date" } },
            items: 1
        }
    },
    { $unwind: "$items" },
    {
        $set: {
            revenue: { $multiply: ["$items.quantity", "$items.price"] },
            itemPrice: "$items.price"
        }
    },
    {
        $group: {
            _id: { store: "$store", month: "$month" },
            totalRevenue: { $sum: "$revenue" },
            totalItems: { $sum: "$items.quantity" },
            totalPrice: { $sum: "$itemPrice" }
        }
    },
    {
        $set: {
            averagePrice: { $divide: ["$totalPrice", "$totalItems"] }
        }
    },
    {
        $project: {
            _id: 0,
            store: "$_id.store",
            month: "$_id.month",
            totalRevenue: 1,
            averagePrice: { $round: ["$averagePrice", 2] }
        }
    },
    {
        $sort: { store: 1, month: 1 }
    }
])
