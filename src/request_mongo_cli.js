const { listen } = require("node:quic");

// Nombre d'annonce par type de location
db.listing_paris
  .aggregate([{ $group: { _id: "$property_type", count: { $sum: 1 } } }])
  .sort({ count: -1 });

// 5 annonces de location avec le plus d’évaluations
db.listing_paris
  .find({}, { name: 1, number_of_reviews: 1, _id: 0 })
  .sort({ number_of_reviews: -1 })
  .limit(5);

// nombre total d’hôtes différents
db.listing_paris.distinct("host_id").length;

// nombre de locations réservables instantanément
db.listing_paris.countDocuments({ instant_bookable: "t" });
(db.listing_paris.countDocuments({ instant_bookable: "t" }) /
  db.listing_paris.countDocuments()) *
  100; // ratio

// hôtes ont plus de 100 annonces sur les plateformes
// quel pourcentage des hôtes
db.listing_paris.aggregate([
  { $group: { _id: "$host_id", count: { $sum: 1 } } },
  {
    $group: {
      _id: null,
      total_host: { $sum: 1 },
      total_host_100: { $sum: { $cond: [{ $gt: ["$count", 100] }, 1, 0] } },
    },
  },
  {
    $project: {
      total_host: "$total_host",
      total_host_100: "$total_host_100",
      proportion: {
        $round: [
          {
            $multiply: [{ $divide: ["$total_host_100", "$total_host"] }, 100],
          },
          2,
        ],
      },
    },
  },
]);

// super hôtes différents et proportion
db.listing_paris.aggregate([
  {
    $group: {
      _id: {
        $cond: [
          {
            $or: [
              { $eq: ["$host_is_superhost", "f"] },
              { $eq: ["$host_is_superhost", ""] },
            ],
          },
          "f",
          "$host_is_superhost",
        ],
      },
      count: { $sum: 1 },
    },
  },
  {
    $group: {
      _id: null,
      is_super_host: {
        $sum: { $cond: [{ $eq: ["$_id", "t"] }, "$count", 0] },
      },
      is_not_super_host: {
        $sum: { $cond: [{ $eq: ["$_id", "f"] }, "$count", 0] },
      },
      total: { $sum: "$count" },
    },
  },
  {
    $project: {
      is_super_host: "$is_super_host",
      is_not_super_host: "$is_not_super_host",
      total: "$total",
      super_host_rate: {
        $round: [
          {
            $multiply: [{ $divide: ["$is_super_host", "$total"] }, 100],
          },
          2,
        ],
      },
    },
  },
]);
