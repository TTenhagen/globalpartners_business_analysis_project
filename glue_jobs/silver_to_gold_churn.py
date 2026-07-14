from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("silver_to_gold_churn").getOrCreate()

orders = spark.read.parquet("s3://restaruant-raw/silver/order_items_cleaned/")
as_of_date = F.current_date()

# One row per customer per active day
daily = (orders
        .groupBy("user_id", "order_date")
        .agg(F.sum("line_revenue").alias("day_spend"))
        )

# Average gap between consecutive order days, per customer
w = Window.partitionBy("user_id").orderBy("order_date")
gaps = (daily
 .withColumn("prev_order_date", F.lag("order_date").over(w))
 .withColumn("gap_days", F.datediff("order_date", "prev_order_date")))

per_user = (gaps.groupBy("user_id")
            .agg(F.max("order_date").alias("last_order_date"),
                 F.round(F.avg("gap_days"), 2).alias("avg_inter_order_gap_days"))
            )

# Spend change: trailing 30 days vs the 30 days before that
recent = (orders.filter(F.col("order_date") >= F.date_sub(as_of_date, 30))
 .groupBy("user_id").agg(F.sum("line_revenue").alias("recent_spend")))
prior = (orders.filter((F.col("order_date") < F.date_sub(as_of_date, 30)) &
 (F.col("order_date") >= F.date_sub(as_of_date, 60)))
 .groupBy("user_id").agg(F.sum("line_revenue").alias("prior_spend")))

churn = (per_user
    .join(recent, "user_id", "left")
    .join(prior, "user_id", "left")
    .fillna(0, subset=["recent_spend", "prior_spend"])
    .withColumn("days_since_last_order",
                F.datediff(as_of_date, F.col("last_order_date")))
    .withColumn("spend_change_pct",
                F.when(F.col("prior_spend") > 0,
                       F.round((F.col("recent_spend") - F.col("prior_spend"))
                               / F.col("prior_spend") * 100, 2)))
    .withColumn("is_at_risk", F.col("days_since_last_order") > 45))

(
    churn.select("user_id", 
                 "days_since_last_order", 
                 "avg_inter_order_gap_days",
                 "spend_change_pct",
                 "is_at_risk")

    .write.mode("overwrite")
)