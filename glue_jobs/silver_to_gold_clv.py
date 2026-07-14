from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("silver_to_gold_clv").getOrCreate()

orders = spark.read.parquet("s3://globalpartners-raw/silver/order_items_cleaned/")
options = spark.read.parquet("s3://globalpartners-raw/silver/order_item_options_cleaned/")

# Total revenue per order_id/lineitem_id including options
opt_rev = (options
           .withColumn("option_revenue", F.col("option_price") * F.col("option_quantity"))
           .groupBy("order_id", "lineitem_id")
           .agg(F.sum("option_revenue").alias("option_revenue"))
           )

orders_full = (orders
               .join(opt_rev, ["order_id", "lineitem_id"], "left")
               .withColumn("option_revenue", F.coalesce(F.col("option_revenue"), F.lit(0)))
               .withColumn("total_revenue", F.col("line_revenue") + F.col("option_revenue"))
               )

# Per-customer per-day spend
daily_spend = (orders_full
               .groupBy("user_id", "order_date")
               .agg(F.sum("total_revenue").alias("daily_spend"),
                    F.countDistinct("order_id").alias("daily_order_count"))
                )

# Cumulative spend window, ordered by date, per customer
w = Window.partitionBy("user_id").orderBy("order_date") \
.rangeBetween(Window.unboundedPreceding, 0)
clv_daily = (daily_spend
             .withColumn("cumulative_spend", F.sum("daily_spend").over(w))
             .withColumn("order_count_to_date", F.sum("daily_order_count").over(w))
             .withColumn("clv_to_date", F.col("cumulative_spend"))
             .withColumnRenamed("order_date", "snapshot_date")
             )

# Tag High/Medium/Low using percentile cutoffs on the LATEST snapshot per customer
latest = clv_daily.groupBy("user_id").agg(F.max("snapshot_date").alias("snapshot_date"))
latest_clv = clv_daily.join(latest, ["user_id", "snapshot_date"])

p80 = latest_clv.approxQuantile("clv_to_date", [0.80], 0.01)[0]
p20 = latest_clv.approxQuantile("clv_to_date", [0.20], 0.01)[0]

clv_tagged = clv_daily.withColumn("clv_tier",
                                  F.when(F.col("clv_to_date") >= p80, "High")
                                  .when(F.col("clv_to_date") <= p20, "Low")
                                  .otherwise("Medium")
                                  )

clv_tagged.write.mode("overwrite").partitionBy("snapshot_date") \
.parquet("s3://globalpartners-raw/gold/customer_clv_daily/")