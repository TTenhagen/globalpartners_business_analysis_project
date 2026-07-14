from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("silver_to_gold_loyalty").getOrCreate()

orders = spark.read.parquet("s3://restaruant-raw/silver/order_items_cleaned/")
clv = spark.read.parquet("s3://restaruant-raw/gold/customer_clv_daily/")

# Latest CLV snapshot per customer
latest = clv.groupBy("user_id").agg(F.max("snapshot_date").alias("snapshot_date"))
latest_clv = clv.join(latest, ["user_id", "snapshot_date"]) \\
                     .select("user_id", "clv_to_date")

# Per-customer rollup, keeping the loyalty flag
per_customer = (orders
                .groupBy("user_id", "is_loyalty")
                .agg(F.sum("line_revenue").alias("total_spend"),
                     F.countDistinct("order_id").alias("order_count"))
                .withColumn("repeat_orders",
                            F.when(F.col("order_count") > 1, F.col("order_count") - 1).otherwise(0))
                .join(latest_clv, "user_id", "left"))

loyalty = (per_customer
           .groupBy("is_loyalty")
           .agg(F.round(F.avg("total_spend"), 2).alias("avg_spend"),
                F.sum("repeat_orders").alias("repeat_orders"),
                F.round(F.avg("clv_to_date"), 2).alias("avg_clv"),
                F.countDistinct("user_id").alias("customer_count")))

loyalty.write.mode("overwrite") \\
    .parquet("s3://restaruant-raw/gold/loyalty_program_comparison/")