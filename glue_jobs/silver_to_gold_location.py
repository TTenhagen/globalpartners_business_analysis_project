from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("silver_to_gold_location").getOrCreate()

orders = spark.read.parquet("s3://restaurant-raw/silver/order_items_cleaned/")

location = (orders
            .groupBy("restaurant_id")
            .agg(F.sum("line_revenue").alias("total_revenue"),
                 F.countDistinct("order_id").alias("order_count"),
                 F.countDistinct("order_date").alias("active_days"))
            .withColumn("avg_order_value",
                         F.round(F.col("total_revenue") / F.col("order_count"), 2))
            .withColumn("orders_per_day",
                         F.round(F.col("order_count") / F.col("active_days"), 2))
            .withColumn("revenue_rank",
                         F.dense_rank().over(Window.orderBy(F.desc("total_revenue"))))
            )

(location.select("restaurant_id", "total_revenue", "avg_order_value",
                 "orders_per_day", "revenue_rank")
         .write.mode("overwrite")
         .parquet("s3://restaurant-raw/gold/location_performance/"))
