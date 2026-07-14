from pyspark.sql import functions as F

options = spark.read.parquet("s3://globalpartners-raw/silver/order_item_options_cleaned/")
orders = spark.read.parquet("s3://globalpartners-raw/silver/order_items_cleaned/")

discount_flag = (options
                 .groupBy("order_id")
                 .agg(F.min("option_price").alias("min_option_price"))
                 .withColumn("has_discount", F.col("min_option_price") < 0))

order_revenue = orders.groupBy("order_id").agg(F.sum("line_revenue").alias("order_revenue"))

discount_analysis = (order_revenue
                     .join(discount_flag, "order_id", "left")
                     .withColumn("has_discount", F.coalesce(F.col("has_discount"), F.lit(False)))
                     .groupBy("has_discount")
                     .agg(
                         F.count("order_id").alias("order_count"),
                         F.avg("order_revenue").alias("avg_order_revenue"),
                         F.sum("order_revenue").alias("total_revenue")))

discount_analysis.write.mode("overwrite") \
.parquet("s3://globalpartners-raw/gold/discount_effectiveness/")