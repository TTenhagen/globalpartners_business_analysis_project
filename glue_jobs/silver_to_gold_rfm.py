from pyspark.sql import functions as F

orders = spark.read.parquet("s3://globalpartners-raw/silver/order_items_cleaned/")
as_of_date = F.current_date()
lookback_months = 6

rfm_base = (orders
            .filter(F.col("order_date") >= F.add_months(as_of_date, -lookback_months))
            .groupBy("user_id")
            .agg(
                F.max("order_date").alias("last_order_date"),
                F.countDistinct("order_id").alias("frequency"),
                F.sum("line_revenue").alias("monetary")))

rfm_scored = (rfm_base
              .withColumn("recency_days", F.datediff(as_of_date, F.col("last_order_date")))
              .withColumn("r_score", F.when(F.col("recency_days") <= 7, 5)
                           .when(F.col("recency_days") <= 30, 3)
                           .otherwise(1))
              .withColumn("f_score", F.when(F.col("frequency") >= 10, 5)
                           .when(F.col("frequency") >= 3, 3)
                           .otherwise(1))
              .withColumn("m_score", F.when(F.col("monetary") >= 500, 5)
                           .when(F.col("monetary") >= 100, 3)
                           .otherwise(1)))
rfm_segmented = (rfm_scored
                 .withColumn("segment",
                             F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), "VIP")
                             .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), "New Customer")
                             .when((F.col("r_score") <= 2) & (F.col("f_score") <= 2), "Churn Risk")
                             .otherwise("Regular")))

rfm_segmented.write.mode("overwrite") \
.parquet("s3://globalpartners-raw/gold/customer_rfm_segments/")
