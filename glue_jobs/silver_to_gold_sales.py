from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("silver_to_gold_sales").getOrCreate()

orders = spark.read.parquet("s3://restaruant-raw/silver/order_items_cleaned/")
date_dim = spark.read.parquet("s3://restaruant-raw/bronze/date_dim/")

sales = (orders
         .groupBy("order_date", "restaurant_id", "item_category")
         .agg(F.sum("line_revenue").alias("total_revenue"),
               F.countDistinct("order_id").alias("order_count"))
         .withColumnRenamed("order_date", "sales_date"))

# Attach calendar context (week, month, year, weekend/holiday flags)
sales_calendar = (sales
                  .join(date_dim, sales.sales_date == date_dim.date_key, "left")
                  .drop("date_key"))

(sales_calendar.write.mode("overwrite")
    .partitionBy("year", "month")
    .parquet("s3://restaruant-raw/gold/sales_trends_daily/"))