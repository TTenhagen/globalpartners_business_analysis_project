from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, TimestampType

spark = SparkSession.builder.appName("bronze_to_silver_orders").getOrCreate()
# order_items
df = spark.read.parquet("s3://globalpartners-raw/bronze/order_items/")
df_clean = (
    df
    .withColumn("user_id", F.col("user_id").cast("string"))
    .withColumn("order_id", F.col("order_id").cast("string"))
    .withColumn("lineitem_id", F.col("lineitem_id").cast("string"))
    .withColumn("restaurant_id", F.col("restaurant_id").cast("string"))
    .withColumn("creation_time_utc", F.to_timestamp("creation_time_utc"))
    .withColumn("order_date", F.to_date("creation_time_utc"))
    .withColumn("item_price", F.col("item_price").cast(DecimalType(10,2)))
    .withColumn("item_quantity", F.col("item_quantity").cast(IntegerType()))
    .withColumn("line_revenue", F.col("item_price") * F.col("item_quantity"))
    .dropDuplicates(["order_id", "lineitem_id"])
    )

df_clean.write.mode("overwrite").partitionBy("restaurant_id") \
.parquet("s3://globalpartners-raw/silver/order_items_cleaned/")

# order_item_options
opts = spark.read.parquet("s3://globalpartners-raw/bronze/order_item_options/")

opts_clean = (
    opts
    .withColumn("order_id", F.col("order_id").cast("string"))
    .withColumn("lineitem_id", F.col("lineitem_id").cast("string"))
    .withColumn("option_price", F.col("option_price").cast(DecimalType(10,2)))
    .withColumn("option_quantity", F.col("option_quantity").cast(IntegerType()))
# Note: NEGATIVE option_price is valid (discounts) -- do NOT filter it out
    .filter(F.col("option_quantity") > 0)
# A lineitem can have many options; dedupe on the full composite grain
    .dropDuplicates(["order_id", "lineitem_id", "option_group_name", "option_name"])
    )

opts_clean.write.mode("overwrite") \
.parquet("s3://globalpartners-raw/silver/order_item_options_cleaned/")
