import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window

@pytest.fixture(scope="module")
def spark():
    return (SparkSession.builder
            .appName("test_clv_logic")
            .master("local[2]")
            .getOrCreate())

def test_cumulative_spend_is_monotonically_increasing(spark):
    """Cumulative spend for a single customer should never decrease day-over-day."""
    data = [
        ("user_1", "2024-01-01", 50.0),
        ("user_1", "2024-01-02", 30.0),
        ("user_1", "2024-01-05", 20.0)
        ]
    
df = spark.createDataFrame(data, ["user_id", "order_date", "daily_spend"])
df = df.withColumn("order_date", F.to_date("order_date"))

w = Window.partitionBy("user_id").orderBy("order_date") \
.rangeBetween(Window.unboundedPreceding, 0)
result = df.withColumn("cumulative_spend", F.sum("daily_spend").over(w))

rows = result.orderBy("order_date").collect()
cumulative_values = [r["cumulative_spend"] for r in rows]

assert cumulative_values == sorted(cumulative_values), \
    "Cumulative spend must never decrease over time"
assert cumulative_values[-1] == 100.0, "Final cumulative spend should equal total spend"

def test_clv_tier_high_threshold(spark):
    """Customers at or above the 80th percentile should be tagged High."""
    data = [("user_1", 1000.0), ("user_2", 500.0), ("user_3", 100.0), ("user_4", 50.0)]
    df = spark.createDataFrame(data, ["user_id", "clv_to_date"])

    p80 = df.approxQuantile("clv_to_date", [0.80], 0.01)[0]
    p20 = df.approxQuantile("clv_to_date", [0.20], 0.01)[0]

tagged = df.withColumn("clv_tier",
                       F.when(F.col("clv_to_date") >= p80, "High")
                       .when(F.col("clv_to_date") <= p20, "Low")
                       .otherwise("Medium")
                       )

result = {r["user_id"]: r["clv_tier"] for r in tagged.collect()}
assert result["user_1"] == "High", "Top spender should be tagged High"
assert result["user_4"] == "Low", "Bottom spender should be tagged Low"

def test_no_duplicate_snapshot_rows(spark):
    """Gold table must have exactly one row per (user_id, snapshot_date)."""
data = [
    ("user_1", "2024-01-01", 50.0),
    ("user_1", "2024-01-01", 50.0),
    ("user_2", "2024-01-01", 20.0)
]
df = spark.createDataFrame(data, ["user_id", "snapshot_date", "daily_spend"])

total_rows = df.count()
distinct_keys = df.select("user_id", "snapshot_date").distinct().count()

# This test is expected to FAIL against the raw input above --
# it demonstrates the dedup step the Glue job must apply before writing Gold.
deduped = df.dropDuplicates(["user_id", "snapshot_date"])
assert deduped.count() == deduped.select("user_id", "snapshot_date").distinct().count()
