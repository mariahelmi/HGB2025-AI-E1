from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lower, count, window, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# 1. Configuration & Session Setup
CHECKPOINT_PATH = "/tmp/spark-checkpoints/crash-monitoring"
spark = (
    SparkSession.builder
    .appName("CrashMonitor")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# 2. Define schema
schema = StructType([
    StructField("timestamp", LongType()), 
    StructField("status", StringType()),
    StructField("severity", StringType()),
    StructField("source_ip", StringType()),
    StructField("user_id", StringType()),
    StructField("content", StringType())
])

# 3. Read Stream
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "logs")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

# 4. Processing, Filtering & Aggregation
# We filter first to keep the state store small, then group and count.
analysis_df = (
    raw_df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp") / 1000)))
    .filter(
        (lower(col("content")).contains("crash")) & 
        (col("severity").isin("High", "Critical"))
    )
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("user_id")
    )
    .agg(count("*").alias("crash_count"))
    .filter(col("crash_count") > 2)
    .select(
        col("window").alias("Interval"),
        col("user_id"),
        col("crash_count")
    )
)

# 5. Writing
query = (
    analysis_df.writeStream
    .outputMode("update") 
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
