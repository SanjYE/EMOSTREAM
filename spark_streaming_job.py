from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, lit, window, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as F
import redis
import json

spark = SparkSession.builder \
    .appName("EmojiAggregator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", StringType(), True)
])

emoji_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emoji_topic") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = emoji_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
)

# Aggregate counts in a 2-second window
aggregated_df = parsed_df \
    .withWatermark("timestamp", "2 seconds") \
    .groupBy(
        window(col("timestamp"), "2 seconds"),
        col("emoji_type")
    ) \
    .agg(
        F.count("*").alias("count")
    )

aggregated_df = aggregated_df.withColumn(
    "scaled_count",
    when(col("count") <= 1000, lit(1)).otherwise(col("count"))
)

aggregated_df = aggregated_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "emoji_type",
    "count",
    "scaled_count"
)

def send_to_redis(df, epoch_id):

    r = redis.Redis(host='localhost', port=6379, db=0)
    data = df.toJSON().collect()

    for record in data:
        record_dict = json.loads(record)
        record_json = json.dumps(record_dict, ensure_ascii=False)
        r.publish('emoji_updates', record_json)

query = aggregated_df.writeStream \
    .outputMode("append") \
    .foreachBatch(send_to_redis) \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .trigger(processingTime='2 seconds') \
    .start()

query.awaitTermination()
