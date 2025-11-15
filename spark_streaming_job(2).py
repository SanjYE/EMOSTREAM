from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import redis
import json

spark = SparkSession.builder \
    .appName("EmojiAggregator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("emoji_type", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

emoji_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emoji_topic") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = emoji_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
    
parsed_df = parsed_df.na.drop(subset=["timestamp"])

aggregated_df = parsed_df.groupBy("emoji_type").count()

def send_to_redis(df, epoch_id):
    if df.rdd.isEmpty():
        print(f"Epoch {epoch_id}: DataFrame is empty. Skipping this batch.")
        return

    r = redis.Redis(host='localhost', port=6379, db=0)
    
    counts = df.collect()
    
    emoji_counts = {}
    for row in counts:
        emoji_type = row['emoji_type']
        count = row['count']
        emoji_counts[emoji_type] = int(count)  
        
    if emoji_counts:
        r.delete('emoji_counts')
        r.hset('emoji_counts', mapping=emoji_counts)
        for emoji_type, count in emoji_counts.items():
            data = {
                'emoji_type': emoji_type,
                'count': count 
            }
            r.publish('emoji_updates', json.dumps(data))
    else:
        print(f"Epoch {epoch_id}: No counts to update.")

query = aggregated_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(send_to_redis) \
    .option("checkpointLocation", "/tmp/spark_checkpoint") \
    .trigger(processingTime='2 seconds') \
    .start()

query.awaitTermination()
