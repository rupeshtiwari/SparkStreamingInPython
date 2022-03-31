from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Kafka Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    # Read
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "simple") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select((col("value").cast("string")).alias("value"))
    word_count_query = value_df.writeStream \
        .format("console") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("append") \
        .start()

    # Sink
    # Stream Processing application will only terminate when you Manual Stop or Kill or Exception & shut down gracefully
    word_count_query.awaitTermination()
