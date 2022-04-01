from pyspark.sql import SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Multi Query Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    # in-line schema for netflow

    jsonFormatSchema = open("netflow.avsc", "r").read()
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "netflow") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(from_avro(col("value"), jsonFormatSchema).alias("value"))

    word_count_query = value_df.writeStream \
        .format("console") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("append") \
        .start()

    # Sink
    # Stream Processing application will only terminate when you Manual Stop or Kill or Exception & shut down gracefully
    word_count_query.awaitTermination()
