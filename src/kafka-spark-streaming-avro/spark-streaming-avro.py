import string

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
    schema_inline = string('''{ 
        "namespace": "netflow.avro",
        "type": "record",
        "name": "netflow",
        "fields": [
          { "name": "event_type", "type": "string"},
          { "name": "peer_ip_src", "type": "string"},
          { "name": "ip_src", "type": "string"},
          { "name": "ip_dst", "type": "string"},
          { "name": "port_src", "type": "long"},
          { "name": "port_dst", "type": "long"},
          { "name": "tcp_flags", "type": "long"},
          { "name": "ip_proto", "type": "string"},
          { "name": "timestamp_start", "type": "string"},
          { "name": "timestamp_end", "type": "string"},
          { "name": "timestamp_arrival", "type": "string"},
          { "name": "export_proto_seqno", "type": "long"},
          { "name": "export_proto_version", "type": "long"},
          { "name": "packets", "type": "long"},
          { "name": "flows", "type": "long"},
          { "name": "bytes", "type": "long"},
          { "name": "writer_id", "type": "string"}
        ]
      }''')

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "netflow") \
        .option("startingOffsets", "earliest") \
        .load()

    value_df = kafka_df.select(from_avro(col("value"), schema_inline).alias("value"))

    word_count_query = value_df.writeStream \
        .format("console") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("append") \
        .start()

    # Sink
    # Stream Processing application will only terminate when you Manual Stop or Kill or Exception & shut down gracefully
    word_count_query.awaitTermination()
