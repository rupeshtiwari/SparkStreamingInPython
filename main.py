# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # we want to stop application gracefully while we quit or kill or on exception so add
    # 'spark.streaming.stopGracefullyOnShutdown' to true
    # We are using groupBy transformation that will internally cause a shuffle operation.
    # That default shuffle partition configuration is 200 so our app will run slow
    # So you should set the spark shuffle partition value to 3
    spark = SparkSession.builder.appName("Streaming word count") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    # Read
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()

    # Transform
    # checkpointLocation is mandatory dataframe writer will use to store the progress information
    words_df = lines_df.select(expr("explode(split(value,' ')) as word"))
    counts_df = words_df.groupby("word").count()
    word_count_query = counts_df.writeStream \
        .format("console") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("complete") \
        .start()

    # Sink
    # Stream Processing application will only terminate when you Manual Stop or Kill or Exception & shut down gracefully
    word_count_query.awaitTermination()
