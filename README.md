# Spark Streaming Example
> Write spark streaming application to listen to port 9999 and read the sentence and print the word count. 
1. Create Spark streaming project using your IDE
2. Typical structure of spark streaming application
    1. Read Streaming Source - Input Dataframe
    2. Process and Transform input Dataframe - Output Dataframe
    3. Write the output - Streaming Sink
## Task 1: Create Server and run on port 9999 

Open terminal and run below to create server 
```
nc -lk 9999
```
## Task 2: Write Spark Streaming Application to listen to port 9999

Use Pycharm Community Edition free IDE to run spark code 

```python
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

    lines_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()

    # checkpointLocation is mandatory dataframe writer will use to store the progress information
    words_df = lines_df.select(expr("explode(split(value,' ')) as word"))
    counts_df = words_df.groupby("word").count()
    word_count_query = counts_df.writeStream \
        .format("console") \
        .option("checkpointLocation", "chk-point-dir") \
        .outputMode("complete") \
        .start()

    # Stream Processing application will only terminate when you Manual Stop or Kill or Exception & shut down gracefully
    word_count_query.awaitTermination()

```
## Task 3: Now type something on terminal and hit enter
![](https://i.imgur.com/wSAWPo2.png)

## Spark Background Thread
![](https://i.imgur.com/2Fp4REK.png)

![](https://i.imgur.com/sBs6gYd.png)

Background thread will keep triggering the micro-batch job to read, transform and sink the stream data. 

![](https://i.imgur.com/9XZ8DV1.png)

You will see the micro batch job on Spark Context UI 
http://localhost:4040/jobs/job/?id=1 