# Real-Time Stream Processing Using Apache Spark 3 for Python Developers

> Write spark streaming application to listen to port 9999 and read the sentence and print the word count.
> Read socket data source and process in real-time. It is not design for production usage this is good for testing & learning.

**Spark Streaming Sources**

1. Socket source (non-production)
2. Rate source (benchmarking & testing spark cluster)
3. File source (most commonly used in production)
4. Kafka source (most commonly used in production)

**What are we doing in this example?**

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
    spark = SparkSession.builder.appName("Streaming word count")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()

lines_df = spark.readStream
.format("socket")
.option("host", "localhost")
.option("port", "9999")
.load()

# checkpointLocation is mandatory dataframe writer will use to store the progress information
words_df = lines_df.select(expr("explode(split(value,' ')) as word"))
counts_df = words_df.groupby("word").count()
word_count_query = counts_df.writeStream
.format("console")
.option("checkpointLocation", "chk-point-dir")
.outputMode("complete")
.start()

# Stream Processing application will only terminate when you Manual Stop or Kill or Exception & shut down gracefully
word_count_query.awaitTermination()

```

## Task 3: Now type something on terminal and hit enter

![](https://i.imgur.com/wSAWPo2.png)

## Spark Micro Batch

![](https://i.imgur.com/2Fp4REK.png)

![](https://i.imgur.com/sBs6gYd.png)

Background thread will keep triggering the micro-batch job to read, transform and sink the stream data.

![](https://i.imgur.com/9XZ8DV1.png)

You will see the micro batch job on Spark Context UI
http://localhost:4040/jobs/job/?id=1

**There are 4 triggers for micro-batch:**

1. Unspecified, 2. Time Interval, 3. One time, 4. Continuous ( millisecond latency )
   ![](https://i.imgur.com/oowkElh.png)

## Setup single node Kafka Cluster on your local MACOS laptop

### Task 1: Download instal kafka

1. download the kafka https://kafka.apache.org/downloads. I downloaded `kafka_2.12-3.1.0`. `server.properties` is used
   by kafka broker.
2. `dataDir=../kafka-logs/zookeeper` on `zookeeper.properties`. This is zookeeper data directory.
3. Uncomment the server socket listener & update `log.dirs` kafka data directory on `server.properties`
    - `listeners=PLAINTEXT://:9092`
    - `dataDir=../kafka-logs/zookeeper`
4. Set kafka home directory up to bin folder in your `.zshrc` file. export `KAFKA_HOME=/Users/rupeshti/kafka2`
5. Add Kafka Bin to PATH
   `export PATH=$PATH:$KAFKA_HOME/bin`
   ![](https://i.imgur.com/o462Xtp.png)

### Task 2: Start the KAFKA

Starting kafka is 2 steps process. Start the zookeeper and start the kafka broker.

```
# Start the ZooKeeper service
# Note: Soon, ZooKeeper will no longer be required by Apache Kafka.
$ bin/zookeeper-server-start.sh config/zookeeper.properties
```

Open another terminal session and run:

```
# Start the Kafka broker service
$ bin/kafka-server-start.sh config/server.properties
```

![](https://i.imgur.com/GYkgIAa.png)

### Task 3: Create Topic, Producer & Consumer

1. Create Topic
2. `bin/kafka-topics.sh --create --topic quickstart-events --bootstrap-server localhost:9092`
3. WRITE SOME EVENTS INTO THE TOPIC
4. `bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092`
   ![](https://i.imgur.com/uxtTAuq.png)
5. READ THE EVENTS
   ![](https://i.imgur.com/l0EdQsU.png)

Learn more about [kafka environment](https://kafka.apache.org/documentation/#introduction)

## Spark Streaming with Apache Kafka

Stream processing with spark works with Apache Kafka mostly. File data-source is used for minute based micro batching.
However, for low latency streaming file-based streaming will not work. Therefore, Apache Kafka is best for streaming
data source.
![](https://i.imgur.com/mMmZrs1.png)

**Apache kafka is working as streaming data-source**. Suppose your invoices are coming to apache kafka. Create
application

1. Read the data from the Kafka Topic
2. Flatten the invoice records
3. Sink the outcome to a file system

Spark Sql & Kafka integration happens by another project called
as [Kafka source for structured streaming](https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10).

Add below on `spark-defaults.cof` file. When you start your pyspark application it will read this and automatically
download the package.

```
spark.jars.packages  org.apache.spark:spark-sql-kafka-0-10_2.13:3.2.1
``` 

You can also add this on your application code however, it is not good practice. So keep it on spark default file.

## References

- https://learning.oreilly.com/videos/real-time-stream-processing
- https://kafka.apache.org/documentation/#introduction 