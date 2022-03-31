from configparser import ConfigParser

from confluent_kafka import Consumer

if __name__ == '__main__':
    # Kafka Consumer
    config_parser = ConfigParser()
    config = dict()
    config["bootstrap.servers"] = 'localhost:9092'
    config["group.id"] = 'my-group'
    consumer = Consumer(config)
    consumer.subscribe(["my-topic"])
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            print("Waiting...")
        elif msg.error():
            print("ERROR: %s".format(msg.error()))
        else:
            print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
