"""
You will learn how to create Kafka producer and Consumer in python.
"""
from kafka import KafkaProducer

bootstrap_servers = ['localhost:9092']
topicName = 'myTopic'
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
ack = producer.send(topicName, b'Hello world!!!')
metadata = ack.get()
print(metadata.topic)
print(metadata.partition)
