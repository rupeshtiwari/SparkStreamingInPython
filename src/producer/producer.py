"""
python3 producer.py <topic> <number_of_messages>
python3 producer.py order 10
"""
import random
import string
import sys

from kafka import KafkaProducer


def get_message():
    letters = string.ascii_uppercase
    return str.encode(''.join(random.choice(letters) for i in range(10)))


bootstrap_servers = ['localhost:9092']
topicName = sys.argv[1]
iteration = int(sys.argv[2])
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
for x in range(iteration):
    message = get_message()
    producer.send(topicName, message)
    print("Sending message " + message.decode())
