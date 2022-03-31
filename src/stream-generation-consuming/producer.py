import json
import random
import string
import time
from datetime import datetime

from confluent_kafka import Producer


def serializer(message):
    return json.dumps(message).encode('utf-8')


# Kafka Producer
producer = Producer({
    'bootstrap.servers': ['localhost:9092']
})

if __name__ == '__main__':
    # Infinite loop - runs until you kill the program

    while True:
        # Generate a message
        dummy_message = ''.join(random.choice(string.ascii_letters) for i in range(32))

        # Send it to our 'messages' topic
        print(f'Producing message @ {datetime.now()} | Message = {dummy_message}')
        producer.produce('simple', dummy_message.encode('utf-8'))

        # Sleep for a random number of seconds
        time_to_sleep = random.randint(1, 11)
        time.sleep(time_to_sleep)
