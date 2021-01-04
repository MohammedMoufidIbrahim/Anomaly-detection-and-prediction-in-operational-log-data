import os
import json
from time import sleep
from kafka import KafkaProducer
from logs import send_message

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")
TRANSACTIONS_PER_SECOND = float(os.environ.get("TRANSACTIONS_PER_SECOND"))
SLEEP_TIME = 1 # / TRANSACTIONS_PER_SECOND

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        # Encode all values as JSON
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    while True:
        # transaction: dict = send_message()
        transaction: dict = send_message()
        producer.send(TRANSACTIONS_TOPIC, value=transaction)
        print(transaction)  # DEBUG
        sleep(SLEEP_TIME)
