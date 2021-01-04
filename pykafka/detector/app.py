# detector/app.py
import os
import json
from kafka import KafkaConsumer, KafkaProducer
from main import Factory
from pymongo import MongoClient
#from lstm import predict

KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL")
TRANSACTIONS_TOPIC = os.environ.get("TRANSACTIONS_TOPIC")
LEGIT_TOPIC = os.environ.get("LEGIT_TOPIC")
FRAUD_TOPIC = os.environ.get("FRAUD_TOPIC")


def is_suspicious(transaction: dict) -> bool:
    """Determine whether a transaction is suspicious."""
    # Add an LSTM model later
    #
    # if predict(transaction[0])>0:
    #   db.logs.insert_one(transaction[0])
    # else:
    #   db.anomaly.insert_one(transaction[0])
    return transaction[0]["event_start_ms"] >= 900


if __name__ == "__main__":
    consumer = KafkaConsumer(
        TRANSACTIONS_TOPIC,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda value: json.loads(value),
    )
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda value: json.dumps(value).encode(),
    )
    client = MongoClient('mongodb://admin:secret@localhost:27888/?authSource=admin')
    db = client.logs
    for message in consumer:
        transaction: dict = message.value
        topic = FRAUD_TOPIC if is_suspicious(transaction) else LEGIT_TOPIC
        # if topic ==  LEGIT_TOPIC:
        #    Factory().run(transaction[0])
            #db.logs.insert_one(message.value)
        producer.send(topic, value=transaction)
        print(transaction[0])  # DEBUG