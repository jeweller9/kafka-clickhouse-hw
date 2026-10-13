import csv
import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
CSV_PATH = os.getenv("CSV_PATH", "data/train.csv")


def create_producer():
    for attempt in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Connected to Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1}/10 - waiting 5s...")
            time.sleep(5)
    raise RuntimeError("Could not connect to Kafka")


def stream_csv_to_kafka():
    producer = create_producer()
    with open(CSV_PATH, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        print(f"Columns: {reader.fieldnames}")
        for i, row in enumerate(reader, start=1):
            message = {k: (str(v) if v is not None else "") for k, v in row.items()}
            producer.send(KAFKA_TOPIC, value=message)
            if i % 1000 == 0:
                print(f"Sent {i} rows...")
                producer.flush()
        producer.flush()
        print(f"Total sent: {i}")
    producer.close()
    print("Producer finished")


if __name__ == "__main__":
    stream_csv_to_kafka()
