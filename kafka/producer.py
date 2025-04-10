import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda v: v.encode('utf-8')  # for partitioning if needed
)

def get_topic(country):
    if country == 'US':
        return 'retail_orders_us'
    elif country == 'IN':
        return 'retail_orders_in'
    else:
        return 'retail_orders_others'

with open('data/sample_orders.csv', 'r') as file:
    reader = csv.DictReader(file)

    for row in reader:
        topic = get_topic(row['country'])
        key = row['country']  # could be used for partitioning logic
        print(f"Sending to {topic}: {row}")
        producer.send(topic=topic, key=key, value=row)
        time.sleep(0.5)  # simulate stream delay

producer.flush()
producer.close()
