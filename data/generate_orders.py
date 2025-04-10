import csv
import random
from datetime import datetime, timedelta

countries = ['US', 'IN', 'UK', 'CA', 'GE', 'FR']
channels = ['online', 'store', 'mobile']
products = [f'P{100+i}' for i in range(50)]

with open('data/sample_orders.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['order_id', 'product_id', 'quantity', 'price', 'country', 'channel', 'timestamp'])

    base_time = datetime(2025, 4, 7, 10, 0)

    for i in range(100):
        order_id = f'ORD{i+1:03d}'
        product_id = random.choice(products)
        quantity = random.randint(1, 5)
        price = round(random.uniform(10, 200), 2)
        country = random.choice(countries)
        channel = random.choice(channels)
        timestamp = (base_time + timedelta(seconds=i*15)).isoformat() + 'Z'

        writer.writerow([order_id, product_id, quantity, price, country, channel, timestamp])
