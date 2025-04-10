from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='retail_admin'
)

topics = [
    NewTopic(name="retail_orders_us", num_partitions=2, replication_factor=1),
    NewTopic(name="retail_orders_in", num_partitions=2, replication_factor=1),
    NewTopic(name="retail_orders_others", num_partitions=2, replication_factor=1)
]

try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Topics created.")
except Exception as e:
    print("Topics may already exist:", e)
