from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config

admin_client = KafkaAdminClient(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password']
)

topics = [
    NewTopic(name="ola_building_sensors", num_partitions=2, replication_factor=1),
    NewTopic(name="ola_temperature_alerts", num_partitions=1, replication_factor=1),
    NewTopic(name="ola_humidity_alerts", num_partitions=1, replication_factor=1),
]

try:
    admin_client.create_topics(new_topics=topics, validate_only=False)
    print("Topics created successfully")
except Exception as e:
    print("Error creating topics:", e)

print([topic for topic in admin_client.list_topics() if "ola_" in topic])

admin_client.close()
