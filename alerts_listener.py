from kafka import KafkaConsumer
from configs import kafka_config
import json

topics = ["ola_temperature_alerts", "ola_humidity_alerts"]

consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: json.loads(k.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='alerts_group'
)

print(f"Subscribed to alerts: {topics}")

try:
    for msg in consumer:

        
        print(f"[{msg.topic}] {msg.value}")
        # print(f"[{msg.topic.upper()}] Alert received from sensor {msg.key}: {msg.value}")
except Exception as e:
    print("An error occurred:", e)
finally:
    consumer.close()
