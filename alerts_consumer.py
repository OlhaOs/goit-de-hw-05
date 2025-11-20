from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config
import json

consumer = KafkaConsumer(
    "ola_building_sensors",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    key_deserializer=lambda k: json.loads(k.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='filter_group'
)

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: json.dumps(k).encode('utf-8')
)

for msg in consumer:
    data = msg.value
    if data['temperature'] > 40:
        producer.send("ola_temperature_alerts", key=data['sensor_id'], value=data)
        print("⚠️ Temperature alert:", data)
    if data['humidity'] > 80 or data['humidity'] < 20:
        producer.send("ola_humidity_alerts", key=data['sensor_id'], value=data)
        print("⚠️ Humidity alert:", data)
    producer.flush()

consumer.close()
producer.close()
