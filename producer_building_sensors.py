from kafka import KafkaProducer
from configs import kafka_config
import json, time, uuid, random
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: json.dumps(k).encode('utf-8')
)

topic = "ola_building_sensors"
sensor_id = str(uuid.uuid4())


for i in range(30):
    message = {
        "sensor_id": sensor_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(25, 45), 2),
        "humidity": round(random.uniform(15, 85), 2)
    }
    producer.send(topic, key=str(i), value=message)
    producer.flush()
    print(f"Sent message {i}: {message}")
    time.sleep(1)

producer.close()
