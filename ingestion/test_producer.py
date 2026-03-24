import pika
import json
import time
import random
import uuid
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))

channel = connection.channel()

# channel.queue_declare(queue="iot_events")

channel.exchange_declare(exchange='iot_fanout', exchange_type='fanout')

while True:

    data = {
        "event_id":str(uuid.uuid4()),
        "buoy_id": 1,
        "air_temperature": random.uniform(25,30),
        "air_humidity": random.uniform(60,90),
        "air_pressure": 1013,
        "particle_matter_01": random.uniform(1,10),
        "particle_matter_25": random.uniform(1,15),
        "particle_matter_10": random.uniform(1,20),
        "luminosity": random.uniform(100,300),
        "co2_concentration": random.uniform(400,600),
        "turbidity": random.uniform(0,5),
        "water_temperature": random.uniform(25,30),
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        # "battery_level": random.uniform(70,100),
        # "latitude": 13.2,
        # "longitude": 100.5,
        # "created_at": "2026-03-08T10:00:00"
    }

    channel.basic_publish(
        exchange="iot_fanout",
        routing_key="",
        body=json.dumps(data)
    )

    print("sent message")

    time.sleep(2)