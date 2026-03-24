import pika
import os
import json
import db_writer

def process_iot_event(body):
    data = json.loads(body)
    print("received:", data)
    db_writer.insert_iot_event(data)

rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
params = pika.URLParameters(rabbitmq_url)
connection = pika.BlockingConnection(params)
channel = connection.channel()

channel.exchange_declare(exchange='iot_fanout', exchange_type='fanout')

# Dead Letter Exchange for ingestion
channel.exchange_declare(exchange='ingestion_dead_letter', exchange_type='direct')
channel.queue_declare(queue='ingestion_dead_letter', durable=True)
channel.queue_bind(
    exchange='ingestion_dead_letter',
    queue='ingestion_dead_letter',
    routing_key='ingestion_queue'
)

# ingestion_queue with DLQ policy
channel.queue_declare(
    queue='ingestion_queue',
    durable=True,
    arguments={
        'x-dead-letter-exchange': 'ingestion_dead_letter',
        'x-dead-letter-routing-key': 'ingestion_queue',
    }
)
channel.queue_bind(exchange='iot_fanout', queue='ingestion_queue')
channel.basic_qos(prefetch_count=1)

def callback(ch, method, properties, body):
    try:
        process_iot_event(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        print("error:", e)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_consume(queue='ingestion_queue', on_message_callback=callback)
print("waiting for messages...")
channel.start_consuming()