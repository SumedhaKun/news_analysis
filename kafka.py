from confluent_kafka import Producer
import json
import socket

bootstrap_servers = 'localhost:9092'
conf = {
    'bootstrap.servers': bootstrap_servers,
    'client.id': 'my_client_id',
}
producer = Producer(conf)

def send_data(data,topic):
    
    # Convert Python dictionary to JSON string
    json_data = json.dumps(data)

    # Delivery callback function
    def delivery_callback(err, msg):
        if err:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    # Produce JSON message to Kafka topic
    producer.produce(topic, key='key', value=json_data.encode('utf-8'), callback=delivery_callback)
    producer.flush()

