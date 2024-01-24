from confluent_kafka import Consumer, KafkaError
import json

def save_to_file(file_name, data):
    with open(file_name, 'a') as f:
        f.write(json.dumps(data) + '\n')

# Define Kafka broker(s)
bootstrap_servers = 'localhost:9092'

# Create Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': '1',  # Provide a unique consumer group ID
    'auto.offset.reset': 'earliest',  # Specify the start offset: earliest or latest
}

# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the topic(s) from which you want to consume messages
topic_name = 'mytopic'
consumer.subscribe([topic_name])

# Continuously poll for messages
try:
    while True:
        msg = consumer.poll(timeout=1000)  # Adjust timeout as needed

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event, not an error
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # Process the received message
        print(f"Received message: {msg.value().decode('utf-8')}")
        save_to_file('data.txt', str(msg.value()))

except KeyboardInterrupt:
    pass

