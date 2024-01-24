'''Kafka Consumer in Python'''

import psycopg2
from kafka import KafkaProducer

def fetch_data_from_postgresql():
    # Connect to PostgreSQL
    connection = psycopg2.connect(
        host="localhost",
        database="prism_db",
        user="tad",
        password="tad"
    )
    cursor = connection.cursor()
    print("cursor created ...")

    # Fetch data from PostgreSQL
    cursor.execute("SELECT * FROM m_product LIMIT 1")
    data = cursor.fetchall()
    print("Data featch ...")

    # Close the cursor and connection
    cursor.close()
    connection.close()

    return data

def produce_to_kafka(data):
    # Kafka producer configuration
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'mytopic'
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda v: str(v).encode('utf-8')
    )

    # Produce messages to Kafka
    print("Producing messages to Kafka:")
    for row in data:
        message = ', '.join(map(str, row))
        producer.send(kafka_topic, value=message)
        print(f"Message produced: {message}")

    # Close the Kafka producer
    producer.close()

if __name__ == "__main__":
    # Fetch data from PostgreSQL
    data = fetch_data_from_postgresql()

    # Produce messages to Kafka
    produce_to_kafka(data)
