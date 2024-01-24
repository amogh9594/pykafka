# pykafka
## postgres data push into txt file using Apache Kafka 
![DB TO TXT](https://github.com/amogh9594/pykafka/blob/main/kafka-2.png)

### Kafka-Producer

### Step 1 : Import necessary modules
```
import psycopg2
from kafka import KafkaProducer
```
The script imports the psycopg2 module for connecting to PostgreSQL and the KafkaProducer class from the kafka module for producing messages to Kafka.

Pip Install
```
pip install psycopg2-binary
pip install kafka-python
```
### Step 2 : Define function to fetch data from PostgreSQL
```
def fetch_data_from_postgresql():
    # Connect to PostgreSQL
    connection = psycopg2.connect(
        host="localhost",
        database="db",
        user="xxx",
        password="xxx"
    )
    cursor = connection.cursor()
    print("cursor created ...")

    # Fetch data from PostgreSQL
    cursor.execute("SELECT * FROM product LIMIT 1")
    data = cursor.fetchall()
    print("Data fetch ...")

    # Close the cursor and connection
    cursor.close()
    connection.close()

    return data
```

This function (fetch_data_from_postgresql) connects to a PostgreSQL database using the psycopg2 library, executes a SELECT query (SELECT * FROM product LIMIT 1), fetches the result, prints a message, and then closes the cursor and connection. Finally, it returns the fetched data.

### Step 3: Define function to produce data to Kafka
```
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
```
This function (produce_to_kafka) takes the fetched data as an argument, configures a Kafka producer with the specified bootstrap servers and topic, and then iterates over the rows of data. For each row, it converts the row elements to a string, joins them with a comma, and sends the resulting message to the Kafka topic. It also prints a message for each produced message. Finally, it closes the Kafka producer.

### Step 4: Main script execution
```
if __name__ == "__main__":
    # Fetch data from PostgreSQL
    data = fetch_data_from_postgresql()

    # Produce messages to Kafka
    produce_to_kafka(data)

```
In the main block, the script checks if it's being run as the main module. If so, it calls the fetch_data_from_postgresql function to retrieve data from PostgreSQL and then passes that data to the produce_to_kafka function to send the data to Kafka.

### Kafka-Consumer

### Step 1:  Import necessary modules 
The script imports the Consumer class and KafkaError from the confluent_kafka module for consuming messages from Kafka. It also imports the json module for working with JSON data and save_to_file function.
```
from confluent_kafka import Consumer, KafkaError
import json
```
Pip Install
```
pip install confluent-kafka
```
### Step 2:  Define a function to save data to a file
This function (save_to_file) takes a file name and data as arguments, opens the specified file in append mode, converts the data to a JSON string, and writes it to the file with a newline character.

### Step 3: Define Kafka broker and consumer configuration
```
# Define Kafka broker(s)
bootstrap_servers = 'localhost:9092'

# Create Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': '1',  # Provide a unique consumer group ID
    'auto.offset.reset': 'earliest',  # Specify the start offset: earliest or latest
}
```
The script specifies the Kafka broker(s) (in this case, 'localhost:9092') and creates a configuration dictionary for the Kafka consumer. It sets the consumer group ID and specifies the auto-offset-reset to 'earliest', meaning it will start consuming messages from the beginning of the topic.

### Step 4: Create a Kafka consumer instance and subscribe to a topic
```
# Create a Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the topic(s) from which you want to consume messages
topic_name = 'mytopic'
consumer.subscribe([topic_name])
```
The script creates a Kafka consumer instance using the specified configuration and subscribes to a Kafka topic ('mytopic' in this case).

### Step 5: Continuously poll for messages
The script enters a continuous loop where it uses the poll method to retrieve messages from Kafka. If there are no messages, it continues the loop. If there is an error, it checks if it's an end-of-partition event or another error. If it's another error, it prints the error and breaks out of the loop.

If a message is received, it prints the message value after decoding it from UTF-8, and then it calls the save_to_file function to save the message to a file named 'data.txt'. The loop continues until a KeyboardInterrupt (Ctrl+C) is received.
