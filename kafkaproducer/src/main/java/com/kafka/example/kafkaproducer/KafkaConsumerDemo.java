package com.kafka.example.kafkaproducer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class);
    private static final String OUTPUT_FILE = "/xxxx/xxxx/eclipse-workspace/kafkaproducer/data.csv";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Change this to 'earliest'

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("myTopic"));

        // Use a HashSet to store the unique keys of the processed records
        HashSet<String> processedKeys = new HashSet<>();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> data : records) {
                    // Check if the key has been processed before
                    if (!processedKeys.contains(data.value())) {
                        String recordInfo = data.key() + ", " +
                                 data.value() + ", " +
                                 data.topic() + ", " +
                                 data.partition() + ", " +
                                 data.offset() + "\n";

                        logger.info(recordInfo);
                        writeToFile(OUTPUT_FILE, recordInfo);

                        // Add the key to the processedKeys set
                        processedKeys.add(data.value());
                    } else {
                        // If the key is a duplicate, print the line to the console
                        System.out.println("Duplicate key found: " + data.value());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void writeToFile(String filePath, String content) {
        try {
            Path path = Path.of(filePath);
            Files.write(path, content.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

