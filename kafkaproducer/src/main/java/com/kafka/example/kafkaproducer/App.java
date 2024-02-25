package com.kafka.example.kafkaproducer;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class App 
{
    public static void main( String[] args)
    {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        System.out.println( "Hello World!" );
        Scanner scanner = new Scanner(System.in);
        
        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        //ProducerRecord<String, String> record = new ProducerRecord<>(topic:"myTopic", key:"key1", value:"value1");
        
        // Accept the first number from the user
        System.out.print("Enter the first number: ");
		int number1 = scanner.nextInt();

        // Accept the second number from the user
        System.out.print("Enter the second number: ");
        int number2 = scanner.nextInt();

        // Perform the addition
        int sum = number1 + number2;

        ProducerRecord<String, String> record = new ProducerRecord<>("myTopic", "key1", String.valueOf(sum));

        producer.send(record);
        producer.flush();
        producer.close();

    }  
}
