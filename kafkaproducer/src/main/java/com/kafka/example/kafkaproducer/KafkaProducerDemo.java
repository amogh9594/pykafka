package com.kafka.example.kafkaproducer;

import java.sql.*;
import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo
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

        try {
            // Assuming the user name and password for the database connection
            String url = "jdbc:postgresql://localhost:5432/xxxx";
            String user = "xxxx";
            String password = "xxxx";

            Connection conn = DriverManager.getConnection(url, user, password);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM m_product LIMIT 1");

            while (rs.next()) {
                String col1 = rs.getString("m_product_id");
                String col2 = rs.getString("created");
                String data = col1 + " " + col2;
                ProducerRecord<String, String> record = new ProducerRecord<>("myTopic", "key1", data);
                producer.send(record);
            }

            producer.flush();
            producer.close();

        } catch (SQLException e) {
            System.out.println("Error occurred while connecting to the database");
            e.printStackTrace();
        }
    }
}
