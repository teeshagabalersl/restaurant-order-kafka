package org.example.restaurant;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.Properties;
import java.util.Scanner;

public class OrderProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "all");
        properties.put("enable.idempotence", "true");

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(properties);
             Scanner scanner = new Scanner(System.in)) {

            System.out.println("Enter Order ID:");
            String orderId = scanner.nextLine();

            System.out.println("Enter Restaurant ID (RESTAURANT-101 or RESTAURANT-102):");
            String restaurantId = scanner.nextLine();

            System.out.println("Enter Customer Name:");
            String customerName = scanner.nextLine();

            System.out.println("Enter Items:");
            String items = scanner.nextLine();

            OrderEvent order = new OrderEvent(
                    orderId,
                    restaurantId,
                    customerName,
                    items,
                    Instant.now().toString(),
                    "CREATED"
            );

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            "restaurant.orders.created",
                            restaurantId,
                            order.toJson()
                    );

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Failed to publish order: "
                            + exception.getMessage());
                } else {
                    System.out.println("Order published successfully!");
                    System.out.println("Topic: " + metadata.topic());
                    System.out.println("Partition: " + metadata.partition());
                    System.out.println("Order: " + order.toJson());
                }
            });

            producer.flush();

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}