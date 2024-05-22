package org.nygen.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Consumer Started");

        String groupId = "my-java-consumer-group";

        //Create Producer Properties to connect to local kafka
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "localhost:9092");

        consumerProperties.setProperty("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.setProperty("value.deserializer", StringDeserializer.class.getName());

        consumerProperties.setProperty("group.id", groupId);
        consumerProperties.setProperty("auto.offset.reset", "earliest");

        //Create a Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

        // Subscribe to a topic
        consumer.subscribe(Arrays.asList("demo_java_new"));

        //Retrieve data from kafka

        while(true) {
            log.info("Polling");
            ConsumerRecords<String, String> records= consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key : " + record.key() + " Value : " + record.value());
                log.info("Partition : " + record.partition() + " Offset : " + record.offset());
            }
        }

    }
}
