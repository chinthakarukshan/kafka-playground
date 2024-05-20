package org.nygen.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer Started");

        //Create Producer Properties to connect to local kafka
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "localhost:9092");

        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer", StringSerializer.class.getName());


        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);

        //Create a Producer Record

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        //Send Data
        producer.send(producerRecord);

        producer.flush();
        producer.close();

        log.info("Producer Completed");

    }
}
