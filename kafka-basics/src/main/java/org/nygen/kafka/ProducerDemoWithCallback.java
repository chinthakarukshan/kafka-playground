package org.nygen.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer Started");

        //Create Producer Properties to connect to local kafka
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "localhost:9092");

        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer", StringSerializer.class.getName());

        producerProperties.setProperty("batch.size", "400");


        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);


        // Testing sticky partitioner scenario. In this scenario the producer send the data as a batch to a single
        // partition because the data produced is submitted to the producer within a shorter time window
        for (int count = 0; count < 600; count++) {
            //Create a Producer Record

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java_new", "hello world" + count);

            //Send Data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        log.info("Successfully Sent the message. \n " +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp() + "\n");
                    } else {
                        log.error("Exception occurred while sending the message", e);
                    }
                }
            });

            if (count  % 30 == 0) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        producer.flush();
        producer.close();

        log.info("Producer Completed");

    }
}
