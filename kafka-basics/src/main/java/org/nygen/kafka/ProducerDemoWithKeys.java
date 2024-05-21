package org.nygen.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Producer Started");

        //Create Producer Properties to connect to local kafka
        Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", "localhost:9092");

        producerProperties.setProperty("key.serializer", StringSerializer.class.getName());
        producerProperties.setProperty("value.serializer", StringSerializer.class.getName());



        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);


        // Key based producer. Check whether the records with the same key end up in the same partition
        for (int i =0 ; i< 2; i++) {
            for (int count = 0; count < 10; count++) {
                //Create a Producer Record

                String topic = "demo_java_new";
                String key = "id_" + count;
                String value = "hello world " + count;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                //Send Data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            log.info("Key : " + key + "|Partition : " + recordMetadata.partition());
                        } else {
                            log.error("Exception occurred while sending the message", e);
                        }
                    }
                });

            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        producer.flush();
        producer.close();

        log.info("Producer Completed");

    }
}
