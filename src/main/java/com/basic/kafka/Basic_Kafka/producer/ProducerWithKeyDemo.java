package com.basic.kafka.Basic_Kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class ProducerWithKeyDemo {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "first_topic";
        for (int i = 0; i < 10; i++) {
            // create a producer record
            String value = "hello world " + Integer.toString(i);
            //providing key we can ensure same key always go to same partition.
            // It is not we decide which key goes to which partition but once partition is decided it will always go for same.
            String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (null == e) {
                        log.info("Received new Metadata. Topic: {} " + recordMetadata.topic() +
                                "\nPartition: {} " + recordMetadata.partition() +
                                "\nOffset: {} " + recordMetadata.offset() +
                                "\nTimestamp: {} " + recordMetadata.timestamp());
                    } else {
                        log.error("error while producing message: {} ", e);
                    }
                }
            });
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();

    }
}