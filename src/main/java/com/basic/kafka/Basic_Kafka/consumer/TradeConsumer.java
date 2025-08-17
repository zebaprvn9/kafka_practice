package com.basic.kafka.Basic_Kafka.consumer;

import com.basic.kafka.Basic_Kafka.model.TradeData;
import com.basic.kafka.Basic_Kafka.model.TradeRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Component
public class TradeConsumer {

    @Autowired
    private TradeRepository tradeRepository;

    public void consumeData() {
        String bootstrapServers = "localhost:9092";
        String groupId = "my-consumer-group";
        String topic = "my-topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        properties.setProperty(JsonDeserializer.VALUE_DEFAULT_TYPE, TradeData.class.getName());
        properties.setProperty(JsonDeserializer.TRUSTED_PACKAGES, "*");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, TradeData> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Listening to topic: " + topic);

            while (true) {
                ConsumerRecords<String, TradeData> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, TradeData> record : records) {
                    System.out.printf("Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                            record.partition(), record.offset(), record.key(), record.value());

                    tradeRepository.save(record.value()); // Persist in MongoDB
                }
                consumer.commitAsync();
            }
        }
    }
}
