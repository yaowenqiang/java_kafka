package com.github.jacky.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoGroup {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroup.class.getName());
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fifth-application";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        consumer.subscribe(Collections.singleton("first"));
//        consumer.subscribe(Arrays.asList("first", "second"));
        while(true) {
//            consumer.poll(100);
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : records) {
                logger.info("Key:" + record.key() + " , value: " + record.value());
                logger.info("Partition:" + record.partition() + " , Offset: " + record.offset());
            }
        }


    }
}
