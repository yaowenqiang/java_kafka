package com.github.jacky.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKey {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);
//        System.out.println("Hello world!");
        // create producer properties
        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create property

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        //send data


        //create a producer record


        for (int i = 0; i < 10; i++) {
            String topic = "first";
            String value = "hello world " + Integer.toString(i);
            //same key always go to the same partition
            final String key = "id_" + Integer.toString(i);
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
//            logger.info("Key:" + key);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Received new metadata.\n" +
                                "Topic:     " + recordMetadata.topic() + "\n" +
                                "Key: "+ key + " Partition: " + recordMetadata.partition() + "\n" +
                                "Offset:    " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
//                    e.printStackTrace()
                        logger.error("Error while producing", e);
                    }
                }
            }).get();//block the .send() to make it synchronous don't do this in production

        }


        //flush
        producer.flush();
        producer.close();
    }
}
