package com.github.aymenmokhtari.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        String bootstrapServer = "127.0.0.1:9092";
        String topic = "first_topic";
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG  , StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG , "earliest");

        // create comsumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        // assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition patitionToReadFrom = new TopicPartition(topic ,0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(patitionToReadFrom));
        consumer.seek(patitionToReadFrom,offsetToReadFrom );
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;
        // poll for new data
        while(keepOnReading) {
            ConsumerRecords<String , String> records =  consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
            for(ConsumerRecord<String, String> record :records) {
                numberOfMessageReadSoFar+=1;
                logger.info("Key: "+record.key() + "Value: "+record.value());
                logger.info("Partition: "+record.partition()+ ", Offset: "+record.offset());
                if(numberOfMessageReadSoFar >= numberOfMessagesToRead) {
                    keepOnReading = false ;
                    break; // to exit the for loop
                }

            }
        }
        logger.info("exittnig the application");

    }

}
