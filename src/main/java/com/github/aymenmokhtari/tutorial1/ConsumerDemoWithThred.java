package com.github.aymenmokhtari.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
// not working
public class ConsumerDemoWithThred {
    public static void main(String[] args) {
        new ConsumerDemoWithThred().run();
    }
    class ConsumerDemoWtihThread {

    }
    public void run() {
          Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";
        // latch for dealing with multiple thread
        CountDownLatch latch = new CountDownLatch(1);
        //create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerThread = new ConsumerThread(bootstrapServer, groupId, topic, latch);
        //start the thread
        Thread myThred = new Thread(myConsumerThread);

        myThred.start();

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) myConsumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrumpter ",e);
        }finally {
            logger.info("Application is closing");

        }

    }
    public class ConsumerThread implements Runnable {
        private  CountDownLatch latch;
        private  KafkaConsumer<String ,String> consumer ;
        private  Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());

        public ConsumerThread(String bootstrapServers,String groupiId ,  String topic ,CountDownLatch latch) {
            this.latch = latch;
        }
        @Override
        public void run() {
            //poll for new data
            try {
                while(true) {
                    ConsumerRecords<String , String> records =  consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
                    for(ConsumerRecord<String, String> record :records) {
                        logger.info("Key: "+record.key() + "Value: "+record.value());
                        logger.info("Partition: "+record.partition()+ ", Offset: "+record.offset());

                    }
                }

            }catch (WakeupException e) {
                logger.info("Recieed shutdown signal!");
            }finally {
                consumer.close();
                //tell our main code w"re done with the consumer
                latch.countDown();
            }
        }
        public void shutdown() {
            // the wake up methode is a special method to interrup consumer.poll()
            //
            consumer.wakeup();
        }
    }
}
