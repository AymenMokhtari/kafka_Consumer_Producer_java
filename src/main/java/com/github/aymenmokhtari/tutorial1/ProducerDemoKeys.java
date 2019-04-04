package com.github.aymenmokhtari.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String bootstrapServer = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer =  new KafkaProducer<String, String>(properties);



        for (int i = 0 ; i<10 ; i++) {

            // create producer record
            String topic = "first_topic";
            String value = "hello world"+ Integer.toString(i);
            String key = "id_"+Integer.toString(i);
            ProducerRecord<String,String > record = new ProducerRecord<String, String>("first_topic" , key, "hello_world"+Integer.toString(i));

        // send data - asynchronous
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record is successfully setn to an exception is thrown
                if(e == null){
                    // the record was successufully sent
                    logger.info("Receiver new metadate."+"\n" +
                             "Topic: "+recordMetadata.topic() +"\n" +
                              "Partition: "+recordMetadata.partition()+"\n"+
                            "Offset: "+recordMetadata.offset() +"\n"+
                            "Timestamp: "+recordMetadata.timestamp());


                } else {
                    logger.error("Error while producing ",e);
                }
            }
        });
        }
        producer.flush();
        producer.close();

    }
}
