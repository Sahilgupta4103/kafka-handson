package org.example;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import  org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class deadConsumer {
    public static void main(String[] args)  throws InterruptedException{
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "dead-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("max.poll.records", "2");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe((List.of("orders")));
        try{
            while(true){
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String, String> record: records){
                    System.out.println("Received: " + record.value() + " | Partition: " + record.partition() + " | Offset: " + record.offset());
                    System.out.println("Processing ---------------------");
                    Thread.sleep(2000);
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }
}
