package org.example;

import  org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class slowConsumer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "slow-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto commit
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start from the earliest message
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1"); // Process one record at a time
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000"); // Set max poll interval to 10 seconds

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("orders"));
        System.out.println("Slow Consumer Started --------------------");
        while (true) {
            System.out.println("Calling poll --------------------");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            System.out.println("records recieved" + records.count());
            for(ConsumerRecord<String, String> record : records) {
                System.out.println(
                        "Processing: " + record.value()
                                + " | Partition: " + record.partition()
                                + " | Offset: " + record.offset()
                );
                Thread.sleep(6000); // Simulate slow processing
                consumer.commitSync(); // Commit offsets after processing;
                System.out.println("Processing complete ---------------------");

            }

        }
    }
}
