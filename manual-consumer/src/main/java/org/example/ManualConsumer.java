package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ManualConsumer {
    public static void main(String args[]) throws InterruptedException, ExecutionException {
        Properties props = new Properties();
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto commit
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start from the earliest message

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("orders"));
        System.out.println("Consumer Started --------------------");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {

                boolean processed = false;



                    try {
                        System.out.println(
                                "Processing: " + record.value()
                                        + " | Partition: " + record.partition()
                                        + " | Offset: " + record.offset()
                        );

                        // Intentionally fail one message
                        if (record.value().contains("POISON")) {
                            throw new RuntimeException("Simulated processing failure");
                        }

                        Thread.sleep(1000);

                        System.out.println("Processing successful ---------------------");
                        consumer.commitSync();
                        System.out.println("Original offset committed");

                    } catch (Exception e) {

                        System.out.println(
                                "Processing FAILED | Attempt: "
                                        + " | Reason: " + e.getMessage()
                        );

                        ProducerRecord<String, String> retryRecord =  new ProducerRecord<>("order.retry", record.key(), record.value());
                        retryRecord.headers().add("retry-count", "1".getBytes());
                        producer.send(retryRecord).get();
                        System.out.println("message sent to order.retry retry Count: 1");
                        consumer.commitSync();
                    }
                    System.out.println("Original offset committed");
            }
        }
    }
}
