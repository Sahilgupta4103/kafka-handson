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

public class TransactionalConsumer {
    public static void main(String[] args) {
        Properties producerProps = new Properties();
        Properties consumerProps = new Properties();

        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "transactional-order-processor");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-processor-1");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);


        producer.initTransactions();
        consumer.subscribe(List.of("orders"));
        System.out.println("Transactional Consumer Started --------------------");

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if(records.isEmpty()){
                    continue;
                }
                producer.beginTransaction();
                try{
                    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                    for(ConsumerRecord<String, String> record: records) {
                        System.out.println("Received: " + record.value() + " | Partition: " + record.partition() + " | Offset: " + record.offset());
                        // Simulate business processing
                        System.out.println("Processing ---------------------");
                        Thread.sleep(1000);
                        System.out.println("Processing complete ---------------------");

                        String result = "Processed -> " + record.value();
                        producer.send(new ProducerRecord<>("order-events", record.key(), result)).get();
                        System.out.println("Processed: " + result);
                        offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    }
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
                    System.out.println("Offsets added to transaction ---------------------");
                    producer.commitTransaction();
                    System.out.println("Transaction committed ---------------------");

                }catch (Exception e){
                    producer.abortTransaction();
                }

        }

    }
}
