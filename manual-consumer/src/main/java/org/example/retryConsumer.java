package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class retryConsumer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        Properties pprops = new Properties();
        pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        pprops.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pprops.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(pprops);

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "explicit-commit-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto commit
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Start from the earliest message
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5"); // Process 5 messages at a time
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("orders"));
        System.out.println("Consumer Started --------------------");

        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1));
            for (ConsumerRecord<String, String> record : records) {
                try {

                    System.out.println(
                            "Retry Processing: " + record.value()
                                    + " | Partition: " + record.partition()
                                    + " | Offset: " + record.offset()
                    );

                    // For now, deliberately fail the poison message
                    if (record.value().contains("FAIL_TRANSIENT")) {
                        throw new TransientProcessingException("Temporary failure");
                    }

                    if (record.value().contains("FAIL_PERMANENT")) {
                        throw new PermanentProcessingException("Permanent failure");
                    }

                    Thread.sleep(1000);

                    System.out.println(
                            "Processing successful ---------------------"
                    );

                    long nextOffset = record.offset() + 1;

                    consumer.commitSync(
                            Map.of(
                                    new TopicPartition(record.topic(), record.partition()),
                                    new OffsetAndMetadata(record.offset())
                            )
                    );

                    System.out.println(
                            "Committed offset: " + nextOffset
                    );

                } catch (TransientProcessingException e) {
                    //send to retry
                    ProducerRecord<String, String> retryRecord =
                            new ProducerRecord<>(
                                    "orders.retry",
                                    record.key(),
                                    record.value()
                            );

                    retryRecord.headers().add(
                            "retryCount",
                            "1".getBytes(StandardCharsets.UTF_8)
                    );

                    producer.send(retryRecord).get();

                    consumer.commitSync();
                }
                catch (PermanentProcessingException e) {
                    //send to dead letter
                    ProducerRecord<String, String> dltRecord =
                            new ProducerRecord<>(
                                    "orders.DLT",
                                    record.key(),
                                    record.value()
                            );

                    producer.send(dltRecord).get();

                    consumer.commitSync();

                }
            }
        }
    }
}
