package org.example;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public KafkaProducerService(
            KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(OrderEvent orderEvent) {

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(
                        "orders",
                        orderEvent.getOrderId(),
                        orderEvent
                );

        future.whenComplete((result, exception) -> {

            if (exception == null) {

                var metadata = result.getRecordMetadata();

                System.out.println(
                        "Kafka ACK"
                                + " | Topic: " + metadata.topic()
                                + " | Partition: " + metadata.partition()
                                + " | Offset: " + metadata.offset()
                );

            } else {

                System.out.println(
                        "Kafka send FAILED: "
                                + exception.getMessage()
                );
            }
        });
    }
}