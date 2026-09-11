package org.example;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.*;

public class orderEvent {

    private String eventId;
    private String eventType;
    private String timestamp;
    private String orderId;
    private String customerId;
    private double amount;

    // Required by Jackson for JSON deserialization
    public orderEvent() {
    }

    public orderEvent(String eventId, String eventType, String timestamp, String orderId, String customerId, double amount) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public static void main(String[] args) throws JsonProcessingException, ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Properties cprops = new Properties();
        cprops.put("bootstrap.servers", "localhost:9092");
        cprops.put("group.id", "json-consumer-group2");
        cprops.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        cprops.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(cprops);
        ObjectMapper objectMapper = new ObjectMapper();
        consumer.subscribe(List.of("orders"));
        System.out.println("JSON Consumer Started --------------------");

        try {
            orderEvent event = new orderEvent(
                    "evt-1003",
                    "ORDER_CREATED",
                    "2026-09-11T10:30:00Z",
                    "ORD-103",
                    "CUST-501",
                    50000
            );
            String json = objectMapper.writeValueAsString(event);
            ProducerRecord<String, String> newRecord = new ProducerRecord<>("orders", event.getOrderId(), json);
            newRecord.headers().add(
                    "correlationId",
                    "order-req-123".getBytes()
            );

            newRecord.headers().add(
                    "source",
                    "order-service".getBytes()
            );
            producer.send(newRecord).get();
            producer.close();

            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String, String> record:records){
                    try {
                        System.out.println("--------Recieved Json---------");
                        System.out.println(record.value());

                        orderEvent recievedEvent = objectMapper.readValue(record.value(), orderEvent.class);
                    System.out.println(
                            "Event ID: " + recievedEvent.getEventId()
                    );

                    System.out.println(
                            "Event Type: " + recievedEvent.getEventType()
                    );

                    System.out.println(
                            "Order ID: " + recievedEvent.getOrderId()
                    );

                    System.out.println(
                            "Customer ID: " + recievedEvent.getCustomerId()
                    );

                    System.out.println(
                            "Amount: " + recievedEvent.getAmount()
                    );
                        var correlationId =
                                record.headers().lastHeader("correlationId");

                        var source =
                                record.headers().lastHeader("source");

                        if (correlationId != null) {
                            System.out.println(
                                    "Correlation ID: " +
                                            new String(correlationId.value())
                            );
                        }

                        if (source != null) {
                            System.out.println(
                                    "Source: " +
                                            new String(source.value())
                            );
                        }
                    consumer.commitSync();
                    break;
                }catch (JsonProcessingException e) {
                    System.out.println("Error processing JSON: " + e.getMessage());
                    continue; // Skip this record and continue with the next one
                }
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
