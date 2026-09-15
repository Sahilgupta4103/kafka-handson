package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService{

    private final OrderRepository orderRepository;

    public KafkaConsumerService(OrderRepository orderRepository){
        this.orderRepository = orderRepository;
    }
    @KafkaListener(topics = "orders", groupId = "order-db-service")
    public void consume(OrderEvent orderEvent){
        System.out.println(
                        "recieved kafka event "+ orderEvent.getEventId()
        );

        if(orderRepository.findByEventId(orderEvent.getEventId()).isPresent()){
            System.out.println("Duplicate event detected: " + orderEvent.getEventId() );
            return;
        }

        OrderEntity order = new OrderEntity(
                orderEvent.getOrderId(),
                orderEvent.getEventId(),
                orderEvent.getCustomerId(),
                orderEvent.getAmount(),
                orderEvent.getEventType()
        );
        orderRepository.save(order);
        System.out.println("Saved to Database");
    }

    public void consume(ConsumerRecord<String, OrderEvent> record) {

        System.out.println(
                "Thread: " + Thread.currentThread().getName()
                        + " | Partition: " + record.partition()
                        + " | Offset: " + record.offset()
                        + " | Order: " + record.value().getOrderId()
        );
    }

}
