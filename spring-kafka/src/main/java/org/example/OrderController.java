package org.example;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {


    private final KafkaProducerService producerService;
    public OrderController(KafkaProducerService producerService){
        this.producerService = producerService;
    }

    @PostMapping
    public String createOrder(@RequestBody OrderEvent orderEvent){
        producerService.sendOrder(orderEvent);
        return "Order created successfully";
    }

}
