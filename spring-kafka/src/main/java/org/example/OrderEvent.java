package org.example;

public class OrderEvent {
    private String eventId;
    private String eventType;
    private String timestamp;
    private String orderId;
    private String customerId;
    private String amount;

    public OrderEvent(){}
    public OrderEvent(String eventId, String eventType, String timestamp, String orderId, String customerId, String amount) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.timestamp = timestamp;
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
    }

    public String getAmount() {
        return amount;
    }

    public void setAmount(String amount) {
        this.amount = amount;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

}
