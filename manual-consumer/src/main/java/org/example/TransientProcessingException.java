package org.example;

public class TransientProcessingException extends RuntimeException{
    public TransientProcessingException(String message) {
        super(message);
    }
}
