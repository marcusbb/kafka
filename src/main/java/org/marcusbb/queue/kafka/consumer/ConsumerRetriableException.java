package org.marcusbb.queue.kafka.consumer;

public abstract class ConsumerRetriableException extends RuntimeException {

    public ConsumerRetriableException(){}

    public ConsumerRetriableException(Throwable cause){
        super(cause);
    }

    public ConsumerRetriableException(String message){
        super(message);
    }

    public ConsumerRetriableException(String message, Throwable cause){
        super(message,cause);
    }
}
