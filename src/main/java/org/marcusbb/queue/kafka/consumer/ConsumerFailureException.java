package org.marcusbb.queue.kafka.consumer;

public abstract class ConsumerFailureException extends RuntimeException {

    public ConsumerFailureException(){}

    public ConsumerFailureException(Throwable cause){
        super(cause);
    }

    public ConsumerFailureException(String message){
        super(message);
    }

    public ConsumerFailureException(String message,Throwable cause){
        super(message,cause);
    }
}
