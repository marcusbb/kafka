package org.marcusbb.queue.serialization.impl.ems.security;

import org.marcusbb.queue.kafka.consumer.ConsumerFailureException;

public class MessageAuthenticatorException extends ConsumerFailureException {
    public MessageAuthenticatorException(){ super(); }

    public MessageAuthenticatorException(Throwable cause){
        super(cause);
    }

    public MessageAuthenticatorException(String message){
        super(message);
    }

    public MessageAuthenticatorException(String message,Throwable cause){
        super(message,cause);
    }
}
