package org.marcusbb.queue.serialization;

import org.marcusbb.queue.kafka.consumer.ConsumerFailureException;

public class SerializationException extends ConsumerFailureException {

	public SerializationException(Throwable e){
		super(e);
	}

	public SerializationException(String message, Throwable cause) {
		super(message,cause);
	}

	public SerializationException(String message) {
		super(message);
	}
}
