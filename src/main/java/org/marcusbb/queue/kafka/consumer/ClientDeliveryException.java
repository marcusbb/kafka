package org.marcusbb.queue.kafka.consumer;

public class ClientDeliveryException extends ConsumerRetriableException {

	public ClientDeliveryException(Throwable cause){
		super(cause);
	}

	public ClientDeliveryException(String message, Throwable cause) {
		super(message,cause);
	}
}
