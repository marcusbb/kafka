package org.marcusbb.queue.kafka.consumer.impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.marcusbb.queue.Envelope;
import org.marcusbb.queue.RoutableEncryptedMessage;

class RetryWrapper<T> {
	/**
	 * 
	 */
	
	ConsumerRecord<byte[], byte[]> record;
	T msg;
	int retryCount = 0;
	long lastTsMillis = System.currentTimeMillis();
	long nextTargetTs;
	
	RetryWrapper( ConsumerRecord<byte[], byte[]> record, T msg,int count,long nextTs) {
		
		this.msg = msg;
		this.record = record;
		this.retryCount = count;
		this.nextTargetTs = nextTs;
	}

	// generate msg string for logging purpose to address issue FS-941 Fix Kafka specific code review flaws
	String getMsgLog() {
		if (msg == null)
			return "<Empty>";
		if (msg instanceof RoutableEncryptedMessage)
			return "msg=" + msg;
		if (msg instanceof Envelope) {
			Envelope envelope = (Envelope)msg;
			return "<Non-encrypted payload was omitted>; Envelope headers are: "+((Envelope) msg).getHeaders().toString();
		}
		return "<Non-encrypted msg was omitted>";
	}
}