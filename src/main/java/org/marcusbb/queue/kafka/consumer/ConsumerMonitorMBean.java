package org.marcusbb.queue.kafka.consumer;

/**
 * 
 * An interface for consumer monitoring (largely for retry and failure)
 *
 */
public interface ConsumerMonitorMBean {

	
	public int getTotalFailures();
	
	
	public void closeConsumer();
}
