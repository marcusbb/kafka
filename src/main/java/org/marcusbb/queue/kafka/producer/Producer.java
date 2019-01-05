package org.marcusbb.queue.kafka.producer;

import org.marcusbb.queue.kafka.producer.impl.KafkaJTAProducer;
import org.marcusbb.queue.kafka.producer.impl.SimpleProducer;

/**
 * This interface will be the main interface for interacting with underlying Queue implementation
 * 
 * Producers can have multiple flavors.
 * 2 described by kafka implementations:
 * <ul>
 * <li> XA integrated message producer {@link KafkaJTAProducer}, 
 * which has a transient in memory queue for eventual flushing to Kafka
 * <li> Simple producer {@link SimpleProducer} that has no in memory queue and simply dispatches to Kafka
 * </ul>
 * 
 * TODO: future improvements
 * a) rename to TxProducer ?
 * a) return result of producing message to topic  e.g. partition , timestamp
 * b) add api to produce to a specific partition
 *
 */
public interface Producer<T> {

	/**
	 * Add messages (transiently) to the in memory queue (for eventual production) to underlying provider
	 * @param message
	 *
	 *
	 */
	public void add(String topic, T message);
	
	/**
	 * Some queue providers may not be able to support this behavior in a distributed manner
	 * 
	 * @param message - the payload
	 * @param partitionKey - this is important if the client wants total ordering within partitionKey
	 * This in fact could be just an hashCode (integer)
	 *   
	 */
	public void add(String topic, T message, Object partitionKey);
	
	/**
	 *
	 * flush the transient messages to underlying queue provider
	 * 
	 */
	@Deprecated
	public void flush();
	
	/**
	 * Drop message(s) from the transient queue: This could now be bounded within the context of transaction
	 */
	public void rollback();

	/**
	 * close producer
	 */
	public void close();
}
