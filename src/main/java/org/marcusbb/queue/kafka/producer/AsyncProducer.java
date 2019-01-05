package org.marcusbb.queue.kafka.producer;

import java.util.concurrent.Future;

/**
 * 
 * 
 *
 * @param <T> - the object type
 * @param <K> - the key type
 */
public interface AsyncProducer<T,K> {

	/**
	 * Produce an object to a queue/topic.
	 * 
	 * @param obj
	 * @return
	 */
	public Future<ProducerResult> add(String topic, T obj);
	
	/**
	 * Produce an object to a queue/topic, with a partition key
	 * @param obj
	 * @param key
	 * @return
	 */
	public Future<ProducerResult> add(String topic, T obj, K key);

	/**
	 * close producer
	 */
	public void close();
}
