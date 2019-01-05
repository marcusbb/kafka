package org.marcusbb.queue.kafka.consumer;

import org.marcusbb.queue.kafka.producer.Producer;

public interface ConsumerRegistry<T> {

	/**
	 * subscribe a consumer to a given topic
	 *  
	 * Multiple consumers can be registered to the same topic,
	 * introducing parallelism but at the potential cost of ordering
	 * 
	 * The underlying implementation may try to preserve ordering if a Message key is
	 * provided by the {@link Producer#add(T message, Object partitionKey)}
	 * 
	 * @param topic
	 * @param consumer
	 *
	 * @throws IllegalStateException when there is already a consumer subscribed to a given topic
	 */
	public void register(String topic, Consumer<T> consumer);

	/**
	 * unsubscribe consumer from the topic(s) and waits for the
	 * consumer to finish consuming messages returned on the last poll.
	 *
	 * @throws IllegalStateException when consumer is alerday unsubscribed from the topic
	 *
	 */
	public void deregister();

	static interface Stats {
		long getDeliveredCount();

		void incrDeliveredCount();

		long getFailureCount();

		void incrFailureCount();

		long getSerFailCount();

		void incrSerFailCount();

		long getRetriedCount();

		void incrRetriedAcount();
	}
}
