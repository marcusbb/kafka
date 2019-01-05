package org.marcusbb.queue.kafka.consumer;

import org.marcusbb.queue.serialization.ByteSerializer;

/**
 * 
 * A JMS like Consumer interface, but only supports discreet binary object message formats
 * - see {@link ByteSerializer}.
 */
public interface Consumer<T> {

	/**
	 * this consumption listening interface  
	 * 
	 * @param msg
	 */
	void onMessage(T msg, byte[] key);
}
