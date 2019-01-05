package org.marcusbb.queue.kafka.consumer.impl;

import java.util.concurrent.TimeUnit;

//delay function
public interface DelayFunc<T> {
	/**
	 * 
	 * @param count - current count
	 * @param ts - the last attempt ts in millis
	 * @param msg - the message
	 * @return - time to next execution 
	 */
	long evaluate(int count, long ts,T msg);
	
	/**
	 * Time constant function with provided constant,
	 * returning a delay in milliseconds.
	 * 
	 * All arguments are ignored
	 * 
	 *
	 * @param <T>
	 */
	public static class TimeConstFunc<T> implements DelayFunc<T> {
		final long delay;
		final TimeUnit delayUnit;
		public TimeConstFunc(long constDelay,TimeUnit delayUnit) {
			this.delay = constDelay;
			this.delayUnit = delayUnit;
		}
		@Override
		public long evaluate( int count, long ts, T msg) {
			return System.currentTimeMillis() + delayUnit.toMillis(delay);
		}
		
	}
	
	public class TimeMultFunc<T> implements DelayFunc<T> {
		final long delay;
		final TimeUnit delayUnit = TimeUnit.MILLISECONDS;
		public TimeMultFunc(long constDelay) {
			this.delay = constDelay;
		}
		@Override
		public long evaluate( int count, long ts, T msg) {
			return System.currentTimeMillis() + delayUnit.toMillis(delay)* count;
		}
		
	}
	public class ExpoFunc<T> implements DelayFunc<T> {
		final long delay;
		final TimeUnit delayUnit = TimeUnit.MILLISECONDS;
		public ExpoFunc(long constDelay) {
			this.delay = constDelay;
		}
		@Override
		public long evaluate( int count, long ts, T msg) {
			long delta = System.currentTimeMillis() - ts;
			return (long) (System.currentTimeMillis() + Math.pow(delta,  count));
		}
		
	}
	
}