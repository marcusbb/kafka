package org.marcusbb.queue.kafka.consumer.impl;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.marcusbb.queue.kafka.consumer.ClientDeliveryException;
import org.marcusbb.queue.kafka.consumer.Consumer;
import org.marcusbb.queue.kafka.consumer.impl.RetryEngine.ExecutionResult;
import org.marcusbb.queue.serialization.ByteSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;



/**
 *
 *
 *
 * Dispatches to {@link Consumer}. A retrying ConsumerDispatcher. Will deliver
 * as many as {@link #retryAttempts}. Attempts are buffered in memory, and the
 * commit offset is not incremented as per each partition within the consumer
 * group.
 *
 * As each message is attempted to be delivered all exceptions emitted from
 * consumer will be caught and put into a {@link RetryWrapper}. An attempt will
 * be made every {@link #retryAttempts} to deliver the event via the
 * {@link Consumer#onMessage(Object)} client invocation.
 *
 * There are several main gotcha's. Serialization exceptions are swallowed -
 * it's possible that they go into another bucket A very bouncy Consumer may
 * have unintended consequences due to pause and resume cycles. Therefore it's
 * encouraged to have Consumers that have long times to fail until it can be
 * determined that pause/resume behavior is well controlled.
 */

public class KafkaRetryConsumerDispatcher<T> extends KafkaConsumerDispatcher<T> {

	int retryAttempts = 3;
	private int batchSizeDefault = 1;

	/**
	 * Current retry buffer
	 */
	private RetryEngine<T> retryEngine;

	
	
	private long pollPeriod = 100; 
	private long delay = 5;
	private TimeUnit delayUnit = TimeUnit.MILLISECONDS;
	DelayFunc<T> delayFunc;
	
	
	
	public KafkaRetryConsumerDispatcher(Properties props, ByteSerializer<T> serializer,Stats stats) {
		super(props, serializer, stats);
		
		
		if (!props.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) 
			props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, batchSizeDefault);
		
		//retry
		if (props.containsKey("consumer.retryAttempts")) {
			retryAttempts = (Integer) props.get("consumer.retryAttempts");
		}
		if (props.containsKey("consumer.retry.schedule.delay"))
			delay = (Long)props.get("consumer.retry.schedule.delay");
		if (props.containsKey("consumer.retry.schedule.delay.unit"))
			delayUnit = (TimeUnit)props.get("consumer.retry.schedule.delay.unit");
		
		retryEngine = new RetryEngine<T>(this,pollPeriod);
		delayFunc = new DelayFunc.TimeConstFunc<T>(delay,delayUnit);
			
	}
	public KafkaRetryConsumerDispatcher(Properties props, ByteSerializer<T> serializer,Stats stats,DelayFunc<T> delayFunc) {
		this(props, serializer, stats);
		
		this.delayFunc = delayFunc;
		
			
	}

	public KafkaRetryConsumerDispatcher(Properties props, ByteSerializer<T> serializer) {

		this(props, serializer, null);
	}

	@Override
	public void runLoop() {

		
		
		//schedule();
		
		while (subscribed) {
			
			
			ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(pollPeriod);

			for (ConsumerRecord<byte[], byte[]> record : records) {
				logger.debug("Got records: count={}, offset={}, topic={}, partition={} ", records.count(), record.offset(), record.topic(), record.partition());

				//this could be a serialization error
				T msg = handleSerialization(record.value());
				logger.info("Adding_retry into buffer count={}, offset={}, topic={}, partition={} ", records.count(), record.offset(), record.topic(), record.partition()); 
				addRetryAndPause(msg,record,0,null);
			}

			ExecutionResult result = retryEngine.retry();
			Map<Integer, Long> completedPartitions = result.completedPartitions();
			pollPeriod = result.suggestedPoll;
			//for completed partitions commit offset
			for (int partition : completedPartitions.keySet()) {
				logger.info("Resuming topic: {}, partition: {}", topicName, partition);
				TopicPartition resumedTopic = new TopicPartition(topicName, partition);
				kafkaConsumer.resume(Collections.singletonList(resumedTopic));

				Long completedOffset = completedPartitions.get(partition);
				//commitOffset(resumedTopic, completedOffset);
				
			}

		}

	}
	
	
	private void addRetryAndPause(T msg,ConsumerRecord<byte[],byte[]> record, int count,ClientDeliveryException cde) {
		T nMsg = msg;
		if (cde != null)
			nMsg = rewrapMessage(cde,msg,count,System.currentTimeMillis());
		RetryWrapper<T> wrapper = new RetryWrapper<T>( record, nMsg,count,delayFunc.evaluate(0, System.currentTimeMillis(), nMsg));
		retryEngine.add(record.partition(), wrapper);
		//pause partitions consumption for retry
		//do not proceed with marker with incremented marker
		TopicPartition tp = new TopicPartition(record.topic(), record.partition());
		if (!kafkaConsumer.paused().contains(tp)) {
			logger.info("Pause topic: {}, partition: {}", record.topic(), record.partition());
			kafkaConsumer.pause(Collections.singletonList(tp));
		}
		
	}
	//expose to retry engine
	
	@Override
	protected void deliver(T msg, byte[] key) {
		super.deliver(msg, key);
	}
	@Override
	protected void produceErrorRecord(String topic, byte[] key, byte[] value) {
		super.produceErrorRecord(topic, key, value);
	}

	//Should re-fold this into the AbstractConsumer class to make it visible to all consumers
	Map<TopicPartition,Long> offsets = new HashMap<>();
	protected void asyncCommitOffset(ConsumerRecord<byte[],byte[]> record) {
		//kafkaConsumer.commitAsync();
		logger.debug("committing offset: {},{},{}",getTopicName(), record.partition(),record.offset()  );
		OffsetAndMetadata ofm = new OffsetAndMetadata(record.offset() +1);
		HashMap<TopicPartition,OffsetAndMetadata> map = new HashMap<>(1);
		map.put(new TopicPartition(record.topic(),record.partition()), ofm);
		offsets.put(new TopicPartition(record.topic(),record.partition()),record.offset() +1);
		kafkaConsumer.commitAsync(map, new OffsetCommitCallback() {
			@Override
			public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
				
			}	
		});
	}
	@Override
	public Map<TopicPartition, Long> getLastCommittedOffsets(){
		return offsets;
	}
	
	protected RetryEngine<T> getRetryEngine() {
		return retryEngine;
	}
}

