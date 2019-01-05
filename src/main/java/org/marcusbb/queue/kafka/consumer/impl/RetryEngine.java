package org.marcusbb.queue.kafka.consumer.impl;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.marcusbb.queue.kafka.consumer.AbstractConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RetryEngine<T> {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	/**
	 * 
	 */
	private final KafkaRetryConsumerDispatcher<T> dispatcher;

	OperatingSystemMXBean system = ManagementFactory.getOperatingSystemMXBean();
	
	private long basePollPeriod;
	
	private long maxPollPeriod = 60*1000; //60 second poll when we're not in retry state
	
	private int pollBackoffFactor = 10;
	
	
	/**
	 * @param kafkaRetryConsumerDispatcher
	 */
	RetryEngine(KafkaRetryConsumerDispatcher<T> kafkaRetryConsumerDispatcher, long baseLinePoll) {
		this.dispatcher = kafkaRetryConsumerDispatcher;
		this.basePollPeriod = baseLinePoll;
	}
	RetryEngine(KafkaRetryConsumerDispatcher<T> kafkaRetryConsumerDispatcher,int parallelism) {
		this.dispatcher = kafkaRetryConsumerDispatcher;

		// Use jdk7-compliant shim
//		execService = Executors.newWorkStealingPool(parallelism);
		execService = Executors.newWorkStealingPool(parallelism);
	}

	HashMap<Integer, LinkedList<RetryWrapper<T>>> retryRecords = new HashMap<>();

	private ExecutorService execService = Executors.newSingleThreadExecutor();
			
	public void add(int partition, RetryWrapper<T> wrapper) {
		LinkedList<RetryWrapper<T>> list = retryRecords.get(partition);
		if (list == null)
			list = new LinkedList<>();
		list.add(wrapper);
		retryRecords.put(partition, list);
	}

	public static class ExecutionResult {
		HashMap<Integer, Long> partList;
		long suggestedPoll;
		public ExecutionResult(long pollPeriod) {
			this.partList = new HashMap<>();
			this.suggestedPoll = pollPeriod;
		}
		public ExecutionResult(HashMap<Integer, Long> partList, long pollPeriod) {
			this.partList = partList;
			this.suggestedPoll = pollPeriod;
		}
		public HashMap<Integer, Long> completedPartitions() {
			return partList;
		}
	}
	public ExecutionResult retry() {

		ExecutionResult result = new ExecutionResult(basePollPeriod);
		HashMap<Integer, Long> partList = result.partList;       
		if (retryRecords.size() == 0)
			return result;

		logger.debug("retry_records: {}", retryRecords.size());

		// possible insertion of thread pooling mechanism
		for (LinkedList<RetryWrapper<T>> retryList : retryRecords.values()) {

			
			boolean loop = true;
			
			while (loop) {
				RetryWrapper<T> wrapper = retryList.peek();
				if (wrapper == null)
					break; //break while
				
				boolean consumed = false;
				Exception persistedException = null;
				try {
					long now = System.currentTimeMillis();
					if (wrapper.nextTargetTs/1000 <= now/1000) {
						wrapper.lastTsMillis = now;
						logger.debug("Attempting_delivery of record partition={}, offset={}", wrapper.record.partition(),wrapper.record.offset());
						this.dispatcher.deliver(wrapper.msg, wrapper.record.key());
						
						//if exception follow catch block
						logger.info("Delivered_msg partition={}, offset={}", wrapper.record.partition(),wrapper.record.offset());
						consumed = true;
						this.dispatcher.stats.incrRetriedAcount();
						
					}else {
						//this could get noisy if poll
						logger.debug("Skipping not achieved target time frame {} ", wrapper.nextTargetTs);
					}
					
				} catch (Exception e) {
					//possible examination of exception
					persistedException = e;
					++wrapper.retryCount;
					
					wrapper.nextTargetTs = dispatcher.delayFunc.evaluate(wrapper.retryCount, wrapper.lastTsMillis, wrapper.msg);
					AbstractConsumer.rewrapMessage(e, wrapper.msg, wrapper.retryCount, wrapper.lastTsMillis);
					logger.warn("Retry_Failed attempt={}, {}", wrapper.retryCount, wrapper.getMsgLog());
					
				}

				if (consumed || wrapper.retryCount == this.dispatcher.retryAttempts) {
					retryList.remove();
					dispatcher.asyncCommitOffset(wrapper.record);
					if (retryList.isEmpty()) {
						partList.put(wrapper.record.partition(), wrapper.record.offset());
					}
					//once exhausted we will put in failure topic
					if (wrapper.retryCount == this.dispatcher.retryAttempts && persistedException != null) {
						// get failure topic name from retry topic
						String failureTopic = dispatcher.getTopicName().replace(KafkaRetryConsumerDispatcher.RETRY_TOPIC_SUFFIX,KafkaRetryConsumerDispatcher.FAILURE_TOPIC_SUFFIX);
						this.dispatcher.produceErrorRecord(failureTopic,wrapper.record.key(),dispatcher.getSerializer().serialize(wrapper.msg));
					}
					loop = true;
				} else {
					loop = false;
				}
			}
		}
		result.suggestedPoll = suggestPoll();
		// clear entry for completed partitions from buffer
		for(Integer partition : partList.keySet()){
			retryRecords.remove(partition);
		}
		
		return result;
	}


	public long getMaxPollPeriod() {
		return maxPollPeriod;
	}
	public void setMaxPollPeriod(long maxPollPeriod) {
		this.maxPollPeriod = maxPollPeriod;
	}
	public int getPollBackoffFactor() {
		return pollBackoffFactor;
	}
	public void setPollBackoffFactor(int pollBackoffFactor) {
		this.pollBackoffFactor = pollBackoffFactor;
	}
	private long suggestPoll() {
		//this is to be nice to CPU when we're not in retry state
		if (retryRecords.size() == 0) 
			return maxPollPeriod;
		//this is a factor of the system load - default 10 - set accordingly
		if (retryRecords.size() > 0) {
			logger.debug("Current system load {}", system.getSystemLoadAverage());
			return (long)( getPollBackoffFactor()* Math.abs(system.getSystemLoadAverage())) * basePollPeriod;
		}
		return basePollPeriod;
	}
}