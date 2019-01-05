package org.marcusbb.queue.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.marcusbb.queue.Envelope;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * 
 * Dispatches to {@link Consumer}.
 * 
 * Consider splitting out to base class of functionality, such as thread management, lifecycle and management.
 *
 */
public abstract class AbstractConsumer<T> implements ConsumerRegistry<T> {

	protected Logger logger = LoggerFactory.getLogger(getClass());
	
	public static String RETRY_TOPIC_SUFFIX = "_retry";
	public static String FAILURE_TOPIC_SUFFIX = "_failure";
	public static String EXCEPTION_STACK_HEADER = "EX_STACK";
	public static String EXCEPTION_MSG_HEADER = "EX_MSG";
	public static String DELIVERY_COUNT_HEADER = "DELIVERY_COUNT";
	public static String LAST_DELIVERY_TS = "DELIVERY_TS";

	// single consumer for now
	protected Consumer<T> consumer;

	protected KafkaConsumer<byte[], byte[]> kafkaConsumer = null;
	protected boolean subscribed = false;
	protected ByteSerializer<T> serializer;
	protected String topicName;
	protected String groupId;
	protected String clientId;
	protected Properties props;
	protected int poll = 5000;
	protected boolean throwOnSerEx = true;
	protected boolean throwOnDeliveryFailure = false;
	protected Map<TopicPartition,Long> deliveredOffsets ;
	protected Map<TopicPartition,Long> lastCommittedOffsets;

	private CountDownLatch readyLatch ;
	private CountDownLatch unsubscribedLatch ;
	public Stats stats;

	private Stats getDefaultStats() {
		return new Stats() {
			private long deliveredCount = 0L;
			private long failureCount = 0L;
			private long serFailCount = 0L;
			private long deadLetterCount = 0L;
			private long retriedCount = 0L;

			@Override
			public long getDeliveredCount() {
				return deliveredCount;
			}
			@Override
			public void incrDeliveredCount() {
				this.deliveredCount++;
			}
			@Override
			public long getFailureCount() {
				return failureCount;
			}
			@Override
			public void incrFailureCount() {
				this.failureCount++;
			}
			@Override
			public long getSerFailCount() {
				return serFailCount;
			}
			@Override
			public void incrSerFailCount() {
				this.serFailCount++;
			}

			@Override
			public long getRetriedCount() {
				return retriedCount;
			}

			@Override
			public void incrRetriedAcount() {
				this.retriedCount++;
			}
		};
	}

	public AbstractConsumer(Properties props, ByteSerializer<T> serializer, Stats stats) {

		this.stats = stats;
		Properties p = new Properties();
		p.putAll(props);
		p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		this.serializer = serializer;

		if (props.containsKey("consumer.poll")) {
			poll = (Integer) props.get("consumer.poll");
		}
		if (props.containsKey("consumer.throwOnSerEx")) {
			this.throwOnSerEx = Boolean.parseBoolean(props.getProperty("consumer.throwOnSerEx"));
		}
		this.props = p;
		groupId = props.getProperty(ConsumerConfig.GROUP_ID_CONFIG);

	}


	public String getGroupId(){
		return groupId;
	}

	public String getTopicName() {
		return topicName;
	}

	public ByteSerializer<T> getSerializer() {
		return serializer;
	}
	/**
	 *
	 * Register a consumer
	 *
	 * @param topic topic name
	 * @param consumer consumer instance
	 *
	 * @throws IllegalStateException if consumer was already registered.
	 * @throws RuntimeException if consumer registration failed
	 */
	@Override
	public void register(final String topic, Consumer<T> consumer) throws IllegalStateException, RuntimeException {
		if (subscribed)
			throw new IllegalStateException("consumer is already registered");

		this.topicName = topic;
		this.consumer = consumer;

		readyLatch = new CountDownLatch(1);
		unsubscribedLatch = new CountDownLatch(1);

		kafkaConsumer = new KafkaConsumer<>(props);

		deliveredOffsets  = new HashMap<>();
		lastCommittedOffsets = new HashMap<>();

		this.stats = (stats == null ? getDefaultStats() : stats);

		String clientIp = null;
		try {
			clientIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			logger.warn("could not determine host ip address");
		}

		clientId = props.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
		String brokers = props.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

		logger.info("register consumer={} instance with clientId={} in group={} with brokers={} from host={} to topic={} ",
				consumer, clientId, groupId, brokers, clientIp, topic);

		// TODO : add subscription for multiple topics
		kafkaConsumer.subscribe(Collections.singletonList(topicName),new RebalanceListener());
		seekOffset();

		try {
			String threadName = "consumerThread_"+ (clientId != null ? clientId : groupId);

			startConsumerThread(threadName);

			readyLatch.await();

			logger.info("registration of consumer={} instance with clientId={} in group={} completed, subscribed to topic={}",consumer, clientId, groupId,topicName);
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			throw new RuntimeException(e);
		}
	}

	private void seekOffset(){

		//poll allows assignments to be queried - possible bug in client
		kafkaConsumer.poll(0); // Jesse suggests 0
		if (props.containsKey("consumer.fromBeginning")) {
			Set<TopicPartition> assignment = kafkaConsumer.assignment();
			for(TopicPartition topicPartition : assignment){
				logger.info("assigned topic={},partition={}",topicPartition.topic(),topicPartition.partition());
			}
			kafkaConsumer.seekToBeginning(assignment);
			// in order to evaluate the position of marker for each assigned partition
			for(TopicPartition topicPartition : assignment){
				kafkaConsumer.position(topicPartition);
			}
		}else if (props.containsKey("consumer.fromTS")) {
			Long fromTs = (Long)props.get("consumer.fromTS");
			HashMap<TopicPartition,Long> seekTsMap = new HashMap<>();
			for (TopicPartition tp: kafkaConsumer.assignment()) {
				seekTsMap.put(tp, fromTs);
			}
			Map<TopicPartition, OffsetAndTimestamp> map = kafkaConsumer.offsetsForTimes(seekTsMap);
			for (TopicPartition tp: map.keySet()) {
				if (map.get(tp) != null) {
					kafkaConsumer.seek(tp,map.get(tp).offset());
				}
			}
		}else if (props.containsKey("consumer.fromEnd")){
			Set<TopicPartition> assignment = kafkaConsumer.assignment();
			for(TopicPartition topicPartition : assignment){
				logger.info("assigned topic={},partition={}",topicPartition.topic(),topicPartition.partition());
			}
			kafkaConsumer.seekToEnd(assignment);
			// in order to evaluate the position of marker for each assigned partition
			for(TopicPartition topicPartition : assignment){
				kafkaConsumer.position(topicPartition);
			}
		}
	}

	private void startConsumerThread(String threadName){
		 new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					subscribed = true;
					readyLatch.countDown();
					runLoop();
				} catch (Exception e) {
					logger.error("unhandled exception={}", e.getMessage(), e);
				} finally {
					logger.info("consumer={} instance with clientId={} in group={} unsubscribe from topic={}", consumer, clientId, groupId, topicName);
					subscribed = false;
					kafkaConsumer.unsubscribe();
					kafkaConsumer.close();
					unsubscribedLatch.countDown();
				}
			}
		},threadName).start();
	}

	protected void commitOffsets(ConsumerRecords<byte[], byte[]> records) {
		for (TopicPartition topicPartition : records.partitions()) {
			List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(topicPartition);
			long lastoffset = partitionRecords.get(partitionRecords.size() - 1).offset();
			commitOffset(topicPartition,lastoffset);
		}
	}

	protected void commitOffset(ConsumerRecord<byte[],byte[]> record) {
		TopicPartition tp = new TopicPartition(record.topic(),record.partition());
		commitOffset(tp,record.offset());
	}

	protected void commitOffset(TopicPartition topicPartition, Long lastOffset){
		try {
			long commitOffset = lastOffset + 1;
			kafkaConsumer.commitSync(Collections.singletonMap(topicPartition,new OffsetAndMetadata(commitOffset)));
			lastCommittedOffsets.put(topicPartition,kafkaConsumer.committed(topicPartition).offset());
			if (deliveredOffsets != null) {
				deliveredOffsets.remove(topicPartition);
			}
			logger.debug("commit offset - topic={},partition={},offset={}",topicPartition.topic(),topicPartition.partition(),commitOffset);
		} catch (CommitFailedException e) {
			logger.error("failed to commit offset - topic={},partition={},offset={},error={}",
					topicPartition.topic(),topicPartition.partition(),(lastOffset+1),e.getMessage(),e);
		}
	}

	public Map<TopicPartition, Long> getLastCommittedOffsets(){
		return lastCommittedOffsets;
	}

	/**
	 /**
	 *
	 * Deserialize a byte array into generic message type T
	 *
	 * @param b byte array input
	 * @return deserialized message
	 * @throws SerializationException if deserialization failed
	 */
	protected  T handleSerialization(byte []b) throws SerializationException{
		T msg = null;
		try {
			msg = serializer.deserialize(b);
		}catch (SerializationException serEx) {
			logger.error("could not de-serialize message from topic={}, stack={}",topicName,exceptionStackTraceAsString(serEx));
			stats.incrSerFailCount();

			if (throwOnSerEx) {
				throw serEx;
			}
		}
		return msg;
	}

	protected void deliver(T msg, byte[] key) {
		try {
			if (msg !=null) {
				consumer.onMessage(msg, key);
				stats.incrDeliveredCount();
			}
			
		} catch (Exception ex) {

			stats.incrFailureCount();

			// TODO : deprecate this flag, handle delivery exceptions depending on it's type i.e retriable or failure
			if (throwOnDeliveryFailure)
				throw ex;

			throw new ClientDeliveryException(ex);
		}
	}
	public abstract void runLoop() throws Exception;
	
	@Override
	public void deregister() {
		if (subscribed) {
			logger.info("start deregister consumer={} instance with clientId={} in group={}",consumer,clientId,groupId);
			try {
				subscribed = false;
				unsubscribedLatch.await();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
				throw new RuntimeException(e);
			}
			logger.info("finished deregister consumer={} instance with clientId={} in group={}",consumer, clientId, groupId);
		}else{
			throw new IllegalStateException("consumer is not subscribed to topic="+topicName);
		}
	}
	

	private class RebalanceListener implements ConsumerRebalanceListener {

		private Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

			for (TopicPartition tp: partitions) {
				logger.info("revoked partition topic={},partition={}" , tp.topic(),tp.partition());
				Long offset = deliveredOffsets.get(tp);
				if ( offset != null){
					logger.info("commit offset before partition gets revoked - topic={},partition={},offset={}",tp.topic(),tp.partition(),offset);
					commitOffset(tp,offset);
				}
				lastCommittedOffsets.remove(tp);
			}
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			for (TopicPartition tp: partitions) {
				Long offset = null;
				if (kafkaConsumer.committed(tp) != null) {
					offset = kafkaConsumer.committed(tp).offset();
					lastCommittedOffsets.put(tp,offset);
				}
				logger.info("assigned partition topic={},partition={},offset={}", tp.topic(),tp.partition(),offset);
			}
		}

	}

	protected abstract KafkaProducer<byte[],byte[]> getErrorProducer();

	protected void handleException(Exception ex,T msg,ConsumerRecord<byte[],byte[]> record){
		byte[] value = record.value();

		String errorTopic = ex instanceof ConsumerRetriableException ? topicName+RETRY_TOPIC_SUFFIX : topicName+FAILURE_TOPIC_SUFFIX;

		Throwable cause = getRootCause(ex);
		String exMessage = cause.getMessage() != null ? cause.getMessage() : ex.getMessage();

		// log exception
		logger.error("unable to consume message={} from topic={},partition={},offset={} => type={},exception={}",msg,record.topic(),
				record.partition(),record.offset(),ex.getClass(),exMessage);

		if (msg != null){
			T exDecoratedMsg = exceptionDecorate(cause,exMessage,msg,0,System.currentTimeMillis());
			value = serializer.serialize(exDecoratedMsg);
		}

		produceErrorRecord(errorTopic,record.key(),value);
	}

	protected void produceErrorRecord(String topic, byte[] key, byte[] value){
		KafkaProducer<byte[], byte[]> kafkaProducer = getErrorProducer();

		ProducerRecord<byte[], byte[]> errRecord = new ProducerRecord<>(topic,key,value);
		try {
			RecordMetadata recordMetadata = kafkaProducer.send(errRecord).get();
			logger.warn("message added to topic={},partition={},offset={}",recordMetadata.topic(),
					recordMetadata.partition(),recordMetadata.offset());
		} catch (InterruptedException | ExecutionException e) {
			logger.error("could not add message to topic={} due to exception={}",topic,e.getMessage(),e);
		}
	}

	protected String exceptionMessage(Exception ex){
		Throwable cause = getRootCause(ex);
		if (cause != null){
			return cause.getMessage();
		}else {
			return "";
		}
	}

	public static String exceptionStackTraceAsString(Throwable ex){
		return Arrays.toString(ex.getStackTrace());
	}

	public static <T> T exceptionDecorate(Throwable cause, String exMessage, T msg,int retryCount,long lastTs) {

		if (msg instanceof Envelope) {
			Envelope envelope = (Envelope) msg;
			envelope.addHeader(EXCEPTION_STACK_HEADER, exceptionStackTraceAsString(cause));
			envelope.addHeader(EXCEPTION_MSG_HEADER, exMessage);
			envelope.addHeader(DELIVERY_COUNT_HEADER,Integer.toString(retryCount));
			envelope.addHeader(LAST_DELIVERY_TS, Long.toString(System.currentTimeMillis()));
		}

		return msg;
	}

	public static <T> T rewrapMessage(Exception ex, T msg, int retryCount, long lastTs){
		Throwable cause = getRootCause(ex);
		String exMessage = cause.getMessage() != null ? cause.getMessage() : ex.getMessage();
		return exceptionDecorate(cause,exMessage,msg, retryCount,lastTs);
	}

	public static Throwable getRootCause(Throwable e) {
		Throwable cause = null;
		Throwable result = e;
		while(null != (cause = result.getCause())  && (result != cause) ) {
			result = cause;
		}
		return result;
	}

}
