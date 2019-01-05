package org.marcusbb.queue.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.kafka.consumer.AbstractConsumer;
import org.marcusbb.queue.kafka.consumer.impl.AtMostOnceMsgConsumer;
import org.marcusbb.queue.kafka.consumer.impl.KafkaConsumerDispatcher;
import org.marcusbb.queue.kafka.consumer.impl.KafkaRetryConsumerDispatcher;
import org.marcusbb.queue.kafka.producer.Producer;
import org.marcusbb.queue.kafka.producer.impl.DefaultAsyncProducer;
import org.marcusbb.queue.kafka.producer.impl.SimpleProducer;
import org.marcusbb.queue.kafka.utils.MockKeyBuilder;
import org.marcusbb.queue.kafka.utils.MockVersionCipher;
import org.marcusbb.queue.kafka.utils.TestConsumer;
import org.marcusbb.queue.kafka.utils.TestMessage;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.impl.JavaMessageSerializer;
import org.marcusbb.queue.serialization.impl.ems.AvroEMS;
import org.marcusbb.queue.serialization.impl.ems.EncryptedMessageSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ConsumerTest extends KafkaTestBase {

	Properties props;
	String topic;
	Producer<RoutableEncryptedMessage<TestMessage>> producer;
	ByteSerializer<RoutableEncryptedMessage<TestMessage>> serializer;
	AbstractConsumer<RoutableEncryptedMessage<TestMessage>> dispatcher;


	@Before
	public void before() throws Exception {
		props = new Properties();
		props.putAll(globalProps);
		String groupId = "test-group-id-" + new Random().nextLong();
		props.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
		serializer = new AvroEMS<>(new MockVersionCipher(),new MockKeyBuilder(),iv,keyName);
		topic = "test-topic-"+new Random().nextLong();
		producer = new SimpleProducer<>(props, serializer);
		props.put(ConsumerConfig.CLIENT_ID_CONFIG,"clientId-"+new Random().nextInt());
	}

	@Test
	public void simpleConsumption() throws Exception {
		createTopic(topic,1);

		TestConsumer<RoutableEncryptedMessage<TestMessage>> consumer = new TestConsumer<>();

		dispatcher = new KafkaConsumerDispatcher<>(props,serializer);
		dispatcher.register(topic,consumer);

		List<TestMessage> testMessages = produceRoutableMessage(producer,topic,2);

		waitForConsumption(dispatcher,2);

		assertEquals(2,dispatcher.stats.getDeliveredCount());
		assertEquals(2, consumer.messagesDelivered.size());
		assertEquals(testMessages.get(0),consumer.messagesDelivered.get(0).getPayload());
		assertEquals(testMessages.get(1),consumer.messagesDelivered.get(1).getPayload());
	}

	@Test
	public void consumptionFromMultiplePartitions() throws Exception {
		createTopic(topic,2);
		
		TestConsumer consumer = new TestConsumer<>();
		dispatcher = new KafkaConsumerDispatcher<>(props,serializer);
		dispatcher.register(topic,consumer);

		int numMessages = 4;
		produceRoutableMessage(producer, topic, numMessages);

		waitForLastCommittedOffset(dispatcher,PARTITION_0,2);
		waitForLastCommittedOffset(dispatcher,PARTITION_1,2);

		assertEquals(numMessages,dispatcher.stats.getDeliveredCount());
		assertEquals(numMessages, consumer.messagesDelivered.size());
	}

	@Test
	public void simpleAsyncProduceAndConsume() throws Exception {
		createTopic(topic);
		
		DefaultAsyncProducer<RoutableEncryptedMessage<TestMessage>,Object> producer = new DefaultAsyncProducer<>(props, serializer);
		
		TestConsumer testConsumer = new TestConsumer<>();
		dispatcher = new KafkaConsumerDispatcher<>(props,serializer);
		dispatcher.register(topic,testConsumer);

		producer.add(topic,new RoutableEncryptedMessage<>(new TestMessage("hello_world_" + System.nanoTime())));
		producer.add(topic,new RoutableEncryptedMessage<>(new TestMessage("hello_world_" + System.nanoTime())));
		
		waitForConsumption(dispatcher,2);

		assertEquals(2,dispatcher.stats.getDeliveredCount());
		producer.close();
	}

	@Test
	public void deliveryFailure() throws Exception {
		String retryTopic = topic+AbstractConsumer.RETRY_TOPIC_SUFFIX;
		createTopic(topic);
		createTopic(retryTopic);

		TestConsumer failureConsumer = new TestConsumer<>(true);
		dispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		dispatcher.register(topic,failureConsumer);

		KafkaConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> retryTopicDispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		TestConsumer retryTopicConsumer = new TestConsumer<>();
		retryTopicDispatcher.register(retryTopic,retryTopicConsumer);

		produceRoutableMessage(producer, topic, 1);

		waitForConsumption(retryTopicDispatcher,1);

		assertEquals(1,retryTopicConsumer.messagesDelivered.size());

		retryTopicDispatcher.deregister();
	}

	@Test
	public void serializationFailure() throws Exception {
		String failureTopic = topic+AbstractConsumer.FAILURE_TOPIC_SUFFIX;
		createTopic(topic);
		createTopic(failureTopic);

		TestConsumer consumer = new TestConsumer<>();
		EncryptedMessageSerializer<TestMessage> ems = new EncryptedMessageSerializer<TestMessage>(new MockVersionCipher(), new MockKeyBuilder(), iv, keyName, new JavaMessageSerializer<TestMessage>()) {
		};
		dispatcher = new KafkaConsumerDispatcher<>(props,ems); // in-compatible serializer
		dispatcher.register(topic,consumer);

		KafkaConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> failureTopicDispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		TestConsumer failureTopicConsumer = new TestConsumer<>();
		failureTopicDispatcher.register(failureTopic,failureTopicConsumer);

		produceRoutableMessage(producer, topic, 2);

		waitForConsumption(failureTopicDispatcher,2);

		assertEquals(0,dispatcher.stats.getDeliveredCount());
		assertEquals(0,dispatcher.stats.getFailureCount());
		assertEquals(2,dispatcher.stats.getSerFailCount());
		assertEquals(2,failureTopicDispatcher.stats.getDeliveredCount());
		assertEquals(0,consumer.messagesDelivered.size());
		assertEquals(2,failureTopicConsumer.messagesDelivered.size());
		
		failureTopicDispatcher.deregister();
	}

	@Test
	public void commitFailure() throws Exception {
		createTopic(topic,1);

		// consume message to get starting offset
		dispatcher = new KafkaConsumerDispatcher<>(props,serializer);
		dispatcher.register(topic,new TestConsumer());
		produceRoutableMessage(producer, topic, 1);

		waitForConsumption(dispatcher,1);

		dispatcher.deregister();

		produceRoutableMessage(producer, topic, 1);

		// change poll timeout to induce commit offset failure
		props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,2000);
		dispatcher = new KafkaConsumerDispatcher(props, serializer);
		TestConsumer slowConsumer = new TestConsumer(){

			public void onMessage(RoutableEncryptedMessage<TestMessage> msg) {
				messagesDelivered.add(msg);
				try {
					Thread.sleep(4000);
				} catch (InterruptedException e) {
					// do nothing
				}
			}

		};
		dispatcher.register(topic,slowConsumer);

		waitForLastCommittedOffset(dispatcher,2);

		assertEquals(1,dispatcher.stats.getDeliveredCount());
		assertEquals(1, slowConsumer.messagesDelivered.size());

	}

	@Test
	public void failureCount() throws Exception {
		String retryTopic = topic+AbstractConsumer.RETRY_TOPIC_SUFFIX;
		createTopic(topic);
		createTopic(retryTopic);
		
		TestConsumer failureConsumer = new TestConsumer<>(true);
		dispatcher = new KafkaConsumerDispatcher<>(props,serializer);
		dispatcher.register(topic,failureConsumer);

		produceRoutableMessage(producer, topic, 2);

		waitForConsumption(dispatcher,2);

		assertEquals(0,dispatcher.stats.getDeliveredCount());
		assertEquals(2,dispatcher.stats.getFailureCount());
		assertEquals(0,dispatcher.stats.getSerFailCount());
		
	}

	@Test
	public void fromCommittedOffset() throws Exception {
		createTopic(topic);

		TestConsumer consumer = new TestConsumer<>();
		dispatcher = new KafkaConsumerDispatcher<>(props,serializer);
		dispatcher.register(topic,consumer);

		produceRoutableMessage(producer, topic, 2);

		waitForConsumption(dispatcher,2);
		assertEquals(2,dispatcher.stats.getDeliveredCount());
		dispatcher.deregister();

		dispatcher = new KafkaConsumerDispatcher<>(props,serializer);
		produceRoutableMessage(producer, topic, 2);
		dispatcher.register(topic,consumer);

		waitForLastCommittedOffset(dispatcher,4);
		assertEquals(2,dispatcher.stats.getDeliveredCount());
	}

	@Test
	public void fromBeginning() throws Exception{
		createTopic(topic);

		TestConsumer consumer = new TestConsumer<>();

		produceRoutableMessage(producer, topic, 2);
		
		props.put("consumer.fromBeginning", "");
		dispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		dispatcher.register(topic,consumer);

		waitForConsumption(dispatcher,2);
		assertEquals(2,dispatcher.stats.getDeliveredCount());
		dispatcher.deregister();

		// register again to consume from committed offset
		produceRoutableMessage(producer, topic, 1);
		dispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		dispatcher.register(topic,consumer);

		waitForConsumption(dispatcher,3);
		assertEquals(3,dispatcher.stats.getDeliveredCount());
	}

	@Test
	public void fromTS() throws Exception {
		createTopic(topic);

		TestConsumer consumer = new TestConsumer<>();

		long t1 = System.currentTimeMillis();
		produceRoutableMessage(producer, topic, 1);
		long t2 = System.currentTimeMillis();
		produceRoutableMessage(producer, topic, 1);

		props.put("consumer.fromTS", t2);
		dispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		dispatcher.register(topic,consumer);
		waitForConsumption(dispatcher,1);
		assertEquals(1,dispatcher.stats.getDeliveredCount());
		dispatcher.deregister();

		props.put("consumer.fromTS", t1);
		dispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		dispatcher.register(topic,consumer);
		waitForConsumption(dispatcher,2);
		assertEquals(2,dispatcher.stats.getDeliveredCount());
		dispatcher.deregister();

		props.put("consumer.fromTS",System.currentTimeMillis());
		dispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		dispatcher.register(topic,consumer);
		
		assertEquals(2,getLastCommittedOffset(dispatcher));
		assertEquals(0,dispatcher.stats.getDeliveredCount());
	}

	@Test
	public void fromEnd() throws Exception {

		createTopic(topic);

		TestConsumer consumer = new TestConsumer<>();

		dispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		dispatcher.register(topic,consumer);
		produceRoutableMessage(producer, topic, 2);

		waitForConsumption(dispatcher,2);
		assertEquals(2,dispatcher.stats.getDeliveredCount());
		dispatcher.deregister();

		props.put("consumer.fromEnd", "");
		dispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		produceRoutableMessage(producer, topic, 2);
		dispatcher.register(topic,consumer);
		produceRoutableMessage(producer, topic, 1);

		waitForLastCommittedOffset(dispatcher,5);
		assertEquals(1,dispatcher.stats.getDeliveredCount());
	}


	@Test
	public void registerDeregisterAndRegister() throws Exception {
		String retryTopic = topic+AbstractConsumer.RETRY_TOPIC_SUFFIX;
		createTopic(topic);
		createTopic(retryTopic);

		Properties retryDispatcherProps = new Properties();
		retryDispatcherProps.putAll(props);
		retryDispatcherProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-retry-id-" + new Random().nextLong());
		KafkaRetryConsumerDispatcher retryConsumerDispatcher = new KafkaRetryConsumerDispatcher(retryDispatcherProps,serializer);
		retryConsumerDispatcher.register(retryTopic,new TestConsumer());

		dispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		dispatcher.register(topic,new TestConsumer());

		produceRoutableMessage(producer, topic, 1);

		waitForConsumption(dispatcher,1);

		dispatcher.deregister();

		produceRoutableMessage(producer, topic, 1);

		// register with a failing consumer to verify the re-initialization of error producer
		// and if it can still produce to "retry" topic
		dispatcher.register(topic,new TestConsumer(true));

		waitForConsumption(dispatcher,1);

		// check if it was able to produce to retry topic
		waitForConsumption(retryConsumerDispatcher,1);
		retryConsumerDispatcher.deregister();
	}


	@Test
	public void atMostOnce() throws Exception {
		String retryTopic = topic + AbstractConsumer.RETRY_TOPIC_SUFFIX;
		createTopic(topic);
		createTopic(retryTopic);
		AtMostOnceMsgConsumer<RoutableEncryptedMessage<TestMessage>> atMostOnceDispatcher = new AtMostOnceMsgConsumer<>(props, serializer);
		atMostOnceDispatcher.register(topic, new TestConsumer(true));

		KafkaRetryConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> retryDispatcher = new KafkaRetryConsumerDispatcher<>(props, serializer);
		retryDispatcher.register(retryTopic,new TestConsumer(false));

		produceRoutableMessage(producer, topic, 1);
		waitForLastCommittedOffset(atMostOnceDispatcher,1);

		assertEquals(0, atMostOnceDispatcher.stats.getDeliveredCount() );
		assertEquals(1, atMostOnceDispatcher.stats.getFailureCount() );
		assertEquals(1,getLastCommittedOffset(atMostOnceDispatcher));
		assertEquals(0,getLastCommittedOffset(retryDispatcher));

		atMostOnceDispatcher.deregister();
		retryDispatcher.deregister();
	}

	@Test
	public void consumerDeregister() throws InterruptedException {
		createTopic(topic);

		int recordCount = 100;
		props.put("max.poll.records",recordCount);
		CountDownLatch latch = new CountDownLatch(recordCount);
		props.setProperty("consumer.fromBeginning","");

		produceRoutableMessage(producer, topic, recordCount);

		TestConsumer testConsumer = new TestConsumer<>();
		dispatcher = new KafkaConsumerDispatcher<>(props,serializer);
		dispatcher.register(topic,testConsumer);

		// sleep to give the dispatcher thread time to schedule and poll
		Thread.sleep(1000);

		// messages delivered
		assertTrue(testConsumer.messagesDelivered.size() > 0);

		dispatcher.deregister();

		// completely drained whole queue
		assertEquals(recordCount,dispatcher.stats.getDeliveredCount());
		assertEquals(recordCount,getLastCommittedOffset(dispatcher));

		dispatcher = null;
	}

	@Test
	public void latestCommittedOffset() throws Exception {
		createTopic(topic);

		dispatcher = new KafkaConsumerDispatcher<>(props, serializer);
		TestConsumer testConsumer = new TestConsumer<>();
		dispatcher.register(topic,testConsumer);

		int numMessages = 2;
		produceRoutableMessage(producer, topic, numMessages);

		waitForConsumption(testConsumer,numMessages);

		// get offset from kafka
		KafkaConsumer consumer = new KafkaConsumer(props, new ByteArrayDeserializer(),new ByteArrayDeserializer());
		TopicPartition topicPartition = new TopicPartition(topic, PARTITION_0);
		HashMap<TopicPartition,Long> committedOffsetMap = new HashMap<>();
		OffsetAndMetadata committed = consumer.committed(topicPartition);
		if (committed != null){
			committedOffsetMap.put(topicPartition,committed.offset());
		}

		assertEquals(committedOffsetMap,dispatcher.getLastCommittedOffsets());
	}

	@Test
	public void consumerGroup() throws Exception {
		createTopic(topic,3);

		TestConsumer testConsumer1 = new TestConsumer();
		TestConsumer testConsumer2 = new TestConsumer();
		TestConsumer testConsumer3 = new TestConsumer();
		//EncryptedMessageSerializer<TestMessage> ems = new AvroEMS<TestMessage>(new MockVersionCipher(),new MockKeyBuilder(),iv,keyName);
		KafkaConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> dispatcher1 = new KafkaConsumerDispatcher<>(props, serializer);
		dispatcher1.register(topic,testConsumer1);

		KafkaConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> dispatcher2 = new KafkaConsumerDispatcher<>(props,serializer);
		dispatcher2.register(topic,testConsumer2);

		KafkaConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> dispatcher3 = new KafkaConsumerDispatcher<>(props, serializer);
		dispatcher3.register(topic,testConsumer3);

		Thread.sleep(1000); // wait for consumer rebalance to complete

		produceRoutableMessage(producer,topic,3);

		waitForConsumption(testConsumer1,1);
		waitForConsumption(testConsumer2,1);
		waitForConsumption(testConsumer3,1);

		dispatcher1.deregister();
		dispatcher2.deregister();
		dispatcher3.deregister();
	}

	@After
	public void close(){
		if (dispatcher != null)
			dispatcher.deregister();
		producer.close();
	}

}

