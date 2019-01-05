package org.marcusbb.queue.kafka;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import javax.transaction.Status;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.marcusbb.queue.kafka.consumer.AbstractConsumer;
import org.marcusbb.queue.kafka.consumer.impl.KafkaConsumerDispatcher;
import org.marcusbb.queue.kafka.producer.impl.SimpleProducer;
import org.marcusbb.queue.kafka.utils.TestConsumer;
import org.marcusbb.queue.kafka.utils.TestMessage;
import org.marcusbb.queue.serialization.impl.JavaMessageSerializer;


public class MultiPartitionConsumerTest extends KafkaTestBase {

	private static final int CONSUMER_REBALANCE_TIME = 1000;
	private static final int PROCESSING_TIME = 1000;
	Properties props;
	String topic ;
	
	SimpleProducer<TestMessage> producer;	
	KafkaConsumerDispatcher<TestMessage> dispatcher1;
	KafkaConsumerDispatcher<TestMessage> dispatcher2;
	KafkaConsumerDispatcher<TestMessage> dispatcher3;
	TestConsumer<TestMessage> consumer1;
	TestConsumer<TestMessage> consumer2;
	TestConsumer<TestMessage> consumer3;
	JavaMessageSerializer<TestMessage>javaMessageSerializer;
	
	@Before
	public void before() throws Exception {
		props = new Properties();
		props.putAll(globalProps);
		props.put("group.id", "MultiPartitionTest-gid");

		javaMessageSerializer = new JavaMessageSerializer<>();
		
		producer = new SimpleProducer<>(props, javaMessageSerializer);
		topic = "test-topic-"+new Random().nextLong();
		consumer1 = new TestConsumer<>();
		consumer2 = new TestConsumer<>();
		consumer3 = new TestConsumer<>();
	}

	@After
	public void after() {

		if (dispatcher1!= null){			
			dispatcher1.deregister();
			dispatcher1 = null;
		}
		if (dispatcher2!= null){
			dispatcher2.deregister();
			dispatcher2 = null;
		}
		if (dispatcher3!= null){
			dispatcher3.deregister();
			dispatcher3 = null;
		}
	}
	
	@Test
	public void consumptionTwoConsumersThreePartitions() throws Exception {
		
		dispatcher1 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		dispatcher2 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		
		int numPartitions = 3;
		createTopic(topic, numPartitions);
		
		int key1 = 1;
		int key2 = 100000000;
		
		dispatcher1.register(topic, consumer1);
		dispatcher2.register(topic, consumer2);

		// give some time to finish re-balancing existing consumers
		Thread.sleep(CONSUMER_REBALANCE_TIME);
		
		// key1: 2 messages
		produceTestMessage(producer,topic,key1,2);

		// key2: 3 messages
		produceTestMessage(producer,topic,key2,3);

		waitForConsumption(5,consumer1,consumer2);

		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2);
		logConsumerMessageDeliveredCount(consumer1, consumer2);
	}
	
	
	@Test
	public void consumptionThreeConsumersThreePartitions() throws Exception {
		
		dispatcher1 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		dispatcher2 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		dispatcher3 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		
		int numPartitions = 3;
		createTopic(topic, numPartitions);
		
		int key1 = 1;
		int key2 = 20;
		int key3 = 2;
		
		dispatcher1.register(topic, consumer1);
		dispatcher2.register(topic, consumer2);
		dispatcher3.register(topic, consumer3);

		// give some time to finish re-balancing existing consumers
		Thread.sleep(CONSUMER_REBALANCE_TIME);
		
		// key1: 2 messages, partition=0
		produceTestMessage(producer,topic,key1,2);
		
		// key2: 3 messages, partition=1
		produceTestMessage(producer,topic,key2,3);
		
		// key3: 5 messages, partition=2
		produceTestMessage(producer,topic,key3,5);

		waitForConsumption(10,consumer1,consumer2,consumer3);

		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2, dispatcher3);
		logConsumerMessageDeliveredCount(consumer1, consumer2, consumer3);

		assertEquals(1,dispatcher1.getLastCommittedOffsets().size());
		assertEquals(1,dispatcher2.getLastCommittedOffsets().size());
		assertEquals(1,dispatcher3.getLastCommittedOffsets().size());

		int [] partitionMessageCount = new int[]{2,3,5};
		assertThat(consumer1.messagesDelivered.size(), 
				equalTo(partitionMessageCount[dispatcher1.getLastCommittedOffsets().keySet().iterator().next().partition()]));
		assertThat(consumer2.messagesDelivered.size(),
				equalTo(partitionMessageCount[dispatcher2.getLastCommittedOffsets().keySet().iterator().next().partition()]));
		assertThat(consumer3.messagesDelivered.size(),
				equalTo(partitionMessageCount[dispatcher3.getLastCommittedOffsets().keySet().iterator().next().partition()]));
	}
	
	@Test
	public void consumptionThreeConsumersThreePartitionsMixedKeyOrder() throws Exception {
		
		dispatcher1 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		dispatcher2 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		dispatcher3 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		
		int numPartitions = 3;
		createTopic(topic, numPartitions);
		
		int key1 = 1;
		int key2 = 20;
		int key3 = 2;
		String msgContent1 = "key_" + key1 + "_" + System.currentTimeMillis();
		String msgContent2 = "key_" + key2 + "_" + System.currentTimeMillis();
		String msgContent3 = "key_" + key3 + "_" + System.currentTimeMillis();
		
		dispatcher1.register(topic, consumer1);
		dispatcher2.register(topic, consumer2);
		dispatcher3.register(topic, consumer3);

		// give some time to finish re-balancing existing consumers
		Thread.sleep(CONSUMER_REBALANCE_TIME);
		
		// key1: 2 messages
		// key2: 3 messages
		// key3: 5 messages
		producer.add(topic, new TestMessage(msgContent3), key3);
		producer.add(topic, new TestMessage(msgContent3), key3);		
		producer.add(topic, new TestMessage(msgContent2), key2);
		producer.add(topic, new TestMessage(msgContent1), key1);
		producer.add(topic, new TestMessage(msgContent1), key1);
		producer.add(topic, new TestMessage(msgContent2), key2);
		producer.add(topic, new TestMessage(msgContent3), key3);
		producer.add(topic, new TestMessage(msgContent3), key3);
		producer.add(topic, new TestMessage(msgContent2), key2);		
		producer.add(topic, new TestMessage(msgContent3), key3);
		
		waitForConsumption(10, consumer1, consumer2, consumer3);

		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2, dispatcher3);
		logConsumerMessageDeliveredCount(consumer1, consumer2, consumer3);
		
		int [] partitionMessageCount = new int[]{2,3,5};	
		assertThat(consumer1.messagesDelivered.size(), 
				equalTo(partitionMessageCount[dispatcher1.getLastCommittedOffsets().keySet().iterator().next().partition()]));
		assertThat(consumer2.messagesDelivered.size(),
				equalTo(partitionMessageCount[dispatcher2.getLastCommittedOffsets().keySet().iterator().next().partition()]));
		assertThat(consumer3.messagesDelivered.size(),
				equalTo(partitionMessageCount[dispatcher3.getLastCommittedOffsets().keySet().iterator().next().partition()]));
		
		// TODO: replace with more deterministic verification - 
		// once partition info is available at produce time
		verifyMessagesConsumedByOneConsumer(msgContent1, 2, consumer1, consumer2, consumer3);
		verifyMessagesConsumedByOneConsumer(msgContent2, 3, consumer1, consumer2, consumer3);
		verifyMessagesConsumedByOneConsumer(msgContent3, 5, consumer1, consumer2, consumer3);
	}
	
	
	@Test
	public void initialConsumptionTwoConsumersThreePartitionsAfterProducingToTopic() throws Exception {
		
		dispatcher1 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		dispatcher2 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		
		int numPartitions = 3;
		createTopic(topic, numPartitions);
		
		int key1 = 1;
		int key2 = 100000000;
		String msgContent1 = "key_" + key1 + "_" + System.currentTimeMillis();
		String msgContent2 = "key_" + key2 + "_" + System.currentTimeMillis();

		// produce before initial consumer registration - will not be consumed
		// key1: 2 messages
		produceTestMessage(producer, topic, key1, 2, msgContent1);
		// key2: 3 messages
		produceTestMessage(producer, topic, key2, 3, msgContent2);		
		
		dispatcher1.register(topic, consumer1);
		dispatcher2.register(topic, consumer2);

		// give some time to finish re-balancing existing consumers
		Thread.sleep(CONSUMER_REBALANCE_TIME);
		
		// key1: 2 messages
		produceTestMessage(producer, topic, key1, 2, msgContent1);
		// key2: 3 messages
		produceTestMessage(producer, topic, key2, 3, msgContent2);
		
		waitForConsumption(5, consumer1, consumer2);

		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2);
		logConsumerMessageDeliveredCount(consumer1, consumer2);
		
		// TODO: replace with more deterministic verification - 
		// once partition info is available at produce time
		verifyMessagesConsumedByOneConsumer(msgContent1, 2, consumer1, consumer2);
		verifyMessagesConsumedByOneConsumer(msgContent2, 3, consumer1, consumer2);
	}
	
	@Test
	public void initialConsumptionTwoConsumersThreePartitionsAfterProducingToTopicWithFromBeginningSetting() throws Exception {
		
		props.put("consumer.fromBeginning", "");
		dispatcher1 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		dispatcher2 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		
		int numPartitions = 3;
		createTopic(topic, numPartitions);
		
		int key1 = 1;
		int key2 = 100000000;
		String msgContent1 = "key_" + key1 + "_" + System.currentTimeMillis();
		String msgContent2 = "key_" + key2 + "_" + System.currentTimeMillis();


		// produce before initial consumer registration - will be consumed with setting: consumer.fromBeginning
		// key1: 2 messages
		produceTestMessage(producer, topic, key1, 2, msgContent1);
		// key2: 3 messages
		produceTestMessage(producer, topic, key2, 3, msgContent2);
		
		dispatcher1.register(topic, consumer1);
		Thread.sleep(CONSUMER_REBALANCE_TIME);
		logDispatcherPartitionsOffsets(numPartitions, dispatcher1);
		logConsumerMessageDeliveredCount(consumer1);
		
		dispatcher2.register(topic, consumer2);

		// give some time to finish re-balancing existing consumers
		Thread.sleep(CONSUMER_REBALANCE_TIME);
		// NOTE: rebalancing causes consumer to consume messages 
		// from all assigned partitions after rebalancing

		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2);
		logConsumerMessageDeliveredCount(consumer1, consumer2);
		
		// key1: 2 messages
		produceTestMessage(producer, topic, key1, 2, msgContent1);
		// key2: 3 messages
		produceTestMessage(producer, topic, key2, 3, msgContent2);
		
		Thread.sleep(CONSUMER_REBALANCE_TIME);
		waitForConsumption(13, consumer1, consumer2);

		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2);
		logConsumerMessageDeliveredCount(consumer1, consumer2);

	}
	
	
	@Test
	public void postInitialConsumptionTwoConsumersThreePartitionsAfterProducingToTopic() throws Exception {
		
		/* Scenario
		 * 1. Produce 5 messages to the topic which has no registered consumers 
		 *  - partition offset is null
		 *  - settings consumer.fromBeginning not set
		 * 2. Register 2 consumers to the topic
		 * 3. Produce 5 messages to the topic
		 *  - consumers will consume messages from step 3
		 *  - partition offset will be set for each partition
		 * 4. Unregister consumers
		 * 5. Produce 5 messages to the topic
		 * 6. Register 1 consumer to the topic
		 * 7. Verify that consumer consumed messages from step 5.
		 */
		
		dispatcher1 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		dispatcher2 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		
		int numPartitions = 3;
		createTopic(topic, numPartitions);
		
		int key1 = 1;
		int key2 = 100000000;
		String msgContent1 = "key_" + key1 + "_" + System.currentTimeMillis();
		String msgContent2 = "key_" + key2 + "_" + System.currentTimeMillis();

		// produce before initial consumer registration - will not be consumed
		// key1: 2 messages
		produceTestMessage(producer, topic, key1, 2, msgContent1);
		// key2: 3 messages
		produceTestMessage(producer, topic, key2, 3, msgContent2);
		
		dispatcher1.register(topic, consumer1);
		dispatcher2.register(topic, consumer2);

		// give some time to finish re-balancing existing consumers
		Thread.sleep(CONSUMER_REBALANCE_TIME);
		
		// key1: 2 messages
		produceTestMessage(producer, topic, key1, 2, msgContent1);
		// key2: 3 messages
		produceTestMessage(producer, topic, key2, 3, msgContent2);
		
		waitForConsumption(5, consumer1, consumer2);

		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2);
		logConsumerMessageDeliveredCount(consumer1, consumer2);
		
		dispatcher1.deregister();
		dispatcher2.deregister();
		
		String msgContent11 = "key_" + key1 + "_" + System.currentTimeMillis();
		String msgContent21 = "key_" + key2 + "_" + System.currentTimeMillis();
		
		// key1: 3 messages
		produceTestMessage(producer, topic, key1, 2, msgContent11);
		// key2: 2 messages
		produceTestMessage(producer, topic, key2, 3, msgContent21);
		
		dispatcher1.register(topic, consumer1);
		// NOTE: if we register another consumer here, 
		//it may not get the messages as consumer1 consumed all messages
		
		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2);
		logConsumerMessageDeliveredCount(consumer1, consumer2);
		
		dispatcher2.register(topic, consumer2);
		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2);
		logConsumerMessageDeliveredCount(consumer1, consumer2);
		
	}
	
	@Test
	public void debugPartitionCleanupWhenConsumerRebalancing() throws Exception {
		dispatcher1 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		dispatcher2 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		dispatcher3 = new KafkaConsumerDispatcher<>(props, javaMessageSerializer);
		
		int numPartitions = 3;
		createTopic(topic, numPartitions);
		
		int key1 = 1;
		int key2 = 0;
		int key3 = 99;
		String msgContent1 = "key_" + key1 + "_" + System.currentTimeMillis();
		String msgContent2 = "key_" + key2 + "_" + System.currentTimeMillis();
		String msgContent3 = "key_" + key3 + "_" + System.currentTimeMillis();

		dispatcher1.register(topic, consumer1);
		dispatcher2.register(topic, consumer2);
		// give some time to finish re-balancing existing consumers
		Thread.sleep(CONSUMER_REBALANCE_TIME);
		
		// key1: 1 message
		producer.add(topic, new TestMessage(msgContent1), key1);
		// key2: 2 messages
		producer.add(topic, new TestMessage(msgContent2), key2);
		producer.add(topic, new TestMessage(msgContent2), key2);
		// key3: 3 messages
		producer.add(topic, new TestMessage(msgContent3), key3);
		producer.add(topic, new TestMessage(msgContent3), key3);
		producer.add(topic, new TestMessage(msgContent3), key3);
		producer.flush();
		Thread.sleep(PROCESSING_TIME);

		System.out.println("************* offsets BEFORE rebalancing *************");
		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2);
		logConsumerMessageDeliveredCount(consumer1, consumer2);
		
		dispatcher3.register(topic, consumer3);
		// give some time to finish re-balancing existing consumers
		Thread.sleep(CONSUMER_REBALANCE_TIME);
		System.out.println("************* offsets AFTER rebalancing *************");
		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2, dispatcher3);
		logConsumerMessageDeliveredCount(consumer1, consumer2, consumer3);
		
		// key1: 1 message
		producer.add(topic, new TestMessage(msgContent1), key1);
		// key2: 2 messages
		producer.add(topic, new TestMessage(msgContent2), key2);
		producer.add(topic, new TestMessage(msgContent2), key2);
		// key3: 3 messages
		producer.add(topic, new TestMessage(msgContent3), key3);
		producer.add(topic, new TestMessage(msgContent3), key3);
		producer.add(topic, new TestMessage(msgContent3), key3);

		Thread.sleep(PROCESSING_TIME);
		System.out.println("************* offsets AFTER producing to rebalanced consumers *************");
		logDispatcherPartitionsOffsets(numPartitions, dispatcher1, dispatcher2, dispatcher3);
		logConsumerMessageDeliveredCount(consumer1, consumer2, consumer3);

	}

	private void logDispatcherPartitionsOffsets(int numPartitions, AbstractConsumer... dispatchers){
		System.out.println("************************************************************");
		for (int i = 0; i < dispatchers.length; i++) {
			System.out.println("--[topic]--[partition]-->[offset]-->dispatcher " + i);
			System.out.println(dispatchers[i].getLastCommittedOffsets());
		}
	}
	
	private void logConsumerMessageDeliveredCount(TestConsumer<?>... consumers){
		System.out.println("************************************************************");
		for (int i = 0; i < consumers.length; i++) {
			System.out.println("----> Consumer " + i + " : messageCount="
					+ consumers[i].messagesDelivered.size());
		}
	}
	
	private void verifyMessagesConsumedByOneConsumer(String messageContent, int expectedMessageCount, TestConsumer<TestMessage>... consumers) {
		Map<Integer, Integer> messageCountMap = new HashMap<>();
		for (int i = 0; i < consumers.length; i++) {
			int messageCount = 0;
			for (TestMessage message : consumers[i].messagesDelivered){
				if (messageContent.equals(message.getContent())){
					messageCount++;
					messageCountMap.put(i, messageCount);
				}
			}
		}
		assertThat( "number of consumers which consumed messages with content: " + messageContent,
				messageCountMap.size(),
				equalTo(1));
		
		Map.Entry<Integer, Integer> messageCountMapEntry = messageCountMap.entrySet().iterator().next();
		assertThat( "number of messages received by consumer with message content: " + messageContent,
				messageCountMapEntry.getValue(),
				equalTo(expectedMessageCount));
	}
}
