package org.marcusbb.queue.kafka;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.kafka.consumer.impl.KafkaConsumerDispatcher;
import org.marcusbb.queue.kafka.producer.impl.KafkaJTAProducer;
import org.marcusbb.queue.kafka.utils.MockKeyBuilder;
import org.marcusbb.queue.kafka.utils.MockVersionCipher;
import org.marcusbb.queue.kafka.utils.TestConsumer;
import org.marcusbb.queue.kafka.utils.TestMessage;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.impl.ems.AvroEMS;

import javax.naming.NamingException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RoundTripTest extends KafkaTestBase {


	@BeforeClass
	public static void setUpOnce()  throws Exception {

	}

	Properties props;

	ByteSerializer<RoutableEncryptedMessage<TestMessage>> avroEMS;

	@Before
	public void before() {
		props = new Properties();
		props.putAll(globalProps);
		props.put("group.id", "test-group-id");

		avroEMS = new AvroEMS<>(new MockVersionCipher(),new MockKeyBuilder(),iv,keyName);
	}

	@Test
	public void testEncryption() {
		RoutableEncryptedMessage<TestMessage> origMsg = new RoutableEncryptedMessage<>(new HashMap<String, String>(), new TestMessage(new String("going my way")));

		byte []encrypted = avroEMS.serialize(origMsg);

		RoutableEncryptedMessage<TestMessage> newMsg = avroEMS.deserialize(encrypted);

		assertEquals(origMsg.getPayload(),newMsg.getPayload());
	}

	@Test
	public void produceAndConsume() throws Exception {

		String topic = "test-enc-topic-"+new Random().nextLong();
		createTopic(topic);

		KafkaJTAProducer<RoutableEncryptedMessage<TestMessage>> producer = new KafkaJTAProducer<>(props, avroEMS);

		CountDownLatch latch = new CountDownLatch(2);
		props.put("group.id", "test-group-id-" + new Random().nextLong() );

		KafkaConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> consumerDispatcher = new KafkaConsumerDispatcher<>(props, avroEMS);
		TestConsumer<RoutableEncryptedMessage<TestMessage>> latchConsumer = new TestConsumer<>(latch);
		consumerDispatcher.register(topic,latchConsumer);

		produceRoutableMessage(producer, topic, 2);

		assertTrue("latch timeout - message delivery failed",latch.await(10,TimeUnit.SECONDS));
		
		consumerDispatcher.deregister();
		producer.close();
	}

}
