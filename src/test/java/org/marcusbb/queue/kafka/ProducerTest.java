package org.marcusbb.queue.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.marcusbb.queue.kafka.consumer.impl.KafkaConsumerDispatcher;
import org.marcusbb.queue.kafka.producer.ProducerResult;
import org.marcusbb.queue.kafka.producer.impl.DefaultAsyncProducer;
import org.marcusbb.queue.kafka.producer.impl.KafkaJTAProducer;
import org.marcusbb.queue.kafka.utils.TestConsumer;
import org.marcusbb.queue.kafka.utils.TestMessage;
import org.marcusbb.queue.serialization.impl.JavaMessageSerializer;

import javax.naming.NamingException;
import javax.transaction.Status;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ProducerTest extends KafkaTestBase {

	Properties props;
	KafkaConsumer<byte [], byte[]> consumer;
	Queue<ConsumerRecords<byte[],byte[]>> consumedRecords = new LinkedList<>();
	boolean polling = true;
	CountDownLatch expectedResult;
	String topic ;
	
	KafkaConsumerDispatcher<TestMessage> konsumer = null;
	TestConsumer<TestMessage> latchConsumer;
	JavaMessageSerializer<TestMessage>javaMessageSerializer = null;
	
	@Before
	public void before() throws Exception {
		txManager.txStatus = Status.STATUS_ACTIVE;
		props = new Properties();
		props.putAll(globalProps);

		txRegistry.resetTxObject();

		//props.put("bootstrap.servers", "localhost:9093");
		props.put("acks", "all");
		//props.put("retries", 0);
		props.put(ProducerConfig.RETRIES_CONFIG,5);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("metadata.fetch.timeout.ms",10000);

		props.put("group.id", "ProducerTest-gid");

		topic = "test-b-topic-"+new Random().nextLong();

		createTopic(topic);

		javaMessageSerializer = new JavaMessageSerializer<>();
		konsumer = new KafkaConsumerDispatcher<>(props,javaMessageSerializer);

		expectedResult = new CountDownLatch(1);
		latchConsumer = new TestConsumer<>(expectedResult);
		konsumer.register(topic, latchConsumer);

	}

	@After
	public void after() {
		polling = false;
		konsumer.deregister();
	}

	@Test
	public void simpleProduceTest() throws NamingException,InterruptedException {
		KafkaJTAProducer<TestMessage> producer = new KafkaJTAProducer<>(props, javaMessageSerializer);
		
		producer.add(topic,new TestMessage("hello_world_ " + System.currentTimeMillis()));
		
		txManager.txStatus = Status.STATUS_COMMITTING;
		producer.beforeCompletion();
		
		producer.afterCompletion(Status.STATUS_COMMITTED);
				
		assertTrue(expectedResult.await(5, TimeUnit.SECONDS));
		//Assert.assertNotNull(consumedRecords.remove());
		producer.close();
		
	}

	@Test
	public void withTxManagerCommittedStatus() throws NamingException,InterruptedException {
		
		KafkaJTAProducer<TestMessage> producer = new KafkaJTAProducer<>(props, javaMessageSerializer,txManager);
		txManager.txStatus = Status.STATUS_ACTIVE;
		producer.add(topic,new TestMessage("hello_world_ " + System.currentTimeMillis()));
		
		txManager.txStatus = Status.STATUS_COMMITTING;
		producer.beforeCompletion();
		
		producer.afterCompletion(Status.STATUS_COMMITTED);
				
		assertTrue(expectedResult.await(5, TimeUnit.SECONDS));
		//Assert.assertNotNull(consumedRecords.remove());
		producer.close();
		
	}
	@Test
	public void withTxRegistryCommittedStatus() throws NamingException,InterruptedException {
		
		KafkaJTAProducer<TestMessage> producer = new KafkaJTAProducer<>(props, javaMessageSerializer,txRegistry);
		
		producer.add(topic,new TestMessage("hello_world_ " + System.currentTimeMillis()));
		//will elicit another "enlist"
		producer.add(topic,new TestMessage("hello_world_1_ " + System.currentTimeMillis()));
		txRegistry.setTransactionStatus(Status.STATUS_COMMITTING);
		producer.beforeCompletion();
		
		producer.afterCompletion(Status.STATUS_COMMITTED);
				
		assertTrue(expectedResult.await(5, TimeUnit.SECONDS));
		//Assert.assertNotNull(consumedRecords.remove());
		producer.close();
		
	}
	
		
	@Test
	public void defaultRollback() throws NamingException,InterruptedException {
		//createTopic("test-rollback");
		KafkaJTAProducer<TestMessage> producer = new KafkaJTAProducer<>(props,  javaMessageSerializer);
		
		
		props.put("key.deserializer", ByteArrayDeserializer.class.getName());
		props.put("value.deserializer", ByteArrayDeserializer.class.getName());//	
		//txManager.txStatus = Status.STATUS_ACTIVE;
		producer.add(topic,new TestMessage("hello_world"));
		
		txRegistry.setTransactionStatus( Status.STATUS_MARKED_ROLLBACK );
		producer.beforeCompletion();
		
				
		//there is no message consumed
		Assert.assertFalse(expectedResult.await(5, TimeUnit.SECONDS));
		producer.close();
	}
	@Test
	public void txManagerRollback() throws NamingException, InterruptedException {
		KafkaJTAProducer<TestMessage> producer = new KafkaJTAProducer<>(props, javaMessageSerializer,txManager);
		
		
		props.put("key.deserializer", ByteArrayDeserializer.class.getName());
		props.put("value.deserializer", ByteArrayDeserializer.class.getName());//	
		txManager.txStatus = Status.STATUS_ACTIVE;
		producer.add(topic,new TestMessage("hello_world"));
		
		txManager.txStatus = Status.STATUS_MARKED_ROLLBACK ;
		producer.beforeCompletion();
		
				
		//there is no message consumed
		Assert.assertFalse(expectedResult.await(5, TimeUnit.SECONDS));
		producer.close();
	}
	
	@Test
	public void exceptionTest() throws NamingException,InterruptedException {
		props.put("retries", 0);
		props.put("timeout.ms",1000);
		props.put("retry.backoff.ms",1000);
		props.put("reconnect.backoff.ms",1000);
		//props.put("max.block.ms",1000);
		KafkaJTAProducer<TestMessage> producer = new KafkaJTAProducer<>(props, javaMessageSerializer);
		
		producer.add(topic,new TestMessage("hello_world_ " + System.currentTimeMillis()));
		
		producer.flush();
		
		producer.close();
		
	}
	/*
	 * This test consistently leads to a timeout on updating metadata.
	 * 
	 */
	//@Test
	public void tightLoopException() throws NamingException,InterruptedException {
		props.put("retries", 0);
		props.put("timeout.ms",1000);
		props.put("retry.backoff.ms",1000);
		props.put("reconnect.backoff.ms",1000);
		props.put("max.block.ms",1000);
		KafkaJTAProducer<TestMessage> producer = new KafkaJTAProducer<>(props, javaMessageSerializer);
		
		for (int i=0;i<1000;i++) {
			producer.add("tightLoop",new TestMessage(""+i));
			producer.flush();
		}
		
		producer.close();
		
	}
	
	@Test
	public void asyncProduce() throws Exception {
		DefaultAsyncProducer<TestMessage,Object> producer = new DefaultAsyncProducer<>(props,javaMessageSerializer);
		ProducerResult<RecordMetadata> result = producer.add(topic,new TestMessage("hello_world_ " + System.currentTimeMillis()) ).get();
		RecordMetadata md = result.getResult();
		assertEquals(0,md.offset());
		assertEquals(topic,md.topic());
		producer.close();
	}
	
}
