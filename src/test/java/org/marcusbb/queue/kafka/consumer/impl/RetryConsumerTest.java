package org.marcusbb.queue.kafka.consumer.impl;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.kafka.KafkaTestBase;
import org.marcusbb.queue.kafka.consumer.AbstractConsumer;
import org.marcusbb.queue.kafka.consumer.Consumer;
import org.marcusbb.queue.kafka.consumer.impl.KafkaConsumerDispatcher;
import org.marcusbb.queue.kafka.consumer.impl.KafkaRetryConsumerDispatcher;
import org.marcusbb.queue.kafka.producer.Producer;
import org.marcusbb.queue.kafka.producer.impl.SimpleProducer;
import org.marcusbb.queue.kafka.utils.MockKeyBuilder;
import org.marcusbb.queue.kafka.utils.MockVersionCipher;
import org.marcusbb.queue.kafka.utils.TestConsumer;
import org.marcusbb.queue.kafka.utils.TestMessage;
import org.marcusbb.queue.serialization.impl.ems.AvroEMS;

import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RetryConsumerTest extends KafkaTestBase {

    Properties props;
    String topic;
    Producer<RoutableEncryptedMessage<TestMessage>> simpleProducer;
    String retryTopic;
    AvroEMS<TestMessage> envelopeSerializer;
    KafkaRetryConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> retryConsumerDispatcher;
    String failureTopic;

    @Before
    public void before() throws Exception {
        props = new Properties();
        props.putAll(globalProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-gid-" + new Random().nextLong() );
        props.put("consumer.retryAttempts", 3);
        props.put("consumer.retry.schedule.delay", 5L);
        props.put("consumer.retry.schedule.delay.unit", TimeUnit.MILLISECONDS);
        topic = "test-topic-"+new Random().nextLong();
        retryTopic = topic + AbstractConsumer.RETRY_TOPIC_SUFFIX;
        failureTopic = topic + AbstractConsumer.FAILURE_TOPIC_SUFFIX;
        envelopeSerializer = new AvroEMS<>(new MockVersionCipher(), new MockKeyBuilder(), iv, keyName);
        simpleProducer = new SimpleProducer<>(props, envelopeSerializer);
        
        retryConsumerDispatcher = new KafkaRetryConsumerDispatcher<>(props,envelopeSerializer);
        retryConsumerDispatcher.getRetryEngine().setMaxPollPeriod(5);
        retryConsumerDispatcher.getRetryEngine().setPollBackoffFactor(1);
    }

    @After
    public void close(){
        retryConsumerDispatcher.deregister();
        simpleProducer.close();
    }
    
    @Test
    public void testRetry() throws Exception {
        createTopic(topic);
        createTopic(retryTopic);

        int retries = 5;
        props.put("consumer.retryAttempts", retries);
        retryConsumerDispatcher = new KafkaRetryConsumerDispatcher<>(props,envelopeSerializer);

        TestConsumer<RoutableEncryptedMessage<TestMessage>> failureConsumer = new TestConsumer<>(true);

        KafkaConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> consumerDispatcher = new KafkaConsumerDispatcher<>(props, envelopeSerializer);
        consumerDispatcher.register(topic,failureConsumer);

        
        retryConsumerDispatcher.register(retryTopic,failureConsumer);

        Producer<RoutableEncryptedMessage<TestMessage>> producer = new SimpleProducer<>(props,envelopeSerializer);
        List<TestMessage> testMessages = produceRoutableMessage(producer, topic,1);

        waitForLastCommittedOffset(retryConsumerDispatcher,1);

        assertEquals(testMessages.get(0),failureConsumer.lastMessage.getPayload());
        assertEquals(retries,failureConsumer.lastMessage.getRetryDeliveryCount());
        assertEquals("consumer failure exception",failureConsumer.lastMessage.getExceptionMessage());
        assertNotNull(failureConsumer.lastMessage.getExceptionStackTrace());
        assertEquals(retries+1,failureConsumer.messagesDelivered.size());
        assertEquals(1,getLastCommittedOffset(consumerDispatcher));

        consumerDispatcher.deregister();
        producer.close();
    }

    @Test
    public void testRetryAttemptsExhausted() throws Exception {
        createTopic(retryTopic);
        createTopic(failureTopic);

        int retries = 3;
        props.put("consumer.retryAttempts", retries);

        TestConsumer<RoutableEncryptedMessage<TestMessage>> failureConsumer = new TestConsumer<>(true);

        
        retryConsumerDispatcher.register(retryTopic,failureConsumer);

        TestConsumer<RoutableEncryptedMessage<TestMessage>> dlqConsumer = new TestConsumer();  // dead letter queue consumer
        KafkaConsumerDispatcher dlqConsumerDispatcher = new KafkaConsumerDispatcher<>(props,envelopeSerializer);
        dlqConsumerDispatcher.register(failureTopic,dlqConsumer);


        Producer<RoutableEncryptedMessage<TestMessage>> producer = new SimpleProducer<>(props, envelopeSerializer);
        List<TestMessage> testMessages = produceRoutableMessage(producer, retryTopic, 1);

        waitForLastCommittedOffset(retryConsumerDispatcher,1);
        waitForConsumption(dlqConsumer,1);

        assertEquals(retries,dlqConsumer.lastMessage.getRetryDeliveryCount());
        assertEquals(failureConsumer.lastMessage.getExceptionMessage(),dlqConsumer.lastMessage.getExceptionMessage());
        assertEquals(failureConsumer.lastMessage.getExceptionStackTrace(),dlqConsumer.lastMessage.getExceptionStackTrace());

        producer.close();
    }


    @Test
    public void testRetryWithMultiplePartitions() throws Exception {
        createTopic(retryTopic,2);
        createTopic(failureTopic, 2);

        TestConsumer<RoutableEncryptedMessage<TestMessage>> consumer = new TestConsumer<>(true);
        
        retryConsumerDispatcher.register(retryTopic,consumer);

        int numMessages = 4;
        int defaultRetries=3;
        produceRoutableMessage(simpleProducer, retryTopic, numMessages);

        waitForLastCommittedOffset(retryConsumerDispatcher,PARTITION_0,2);
        waitForLastCommittedOffset(retryConsumerDispatcher,PARTITION_1,2);

        assertEquals(numMessages*defaultRetries,consumer.messagesDelivered.size());

    }

    @Test
    public void testCommitOffsetWhenMultipleRetryFailures() throws Exception {
        createTopic(retryTopic,1);
        createTopic(failureTopic, 1);
        
        retryConsumerDispatcher.register(retryTopic, new Consumer<RoutableEncryptedMessage<TestMessage>>() {

            @Override
            public void onMessage(RoutableEncryptedMessage<TestMessage> envelope, byte[] key) {
                TestMessage msg = envelope.getPayload();
                int i = Integer.parseInt(msg.getContent());
                if (i%4 == 0){
                    throw new RuntimeException("consumer exception - "+i);
                }
            }
        });

        int numMessages = 10;
        List<TestMessage> messageList = produceRoutableMessage(simpleProducer, retryTopic, numMessages);

        waitForLastCommittedOffset(retryConsumerDispatcher,numMessages);
        assertEquals(8,retryConsumerDispatcher.stats.getDeliveredCount()); // number of messages delivered on retry
        assertEquals(6,retryConsumerDispatcher.stats.getFailureCount()); // number of attempts made to deliver messages

    }

    @Test
    public void testCommitOffsetWhenRetryFailedForLastMessage() throws Exception {
        createTopic(retryTopic,1);

        

        TestConsumer<RoutableEncryptedMessage<TestMessage>> failureConsumer = new TestConsumer<RoutableEncryptedMessage<TestMessage>>(){

            @Override
            public void onMessage(RoutableEncryptedMessage<TestMessage> envelope, byte[] key) {
                    messagesDelivered.add(envelope);
                    TestMessage msg = envelope.getPayload();
                    int i = Integer.parseInt(msg.getContent());
                    if (i%5 == 0){
                        throw new RuntimeException("consumer exception - "+i);
                    }
                }
        };
        retryConsumerDispatcher.register(retryTopic,failureConsumer);

        int numMessages = 5;
        produceRoutableMessage(simpleProducer, retryTopic, numMessages);

        waitForLastCommittedOffset(retryConsumerDispatcher,numMessages);
        assertEquals(7,failureConsumer.messagesDelivered.size());
        assertEquals(3,failureConsumer.messagesDelivered.get(6).getRetryDeliveryCount());

    }

    @Test
    public void testRetryWhenFailuresAreInSinglePartition() throws Exception {
        createTopic(retryTopic,2);
        createTopic(failureTopic, 1);
        

        TestConsumer<RoutableEncryptedMessage<TestMessage>> failureConsumer = new TestConsumer<RoutableEncryptedMessage<TestMessage>>(){
            @Override
            public void onMessage(RoutableEncryptedMessage<TestMessage> envelope, byte[] key) {
                messagesDelivered.add(envelope);
                TestMessage msg = envelope.getPayload();
                int i = Integer.parseInt(msg.getContent());
                if (i != 1 && i%2 == 0){
                    throw new RuntimeException("consumer exception - "+i);
                }
            }
        };

        retryConsumerDispatcher.register(retryTopic,failureConsumer);

        int numMessages = 6;
        List<TestMessage> messageList = produceRoutableMessage(simpleProducer, retryTopic, numMessages);

        waitForLastCommittedOffset(retryConsumerDispatcher,PARTITION_0,3);
        waitForLastCommittedOffset(retryConsumerDispatcher,PARTITION_1,3);
        waitForConsumption(retryConsumerDispatcher,12); // 3 (delivered) + 3 (failed) * 3 (retryCount)
        assertEquals(3,retryConsumerDispatcher.stats.getDeliveredCount());

    }

    /**
     * Produce large number of messages in happy path
     * @throws Exception
     */
    @Test
    public void testNoError() throws Exception {
        //createTopic(topic,2);
        createTopic(retryTopic,2);

        int numMsgs = 1000;
       
        props.put("consumer.retryAttempts", 10);

        TestConsumer<RoutableEncryptedMessage<TestMessage>> failureConsumer = new TestConsumer<>(false);
    

        
        retryConsumerDispatcher.register(retryTopic,failureConsumer);

        Producer<RoutableEncryptedMessage<TestMessage>> producer = new SimpleProducer<>(props,envelopeSerializer);
        produceRoutableMessage(producer, retryTopic,numMsgs);

        waitForConsumption(retryConsumerDispatcher,numMsgs);
               
        assertEquals(numMsgs/2,getLastCommittedOffset(retryConsumerDispatcher, PARTITION_0));
        assertEquals(numMsgs/2,getLastCommittedOffset(retryConsumerDispatcher, PARTITION_1));

        
        producer.close();
    }

    

}
