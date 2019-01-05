package org.marcusbb.queue.kafka.consumer.impl;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.marcusbb.queue.Envelope;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.kafka.KafkaTestBase;
import org.marcusbb.queue.kafka.consumer.AbstractConsumer;
import org.marcusbb.queue.kafka.producer.Producer;
import org.marcusbb.queue.kafka.producer.impl.SimpleProducer;
import org.marcusbb.queue.kafka.utils.ErrorOnNthTestConsumer;
import org.marcusbb.queue.kafka.utils.MockKeyBuilder;
import org.marcusbb.queue.kafka.utils.MockVersionCipher;
import org.marcusbb.queue.kafka.utils.TestAppender;
import org.marcusbb.queue.kafka.utils.TestConsumer;
import org.marcusbb.queue.kafka.utils.TestMessage;
import org.marcusbb.queue.serialization.impl.JavaMessageSerializer;
import org.marcusbb.queue.serialization.impl.ems.AvroEMS;

/**
 * Created by jim.lin on 7/5/2017.
 */

public class RetryWrapperTest extends KafkaTestBase {
    @Rule
    public TemporaryFolder folder= new TemporaryFolder();

    Properties props;
    String topic;

    String retryTopic;

    String failureTopic;

    @Test
    public void testEncryptedLogMessage() throws Exception {
        final TestAppender appender = new TestAppender();
        Logger logger = Logger.getLogger(RetryEngine.class);
        logger.addAppender(appender);

        props = new Properties();
        props.putAll(globalProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-gid-encrypted");
        props.put("consumer.retryAttempts", 3);
        topic = "test-topic-log-encrypted";
        retryTopic = topic + AbstractConsumer.RETRY_TOPIC_SUFFIX;
        failureTopic = topic + AbstractConsumer.FAILURE_TOPIC_SUFFIX;
        AvroEMS<TestMessage> envelopeSerializer = new AvroEMS<>(new MockVersionCipher(), new MockKeyBuilder(), iv, keyName);

        createTopic(topic);
        createTopic(retryTopic);

        int retries = 0;
        props.put("consumer.retryAttempts", retries);

        TestConsumer<RoutableEncryptedMessage<TestMessage>> failureConsumer = new ErrorOnNthTestConsumer<>(true,3);

        KafkaConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> consumerDispatcher = new KafkaConsumerDispatcher<>(props, envelopeSerializer);
        consumerDispatcher.register(topic,failureConsumer);

        final KafkaRetryConsumerDispatcher<RoutableEncryptedMessage<TestMessage>> retryConsumerDispatcher = new KafkaRetryConsumerDispatcher<>(props,envelopeSerializer);
        retryConsumerDispatcher.getRetryEngine().setMaxPollPeriod(5);
        retryConsumerDispatcher.getRetryEngine().setPollBackoffFactor(1);
        retryConsumerDispatcher.register(retryTopic,failureConsumer);

        Producer<RoutableEncryptedMessage<TestMessage>> producer = new SimpleProducer<>(props,envelopeSerializer);
        List<TestMessage> testMessages = produceRoutableMessage(producer, topic,1);
        await(10000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                long total = retryConsumerDispatcher.stats.getDeliveredCount() + retryConsumerDispatcher.stats.getFailureCount()
                        + retryConsumerDispatcher.stats.getSerFailCount();
                return total == 2;

            }
        });
        for (LoggingEvent le : appender.getLog()) {
            if (((String) le.getMessage()).contains("Retry_Failed attempt=1"))
                return;
        }
        consumerDispatcher.deregister();
        producer.close();
        retryConsumerDispatcher.deregister();

        assertTrue("not able to find expected message <Retry_Failed attempt=1>", false);
    }

    @Test
    public void testLogClearMessage() throws Exception {
        final TestAppender appender = new TestAppender();
        Logger logger = Logger.getLogger(RetryEngine.class);
        logger.addAppender(appender);

        props = new Properties();
        props.putAll(globalProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-gid-clear");
        props.put("consumer.retryAttempts", 3);
        topic = "test-topic-log-clear";
        retryTopic = topic + AbstractConsumer.RETRY_TOPIC_SUFFIX;
        failureTopic = topic + AbstractConsumer.FAILURE_TOPIC_SUFFIX;
        JavaMessageSerializer envelopeSerializer = new JavaMessageSerializer<>();

        createTopic(topic);
        createTopic(retryTopic);

        int retries = 0;
        props.put("consumer.retryAttempts", retries);

        TestConsumer<TestMessage> failureConsumer = new ErrorOnNthTestConsumer<>(true,3);

        KafkaConsumerDispatcher<TestMessage> consumerDispatcher = new KafkaConsumerDispatcher<>(props, envelopeSerializer);
        consumerDispatcher.register(topic,failureConsumer);

        final KafkaRetryConsumerDispatcher<TestMessage> retryConsumerDispatcher = new KafkaRetryConsumerDispatcher<>(props,envelopeSerializer);
        retryConsumerDispatcher.getRetryEngine().setMaxPollPeriod(5);
        retryConsumerDispatcher.getRetryEngine().setPollBackoffFactor(1);
        retryConsumerDispatcher.register(retryTopic,failureConsumer);

        SimpleProducer<TestMessage> producer = new SimpleProducer<>(props,envelopeSerializer);
        List<TestMessage> testMessages = produceTestMessage( producer, topic, new String("key"), 1, "this is test message");
        await(10000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                long total = retryConsumerDispatcher.stats.getDeliveredCount() + retryConsumerDispatcher.stats.getFailureCount()
                        + retryConsumerDispatcher.stats.getSerFailCount();
                return total == 2;

            }
        });
        consumerDispatcher.deregister();
        producer.close();
        retryConsumerDispatcher.deregister();
        for(LoggingEvent le: appender.getLog()) {
            String loggedMsg = (String)le.getMessage();
            if ((loggedMsg.contains("Retry_Failed attempt=1"))
                    && (loggedMsg.contains("<Non-encrypted msg was omitted>")))
                return;
        }
        assertTrue("not able to find expected message contains <Retry_Failed attempt=1> and <Non-encrypted msg was omitted>", false);
    }

    @Test
    public void testLogEnvelope() throws Exception {
        final TestAppender appender = new TestAppender();
        final Logger logger = Logger.getLogger(RetryEngine.class);
        logger.addAppender(appender);

        props = new Properties();
        props.putAll(globalProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-gid-envelope");
        props.put("consumer.retryAttempts", 3);
        topic = "test-topic-log-envelope";
        retryTopic = topic + AbstractConsumer.RETRY_TOPIC_SUFFIX;
        failureTopic = topic + AbstractConsumer.FAILURE_TOPIC_SUFFIX;
        JavaMessageSerializer envelopeSerializer = new JavaMessageSerializer<>();

        createTopic(topic);
        createTopic(retryTopic);

        int retries = 1;
        props.put("consumer.retryAttempts", retries);

        TestConsumer<Envelope> failureConsumer = new ErrorOnNthTestConsumer<>(true,3);

        KafkaConsumerDispatcher<Envelope> consumerDispatcher = new KafkaConsumerDispatcher<>(props, envelopeSerializer);
        consumerDispatcher.register(topic,failureConsumer);

        final KafkaRetryConsumerDispatcher<Envelope> retryConsumerDispatcher = new KafkaRetryConsumerDispatcher<>(props,envelopeSerializer);
        retryConsumerDispatcher.getRetryEngine().setMaxPollPeriod(5);
        retryConsumerDispatcher.getRetryEngine().setPollBackoffFactor(1);
        retryConsumerDispatcher.register(retryTopic,failureConsumer);

        SimpleProducer<Envelope> producer = new SimpleProducer<>(props,envelopeSerializer);
        List<TestEnvelope> messages = new ArrayList<>();
        TestEnvelope testEnvelope = new TestEnvelope<>("this is a test message");
        producer.add(topic,testEnvelope,1);
        producer.flush();
        messages.add(testEnvelope);
        await(10000, new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                long total = retryConsumerDispatcher.stats.getDeliveredCount() + retryConsumerDispatcher.stats.getFailureCount()
                        + retryConsumerDispatcher.stats.getSerFailCount();
                return total >= 1;

            }
        });
        //consumerDispatcher.deregister();
        producer.close();
        retryConsumerDispatcher.deregister();
        for(LoggingEvent le: appender.getLog()) {
            String loggedMsg = (String)le.getMessage();
            if ((loggedMsg.contains("Retry_Failed attempt=1"))
                    && (loggedMsg.contains("<Non-encrypted payload was omitted>; Envelope headers are:")))
                return;
        }
        assertTrue("not able to find expected message contains <Retry_Failed attempt=1> and <Non-encrypted payload was omitted>; Envelope headers are:", false);
    }

    private static class TestEnvelope<T> implements Envelope<T> {
        private Map<String, String> headers = new HashMap<>();
        private T payload;

        public TestEnvelope(T payload) {
            this.payload = payload;
        }

        @Override
        public long getCreateTs() {
            return 0;
        }

        @Override
        public Map<String, String> getHeaders() {
            return headers;
        }

        @Override
        public void addHeader(String key, String value) {
            headers.put(key, value);
        }

        @Override
        public String getHeader(String key) {
            return headers.get(key);
        }

        @Override
        public T getPayload() {
            return null;
        }
    }
}
