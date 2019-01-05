package org.marcusbb.queue.kafka;

import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.naming.InitialContext;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.plain.PlainSaslServerProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.marcusbb.crypto.spi.vault.VaultConfiguration;
import org.marcusbb.crypto.spi.vault.VaultKeyStoreManager;
import org.marcusbb.crypto.spi.vault.VaultTestBase;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.kafka.consumer.AbstractConsumer;
import org.marcusbb.queue.kafka.consumer.impl.KafkaConsumerDispatcher;
import org.marcusbb.queue.kafka.producer.JTAProducerSync;
import org.marcusbb.queue.kafka.producer.Producer;
import org.marcusbb.queue.kafka.utils.EmbeddedKafkaBroker;
import org.marcusbb.queue.kafka.utils.EmbeddedZookeeper;
import org.marcusbb.queue.kafka.utils.TestConsumer;
import org.marcusbb.queue.kafka.utils.TestMessage;
import org.marcusbb.queue.kafka.utils.TxMockManager;
import org.marcusbb.queue.kafka.utils.TxMockRegistry;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.impl.ems.EncryptedMessageSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class KafkaTestBase {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestBase.class);

    private static final int ZK_PORT = 6066;
    private static final int BROKER_PORT = 6068;
    private static final int BROKER_ID = 1;
	public static final int PARTITION_0 = 0;
	public static final int PARTITION_1 = 1;
	public static EmbeddedZookeeper zookeeper;
	public static EmbeddedKafkaBroker broker;
	public static InitialContext ctx = null;
	public static String TX_SYNC_REG_NAME = JTAProducerSync.DEF_TX_MANAGER;
	public static TxMockManager txManager;
	public static TxMockRegistry txRegistry;
	public static Properties globalProps = new Properties();
	public static String iv = "credit_card__key";
	public static String keyName = "queue1";
	private static long WAIT_TIMEOUT=10000; // 10 secs

	static class MyServerProvider extends PlainSaslServerProvider {

		/**
		 * 
		 */
		private static final long serialVersionUID = -7326288147254400904L;
		
	}

	@Rule
	public TestWatcher testWatcher = new TestName() {
		@Override
		protected void starting(Description description) {
			super.starting(description);
			LOGGER.info("starting test -------> {}",getMethodName());
		}

		@Override
		protected void finished(Description description) {
			super.finished(description);
			LOGGER.info("finished test -------> {}",getMethodName());
		}
	};


	@BeforeClass
	public static void bootstrap() throws Exception {
		Security.addProvider(new MyServerProvider());
		System.setProperty("java.naming.factory.initial", "org.marcusbb.queue.kafka.utils.MockCtxFactory");
		System.setProperty("zookeeper.serverCnxnFactory","org.marcusbb.queue.kafka.utils.CustomServerCnxnFactory");
		ctx = new InitialContext();
		ctx.bind(TX_SYNC_REG_NAME, txManager = new TxMockManager());
		//Websphere intented target as a TxRegistry
		ctx.bind(JTAProducerSync.DEF_TX_REGISTRY, txRegistry = new TxMockRegistry());

		globalProps.put("bootstrap.servers", "localhost:" + BROKER_PORT);
		globalProps.setProperty("zookeeper.connect", "localhost:" + ZK_PORT);
		globalProps.setProperty("broker.id", String.valueOf(BROKER_ID));
		globalProps.setProperty("host.name", "localhost");
		globalProps.put("consumer.poll",100);

		zookeeper = new EmbeddedZookeeper(ZK_PORT);
		broker = new EmbeddedKafkaBroker(BROKER_ID, BROKER_PORT, zookeeper.getConnection(), new Properties());

		zookeeper.startup();
		broker.startup();
	}

	public static VaultConfiguration getVaultConfig() {
		return VaultTestBase.getConfig();
	}

	public static void createTopic(String topicName,int numPartitions) {

		try {
			broker.createTopic(topicName, numPartitions);
		} catch(Exception e) {
			LOGGER.error("Topic, {} already exists", topicName);
		}
	}

	public static void createTopic(String topicName) {
		createTopic(topicName,1);
	}

	public static void generateAndStore(String keyName,String iv) throws NoSuchAlgorithmException {
		//Generation of key and insertion into vault
		KeyGenerator kg = KeyGenerator.getInstance("AES");
		SecretKey key = kg.generateKey();
		final byte[] keyMaterial = iv.getBytes( Charset.forName("UTF-8"));
		VaultKeyStoreManager store = new VaultKeyStoreManager(getVaultConfig());
		store.postKey(VaultKeyStoreManager.SECR_KEY_PREFIX + "/v1_" + keyName, key , key.getAlgorithm());
		store.postKey(VaultKeyStoreManager.IV_PREFIX + "/" + iv, new SecretKeySpec(keyMaterial,"AES"));
		store.createOrUpdateSecretVersion(keyName);
		store.createOrUpdateIv(iv, iv.getBytes());
	}

	public List<TestMessage> produceTestMessage(Producer<TestMessage> producer, String topic, int numMessages){
		return produceTestMessage(producer,topic,null,numMessages);
	}

	public List<TestMessage> produceTestMessage(Producer<TestMessage> producer, String topic, Object key, int numMessages){
		List<TestMessage> messages = new ArrayList<>();
		for(int i=0 ; i < numMessages; i ++){
			TestMessage testMessage = new TestMessage("msg_"+i);
			producer.add(topic,testMessage,key);
			producer.flush();
			messages.add(testMessage);
		}

		return messages;
	}

	public List<TestMessage> produceTestMessage(Producer<TestMessage> producer, String topic, Object key, int numMessages, String messageContent){
		List<TestMessage> messages = new ArrayList<>();
		for(int i=0 ; i < numMessages; i ++){
			TestMessage testMessage = new TestMessage(messageContent);
			producer.add(topic,testMessage,key);
			producer.flush();
			messages.add(testMessage);
		}

		return messages;
	}


	public List<TestMessage> produceRoutableMessage(Producer<RoutableEncryptedMessage<TestMessage>> producer, String topic, int numMessages){
		List<TestMessage> messages = new ArrayList<>();
		for(int i=1 ; i <= numMessages; i ++){
			TestMessage testMessage = new TestMessage(""+i);
			final int index = i;
			RoutableEncryptedMessage<TestMessage> routableEncryptedMessage = new RoutableEncryptedMessage<>(new HashMap<String, String>() {{ put("id",UUID.randomUUID().toString()); put("index",""+index); }},
					testMessage);
			producer.add(topic,routableEncryptedMessage);
			producer.flush();
			messages.add(testMessage);
		}

		return messages;
	}

	public static TxMockManager getTxRegistry() {
		return txManager;
	}

	public static Properties getGlobalProps() {
		return globalProps;
	}

	public <T> AbstractConsumer<T> getConsumerDispatcher(Properties props, ByteSerializer<T> serializer){
		return new KafkaConsumerDispatcher<>(props,serializer, null);
	}

	public <T> AbstractConsumer<RoutableEncryptedMessage<T>> getConsumerDispatcher(Properties props, EncryptedMessageSerializer<T> serializer){
		return new KafkaConsumerDispatcher<>(props,serializer, null);
	}

	public long getLastCommittedOffset(AbstractConsumer dispatcher){
		return getLastCommittedOffset(dispatcher,PARTITION_0);
	}

	public static long getLastCommittedOffset(AbstractConsumer<RoutableEncryptedMessage<TestMessage>> dispatcher, int partition){
		Long offset = dispatcher.getLastCommittedOffsets().get(new TopicPartition(dispatcher.getTopicName(), partition));
		if (offset == null){
			offset = 0L;
		}
		return offset.longValue();
	}

	public static void await(long waitTimeOut, Callable<Boolean> condition) throws Exception {
		long startTime = System.currentTimeMillis();
		while ((System.currentTimeMillis() - startTime) <= waitTimeOut) {
			Boolean result = condition.call();
			if (result != null && result) {
				return;
			}else{
				Thread.sleep(1000);
			}
		}
		throw new TimeoutException("startTime: "+new Date(startTime)+" - timed out waiting for condition to be true");
	}

	public static void waitForConsumption (final TestConsumer testConsumer, final int numMessages) throws Exception {
		waitForConsumption(numMessages,testConsumer);
	}

	public static void waitForConsumption (final int numMessages,final TestConsumer... testConsumers) throws Exception {
		await(WAIT_TIMEOUT,new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				int totalMessagesDelievered=0;
				for(TestConsumer testConsumer : testConsumers) {
					totalMessagesDelievered = totalMessagesDelievered + testConsumer.messagesDelivered.size();
				}
				return numMessages == totalMessagesDelievered;
			}
		});
	}

	public static void waitForConsumption(final AbstractConsumer consumerDispatcher, final int numMessages) throws Exception {
		await(WAIT_TIMEOUT,new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				long total = consumerDispatcher.stats.getDeliveredCount() + consumerDispatcher.stats.getFailureCount()
						+ consumerDispatcher.stats.getSerFailCount();
				return total == numMessages;
			}
		});
	}

	public static void waitForLastCommittedOffset(final AbstractConsumer consumerDispatcher, final int lastCommittedOffset) throws Exception {
		waitForLastCommittedOffset(consumerDispatcher,PARTITION_0, lastCommittedOffset);
	}

	public static void waitForLastCommittedOffset(final AbstractConsumer consumerDispatcher, final int partition,
                                                  final int lastCommittedOffset) throws Exception {
		await(WAIT_TIMEOUT,new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				return getLastCommittedOffset(consumerDispatcher,partition) == lastCommittedOffset;
			}
		});
	}

	@AfterClass
	public static void afterClass() {
		broker.shutdown();
		zookeeper.shutdown();
	}
}
