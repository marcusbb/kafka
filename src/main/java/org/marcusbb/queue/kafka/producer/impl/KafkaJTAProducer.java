package org.marcusbb.queue.kafka.producer.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;
import javax.transaction.TransactionSynchronizationRegistry;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.marcusbb.queue.kafka.producer.JTAProducerSync;
import org.marcusbb.queue.kafka.producer.LoggingProducerInterceptor;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaJTAProducer<T> extends JTAProducerSync<T> {

	Logger logger = LoggerFactory.getLogger(KafkaJTAProducer.class);

	KafkaProducer<byte[], byte[]> producer = null;
	ByteSerializer<T> serializer;

	public KafkaJTAProducer(Properties props,ByteSerializer<T> serializer) throws NamingException {
		super();
		init(props,serializer);
	}
	public KafkaJTAProducer(Properties props,ByteSerializer<T> serializer,TransactionManager txManager) throws NamingException {
		super(txManager);
		init(props,serializer);
	}
	public KafkaJTAProducer(Properties props, ByteSerializer<T> serializer,TransactionSynchronizationRegistry txSync) throws NamingException {
		super(txSync);
		init(props,serializer);
	} 
	private void init(Properties props, ByteSerializer<T> serializer) {
		Properties p = new Properties();
		p.putAll(props);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		p.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());

		producer = new KafkaProducer<>(p);

		this.serializer = serializer;

		String clientId = props.getProperty(ProducerConfig.CLIENT_ID_CONFIG);
		String brokers = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
		String clientIp = null;
		try {
			clientIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			logger.warn("Could not retrieve producer IP address, while it connects the brokers");
		}

		logger.info("register producer from instance={} with brokers={}", clientIp, brokers);
	}
	 
	//on failure, will we be able to recover? - perhaps signal to the XA manager on
	//before commit to rollback by throwing an exception
	@Override
	public void flush(Queue<MessageWrapper> messages) {
		for (MessageWrapper wrapper: messages) {
			ProducerRecord<byte[], byte[]> record = null;

			// TODO : add type safety for MessageWrapper to avoid cast
			byte []payload = serializer.serialize((T) wrapper.getPayload());
			if (wrapper.getPartitionKey() !=null) {
				byte[] kbytes;
				if (wrapper.getPartitionKey() instanceof byte[]) {
					kbytes = (byte[])wrapper.getPartitionKey();
				} else {
					kbytes = wrapper.getPartitionKey().toString().getBytes(StandardCharsets.UTF_8);
				}
				record = new ProducerRecord<>(topic,kbytes, payload );
			}else {
				record = new ProducerRecord<>(topic, payload );
			}

			try {
				producer.send(record).get();
				logger.info("message {} sent to topic {}",wrapper.getPayload(),topic);
			} catch (InterruptedException | ExecutionException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	public void close() {
		if (producer != null)
			producer.close();
	}

}
